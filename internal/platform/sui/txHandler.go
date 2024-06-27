package sui

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/platform/sui/stypes"
	suiswap "block-crawling/internal/platform/sui/swap"
	"block-crawling/internal/platform/swap"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type txHandler struct {
	chainName   string
	block       *chain.Block
	txByHash    *chain.Transaction
	chainHeight uint64
	curHeight   uint64
	now         int64
	newTxs      bool

	txRecords []*data.SuiTransactionRecord
}

type AmountChange struct {
	FromAddress  string
	ToAddress    string
	TxType       string
	TokenAddress string
	TokenId      string
	Amount       string
}

func (h *txHandler) OnNewTx(c chain.Clienter, chainBlock *chain.Block, chainTx *chain.Transaction) (err error) {
	curHeight := chainBlock.Number
	transactionInfo := chainTx.Raw.(*stypes.TransactionInfo)
	transactionHash := transactionInfo.Digest

	var status string
	if transactionInfo.Effects.Status.Status == "success" {
		status = biz.SUCCESS
	} else {
		status = biz.FAIL
	}

	tx := transactionInfo.Transaction
	kind := tx.Data.Transaction.Kind
	if kind == "ConsensusCommitPrologue" {
		return nil
	}

	go func() {
		results, err := swap.AttemptToPushSwapPairs(h.chainName, chainTx.ToAddress, chainBlock, chainTx, &tx)
		if err != nil {
			log.Info("EXTRACT SWAP FAILED", zap.String("chainName", h.chainName), zap.Error(err))
			return
		}
		bfh := &bfstationHandler{
			chainName:   h.chainName,
			repo:        data.BFCStationRepoIns,
			blockNumber: curHeight,
			blockHash:   chainBlock.Hash,
		}
		if biz.IsBenfenNet(h.chainName) {
			bfh.Handle(transactionInfo, results, status)
		}
	}()

	fromAmountChangeMap := make(map[string]*AmountChange)
	toAmountChangeMap := make(map[string][]*AmountChange)
	var amountChanges []*AmountChange
	balanceChanges := transactionInfo.BalanceChanges
	for _, balanceChange := range balanceChanges {
		txType := biz.NATIVE
		var tokenAddress string
		address := getOwnerAddress(balanceChange.Owner)
		amount := balanceChange.Amount
		if !IsNative(balanceChange.CoinType) {
			txType = biz.TRANSFER
			tokenAddress = suiswap.NormalizeBenfenCoinType(h.chainName, balanceChange.CoinType)
		}
		if strings.HasPrefix(balanceChange.Amount, "-") {
			fromAmountChangeMap[tokenAddress] = &AmountChange{
				FromAddress:  utils.EVMAddressToBFC(h.chainName, address),
				TxType:       txType,
				TokenAddress: tokenAddress,
				Amount:       amount,
			}
		} else {
			toAmountChangeMap[tokenAddress] = append(toAmountChangeMap[tokenAddress], &AmountChange{
				ToAddress:    utils.EVMAddressToBFC(h.chainName, address),
				TxType:       txType,
				TokenAddress: tokenAddress,
				Amount:       amount,
			})
		}
	}

	gasObjecType := h.getGasObjectType(transactionInfo)

	for tokenAddress, toAmountChangeList := range toAmountChangeMap {
		fromAmountChange := fromAmountChangeMap[tokenAddress]
		toTotalAmount := new(big.Int)
		if fromAmountChange != nil {
			for _, toAmountChange := range toAmountChangeList {
				toAmountChange.FromAddress = fromAmountChange.FromAddress
				toAmount, _ := new(big.Int).SetString(toAmountChange.Amount, 0)
				toTotalAmount = toTotalAmount.Add(toTotalAmount, toAmount)
			}
			fromAmount, _ := new(big.Int).SetString(fromAmountChange.Amount, 0)
			fromAmount = fromAmount.Abs(fromAmount)
			if h.isGasCoinAmountChange(transactionInfo, fromAmountChange, gasObjecType) {
				gasUsedInt := transactionInfo.GasUsedInt()
				fromAmount = fromAmount.Sub(fromAmount, new(big.Int).SetInt64(int64(gasUsedInt)))
			}
			if fromAmount.Cmp(toTotalAmount) > 0 {
				fromAmountChange.Amount = fromAmount.Sub(fromAmount, toTotalAmount).String()
				amountChanges = append(amountChanges, fromAmountChange)
			}
			fromAmountChangeMap[tokenAddress] = nil
		}
		amountChanges = append(amountChanges, toAmountChangeList...)
	}

	for _, fromAmountChange := range fromAmountChangeMap {
		if fromAmountChange != nil {
			fromAmount, _ := new(big.Int).SetString(fromAmountChange.Amount, 0)
			fromAmount = fromAmount.Abs(fromAmount)
			if h.isGasCoinAmountChange(transactionInfo, fromAmountChange, gasObjecType) {
				gasUsedInt := transactionInfo.GasUsedInt()
				fromAmount = fromAmount.Sub(fromAmount, new(big.Int).SetInt64(int64(gasUsedInt)))
			}
			if fromAmount.Cmp(new(big.Int)) > 0 {
				fromAmountChange.Amount = fromAmount.String()
				amountChanges = append(amountChanges, fromAmountChange)
			}
		}
	}

	objectChanges := transactionInfo.ObjectChanges
	for _, objectChange := range objectChanges {
		sender := objectChange.Sender
		owner := objectChange.Owner
		objectType := objectChange.ObjectType
		objectId := objectChange.ObjectId
		if owner == nil {
			continue
		}
		if IsNativePrefixs(objectType) && !IsNativeStakedBfc(objectType) {
			continue
		}
		var toAddress string
		if objectChange.Owner != nil {
			toAddress = getOwnerAddress(objectChange.Owner)
		}
		if sender != "" && toAddress != "" && objectType != "" && objectId != "" {
			amountChange := &AmountChange{
				FromAddress:  utils.EVMAddressToBFC(h.chainName, sender),
				ToAddress:    utils.EVMAddressToBFC(h.chainName, toAddress),
				TxType:       biz.TRANSFERNFT,
				TokenAddress: suiswap.NormalizeBenfenCoinType(h.chainName, objectType),
				TokenId:      objectId,
				Amount:       "1",
			}
			amountChanges = append(amountChanges, amountChange)
		}
	}

	var moveCallPackageID string
	isContract := false
	transactions := tx.Data.Transaction.Transactions()
	for _, transaction := range transactions {
		if transaction.RawMoveCall != nil {
			isContract = true
			if moveCall, err := transaction.MoveCall(); err == nil {
				moveCallPackageID = moveCall.Package
			}
			break
		}
	}
	if len(amountChanges) == 0 && !isContract {
		//https://suiexplorer.com/txblock/DB85AUiCAavmVwfV8QqR8ubSysRvR4Nu48PMZwSYbnt3
		var toAddress, value, txType string
		inputs := tx.Data.Transaction.Inputs
		for _, input := range inputs {
			if input.Type == "pure" {
				if input.ValueType == "address" {
					toAddress = utils.GetString(input.Value)
				} else if input.ValueType == "u64" {
					value = utils.GetString(input.Value)
				}
			}
		}
		if toAddress != "" && value != "" && len(inputs) == 2 {
			txType = biz.NATIVE
		}
		amountChange := &AmountChange{
			FromAddress:  utils.EVMAddressToBFC(h.chainName, tx.Data.Sender),
			ToAddress:    utils.EVMAddressToBFC(h.chainName, toAddress),
			TxType:       txType,
			TokenAddress: "",
			Amount:       value,
		}
		amountChanges = append(amountChanges, amountChange)
	}

	// 资产变更数量大于1个的话就变成合约类型，因为普通类型没法展示多个资产变化
	if len(amountChanges) > 1 {
		isContract = true
	}
	gasLimit, gasUsed, feeAmount := getFees(transactionInfo)
	tokenGasless := h.getTokenGasless(gasObjecType, gasUsed)

	index := 0
	if !isContract {
		for _, amountChange := range amountChanges {
			txType := biz.NATIVE
			var tokenInfo types.TokenInfo
			var amount, contractAddress string
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool

			if amountChange.TxType != "" {
				txType = amountChange.TxType
			}
			contractAddress = amountChange.TokenAddress
			fromAddress = amountChange.FromAddress
			toAddress = amountChange.ToAddress
			amount = amountChange.Amount
			if fromAddress != "" {
				fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
				if err != nil {
					log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
					return
				}
			}

			if toAddress != "" {
				toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, toAddress)
				if err != nil {
					log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
					return
				}
			}
			if !fromAddressExist && !toAddressExist {
				continue
			}

			index++
			txHash := transactionHash
			if index > 1 {
				txHash += "#result-" + fmt.Sprintf("%v", index)
			}

			if txType == biz.TRANSFER {
				tokenInfo, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, contractAddress)
				if err != nil {
					log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
				}
				tokenInfo.Amount = amount
				tokenInfo.Address = contractAddress
			} else if txType == biz.TRANSFERNFT {
				tokenId := amountChange.TokenId
				tokenInfo, err = biz.GetNftInfoDirectlyRetryAlert(nil, h.chainName, contractAddress, tokenId)
				if err != nil {
					log.Error("扫块，从nodeProxy中获取NFT信息失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", contractAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
				}
				tokenInfo.Amount = amount
				tokenInfo.Address = contractAddress
				if tokenInfo.TokenType == "" {
					tokenInfo.TokenType = biz.SUINFT
				}
				if tokenInfo.TokenId == "" {
					tokenInfo.TokenId = tokenId
				}
			}
			suiMap := map[string]interface{}{
				"token": tokenInfo,
			}
			parseData, _ := utils.JsonEncode(suiMap)
			tokenInfoStr, _ := utils.JsonEncode(tokenInfo)
			amountValue, _ := decimal.NewFromString(amount)

			suiTransactionRecord := &data.SuiTransactionRecord{
				BlockHash:       chainBlock.Hash,
				BlockNumber:     int(curHeight),
				TransactionHash: txHash,
				FromAddress:     fromAddress,
				ToAddress:       toAddress,
				FromUid:         fromUid,
				ToUid:           toUid,
				FeeAmount:       feeAmount,
				Amount:          amountValue,
				Status:          status,
				TxTime:          transactionInfo.TxTime(),
				ContractAddress: contractAddress,
				ParseData:       parseData,
				GasLimit:        gasLimit,
				GasUsed:         gasUsed,
				TokenGasless:    tokenGasless,
				Data:            "",
				EventLog:        "",
				TransactionType: txType,
				TokenInfo:       tokenInfoStr,
				CreatedAt:       h.now,
				UpdatedAt:       h.now,
			}
			h.txRecords = append(h.txRecords, suiTransactionRecord)
		}
	} else {
		var payload string
		var eventLogs []*types.EventLogUid
		var suiContractRecord *data.SuiTransactionRecord

		txType := biz.CONTRACT
		var tokenInfo types.TokenInfo
		var amount, contractAddress string
		var fromAddress, toAddress, fromUid, toUid string
		var fromAddressExist, toAddressExist bool

		fromAddress = tx.Data.Sender

		if fromAddress != "" {
			fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
			if err != nil {
				log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
				return
			}
		}

		if toAddress != "" {
			toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, toAddress)
			if err != nil {
				log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
				return
			}
		}
		if fromAddress == toAddress {
			return
		}

		if !IsNative(contractAddress) && contractAddress != "" {
			tokenInfo, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, contractAddress)
			if err != nil {
				log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
			}
			tokenInfo.Amount = amount
			tokenInfo.Address = contractAddress
		}
		suiMap := map[string]interface{}{
			"token": tokenInfo,
		}
		parseData, _ := utils.JsonEncode(suiMap)
		tokenInfoStr, _ := utils.JsonEncode(tokenInfo)
		amountValue, _ := decimal.NewFromString(amount)
		if moveCallPackageID != "" && contractAddress == "" {
			contractAddress = moveCallPackageID
			if toAddress == "" {
				toAddress = contractAddress
			}
		}

		suiContractRecord = &data.SuiTransactionRecord{
			BlockHash:       chainBlock.Hash,
			BlockNumber:     int(curHeight),
			TransactionHash: transactionHash,
			FromAddress:     fromAddress,
			ToAddress:       toAddress,
			FromUid:         fromUid,
			ToUid:           toUid,
			FeeAmount:       feeAmount,
			Amount:          amountValue,
			Status:          status,
			TxTime:          transactionInfo.TxTime(),
			ContractAddress: contractAddress,
			ParseData:       parseData,
			GasLimit:        gasLimit,
			GasUsed:         gasUsed,
			TokenGasless:    tokenGasless,
			Data:            payload,
			EventLog:        "",
			TransactionType: txType,
			TokenInfo:       tokenInfoStr,
			CreatedAt:       h.now,
			UpdatedAt:       h.now,
		}

		txType = biz.EVENTLOG

		for _, event := range amountChanges {
			var tokenInfo types.TokenInfo
			var amount, contractAddress string
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool

			contractAddress = event.TokenAddress
			fromAddress = event.FromAddress
			toAddress = event.ToAddress
			amount = event.Amount

			if fromAddress != "" {
				fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
				if err != nil {
					log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
					return
				}
			}

			if toAddress != "" {
				toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, toAddress)
				if err != nil {
					log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
					return
				}
			}
			if !fromAddressExist && !toAddressExist {
				continue
			}

			if event.TxType == biz.TRANSFER {
				tokenInfo, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, contractAddress)
				if err != nil {
					log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
				}
				tokenInfo.Amount = amount
				tokenInfo.Address = contractAddress
			} else if event.TxType == biz.TRANSFERNFT {
				tokenId := event.TokenId
				tokenInfo, err = biz.GetNftInfoDirectlyRetryAlert(nil, h.chainName, contractAddress, tokenId)
				if err != nil {
					log.Error("扫块，从nodeProxy中获取NFT信息失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", contractAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
				}
				tokenInfo.Amount = amount
				tokenInfo.Address = contractAddress
				if tokenInfo.TokenType == "" {
					tokenInfo.TokenType = biz.SUINFT
				}
				if tokenInfo.TokenId == "" {
					tokenInfo.TokenId = tokenId
				}
			}
			amountValue, _ := decimal.NewFromString(amount)
			eventLogInfo := &types.EventLogUid{
				EventLog: types.EventLog{
					From:   fromAddress,
					To:     toAddress,
					Amount: amountValue.BigInt(),
					Token:  tokenInfo,
				},
				FromUid: fromUid,
				ToUid:   toUid,
			}

			var isContinue bool
			for i, eventLog := range eventLogs {
				if eventLog == nil {
					continue
				}
				if eventLog.From == eventLogInfo.To && eventLog.To == eventLogInfo.From && eventLog.Token.Address == eventLogInfo.Token.Address &&
					eventLog.Token.TokenId == eventLogInfo.Token.TokenId {
					cmp := eventLog.Amount.Cmp(eventLogInfo.Amount)
					if cmp == 1 {
						isContinue = true
						subAmount := new(big.Int).Sub(eventLog.Amount, eventLogInfo.Amount)
						eventLogs[i].Amount = subAmount
					} else if cmp == 0 {
						isContinue = true
						eventLogs[i] = nil
					} else if cmp == -1 {
						eventLogs[i] = nil
					}
					break
				} else if eventLog.From == eventLogInfo.From && eventLog.To == eventLogInfo.To && eventLog.Token.Address == eventLogInfo.Token.Address &&
					eventLog.Token.TokenId == eventLogInfo.Token.TokenId {
					isContinue = true
					addAmount := new(big.Int).Add(eventLog.Amount, eventLogInfo.Amount)
					eventLogs[i].Amount = addAmount
					break
				}
			}
			if isContinue {
				continue
			}
			eventLogs = append(eventLogs, eventLogInfo)
		}

		if len(eventLogs) > 0 {
			var eventLogList []*types.EventLogUid
			for _, eventLog := range eventLogs {
				if eventLog != nil {
					eventLogList = append(eventLogList, eventLog)
				}
			}
			eventLogs = eventLogList
		}

		if fromAddressExist || toAddressExist || len(eventLogs) > 0 {
			h.txRecords = append(h.txRecords, suiContractRecord)
		}

		if eventLogs != nil {
			eventLog, _ := utils.JsonEncode(eventLogs)
			suiContractRecord.EventLog = eventLog
		}

		if len(eventLogs) > 0 {
			logAddress := biz.GetLogAddressFromEventLogUid(eventLogs)
			// database btree index maximum is 2704
			logAddressLen := len(logAddress)
			if logAddressLen > 2704 {
				log.Error("扫块，logAddress长度超过最大限制", zap.Any("chainName", h.chainName), zap.Any("txHash", transactionHash), zap.Any("logAddressLen", logAddressLen))
				logAddress = nil
			}
			suiContractRecord.LogAddress = logAddress
		}

		for index, eventLog := range eventLogs {
			eventMap := map[string]interface{}{
				"token": eventLog.Token,
			}
			eventParseData, _ := utils.JsonEncode(eventMap)
			eventTokenInfoStr, _ := utils.JsonEncode(eventLog.Token)
			txHash := transactionHash + "#result-" + fmt.Sprintf("%v", index+1)
			txType := biz.EVENTLOG
			contractAddress := eventLog.Token.Address
			amountValue := decimal.NewFromBigInt(eventLog.Amount, 0)

			suiTransactionRecord := &data.SuiTransactionRecord{
				BlockHash:       chainBlock.Hash,
				BlockNumber:     int(curHeight),
				TransactionHash: txHash,
				FromAddress:     eventLog.From,
				ToAddress:       eventLog.To,
				FromUid:         eventLog.FromUid,
				ToUid:           eventLog.ToUid,
				FeeAmount:       feeAmount,
				Amount:          amountValue,
				Status:          status,
				TxTime:          transactionInfo.TxTime(),
				ContractAddress: contractAddress,
				ParseData:       eventParseData,
				GasLimit:        gasLimit,
				GasUsed:         gasUsed,
				Data:            payload,
				EventLog:        "",
				TransactionType: txType,
				TokenInfo:       eventTokenInfoStr,
				CreatedAt:       h.now,
				UpdatedAt:       h.now,
			}
			h.txRecords = append(h.txRecords, suiTransactionRecord)
		}
		eventLogLen := len(eventLogs)
		if eventLogLen == 2 && ((suiContractRecord.FromAddress == eventLogs[0].From && suiContractRecord.FromAddress == eventLogs[1].To) ||
			(suiContractRecord.FromAddress == eventLogs[0].To && suiContractRecord.FromAddress == eventLogs[1].From)) && !isNFTEventLog(eventLogs[0]) && !isNFTEventLog(eventLogs[1]) {
			suiContractRecord.TransactionType = biz.SWAP
		}
	}
	return nil
}

func isNFTEventLog(eventLog *types.EventLogUid) bool {
	return eventLog.Token.TokenType == biz.BENFENNFT || eventLog.Token.TokenType == biz.SUINFT
}

func (h *txHandler) getGasObjectType(transactionInfo *stypes.TransactionInfo) string {
	if !strings.Contains(h.chainName, "Benfen") {
		return SUI_CODE
	}
	objectIDTypes := make(map[string]string)
	for _, oc := range transactionInfo.ObjectChanges {
		objectIDTypes[oc.ObjectId] = oc.ObjectType
	}
	gasObjectID := transactionInfo.Effects.GasObject.Reference.ObjectId
	gasObjecType := UnwrapTokenIDFromCoinType(objectIDTypes[gasObjectID])
	return suiswap.NormalizeBenfenCoinType(h.chainName, gasObjecType)
}

func (h *txHandler) getTokenGasless(gasObjectType, gasUsed string) string {
	tokenID := gasObjectType
	if IsNative(tokenID) {
		return ""
	}
	tokenInfo, err := biz.GetTokenInfoRetryAlert(nil, h.chainName, tokenID)
	if err != nil {
		log.Warn(
			"BENFEN FEE TOKEN INFO",
			zap.Error(err), zap.String("gasObjectType", gasObjectType),
			zap.String("chainName", h.chainName),
		)
		return ""
	}
	tokenInfo.Amount = gasUsed
	tokenGasless, _ := json.Marshal(ChainPayTokenGasless{
		GasToken:  gasObjectType,
		TokenInfo: tokenInfo,
	})
	return string(tokenGasless)
}

type ChainPayTokenGasless struct {
	GasToken  string          `json:"gasToken"`
	TokenInfo types.TokenInfo `json:"chainPayTokenInfo"`
}

func getFees(transactionInfo *stypes.TransactionInfo) (gasLimit, gasUsed string, feeAmount decimal.Decimal) {
	return transactionInfo.GasLimit(), transactionInfo.GasUsed(), transactionInfo.FeeAmount()
}

func getOwnerAddress(owner interface{}) (address string) {
	if taddress, ok := owner.(string); ok {
		//https://suiexplorer.com/txblock/6xKvAZfQANGnJPbuReez3RjnYNqWDt73WkiEThMsCGze
		address = taddress
	} else {
		owner, ok := owner.(map[string]interface{})
		if !ok {
			return
		}
		if owner != nil {
			if owner["AddressOwner"] != nil {
				address = owner["AddressOwner"].(string)
			} else if owner["ObjectOwner"] != nil {
				//https://suiexplorer.com/txblock/5XaTqu2CJ4WBpHw5NeSt46EVYPPYDTxirRUNYZbU7Sb3
				address = owner["ObjectOwner"].(string)
			}
		}
	}
	return
}

func (h *txHandler) isGasCoinAmountChange(transactionInfo *stypes.TransactionInfo, amountChange *AmountChange, gasObjecType string) bool {
	if !biz.IsBenfenNet(h.chainName) {
		return amountChange.TxType == biz.NATIVE
	}
	if IsNative(gasObjecType) {
		gasObjecType = ""
	}
	return amountChange.TokenAddress == gasObjecType
}

func (h *txHandler) OnSealedTx(c chain.Clienter, tx *chain.Transaction) (err error) {
	client := c.(*Client)
	transactionInfo := tx.Raw.(*stypes.TransactionInfo)

	height, _ := strconv.ParseInt(transactionInfo.Checkpoint, 10, 64)
	curHeight := uint64(height)
	block, err := client.GetBlockByNumber(curHeight)
	if err != nil {
		log.Error("扫块，从链上获取区块信息失败", zap.Any("chainName", h.chainName), zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
		return err
	}
	blkTime, _ := strconv.ParseInt(block.TimestampMs, 10, 64)
	chainBlock := &chain.Block{
		Hash:       block.Digest,
		ParentHash: block.PreviousDigest,
		Number:     curHeight,
		Raw:        block,
		Time:       blkTime / 1000,
		//Transactions: chainTransactions,
	}
	tx.BlockNumber = curHeight

	err = h.OnNewTx(c, chainBlock, tx)

	return err
}

func (h *txHandler) OnDroppedTx(c chain.Clienter, tx *chain.Transaction) error {
	record := tx.Record.(*data.SuiTransactionRecord)

	nowTime := time.Now().Unix()
	if record.CreatedAt+180 > nowTime {
		if record.Status == biz.PENDING {
			record.Status = biz.NO_STATUS
			record.UpdatedAt = h.now
			h.txRecords = append(h.txRecords, record)
			log.Info(
				"更新 PENDING txhash对象无状态",
				zap.String("chainName", h.chainName),
				zap.Any("txHash", record.TransactionHash),
				zap.String("nodeUrl", c.URL()),
				zap.Int64("nowTime", nowTime),
				zap.Int64("createTime", record.CreatedAt),
			)
		}
	} else {
		record.Status = biz.DROPPED
		record.UpdatedAt = h.now
		h.txRecords = append(h.txRecords, record)
		log.Info(
			"更新 PENDING txhash对象为终态:交易被抛弃",
			zap.String("chainName", h.chainName),
			zap.Any("txHash", record.TransactionHash),
			zap.String("nodeUrl", c.URL()),
			zap.Int64("nowTime", nowTime),
			zap.Int64("createTime", record.CreatedAt),
		)
	}

	return nil
}

func (h *txHandler) Save(c chain.Clienter) error {
	client := c.(*Client)

	if h.txRecords != nil && len(h.txRecords) > 0 {
		//保存交易数据
		err := BatchSaveOrUpdate(h.txRecords, biz.GetTableName(h.chainName))
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链扫块，将数据插入到数据库中失败", h.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("扫块，将数据插入到数据库中失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
			return err
		}
		if h.newTxs {
			go HandleRecord(h.chainName, *client, h.txRecords)
		} else {
			go HandlePendingRecord(h.chainName, *client, h.txRecords)
		}

		if h.newTxs {
			records := make([]interface{}, 0, len(h.txRecords))
			for _, r := range h.txRecords {
				records = append(records, r)
			}
			common.SetResultOfTxs(h.block, records)
		} else {
			common.SetTxResult(h.txByHash, h.txRecords[0])
		}
	}
	return nil
}
