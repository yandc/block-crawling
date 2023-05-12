package sui

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"gorm.io/datatypes"

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
	transactionInfo := chainTx.Raw.(*TransactionInfo)
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

	fromAmountChangeMap := make(map[string]*AmountChange)
	toAmountChangeMap := make(map[string][]*AmountChange)
	var amountChanges []*AmountChange
	balanceChanges := transactionInfo.BalanceChanges
	for _, balanceChange := range balanceChanges {
		txType := biz.NATIVE
		var address, tokenAddress string
		if taddress, ok := balanceChange.Owner.(string); ok {
			address = taddress
		} else {
			owner := balanceChange.Owner.(map[string]interface{})
			address = owner["AddressOwner"].(string)
		}
		amount := balanceChange.Amount
		if balanceChange.CoinType != SUI_CODE {
			txType = biz.TRANSFER
			tokenAddress = balanceChange.CoinType
		}
		if strings.HasPrefix(balanceChange.Amount, "-") {
			fromAmountChangeMap[tokenAddress] = &AmountChange{
				FromAddress:  address,
				TxType:       txType,
				TokenAddress: tokenAddress,
				Amount:       amount,
			}
		} else {
			toAmountChangeMap[tokenAddress] = append(toAmountChangeMap[tokenAddress], &AmountChange{
				ToAddress:    address,
				TxType:       txType,
				TokenAddress: tokenAddress,
				Amount:       amount,
			})
		}
	}
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
			if fromAmountChange.TxType == biz.NATIVE {
				computationCost, _ := strconv.Atoi(transactionInfo.Effects.GasUsed.ComputationCost)
				storageCost, _ := strconv.Atoi(transactionInfo.Effects.GasUsed.StorageCost)
				storageRebate, _ := strconv.Atoi(transactionInfo.Effects.GasUsed.StorageRebate)
				gasUsedInt := computationCost + storageCost - storageRebate
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
			if fromAmountChange.TxType == biz.NATIVE {
				computationCost, _ := strconv.Atoi(transactionInfo.Effects.GasUsed.ComputationCost)
				storageCost, _ := strconv.Atoi(transactionInfo.Effects.GasUsed.StorageCost)
				storageRebate, _ := strconv.Atoi(transactionInfo.Effects.GasUsed.StorageRebate)
				gasUsedInt := computationCost + storageCost - storageRebate
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
		if strings.HasPrefix(objectType, "0x2::") || strings.HasPrefix(objectType, "0x3::") {
			continue
		}
		var toAddress string
		if objectChange.Owner != nil {
			ownerMap, ok := owner.(map[string]interface{})
			if !ok {
				continue
			}
			addressOwner := ownerMap["AddressOwner"]
			if addressOwner == nil {
				continue
			}
			toAddress = addressOwner.(string)
		}
		if sender != "" && toAddress != "" && objectType != "" && objectId != "" {
			amountChange := &AmountChange{
				FromAddress:  sender,
				ToAddress:    toAddress,
				TxType:       biz.TRANSFERNFT,
				TokenAddress: objectType,
				TokenId:      objectId,
				Amount:       "1",
			}
			amountChanges = append(amountChanges, amountChange)
		}
	}

	isContract := false
	transactions := tx.Data.Transaction.Transactions
	for _, transaction := range transactions {
		if transaction.MoveCall != nil {
			isContract = true
			break
		}
	}

	if len(amountChanges) == 0 && !isContract {
		var toAddress string
		inputs := tx.Data.Transaction.Inputs
		for _, input := range inputs {
			if input.Type == "pure" && input.ValueType == "address" {
				toAddress = utils.GetString(input.Value)
			}
		}
		amountChange := &AmountChange{
			FromAddress:  tx.Data.Sender,
			ToAddress:    toAddress,
			TxType:       "",
			TokenAddress: "",
			Amount:       "0",
		}
		amountChanges = append(amountChanges, amountChange)
	}

	if len(amountChanges) > 1 {
		isContract = true
	}

	index := 0
	if !isContract {
		txTime, _ := strconv.ParseInt(transactionInfo.TimestampMs, 10, 64)
		txTime = txTime / 1000
		gasLimit := transactionInfo.Transaction.Data.GasData.Budget
		computationCost, _ := strconv.Atoi(transactionInfo.Effects.GasUsed.ComputationCost)
		storageCost, _ := strconv.Atoi(transactionInfo.Effects.GasUsed.StorageCost)
		storageRebate, _ := strconv.Atoi(transactionInfo.Effects.GasUsed.StorageRebate)
		gasUsedInt := computationCost + storageCost - storageRebate
		gasUsed := strconv.Itoa(gasUsedInt)
		feeAmount := decimal.NewFromInt(int64(gasUsedInt))

		for _, amountChange := range amountChanges {
			txType := biz.NATIVE
			var tokenInfo types.TokenInfo
			var amount, contractAddress string
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool

			txType = amountChange.TxType
			contractAddress = amountChange.TokenAddress
			fromAddress = amountChange.FromAddress
			toAddress = amountChange.ToAddress
			amount = amountChange.Amount

			if fromAddress != "" {
				fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
				if err != nil {
					log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
					return
				}
			}

			if toAddress != "" {
				toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, toAddress)
				if err != nil {
					log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
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
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
				}
				tokenInfo.Amount = amount
				tokenInfo.Address = contractAddress
			} else if txType == biz.TRANSFERNFT {
				tokenId := amountChange.TokenId
				tokenInfo, err = biz.GetNftInfoDirectlyRetryAlert(nil, h.chainName, contractAddress, tokenId)
				if err != nil {
					log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", contractAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
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
				TxTime:          txTime,
				ContractAddress: contractAddress,
				ParseData:       parseData,
				GasLimit:        gasLimit,
				GasUsed:         gasUsed,
				Data:            "",
				EventLog:        "",
				TransactionType: txType,
				DappData:        "",
				ClientData:      "",
				CreatedAt:       h.now,
				UpdatedAt:       h.now,
			}
			h.txRecords = append(h.txRecords, suiTransactionRecord)
		}
	} else {
		var txTime int64
		var gasLimit string
		var gasUsed string
		var feeAmount decimal.Decimal
		var payload string
		var eventLogs []*types.EventLog
		var suiTransactionRecords []*data.SuiTransactionRecord
		var suiContractRecord *data.SuiTransactionRecord

		txType := biz.CONTRACT
		var tokenInfo types.TokenInfo
		var amount, contractAddress string
		var fromAddress, toAddress, fromUid, toUid string
		var fromAddressExist, toAddressExist bool

		fromAddress = tx.Data.Sender
		toAddress = transactionInfo.Effects.EventsDigest

		if fromAddress != "" {
			fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
			if err != nil {
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
				return
			}
		}

		if toAddress != "" {
			toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, toAddress)
			if err != nil {
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
				return
			}
		}
		if fromAddress == toAddress {
			return
		}

		txTime, _ = strconv.ParseInt(transactionInfo.TimestampMs, 10, 64)
		txTime = txTime / 1000
		gasLimit = transactionInfo.Transaction.Data.GasData.Budget
		computationCost, _ := strconv.Atoi(transactionInfo.Effects.GasUsed.ComputationCost)
		storageCost, _ := strconv.Atoi(transactionInfo.Effects.GasUsed.StorageCost)
		storageRebate, _ := strconv.Atoi(transactionInfo.Effects.GasUsed.StorageRebate)
		gasUsedInt := computationCost + storageCost - storageRebate
		gasUsed = strconv.Itoa(gasUsedInt)
		feeAmount = decimal.NewFromInt(int64(gasUsedInt))

		if contractAddress != SUI_CODE && contractAddress != "" {
			tokenInfo, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, contractAddress)
			if err != nil {
				log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
			}
			tokenInfo.Amount = amount
			tokenInfo.Address = contractAddress
		}
		suiMap := map[string]interface{}{
			"token": tokenInfo,
		}
		parseData, _ := utils.JsonEncode(suiMap)
		amountValue, _ := decimal.NewFromString(amount)

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
			TxTime:          txTime,
			ContractAddress: contractAddress,
			ParseData:       parseData,
			GasLimit:        gasLimit,
			GasUsed:         gasUsed,
			Data:            payload,
			EventLog:        "",
			TransactionType: txType,
			DappData:        "",
			ClientData:      "",
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
					log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
					return
				}
			}

			if toAddress != "" {
				toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, toAddress)
				if err != nil {
					log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
					return
				}
			}
			if !fromAddressExist && !toAddressExist {
				continue
			}

			index++
			txHash := transactionHash + "#result-" + fmt.Sprintf("%v", index)

			if event.TxType == biz.TRANSFER {
				tokenInfo, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, contractAddress)
				if err != nil {
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
				}
				tokenInfo.Amount = amount
				tokenInfo.Address = contractAddress
			} else if event.TxType == biz.TRANSFERNFT {
				tokenId := event.TokenId
				tokenInfo, err = biz.GetNftInfoDirectlyRetryAlert(nil, h.chainName, contractAddress, tokenId)
				if err != nil {
					log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", contractAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
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
			amountValue, _ := decimal.NewFromString(amount)
			eventLogInfo := &types.EventLog{
				From:   fromAddress,
				To:     toAddress,
				Amount: amountValue.BigInt(),
				Token:  tokenInfo,
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
						suiTransactionRecords[i].Amount = decimal.NewFromBigInt(subAmount, 0)
					} else if cmp == 0 {
						isContinue = true
						eventLogs[i] = nil
						suiTransactionRecords[i] = nil
					} else if cmp == -1 {
						eventLogs[i] = nil
						suiTransactionRecords[i] = nil
					}
					break
				} else if eventLog.From == eventLogInfo.From && eventLog.To == eventLogInfo.To && eventLog.Token.Address == eventLogInfo.Token.Address &&
					eventLog.Token.TokenId == eventLogInfo.Token.TokenId {
					isContinue = true
					addAmount := new(big.Int).Add(eventLog.Amount, eventLogInfo.Amount)
					eventLogs[i].Amount = addAmount
					suiTransactionRecords[i].Amount = decimal.NewFromBigInt(addAmount, 0)
					break
				}
			}
			if isContinue {
				continue
			}
			eventLogs = append(eventLogs, eventLogInfo)

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
				TxTime:          txTime,
				ContractAddress: contractAddress,
				ParseData:       parseData,
				GasLimit:        gasLimit,
				GasUsed:         gasUsed,
				Data:            payload,
				EventLog:        "",
				TransactionType: txType,
				DappData:        "",
				ClientData:      "",
				CreatedAt:       h.now,
				UpdatedAt:       h.now,
			}
			suiTransactionRecords = append(suiTransactionRecords, suiTransactionRecord)
		}

		if fromAddressExist || toAddressExist || len(eventLogs) > 0 {
			h.txRecords = append(h.txRecords, suiContractRecord)
		}
		if len(eventLogs) > 0 {
			for _, suiTransactionRecord := range suiTransactionRecords {
				if suiTransactionRecord != nil {
					h.txRecords = append(h.txRecords, suiTransactionRecord)
				}
			}

			var eventLogList []*types.EventLog
			for _, eventLog := range eventLogs {
				if eventLog != nil {
					eventLogList = append(eventLogList, eventLog)
				}
			}
			if len(eventLogList) > 0 {
				eventLog, _ := utils.JsonEncode(eventLogList)
				suiContractRecord.EventLog = eventLog

				var logAddress datatypes.JSON
				var logFromAddress []string
				var logToAddress []string
				for _, log := range eventLogList {
					logFromAddress = append(logFromAddress, log.From)
					logToAddress = append(logToAddress, log.To)
				}
				logAddressList := [][]string{logFromAddress, logToAddress}
				logAddress, _ = json.Marshal(logAddressList)
				suiContractRecord.LogAddress = logAddress
			}
		}
	}
	return nil
}

func (h *txHandler) OnSealedTx(c chain.Clienter, tx *chain.Transaction) (err error) {
	client := c.(*Client)
	transactionInfo := tx.Raw.(*TransactionInfo)

	height, _ := strconv.ParseInt(transactionInfo.Checkpoint, 10, 64)
	curHeight := uint64(height)
	block, err := client.GetBlockByNumber(curHeight)
	if err != nil {
		log.Error(h.chainName+"扫块，从链上获取区块信息失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
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
			alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", h.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(h.chainName+"扫块，将数据插入到数据库中失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
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
