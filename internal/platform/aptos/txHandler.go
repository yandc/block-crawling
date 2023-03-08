package aptos

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"encoding/json"
	"fmt"
	"gorm.io/datatypes"
	"math/big"
	"strconv"
	"strings"

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

	txRecords      []*data.AptTransactionRecord
	txHashIndexMap map[string]int
}

func (h *txHandler) OnNewTx(c chain.Clienter, chainBlock *chain.Block, chainTx *chain.Transaction) (err error) {
	block := chainBlock.Raw.(BlockerInfo)
	curHeight := chainBlock.Number
	height := h.chainHeight

	tx := chainTx.Raw.(TransactionInfo)
	blockType := tx.Type
	transactionHash := tx.Hash
	var status string
	if blockType == "pending_transaction" {
		status = biz.PENDING
	} else if blockType == "user_transaction" {
		if tx.Success {
			status = biz.SUCCESS
		} else {
			status = biz.FAIL
		}
	}

	if status == "" || tx.Payload == nil || tx.Payload.Type == "module_bundle_payload" {
		return nil
	}

	if tx.Payload.Function == APT_DIRECT_TRANSFER || tx.Payload.Function == APT_CREATE_ACCOUNT || tx.Payload.Function == APT_REGISTER ||
		tx.Payload.Function == APT_ACCOUNT_TRANSFER || tx.Payload.Function == APT_TRANSFER || tx.Payload.Function == APT_MINT {
		txType := ""
		var tokenInfo types.TokenInfo
		var amount, contractAddress string
		var fromAddress, toAddress, fromUid, toUid string
		var fromAddressExist, toAddressExist bool

		fromAddress = tx.Sender
		if tx.Payload.Function == APT_DIRECT_TRANSFER {
			txType = biz.DIRECTTRANSFERNFTSWITCH
		} else if tx.Payload.Function == APT_CREATE_ACCOUNT {
			txType = biz.CREATEACCOUNT
			if len(tx.Payload.Arguments) > 0 {
				if toAddressStr, ok := tx.Payload.Arguments[0].(string); ok {
					toAddress = toAddressStr
				}
			}
		} else if tx.Payload.Function == APT_REGISTER {
			txType = biz.REGISTERTOKEN
			if len(tx.Payload.TypeArguments) > 0 {
				tyArgs := tx.Payload.TypeArguments[0]
				mode := strings.Split(tyArgs, "::")
				if len(mode) == 3 {
					toAddress = mode[0]
				}
			}
		} else {
			txType = biz.NATIVE
			if len(tx.Payload.Arguments) > 0 {
				if toAddressStr, ok := tx.Payload.Arguments[0].(string); ok {
					toAddress = toAddressStr
				}
			}
			if len(tx.Payload.Arguments) > 1 {
				if amountStr, ok := tx.Payload.Arguments[1].(string); ok {
					amount = amountStr
				}
			}
			if len(tx.Payload.TypeArguments) > 0 {
				tyArgs := tx.Payload.TypeArguments[0]
				if tyArgs != APT_CODE {
					contractAddress = tyArgs
					txType = biz.TRANSFER
				}
			}
		}

		if fromAddress != "" {
			fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, utils.AddressAdd0(fromAddress))
			if err != nil {
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
				return
			}
		}

		if toAddress != "" {
			toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, utils.AddressAdd0(toAddress))
			if err != nil {
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
				return
			}
		}
		if !fromAddressExist && !toAddressExist {
			return
		}

		var changes = make(map[string]map[string]string)
		for _, change := range tx.Changes {
			dataType := change.Data.Type
			if strings.HasPrefix(dataType, "0x1::coin::CoinStore<") {
				tokenAddress := dataType[len("0x1::coin::CoinStore<") : len(dataType)-1]
				var tokenBalance string
				dataData := change.Data.Data
				if dataData != nil {
					dataDataCoin := dataData.Coin
					if dataDataCoin != nil {
						tokenBalance = dataDataCoin.Value
					}
				}
				if tokenAddress != "" && tokenBalance != "" {
					changeMap, ok := changes[change.Address]
					if !ok {
						changeMap = make(map[string]string)
						changes[change.Address] = changeMap
					}
					changeMap[tokenAddress] = tokenBalance
				}
			}
		}

		version, _ := strconv.Atoi(tx.Version)
		nonce, _ := strconv.Atoi(tx.SequenceNumber)
		txTime, _ := strconv.ParseInt(tx.Timestamp, 10, 64)
		txTime = txTime / 1000000
		gasUsed, _ := strconv.Atoi(tx.GasUsed)
		gasPrice, _ := strconv.Atoi(tx.GasUnitPrice)
		feeAmount := decimal.NewFromInt(int64(gasUsed * gasPrice))
		payload, _ := utils.JsonEncode(map[string]interface{}{"changes": changes, "payload": tx.Payload})

		if txType == biz.TRANSFER {
			tokenInfo, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, contractAddress)
			if err != nil {
				log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
			}
			tokenInfo.Amount = amount
			tokenInfo.Address = contractAddress
		}
		aptosMap := map[string]interface{}{
			"aptos": map[string]string{
				"sequence_number": tx.SequenceNumber,
			},
			"token": tokenInfo,
		}
		parseData, _ := utils.JsonEncode(aptosMap)
		amountValue, _ := decimal.NewFromString(amount)

		aptTransactionRecord := &data.AptTransactionRecord{
			BlockHash:           block.BlockHash,
			BlockNumber:         int(curHeight),
			Nonce:               int64(nonce),
			TransactionVersion:  version,
			TransactionHash:     transactionHash,
			FromAddress:         fromAddress,
			ToAddress:           toAddress,
			FromUid:             fromUid,
			ToUid:               toUid,
			FeeAmount:           feeAmount,
			Amount:              amountValue,
			Status:              status,
			TxTime:              txTime,
			ContractAddress:     contractAddress,
			ParseData:           parseData,
			StateRootHash:       tx.StateRootHash,
			EventRootHash:       tx.EventRootHash,
			AccumulatorRootHash: tx.AccumulatorRootHash,
			GasLimit:            tx.MaxGasAmount,
			GasUsed:             tx.GasUsed,
			GasPrice:            tx.GasUnitPrice,
			Data:                payload,
			EventLog:            "",
			TransactionType:     txType,
			DappData:            "",
			ClientData:          "",
			CreatedAt:           h.now,
			UpdatedAt:           h.now,
		}
		h.txRecords = append(h.txRecords, aptTransactionRecord)
	} else if tx.Payload.Function == APT_OFFER_NFT || tx.Payload.Function == APT_CLAIM_NFT || tx.Payload.Function == APT_CANCEL_OFFER_NFT ||
		strings.HasSuffix(tx.Payload.Function, "::token::transfer_with_opt_in") ||
		strings.HasSuffix(tx.Payload.Function, "::transfer_token::transfer_token") {
		txType := biz.TRANSFERNFT
		var tokenInfo types.TokenInfo
		var amount, contractAddress string
		var fromAddress, toAddress, fromUid, toUid string
		var fromAddressExist, toAddressExist bool
		var itemName, creatorAddress, collectionName, propertyVersion string

		fromAddress = tx.Sender
		if tx.Payload.Function == APT_OFFER_NFT || tx.Payload.Function == APT_CLAIM_NFT || tx.Payload.Function == APT_CANCEL_OFFER_NFT {
			if len(tx.Payload.Arguments) > 0 {
				if toAddressStr, ok := tx.Payload.Arguments[0].(string); ok {
					toAddress = toAddressStr
				}
			}
			if len(tx.Payload.Arguments) > 1 {
				if creatorAddressStr, ok := tx.Payload.Arguments[1].(string); ok {
					creatorAddress = creatorAddressStr
				}
			}
			if len(tx.Payload.Arguments) > 2 {
				if collectionNameStr, ok := tx.Payload.Arguments[2].(string); ok {
					collectionName = collectionNameStr
				}
			}
			if len(tx.Payload.Arguments) > 3 {
				if nameStr, ok := tx.Payload.Arguments[3].(string); ok {
					itemName = nameStr
				}
			}
			if len(tx.Payload.Arguments) > 4 {
				if propertyVersionStr, ok := tx.Payload.Arguments[4].(string); ok {
					propertyVersion = propertyVersionStr
				}
			}
		} else {
			if len(tx.Payload.Arguments) > 0 {
				if creatorAddressStr, ok := tx.Payload.Arguments[0].(string); ok {
					creatorAddress = creatorAddressStr
				}
			}
			if len(tx.Payload.Arguments) > 1 {
				if collectionNameStr, ok := tx.Payload.Arguments[1].(string); ok {
					collectionName = collectionNameStr
				}
			}
			if len(tx.Payload.Arguments) > 2 {
				if nameStr, ok := tx.Payload.Arguments[2].(string); ok {
					itemName = nameStr
				}
			}
			if len(tx.Payload.Arguments) > 3 {
				if propertyVersionStr, ok := tx.Payload.Arguments[3].(string); ok {
					propertyVersion = propertyVersionStr
				}
			}
			if len(tx.Payload.Arguments) > 4 {
				if toAddressStr, ok := tx.Payload.Arguments[4].(string); ok {
					toAddress = toAddressStr
				}
			}
		}
		if len(tx.Payload.Arguments) > 5 {
			if amountStr, ok := tx.Payload.Arguments[5].(string); ok {
				amount = amountStr
			}
		} else {
			if len(tx.Events) > 0 {
				eventData := tx.Events[0].Data
				if eventData == nil {
					return
				}
				dataMap := eventData.(map[string]interface{})
				amounti := dataMap["amount"]
				if amounti == nil {
					return
				}

				amount = amounti.(string)
			}
		}
		contractAddress = creatorAddress + "::" + collectionName + "::" + propertyVersion

		if fromAddress != "" {
			fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, utils.AddressAdd0(fromAddress))
			if err != nil {
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
				return
			}
		}

		if toAddress != "" {
			toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, utils.AddressAdd0(toAddress))
			if err != nil {
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
				return
			}
		}
		if !fromAddressExist && !toAddressExist {
			return
		}

		var changes = make(map[string]map[string]string)
		for _, change := range tx.Changes {
			dataType := change.Data.Type
			if strings.HasPrefix(dataType, "0x1::coin::CoinStore<") {
				tokenAddress := dataType[len("0x1::coin::CoinStore<") : len(dataType)-1]
				var tokenBalance string
				dataData := change.Data.Data
				if dataData != nil {
					dataDataCoin := dataData.Coin
					if dataDataCoin != nil {
						tokenBalance = dataDataCoin.Value
					}
				}
				if tokenAddress != "" && tokenBalance != "" {
					changeMap, ok := changes[change.Address]
					if !ok {
						changeMap = make(map[string]string)
						changes[change.Address] = changeMap
					}
					changeMap[tokenAddress] = tokenBalance
				}
			}
		}

		version, _ := strconv.Atoi(tx.Version)
		nonce, _ := strconv.Atoi(tx.SequenceNumber)
		txTime, _ := strconv.ParseInt(tx.Timestamp, 10, 64)
		txTime = txTime / 1000000
		gasUsed, _ := strconv.Atoi(tx.GasUsed)
		gasPrice, _ := strconv.Atoi(tx.GasUnitPrice)
		feeAmount := decimal.NewFromInt(int64(gasUsed * gasPrice))
		payload, _ := utils.JsonEncode(map[string]interface{}{"changes": changes, "payload": tx.Payload})

		if contractAddress != APT_CODE && contractAddress != "" {
			tokenInfo, err = biz.GetNftInfoDirectlyRetryAlert(nil, h.chainName, contractAddress, itemName)
			if err != nil {
				log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", contractAddress), zap.Any("tokenId", itemName), zap.Any("error", err))
			}
			tokenInfo.Amount = amount
			tokenInfo.Address = contractAddress
			if tokenInfo.TokenType == "" {
				tokenInfo.TokenType = biz.APTOSNFT
			}
			if tokenInfo.TokenId == "" {
				tokenInfo.TokenId = itemName
			}
		}
		aptosMap := map[string]interface{}{
			"aptos": map[string]string{
				"sequence_number": tx.SequenceNumber,
			},
			"token": tokenInfo,
		}
		parseData, _ := utils.JsonEncode(aptosMap)
		amountValue, _ := decimal.NewFromString(amount)

		aptTransactionRecord := &data.AptTransactionRecord{
			BlockHash:           block.BlockHash,
			BlockNumber:         int(curHeight),
			Nonce:               int64(nonce),
			TransactionVersion:  version,
			TransactionHash:     transactionHash,
			FromAddress:         fromAddress,
			ToAddress:           toAddress,
			FromUid:             fromUid,
			ToUid:               toUid,
			FeeAmount:           feeAmount,
			Amount:              amountValue,
			Status:              status,
			TxTime:              txTime,
			ContractAddress:     contractAddress,
			ParseData:           parseData,
			StateRootHash:       tx.StateRootHash,
			EventRootHash:       tx.EventRootHash,
			AccumulatorRootHash: tx.AccumulatorRootHash,
			GasLimit:            tx.MaxGasAmount,
			GasUsed:             tx.GasUsed,
			GasPrice:            tx.GasUnitPrice,
			Data:                payload,
			EventLog:            "",
			TransactionType:     txType,
			DappData:            "",
			ClientData:          "",
			CreatedAt:           h.now,
			UpdatedAt:           h.now,
		}
		h.txRecords = append(h.txRecords, aptTransactionRecord)
	} else {
		var version int
		var nonce int
		var txTime int64
		var gasUsed int
		var gasPrice int
		var feeAmount decimal.Decimal
		var payload string
		var eventLogs []*types.EventLog
		var aptTransactionRecords []*data.AptTransactionRecord
		var aptContractRecord *data.AptTransactionRecord

		txType := biz.CONTRACT
		var tokenInfo types.TokenInfo
		var amount, contractAddress string
		var fromAddress, toAddress, fromUid, toUid string
		var fromAddressExist, toAddressExist bool

		fromAddress = tx.Sender
		mode := strings.Split(tx.Payload.Function, "::")
		if len(mode) == 3 {
			toAddress = mode[0]
		}

		if fromAddress != "" {
			fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, utils.AddressAdd0(fromAddress))
			if err != nil {
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
				return
			}
		}

		if toAddress != "" {
			toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, utils.AddressAdd0(toAddress))
			if err != nil {
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
				return
			}
		}
		if fromAddress == toAddress {
			return nil
		}

		var changes = make(map[string]map[string]string)
		for _, change := range tx.Changes {
			dataType := change.Data.Type
			if strings.HasPrefix(dataType, "0x1::coin::CoinStore<") {
				tokenAddress := dataType[len("0x1::coin::CoinStore<") : len(dataType)-1]
				var tokenBalance string
				dataData := change.Data.Data
				if dataData != nil {
					dataDataCoin := dataData.Coin
					if dataDataCoin != nil {
						tokenBalance = dataDataCoin.Value
					}
				}
				if tokenAddress != "" && tokenBalance != "" {
					changeMap, ok := changes[change.Address]
					if !ok {
						changeMap = make(map[string]string)
						changes[change.Address] = changeMap
					}
					changeMap[tokenAddress] = tokenBalance
				}
			}
		}

		version, _ = strconv.Atoi(tx.Version)
		nonce, _ = strconv.Atoi(tx.SequenceNumber)
		txTime, _ = strconv.ParseInt(tx.Timestamp, 10, 64)
		txTime = txTime / 1000000
		gasUsed, _ = strconv.Atoi(tx.GasUsed)
		gasPrice, _ = strconv.Atoi(tx.GasUnitPrice)
		feeAmount = decimal.NewFromInt(int64(gasUsed * gasPrice))
		payload, _ = utils.JsonEncode(map[string]interface{}{"changes": changes, "payload": tx.Payload})

		if contractAddress != APT_CODE && contractAddress != "" {
			tokenInfo, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, contractAddress)
			if err != nil {
				log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
			}
			tokenInfo.Amount = amount
			tokenInfo.Address = contractAddress
		}
		aptosMap := map[string]interface{}{
			"aptos": map[string]string{
				"sequence_number": tx.SequenceNumber,
			},
			"token": tokenInfo,
		}
		parseData, _ := utils.JsonEncode(aptosMap)
		amountValue, _ := decimal.NewFromString(amount)

		aptContractRecord = &data.AptTransactionRecord{
			BlockHash:           block.BlockHash,
			BlockNumber:         int(curHeight),
			Nonce:               int64(nonce),
			TransactionVersion:  version,
			TransactionHash:     transactionHash,
			FromAddress:         fromAddress,
			ToAddress:           toAddress,
			FromUid:             fromUid,
			ToUid:               toUid,
			FeeAmount:           feeAmount,
			Amount:              amountValue,
			Status:              status,
			TxTime:              txTime,
			ContractAddress:     contractAddress,
			ParseData:           parseData,
			StateRootHash:       tx.StateRootHash,
			EventRootHash:       tx.EventRootHash,
			AccumulatorRootHash: tx.AccumulatorRootHash,
			GasLimit:            tx.MaxGasAmount,
			GasUsed:             tx.GasUsed,
			GasPrice:            tx.GasUnitPrice,
			Data:                payload,
			EventLog:            "",
			TransactionType:     txType,
			DappData:            "",
			ClientData:          "",
			CreatedAt:           h.now,
			UpdatedAt:           h.now,
		}

		txType = biz.EVENTLOG
		index := 0

		for _, event := range tx.Events {
			var tokenInfo types.TokenInfo
			var amount, contractAddress string
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool
			var tokenType, itemName string

			if event.Type == "0x1::coin::WithdrawEvent" ||
				event.Type == "0x1::coin::DepositEvent" {
				eventData := event.Data
				if eventData == nil {
					continue
				}
				dataMap := eventData.(map[string]interface{})
				amounti := dataMap["amount"]
				if amounti == nil {
					continue
				}

				amount = amounti.(string)
				mode := strings.Split(tx.Payload.Function, "::")
				if event.Type == "0x1::coin::WithdrawEvent" {
					fromAddress = event.Guid.AccountAddress
					if len(mode) == 3 {
						toAddress = mode[0]
					}
				} else if event.Type == "0x1::coin::DepositEvent" {
					toAddress = event.Guid.AccountAddress
					if len(mode) == 3 {
						fromAddress = mode[0]
					}
				}
				if fromAddress == toAddress {
					continue
				}

				for _, change := range tx.Changes {
					if event.Guid.AccountAddress == change.Address {
						dataType := change.Data.Type
						if strings.HasPrefix(dataType, "0x1::coin::CoinStore<") {
							tokenAddress := dataType[len("0x1::coin::CoinStore<") : len(dataType)-1]
							dataData := change.Data.Data
							if dataData != nil {
								if event.Type == "0x1::coin::WithdrawEvent" {
									withdrawEvents := dataData.WithdrawEvents
									if withdrawEvents != nil {
										if event.Guid.CreationNumber == withdrawEvents.Guid.Id.CreationNum {
											contractAddress = tokenAddress
											break
										}
									}
								} else if event.Type == "0x1::coin::DepositEvent" {
									depositEvents := dataData.DepositEvents
									if depositEvents != nil {
										if event.Guid.CreationNumber == depositEvents.Guid.Id.CreationNum {
											contractAddress = tokenAddress
											break
										}
									}
								}
							}
						}
					}
				}
			} else if event.Type == "0x3::token::WithdrawEvent" ||
				event.Type == "0x3::token::DepositEvent" {
				tokenType = biz.APTOSNFT
				eventData := event.Data
				if eventData == nil {
					continue
				}
				dataMap := eventData.(map[string]interface{})
				amounti := dataMap["amount"]
				id := dataMap["id"]
				if amounti == nil || id == nil {
					continue
				}

				amount := amounti.(string)
				if amount == "0" {
					continue
				}
				mode := strings.Split(tx.Payload.Function, "::")
				if event.Type == "0x3::token::WithdrawEvent" {
					fromAddress = event.Guid.AccountAddress
					if len(mode) == 3 {
						toAddress = mode[0]
					}
				} else if event.Type == "0x3::token::DepositEvent" {
					toAddress = event.Guid.AccountAddress
					if len(mode) == 3 {
						fromAddress = mode[0]
					}
				}
				if fromAddress == toAddress {
					continue
				}

				idMap := id.(map[string]interface{})
				propertyVersion := idMap["property_version"].(string)
				tokenDataIdMap := idMap["token_data_id"].(map[string]interface{})
				itemName = tokenDataIdMap["name"].(string)
				creator := tokenDataIdMap["creator"].(string)
				collection := tokenDataIdMap["collection"].(string)
				contractAddress = creator + "::" + collection + "::" + propertyVersion
			} else {
				continue
			}

			if fromAddress != "" {
				fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, utils.AddressAdd0(fromAddress))
				if err != nil {
					log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
					return
				}
			}

			if toAddress != "" {
				toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, utils.AddressAdd0(toAddress))
				if err != nil {
					log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
					return
				}
			}
			if !fromAddressExist && !toAddressExist {
				continue
			}

			index++
			txHash := transactionHash + "#result-" + fmt.Sprintf("%v", index)

			if contractAddress != APT_CODE && contractAddress != "" {
				if tokenType != "" {
					tokenInfo, err = biz.GetNftInfoDirectlyRetryAlert(nil, h.chainName, contractAddress, itemName)
					if err != nil {
						log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", contractAddress), zap.Any("tokenId", itemName), zap.Any("error", err))
					}
				} else {
					tokenInfo, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, contractAddress)
					if err != nil {
						log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
					}
				}
				tokenInfo.Amount = amount
				tokenInfo.Address = contractAddress
				if tokenType != "" {
					if tokenInfo.TokenType == "" {
						tokenInfo.TokenType = tokenType
					}
					if tokenType == biz.APTOSNFT && tokenInfo.TokenId == "" {
						tokenInfo.TokenId = itemName
					}
				}
			}
			aptosMap := map[string]interface{}{
				"aptos": map[string]string{
					"sequence_number": tx.SequenceNumber,
				},
				"token": tokenInfo,
			}
			parseData, _ := utils.JsonEncode(aptosMap)
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
						aptTransactionRecords[i].Amount = decimal.NewFromBigInt(subAmount, 0)
					} else if cmp == 0 {
						isContinue = true
						eventLogs[i] = nil
						aptTransactionRecords[i] = nil
					} else if cmp == -1 {
						eventLogs[i] = nil
						aptTransactionRecords[i] = nil
					}
					break
				} else if eventLog.From == eventLogInfo.From && eventLog.To == eventLogInfo.To && eventLog.Token.Address == eventLogInfo.Token.Address &&
					eventLog.Token.TokenId == eventLogInfo.Token.TokenId {
					isContinue = true
					addAmount := new(big.Int).Add(eventLog.Amount, eventLogInfo.Amount)
					eventLogs[i].Amount = addAmount
					aptTransactionRecords[i].Amount = decimal.NewFromBigInt(addAmount, 0)
					break
				}
			}
			if isContinue {
				continue
			}
			eventLogs = append(eventLogs, eventLogInfo)

			aptTransactionRecord := &data.AptTransactionRecord{
				BlockHash:           block.BlockHash,
				BlockNumber:         int(curHeight),
				Nonce:               int64(nonce),
				TransactionVersion:  version,
				TransactionHash:     txHash,
				FromAddress:         fromAddress,
				ToAddress:           toAddress,
				FromUid:             fromUid,
				ToUid:               toUid,
				FeeAmount:           feeAmount,
				Amount:              amountValue,
				Status:              status,
				TxTime:              txTime,
				ContractAddress:     contractAddress,
				ParseData:           parseData,
				StateRootHash:       tx.StateRootHash,
				EventRootHash:       tx.EventRootHash,
				AccumulatorRootHash: tx.AccumulatorRootHash,
				GasLimit:            tx.MaxGasAmount,
				GasUsed:             tx.GasUsed,
				GasPrice:            tx.GasUnitPrice,
				Data:                payload,
				EventLog:            "",
				TransactionType:     txType,
				DappData:            "",
				ClientData:          "",
				CreatedAt:           h.now,
				UpdatedAt:           h.now,
			}
			aptTransactionRecords = append(aptTransactionRecords, aptTransactionRecord)
		}

		if fromAddressExist || toAddressExist || len(eventLogs) > 0 {
			h.txRecords = append(h.txRecords, aptContractRecord)
		}
		if len(eventLogs) > 0 {
			for _, aptTransactionRecord := range aptTransactionRecords {
				if aptTransactionRecord != nil {
					h.txRecords = append(h.txRecords, aptTransactionRecord)
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
				aptContractRecord.EventLog = eventLog

				var logAddress datatypes.JSON
				var logFromAddress []string
				var logToAddress []string
				for _, log := range eventLogList {
					logFromAddress = append(logFromAddress, log.From)
					logToAddress = append(logToAddress, log.To)
				}
				logAddressList := [][]string{logFromAddress, logToAddress}
				logAddress, _ = json.Marshal(logAddressList)
				aptContractRecord.LogAddress = logAddress
			}
		}
	}
	return nil
}

func (h *txHandler) Save(c chain.Clienter) error {
	txRecords := h.txRecords
	if txRecords != nil && len(txRecords) > 0 {
		//保存交易数据
		err := BatchSaveOrUpdate(txRecords, biz.GetTableName(h.chainName))
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", h.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(h.chainName+"扫块，将数据插入到数据库中失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
			return err
		}
		if h.newTxs {
			go HandleRecord(h.chainName, *c.(*Client), txRecords)
		} else {
			go HandlePendingRecord(h.chainName, *c.(*Client), txRecords)
		}
		if h.newTxs {
			records := make([]interface{}, 0, len(txRecords))
			for _, r := range txRecords {
				records = append(records, r)
			}
			common.SetResultOfTxs(h.block, records)
		} else {
			common.SetTxResult(h.txByHash, txRecords[0])
		}
	}
	return nil
}

func (h *txHandler) OnSealedTx(c chain.Clienter, tx *chain.Transaction) (err error) {
	client := c.(*Client)
	transactionInfo := tx.Raw.(TransactionInfo)
	record := tx.Record.(*data.AptTransactionRecord)

	transactionVersion, _ := strconv.Atoi(transactionInfo.Version)
	if transactionInfo.ErrorCode == "transaction_not_found" {
		status := biz.DROPPED
		record.Status = status
		record.UpdatedAt = h.now
		h.txRecords = append(h.txRecords, record)
		return nil
	} else if transactionInfo.Type == "pending_transaction" {
		return nil
	} else if transactionInfo.Type != "user_transaction" || transactionInfo.Payload == nil || transactionInfo.Payload.Type == "module_bundle_payload" {
		status := biz.SUCCESS
		record.Status = status
		record.UpdatedAt = h.now
		h.txRecords = append(h.txRecords, record)
		return nil
	}

	block, err := client.GetBlockByTxVersion(transactionVersion)
	if err != nil {
		log.Error(h.chainName+"扫块，从链上获取区块信息失败", zap.Any("transactionVersion", transactionVersion) /*, zap.Any("new", height)*/, zap.Any("error", err))
		return err
	}
	tx.BlockNumber = block.Number

	err = h.OnNewTx(c, block, tx)

	return err
}

func (h *txHandler) OnDroppedTx(c chain.Clienter, tx *chain.Transaction) error {
	return nil
}
