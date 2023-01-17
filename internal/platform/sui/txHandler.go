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

func (h *txHandler) OnNewTx(c chain.Clienter, chainBlock *chain.Block, chainTx *chain.Transaction) (err error) {
	block := chainBlock.Raw.(TransactionInfo)
	transactionHash := block.Certificate.TransactionDigest

	var status string
	if block.Effects.Status.Status == "success" {
		status = biz.SUCCESS
	} else {
		status = biz.FAIL
	}

	index := 0
	if len(block.Certificate.Data.Transactions) > 0 && block.Certificate.Data.Transactions[0].TransferObject != nil {
		var newTxEvents []*Event
		var newTxs []*Transaction
		newTxEventMap := make(map[string]*Event)
		newTxMap := make(map[string]*Transaction)
		txEvents := block.Effects.Events
		for _, txEvent := range txEvents {
			coinBalanceChange := txEvent.CoinBalanceChange
			if coinBalanceChange != nil {
				if coinBalanceChange.ChangeType == "Pay" || coinBalanceChange.ChangeType == "Receive" {
					key := coinBalanceChange.ChangeType + coinBalanceChange.Owner.AddressOwner + coinBalanceChange.CoinType
					newTxEvent, ok := newTxEventMap[key]
					if !ok {
						newTxEventMap[key] = txEvent
					} else {
						newTxEvent.CoinBalanceChange.Amount = new(big.Int).Add(newTxEvent.CoinBalanceChange.Amount, coinBalanceChange.Amount)
					}
				} else {
					newTxEvents = append(newTxEvents, txEvent)
				}
			} else {
				newTxEvents = append(newTxEvents, txEvent)
			}
		}
		for _, tx := range block.Certificate.Data.Transactions {
			key := tx.TransferObject.Recipient + tx.TransferObject.ObjectRef.ObjectId
			newTxMap[key] = tx
		}
		for key, event := range newTxEventMap {
			newTxEvents = append(newTxEvents, event)
			if strings.HasPrefix(key, "Receive") {
				txKey := event.CoinBalanceChange.Owner.AddressOwner + event.CoinBalanceChange.CoinObjectId
				tx, ok := newTxMap[txKey]
				if ok {
					newTxs = append(newTxs, tx)
				}
			}
		}
		block.Effects.Events = newTxEvents
		block.Certificate.Data.Transactions = newTxs
	}

	for _, tx := range block.Certificate.Data.Transactions {
		var recipients []string
		var amounts []*big.Int
		if tx.PaySui != nil && len(tx.PaySui.Recipients) > 1 {
			recipientMap := make(map[string]*big.Int)
			for i, recipient := range tx.PaySui.Recipients {
				var amount *big.Int
				if len(tx.PaySui.Amounts) > i {
					amount = tx.PaySui.Amounts[i]
				}
				txAmount, ok := recipientMap[recipient]
				if !ok {
					recipientMap[recipient] = amount
				} else if amount != nil {
					recipientMap[recipient] = new(big.Int).Add(txAmount, amount)
				}
			}

			for key, txAmount := range recipientMap {
				recipients = append(recipients, key)
				amounts = append(amounts, txAmount)
			}
			tx.PaySui.Recipients = recipients
			tx.PaySui.Amounts = amounts
		} else if tx.Pay != nil && len(tx.Pay.Recipients) > 1 {
			recipientMap := make(map[string]*big.Int)
			for i, recipient := range tx.Pay.Recipients {
				txAmount, ok := recipientMap[recipient]
				if !ok {
					recipientMap[recipient] = tx.Pay.Amounts[i]
				} else {
					recipientMap[recipient] = new(big.Int).Add(txAmount, tx.Pay.Amounts[i])
				}
			}

			for key, txAmount := range recipientMap {
				recipients = append(recipients, key)
				amounts = append(amounts, txAmount)
			}
			tx.Pay.Recipients = recipients
			tx.Pay.Amounts = amounts
		}

		if tx.TransferSui != nil || (tx.PaySui != nil && len(tx.PaySui.Recipients) <= 1) ||
			(tx.Pay != nil && len(tx.Pay.Recipients) <= 1) || tx.TransferObject != nil {
			txType := biz.NATIVE
			var tokenInfo types.TokenInfo
			var amount, contractAddress string
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool

			if tx.TransferSui != nil {
				fromAddress = block.Certificate.Data.Sender
				toAddress = tx.TransferSui.Recipient
				amount = tx.TransferSui.Amount.String()
			} else if tx.PaySui != nil {
				fromAddress = block.Certificate.Data.Sender
				if len(tx.PaySui.Recipients) > 0 {
					toAddress = tx.PaySui.Recipients[0]
				}
				if len(tx.PaySui.Amounts) > 0 {
					amount = tx.PaySui.Amounts[0].String()
				}
			} else if tx.Pay != nil {
				fromAddress = block.Certificate.Data.Sender
				if len(tx.Pay.Recipients) > 0 {
					toAddress = tx.Pay.Recipients[0]
				}
				if len(tx.Pay.Amounts) > 0 {
					amount = tx.Pay.Amounts[0].String()
				}
				txEvents := block.Effects.Events
				for _, txEvent := range txEvents {
					coinBalanceChange := txEvent.CoinBalanceChange
					if coinBalanceChange != nil && coinBalanceChange.ChangeType == "Receive" &&
						coinBalanceChange.Owner != nil && coinBalanceChange.Owner.AddressOwner == toAddress {
						//amount = coinBalanceChange.Amount.String()
						coinType := coinBalanceChange.CoinType
						if coinType != SUI_CODE {
							contractAddress = coinType
							txType = biz.TRANSFER
						}
						break
					}
				}
			} else {
				fromAddress = block.Certificate.Data.Sender
				toAddress = tx.TransferObject.Recipient
				var isTransfer bool
				txEvents := block.Effects.Events
				for _, txEvent := range txEvents {
					coinBalChange := txEvent.CoinBalanceChange
					if coinBalChange != nil && coinBalChange.ChangeType == "Receive" &&
						coinBalChange.Owner != nil && coinBalChange.Owner.AddressOwner == toAddress {
						amount = coinBalChange.Amount.String()
						coinType := coinBalChange.CoinType
						if coinType != SUI_CODE {
							contractAddress = coinType
							txType = biz.TRANSFER
						}
						isTransfer = true
						break
					}
				}
				if !isTransfer {
					continue
				}
			}

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
			txTime := block.TimestampMs / 1000
			gasLimit := strconv.Itoa(block.Certificate.Data.GasBudget)
			computationCost := block.Effects.GasUsed.ComputationCost
			storageCost := block.Effects.GasUsed.StorageCost
			storageRebate := block.Effects.GasUsed.StorageRebate
			gasUsedInt := computationCost + storageCost - storageRebate
			gasUsed := strconv.Itoa(gasUsedInt)
			feeAmount := decimal.NewFromInt(int64(gasUsedInt))

			if txType == biz.TRANSFER {
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

			suiTransactionRecord := &data.SuiTransactionRecord{
				TransactionVersion: int(h.curHeight),
				TransactionHash:    txHash,
				FromAddress:        fromAddress,
				ToAddress:          toAddress,
				FromUid:            fromUid,
				ToUid:              toUid,
				FeeAmount:          feeAmount,
				Amount:             amountValue,
				Status:             status,
				TxTime:             txTime,
				ContractAddress:    contractAddress,
				ParseData:          parseData,
				GasLimit:           gasLimit,
				GasUsed:            gasUsed,
				Data:               "",
				EventLog:           "",
				TransactionType:    txType,
				DappData:           "",
				ClientData:         "",
				CreatedAt:          h.now,
				UpdatedAt:          h.now,
			}
			h.txRecords = append(h.txRecords, suiTransactionRecord)
		} else if tx.PaySui != nil || tx.Pay != nil {
			txType := biz.NATIVE
			var tokenInfo types.TokenInfo
			var amount, contractAddress string
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool

			fromAddress = block.Certificate.Data.Sender

			if fromAddress != "" {
				fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
				if err != nil {
					log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
					return
				}
			}

			for i, recipient := range recipients {
				toAddress = recipient
				amount = amounts[i].String()
				if tx.Pay != nil {
					txEvents := block.Effects.Events
					for _, txEvent := range txEvents {
						coinBalChange := txEvent.CoinBalanceChange
						if coinBalChange != nil && coinBalChange.ChangeType == "Receive" &&
							coinBalChange.Owner != nil && coinBalChange.Owner.AddressOwner == toAddress {
							//amount = coinBalChange.Amount.String()
							coinType := coinBalChange.CoinType
							if coinType != SUI_CODE {
								contractAddress = coinType
								txType = biz.TRANSFER
							}
							break
						}
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
				txTime := block.TimestampMs / 1000
				gasLimit := strconv.Itoa(block.Certificate.Data.GasBudget)
				computationCost := block.Effects.GasUsed.ComputationCost
				storageCost := block.Effects.GasUsed.StorageCost
				storageRebate := block.Effects.GasUsed.StorageRebate
				gasUsedInt := computationCost + storageCost - storageRebate
				gasUsed := strconv.Itoa(gasUsedInt)
				feeAmount := decimal.NewFromInt(int64(gasUsedInt))

				if txType == biz.TRANSFER {
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

				suiTransactionRecord := &data.SuiTransactionRecord{
					TransactionVersion: int(h.curHeight),
					TransactionHash:    txHash,
					FromAddress:        fromAddress,
					ToAddress:          toAddress,
					FromUid:            fromUid,
					ToUid:              toUid,
					FeeAmount:          feeAmount,
					Amount:             amountValue,
					Status:             status,
					TxTime:             txTime,
					ContractAddress:    contractAddress,
					ParseData:          parseData,
					GasLimit:           gasLimit,
					GasUsed:            gasUsed,
					Data:               "",
					EventLog:           "",
					TransactionType:    txType,
					DappData:           "",
					ClientData:         "",
					CreatedAt:          h.now,
					UpdatedAt:          h.now,
				}
				h.txRecords = append(h.txRecords, suiTransactionRecord)
			}
		} else if tx.Call != nil {
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

			fromAddress = block.Certificate.Data.Sender
			toAddress = tx.Call.Package.ObjectId

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

			index++
			var txHash string
			if index == 1 {
				txHash = transactionHash
			} else {
				txHash = transactionHash + "#result-" + fmt.Sprintf("%v", index)
			}
			txTime = block.TimestampMs / 1000
			gasLimit = strconv.Itoa(block.Certificate.Data.GasBudget)
			computationCost := block.Effects.GasUsed.ComputationCost
			storageCost := block.Effects.GasUsed.StorageCost
			storageRebate := block.Effects.GasUsed.StorageRebate
			gasUsedi := computationCost + storageCost - storageRebate
			gasUsed = strconv.Itoa(gasUsedi)
			feeAmount = decimal.NewFromInt(int64(gasUsedi))

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
				TransactionVersion: int(h.curHeight),
				TransactionHash:    txHash,
				FromAddress:        fromAddress,
				ToAddress:          toAddress,
				FromUid:            fromUid,
				ToUid:              toUid,
				FeeAmount:          feeAmount,
				Amount:             amountValue,
				Status:             status,
				TxTime:             txTime,
				ContractAddress:    contractAddress,
				ParseData:          parseData,
				GasLimit:           gasLimit,
				GasUsed:            gasUsed,
				Data:               payload,
				EventLog:           "",
				TransactionType:    txType,
				DappData:           "",
				ClientData:         "",
				CreatedAt:          h.now,
				UpdatedAt:          h.now,
			}

			txType = biz.EVENTLOG
			//index := 0

			txEvents := block.Effects.Events
			for _, event := range txEvents {
				var tokenInfo types.TokenInfo
				var amount, contractAddress string
				var fromAddress, toAddress, fromUid, toUid string
				var fromAddressExist, toAddressExist bool

				coinBalChange := event.CoinBalanceChange
				if coinBalChange != nil && (coinBalChange.ChangeType == "Pay" ||
					coinBalChange.ChangeType == "Receive") {
					contractAddress = coinBalChange.CoinType
					if coinBalChange.ChangeType == "Pay" {
						fromAddress = coinBalChange.Sender
						toAddress = coinBalChange.PackageId
						amount = new(big.Int).Abs(coinBalChange.Amount).String()
					} else if coinBalChange.ChangeType == "Receive" {
						fromAddress = coinBalChange.PackageId
						toAddress = coinBalChange.Owner.AddressOwner
						amount = coinBalChange.Amount.String()
					}
					if fromAddress == toAddress {
						continue
					}
				} else {
					continue
				}

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
					TransactionVersion: int(h.curHeight),
					TransactionHash:    txHash,
					FromAddress:        fromAddress,
					ToAddress:          toAddress,
					FromUid:            fromUid,
					ToUid:              toUid,
					FeeAmount:          feeAmount,
					Amount:             amountValue,
					Status:             status,
					TxTime:             txTime,
					ContractAddress:    contractAddress,
					ParseData:          parseData,
					GasLimit:           gasLimit,
					GasUsed:            gasUsed,
					Data:               payload,
					EventLog:           "",
					TransactionType:    txType,
					DappData:           "",
					ClientData:         "",
					CreatedAt:          h.now,
					UpdatedAt:          h.now,
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
	}
	return nil
}

func (h *txHandler) OnSealedTx(c chain.Clienter, tx *chain.Transaction) (err error) {
	transactionInfo := tx.Raw.(TransactionInfo)
	record := tx.Record.(*data.SuiTransactionRecord)

	if transactionInfo.Error != nil && strings.HasPrefix(transactionInfo.Error.Message, "Could not find the referenced transaction") {
		nowTime := time.Now().Unix()
		if record.CreatedAt+180 > nowTime {
			if record.Status == biz.PENDING {
				status := biz.NO_STATUS
				record.Status = status
				record.UpdatedAt = h.now
				h.txRecords = append(h.txRecords, record)
			}
		} else {
			status := biz.DROPPED
			record.Status = status
			record.UpdatedAt = h.now
			h.txRecords = append(h.txRecords, record)
		}
		return nil
	}

	block := &chain.Block{
		Hash: transactionInfo.Certificate.TransactionDigest,
		Raw:  transactionInfo,
		Transactions: []*chain.Transaction{
			{
				Raw: transactionInfo,
			},
		},
	}

	err = h.OnNewTx(c, block, tx)

	return err
}

func (h *txHandler) OnDroppedTx(c chain.Clienter, tx *chain.Transaction) error {
	return nil
}

func (h *txHandler) Save(c chain.Clienter) error {
	client := c.(*Client)

	if h.txRecords != nil && len(h.txRecords) > 0 {
		//保存交易数据
		err := BatchSaveOrUpdate(h.txRecords, biz.GetTalbeName(h.chainName))
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
