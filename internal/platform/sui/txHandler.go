package sui

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"fmt"
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
	chainHeight uint64
	curHeight   uint64
	now         int64
	newTxs      bool

	txRecords []*data.SuiTransactionRecord
}

func (h *txHandler) OnNewTx(c chain.Clienter, blk *chain.Block, chainTx *chain.Transaction) (err error) {
	index := 0
	client := c.(*Client)
	block := blk.Raw.(TransactionInfo)

	var status string
	if block.Effects.Status.Status == "success" {
		status = biz.SUCCESS
	} else {
		status = biz.FAIL
	}

	for _, tx := range block.Certificate.Data.Transactions {
		if tx.TransferSui != nil || tx.TransferObject != nil || tx.Pay != nil {
			txType := biz.NATIVE
			var tokenInfo types.TokenInfo
			var amount, contractAddress string
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool

			if tx.TransferSui != nil {
				fromAddress = block.Certificate.Data.Sender
				toAddress = tx.TransferSui.Recipient
				amount = strconv.Itoa(tx.TransferSui.Amount)
			} else if tx.Pay != nil {
				fromAddress = block.Certificate.Data.Sender
				toAddress = tx.Pay.Recipients[0]
				amount = strconv.Itoa(tx.Pay.Amounts[0])
			} else {
				var inEvent *Event
				var eventAmount int
				var payAmount int
				txEvents := block.Effects.Events
				for _, txEvent := range txEvents {
					moveEvent := txEvent.TransferObject
					coinBalChange := txEvent.CoinBalanceChange
					if moveEvent != nil {
						event := &Event{
							FromAddress: moveEvent.Sender,
							ToAddress:   moveEvent.Recipient.AddressOwner,
							ObjectId:    moveEvent.ObjectId,
							Amount:      strconv.Itoa(moveEvent.Amount),
						}
						eventAmount += moveEvent.Amount
						if inEvent == nil {
							inEvent = event
						} else {
							inEvent.Amount = strconv.Itoa(eventAmount)
						}
					}
					if coinBalChange != nil && coinBalChange.ChangeType == "Receive" && coinBalChange.Owner != nil && coinBalChange.Owner.AddressOwner == tx.TransferObject.Recipient {
						payAmount += coinBalChange.Amount
					}
				}
				if inEvent != nil {
					objectId := inEvent.ObjectId
					object, err := client.GetObject(objectId)
					for i := 0; i < 10 && err != nil && fmt.Sprintf("%s", err) != "Deleted"; i++ {
						time.Sleep(time.Duration(i*5) * time.Second)
						object, err = client.GetObject(objectId)
					}
					if err != nil {
						if fmt.Sprintf("%s", err) == "Deleted" {
							log.Error(h.chainName+"扫块，从链上获取object信息被删除", zap.Any("objectId", objectId), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
							//continue
						} else {
							log.Error(h.chainName+"扫块，从链上获取object信息失败", zap.Any("objectId", objectId), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
							return err
						}
					}

					fromAddress = inEvent.FromAddress
					toAddress = inEvent.ToAddress
					amount = inEvent.Amount
					if object != nil {
						dataType := object.Details.Data.Type
						if dataType != NATIVE_TYPE {
							if strings.HasPrefix(dataType, TYPE_PREFIX) {
								contractAddress = dataType[len(TYPE_PREFIX)+1 : len(dataType)-1]
							} else {
								contractAddress = dataType
							}
							txType = biz.TRANSFER
						}
					}
				} else {
					fromAddress = block.Certificate.Data.Sender
					toAddress = tx.TransferObject.Recipient
					amount = strconv.Itoa(payAmount)
				}
			}

			if fromAddress != "" {
				fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
				if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
					// redis出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
					return
				}
			}

			if toAddress != "" {
				toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
				if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
					// redis出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
					return
				}
			}

			if fromAddressExist || toAddressExist {
				index++
				txHash := block.Certificate.TransactionDigest
				if index > 1 {
					txHash += "#result-" + fmt.Sprintf("%v", index)
				}
				txTime := h.now
				gasLimit := strconv.Itoa(block.Certificate.Data.GasBudget)
				computationCost := block.Effects.GasUsed.ComputationCost
				storageCost := block.Effects.GasUsed.StorageCost
				storageRebate := block.Effects.GasUsed.StorageRebate
				gasUsedi := computationCost + storageCost - storageRebate
				gasUsed := strconv.Itoa(gasUsedi)
				feeAmount := decimal.NewFromInt(int64(gasUsedi))

				contractAddressSplit := strings.Split(contractAddress, "::")
				if len(contractAddressSplit) == 3 && txType == biz.TRANSFER {
					var decimals int64 = 0
					getTokenInfo, err := biz.GetTokenInfo(nil, h.chainName, contractAddress)
					for i := 0; i < 3 && err != nil; i++ {
						time.Sleep(time.Duration(i*1) * time.Second)
						getTokenInfo, err = biz.GetTokenInfo(nil, h.chainName, contractAddress)
					}
					if err != nil {
						// nodeProxy出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
					} else if getTokenInfo.Symbol != "" {
						decimals = getTokenInfo.Decimals
					}
					tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amount}
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
				if tx.TransferObject != nil {
					return
				}
			}
		} else if tx.Call != nil {
			flag := false
			var txTime int64
			var gasLimit string
			var gasUsed string
			var feeAmount decimal.Decimal
			var payload string
			var eventLogs []types.EventLog
			var suiContractRecord *data.SuiTransactionRecord

			txType := biz.CONTRACT
			var tokenInfo types.TokenInfo
			var amount, contractAddress string
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool

			fromAddress = block.Certificate.Data.Sender
			toAddress = tx.Call.Package.ObjectId

			if fromAddress != "" {
				fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
				if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
					// redis出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
					return
				}
			}

			if toAddress != "" {
				toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
				if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
					// redis出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
					return
				}
			}
			if fromAddress == toAddress {
				return
			}

			if fromAddressExist || toAddressExist {
				index++
				var txHash string
				if index == 1 {
					txHash = block.Certificate.TransactionDigest
				} else {
					txHash = block.Certificate.TransactionDigest + "#result-" + fmt.Sprintf("%v", index)
				}
				txTime = h.now
				gasLimit = strconv.Itoa(block.Certificate.Data.GasBudget)
				computationCost := block.Effects.GasUsed.ComputationCost
				storageCost := block.Effects.GasUsed.StorageCost
				storageRebate := block.Effects.GasUsed.StorageRebate
				gasUsedi := computationCost + storageCost - storageRebate
				gasUsed = strconv.Itoa(gasUsedi)
				feeAmount = decimal.NewFromInt(int64(gasUsedi))

				flag = true

				contractAddressSplit := strings.Split(contractAddress, "::")
				if len(contractAddressSplit) == 3 && contractAddress != SUI_CODE && contractAddress != "" {
					var decimals int64 = 0
					getTokenInfo, err := biz.GetTokenInfo(nil, h.chainName, contractAddress)
					for i := 0; i < 3 && err != nil; i++ {
						time.Sleep(time.Duration(i*1) * time.Second)
						getTokenInfo, err = biz.GetTokenInfo(nil, h.chainName, contractAddress)
					}
					if err != nil {
						// nodeProxy出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
					} else if getTokenInfo.Symbol != "" {
						decimals = getTokenInfo.Decimals
					}
					tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amount}
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
				h.txRecords = append(h.txRecords, suiContractRecord)
			}

			txType = biz.EVENTLOG
			//index := 0

			var events []Event
			if tx.Call.Function == "swap_x_to_y" {
				txEvents := block.Effects.Events
				for _, txEvent := range txEvents {
					moveEvent := txEvent.MoveEvent
					if moveEvent != nil {
						inEvent := Event{
							FromAddress:  moveEvent.Sender,
							ToAddress:    moveEvent.Fields.PoolId,
							TokenAddress: tx.Call.TypeArguments[0],
							Amount:       strconv.Itoa(moveEvent.Fields.InAmount),
						}
						events = append(events, inEvent)

						outEvent := Event{
							FromAddress:  moveEvent.Fields.PoolId,
							ToAddress:    moveEvent.Sender,
							TokenAddress: tx.Call.TypeArguments[1],
							Amount:       strconv.Itoa(moveEvent.Fields.OutAmount),
						}
						events = append(events, outEvent)
					}
				}
			} else if tx.Call.Function == "add_liquidity" {
				txEvents := block.Effects.Events
				for _, txEvent := range txEvents {
					moveEvent := txEvent.MoveEvent
					if moveEvent != nil {
						xEvent := Event{
							FromAddress:  moveEvent.Sender,
							ToAddress:    moveEvent.Fields.PoolId,
							TokenAddress: tx.Call.TypeArguments[0],
							Amount:       strconv.Itoa(moveEvent.Fields.XAmount),
						}
						events = append(events, xEvent)

						yEvent := Event{
							FromAddress:  moveEvent.Sender,
							ToAddress:    moveEvent.Fields.PoolId,
							TokenAddress: tx.Call.TypeArguments[1],
							Amount:       strconv.Itoa(moveEvent.Fields.YAmount),
						}
						events = append(events, yEvent)

						lspEvent := Event{
							FromAddress:  moveEvent.Fields.PoolId,
							ToAddress:    moveEvent.Sender,
							TokenAddress: moveEvent.PackageId + "::pool::Pool<" + tx.Call.TypeArguments[0] + ", " + tx.Call.TypeArguments[1] + ">",
							Amount:       strconv.Itoa(moveEvent.Fields.LspAmount),
						}
						events = append(events, lspEvent)
					}
				}
			} else if tx.Call.Function == "remove_liquidity" {
				txEvents := block.Effects.Events
				for _, txEvent := range txEvents {
					moveEvent := txEvent.MoveEvent
					if moveEvent != nil {
						xEvent := Event{
							FromAddress:  moveEvent.Fields.PoolId,
							ToAddress:    moveEvent.Sender,
							TokenAddress: tx.Call.TypeArguments[0],
							Amount:       strconv.Itoa(moveEvent.Fields.XAmount),
						}
						events = append(events, xEvent)

						yEvent := Event{
							FromAddress:  moveEvent.Fields.PoolId,
							ToAddress:    moveEvent.Sender,
							TokenAddress: tx.Call.TypeArguments[1],
							Amount:       strconv.Itoa(moveEvent.Fields.YAmount),
						}
						events = append(events, yEvent)

						lspEvent := Event{
							FromAddress:  moveEvent.Sender,
							ToAddress:    moveEvent.Fields.PoolId,
							TokenAddress: moveEvent.PackageId + "::pool::Pool<" + tx.Call.TypeArguments[0] + ", " + tx.Call.TypeArguments[1] + ">",
							Amount:       strconv.Itoa(moveEvent.Fields.LspAmount),
						}
						events = append(events, lspEvent)
					}
				}
			}

			for _, event := range events {
				var tokenInfo types.TokenInfo
				var amount, contractAddress string
				var fromAddress, toAddress, fromUid, toUid string
				var fromAddressExist, toAddressExist bool

				amount = event.Amount
				fromAddress = event.FromAddress
				toAddress = event.ToAddress
				contractAddress = event.TokenAddress

				if fromAddress == toAddress {
					continue
				}

				if fromAddress != "" {
					fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
					if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
						// redis出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
						return
					}
				}

				if toAddress != "" {
					toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
					if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
						// redis出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
						return
					}
				}

				if fromAddressExist || toAddressExist {
					index++
					txHash := block.Certificate.TransactionDigest + "#result-" + fmt.Sprintf("%v", index)

					if !flag {
						txTime = h.now
						gasLimit = strconv.Itoa(block.Certificate.Data.GasBudget)
						computationCost := block.Effects.GasUsed.ComputationCost
						storageCost := block.Effects.GasUsed.StorageCost
						storageRebate := block.Effects.GasUsed.StorageRebate
						gasUsedi := computationCost + storageCost - storageRebate
						gasUsed = strconv.Itoa(gasUsedi)
						feeAmount = decimal.NewFromInt(int64(gasUsedi))

						flag = true
					}

					contractAddressSplit := strings.Split(contractAddress, "::")
					if len(contractAddressSplit) == 3 && contractAddress != SUI_CODE && contractAddress != "" {
						var decimals int64 = 0
						getTokenInfo, err := biz.GetTokenInfo(nil, h.chainName, contractAddress)
						for i := 0; i < 3 && err != nil; i++ {
							time.Sleep(time.Duration(i*1) * time.Second)
							getTokenInfo, err = biz.GetTokenInfo(nil, h.chainName, contractAddress)
						}
						if err != nil {
							// nodeProxy出错 接入lark报警
							alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
							log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
						} else if getTokenInfo.Symbol != "" {
							decimals = getTokenInfo.Decimals
						}
						tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amount}
					}
					suiMap := map[string]interface{}{
						"token": tokenInfo,
					}
					parseData, _ := utils.JsonEncode(suiMap)
					amountValue, _ := decimal.NewFromString(amount)
					eventLogInfo := types.EventLog{
						From:   fromAddress,
						To:     toAddress,
						Amount: amountValue.BigInt(),
						Token:  tokenInfo,
					}
					eventLogs = append(eventLogs, eventLogInfo)
					eventLog, _ := utils.JsonEncode(eventLogInfo)

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
						EventLog:           eventLog,
						TransactionType:    txType,
						DappData:           "",
						ClientData:         "",
						CreatedAt:          h.now,
						UpdatedAt:          h.now,
					}
					h.txRecords = append(h.txRecords, suiTransactionRecord)
				}
			}

			if suiContractRecord != nil && eventLogs != nil {
				eventLog, _ := utils.JsonEncode(eventLogs)
				suiContractRecord.EventLog = eventLog
			}
		}
	}
	return nil
}

func (h *txHandler) OnSealedTx(c chain.Clienter, txByHash *chain.Transaction) (err error) {
	client := c.(*Client)
	block := txByHash.Raw.(TransactionInfo)

	curHeight := 0

	index := 0
	for _, tx := range block.Certificate.Data.Transactions {
		var status string
		if block.Effects.Status.Status == "success" {
			status = biz.SUCCESS
		} else {
			status = biz.FAIL
		}

		if tx.TransferSui != nil || tx.TransferObject != nil || tx.Pay != nil {
			txType := biz.NATIVE
			var tokenInfo types.TokenInfo
			var amount, contractAddress string
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool

			if tx.TransferSui != nil {
				fromAddress = block.Certificate.Data.Sender
				toAddress = tx.TransferSui.Recipient
				amount = strconv.Itoa(tx.TransferSui.Amount)
			} else if tx.Pay != nil {
				fromAddress = block.Certificate.Data.Sender
				toAddress = tx.Pay.Recipients[0]
				amount = strconv.Itoa(tx.Pay.Amounts[0])
			} else {
				var inEvent *Event
				var eventAmount int
				var payAmount int
				txEvents := block.Effects.Events
				for _, txEvent := range txEvents {
					moveEvent := txEvent.TransferObject
					coinBalChange := txEvent.CoinBalanceChange
					if moveEvent != nil {
						event := &Event{
							FromAddress: moveEvent.Sender,
							ToAddress:   moveEvent.Recipient.AddressOwner,
							ObjectId:    moveEvent.ObjectId,
							Amount:      strconv.Itoa(moveEvent.Amount),
						}
						eventAmount += moveEvent.Amount
						if inEvent == nil {
							inEvent = event
						} else {
							inEvent.Amount = strconv.Itoa(eventAmount)
						}
					}
					if coinBalChange != nil && coinBalChange.ChangeType == "Receive" && coinBalChange.Owner != nil && coinBalChange.Owner.AddressOwner == tx.TransferObject.Recipient {
						payAmount += coinBalChange.Amount
					}
				}
				if inEvent != nil {
					objectId := inEvent.ObjectId
					object, err := client.GetObject(objectId)
					for i := 0; i < 10 && err != nil && fmt.Sprintf("%s", err) != "Deleted"; i++ {
						time.Sleep(time.Duration(i*5) * time.Second)
						object, err = client.GetObject(objectId)
					}
					if err != nil {
						if fmt.Sprintf("%s", err) == "Deleted" {
							log.Error(h.chainName+"扫块，从链上获取object信息被删除", zap.Any("objectId", objectId), zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
							//continue
						} else {
							log.Error(h.chainName+"扫块，从链上获取object信息失败", zap.Any("objectId", objectId), zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
							return err
						}
					}

					fromAddress = inEvent.FromAddress
					toAddress = inEvent.ToAddress
					amount = inEvent.Amount
					if object != nil {
						dataType := object.Details.Data.Type
						if dataType != NATIVE_TYPE {
							if strings.HasPrefix(dataType, TYPE_PREFIX) {
								contractAddress = dataType[len(TYPE_PREFIX)+1 : len(dataType)-1]
							} else {
								contractAddress = dataType
							}
							txType = biz.TRANSFER
						}
					}
				} else {
					fromAddress = block.Certificate.Data.Sender
					toAddress = tx.TransferObject.Recipient
					amount = strconv.Itoa(payAmount)
				}
			}

			if fromAddress != "" {
				fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
				if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
					// redis出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
					return
				}
			}

			if toAddress != "" {
				toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
				if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
					// redis出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
					return
				}
			}

			if fromAddressExist || toAddressExist {
				txHash := block.Certificate.TransactionDigest
				txTime := h.now
				gasLimit := strconv.Itoa(block.Certificate.Data.GasBudget)
				computationCost := block.Effects.GasUsed.ComputationCost
				storageCost := block.Effects.GasUsed.StorageCost
				storageRebate := block.Effects.GasUsed.StorageRebate
				gasUsedi := computationCost + storageCost - storageRebate
				gasUsed := strconv.Itoa(gasUsedi)
				feeAmount := decimal.NewFromInt(int64(gasUsedi))

				contractAddressSplit := strings.Split(contractAddress, "::")
				if len(contractAddressSplit) == 3 && txType == biz.TRANSFER {
					var decimals int64 = 0
					getTokenInfo, err := biz.GetTokenInfo(nil, h.chainName, contractAddress)
					for i := 0; i < 3 && err != nil; i++ {
						time.Sleep(time.Duration(i*1) * time.Second)
						getTokenInfo, err = biz.GetTokenInfo(nil, h.chainName, contractAddress)
					}
					if err != nil {
						// nodeProxy出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
					} else if getTokenInfo.Symbol != "" {
						decimals = getTokenInfo.Decimals
					}
					tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amount}
				}
				suiMap := map[string]interface{}{
					"token": tokenInfo,
				}
				parseData, _ := utils.JsonEncode(suiMap)
				amountValue, _ := decimal.NewFromString(amount)

				suiTransactionRecord := &data.SuiTransactionRecord{
					TransactionVersion: curHeight,
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
				if tx.TransferObject != nil {
					break
				}
			}
		} else if tx.Call != nil {
			flag := false
			var txTime int64
			var gasLimit string
			var gasUsed string
			var feeAmount decimal.Decimal
			var payload string
			var eventLogs []types.EventLog
			var suiContractRecord *data.SuiTransactionRecord

			txType := biz.CONTRACT
			var tokenInfo types.TokenInfo
			var amount, contractAddress string
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool

			fromAddress = block.Certificate.Data.Sender
			toAddress = tx.Call.Package.ObjectId

			if fromAddress != "" {
				fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
				if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
					// redis出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
					return
				}
			}

			if toAddress != "" {
				toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
				if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
					// redis出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
					return
				}
			}
			if fromAddress == toAddress {
				continue
			}

			if fromAddressExist || toAddressExist {
				index++
				var txHash string
				if index == 1 {
					txHash = block.Certificate.TransactionDigest
				} else {
					txHash = block.Certificate.TransactionDigest + "#result-" + fmt.Sprintf("%v", index)
				}
				txTime = h.now
				gasLimit = strconv.Itoa(block.Certificate.Data.GasBudget)
				computationCost := block.Effects.GasUsed.ComputationCost
				storageCost := block.Effects.GasUsed.StorageCost
				storageRebate := block.Effects.GasUsed.StorageRebate
				gasUsedi := computationCost + storageCost - storageRebate
				gasUsed = strconv.Itoa(gasUsedi)
				feeAmount = decimal.NewFromInt(int64(gasUsedi))

				flag = true

				contractAddressSplit := strings.Split(contractAddress, "::")
				if len(contractAddressSplit) == 3 && contractAddress != SUI_CODE && contractAddress != "" {
					var decimals int64 = 0
					getTokenInfo, err := biz.GetTokenInfo(nil, h.chainName, contractAddress)
					for i := 0; i < 3 && err != nil; i++ {
						time.Sleep(time.Duration(i*1) * time.Second)
						getTokenInfo, err = biz.GetTokenInfo(nil, h.chainName, contractAddress)
					}
					if err != nil {
						// nodeProxy出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
					} else if getTokenInfo.Symbol != "" {
						decimals = getTokenInfo.Decimals
					}
					tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amount}
				}
				suiMap := map[string]interface{}{
					"token": tokenInfo,
				}
				parseData, _ := utils.JsonEncode(suiMap)
				amountValue, _ := decimal.NewFromString(amount)

				suiContractRecord = &data.SuiTransactionRecord{
					TransactionVersion: curHeight,
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
				h.txRecords = append(h.txRecords, suiContractRecord)
			}

			txType = biz.EVENTLOG
			//index := 0

			var events []Event
			if tx.Call.Function == "swap_x_to_y" {
				txEvents := block.Effects.Events
				for _, txEvent := range txEvents {
					moveEvent := txEvent.MoveEvent
					if moveEvent != nil {
						inEvent := Event{
							FromAddress:  moveEvent.Sender,
							ToAddress:    moveEvent.Fields.PoolId,
							TokenAddress: tx.Call.TypeArguments[0],
							Amount:       strconv.Itoa(moveEvent.Fields.InAmount),
						}
						events = append(events, inEvent)

						outEvent := Event{
							FromAddress:  moveEvent.Fields.PoolId,
							ToAddress:    moveEvent.Sender,
							TokenAddress: tx.Call.TypeArguments[1],
							Amount:       strconv.Itoa(moveEvent.Fields.OutAmount),
						}
						events = append(events, outEvent)
					}
				}
			} else if tx.Call.Function == "add_liquidity" {
				txEvents := block.Effects.Events
				for _, txEvent := range txEvents {
					moveEvent := txEvent.MoveEvent
					if moveEvent != nil {
						xEvent := Event{
							FromAddress:  moveEvent.Sender,
							ToAddress:    moveEvent.Fields.PoolId,
							TokenAddress: tx.Call.TypeArguments[0],
							Amount:       strconv.Itoa(moveEvent.Fields.XAmount),
						}
						events = append(events, xEvent)

						yEvent := Event{
							FromAddress:  moveEvent.Sender,
							ToAddress:    moveEvent.Fields.PoolId,
							TokenAddress: tx.Call.TypeArguments[1],
							Amount:       strconv.Itoa(moveEvent.Fields.YAmount),
						}
						events = append(events, yEvent)

						lspEvent := Event{
							FromAddress:  moveEvent.Fields.PoolId,
							ToAddress:    moveEvent.Sender,
							TokenAddress: moveEvent.PackageId + "::pool::Pool<" + tx.Call.TypeArguments[0] + ", " + tx.Call.TypeArguments[1] + ">",
							Amount:       strconv.Itoa(moveEvent.Fields.LspAmount),
						}
						events = append(events, lspEvent)
					}
				}
			} else if tx.Call.Function == "remove_liquidity" {
				txEvents := block.Effects.Events
				for _, txEvent := range txEvents {
					moveEvent := txEvent.MoveEvent
					if moveEvent != nil {
						xEvent := Event{
							FromAddress:  moveEvent.Fields.PoolId,
							ToAddress:    moveEvent.Sender,
							TokenAddress: tx.Call.TypeArguments[0],
							Amount:       strconv.Itoa(moveEvent.Fields.XAmount),
						}
						events = append(events, xEvent)

						yEvent := Event{
							FromAddress:  moveEvent.Fields.PoolId,
							ToAddress:    moveEvent.Sender,
							TokenAddress: tx.Call.TypeArguments[1],
							Amount:       strconv.Itoa(moveEvent.Fields.YAmount),
						}
						events = append(events, yEvent)

						lspEvent := Event{
							FromAddress:  moveEvent.Sender,
							ToAddress:    moveEvent.Fields.PoolId,
							TokenAddress: moveEvent.PackageId + "::pool::Pool<" + tx.Call.TypeArguments[0] + ", " + tx.Call.TypeArguments[1] + ">",
							Amount:       strconv.Itoa(moveEvent.Fields.LspAmount),
						}
						events = append(events, lspEvent)
					}
				}
			}

			for _, event := range events {
				var tokenInfo types.TokenInfo
				var amount, contractAddress string
				var fromAddress, toAddress, fromUid, toUid string
				var fromAddressExist, toAddressExist bool

				amount = event.Amount
				fromAddress = event.FromAddress
				toAddress = event.ToAddress
				contractAddress = event.TokenAddress

				if fromAddress == toAddress {
					continue
				}

				if fromAddress != "" {
					fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
					if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
						// redis出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
						return
					}
				}

				if toAddress != "" {
					toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
					if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
						// redis出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
						return
					}
				}

				if fromAddressExist || toAddressExist {
					index++
					txHash := block.Certificate.TransactionDigest + "#result-" + fmt.Sprintf("%v", index)

					if !flag {
						txTime = h.now
						gasLimit = strconv.Itoa(block.Certificate.Data.GasBudget)
						computationCost := block.Effects.GasUsed.ComputationCost
						storageCost := block.Effects.GasUsed.StorageCost
						storageRebate := block.Effects.GasUsed.StorageRebate
						gasUsedi := computationCost + storageCost - storageRebate
						gasUsed = strconv.Itoa(gasUsedi)
						feeAmount = decimal.NewFromInt(int64(gasUsedi))

						flag = true
					}

					contractAddressSplit := strings.Split(contractAddress, "::")
					if len(contractAddressSplit) == 3 && contractAddress != SUI_CODE && contractAddress != "" {
						var decimals int64 = 0
						getTokenInfo, err := biz.GetTokenInfo(nil, h.chainName, contractAddress)
						for i := 0; i < 3 && err != nil; i++ {
							time.Sleep(time.Duration(i*1) * time.Second)
							getTokenInfo, err = biz.GetTokenInfo(nil, h.chainName, contractAddress)
						}
						if err != nil {
							// nodeProxy出错 接入lark报警
							alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
							log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
						} else if getTokenInfo.Symbol != "" {
							decimals = getTokenInfo.Decimals
						}
						tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amount}
					}
					suiMap := map[string]interface{}{
						"token": tokenInfo,
					}
					parseData, _ := utils.JsonEncode(suiMap)
					amountValue, _ := decimal.NewFromString(amount)
					eventLogInfo := types.EventLog{
						From:   fromAddress,
						To:     toAddress,
						Amount: amountValue.BigInt(),
						Token:  tokenInfo,
					}
					eventLogs = append(eventLogs, eventLogInfo)
					eventLog, _ := utils.JsonEncode(eventLogInfo)

					suiTransactionRecord := &data.SuiTransactionRecord{
						TransactionVersion: curHeight,
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
						EventLog:           eventLog,
						TransactionType:    txType,
						DappData:           "",
						ClientData:         "",
						CreatedAt:          h.now,
						UpdatedAt:          h.now,
					}
					h.txRecords = append(h.txRecords, suiTransactionRecord)
				}
			}

			if suiContractRecord != nil && eventLogs != nil {
				eventLog, _ := utils.JsonEncode(eventLogs)
				suiContractRecord.EventLog = eventLog
			}
		}
	}
	return
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
			log.Error(h.chainName+"扫块，将数据插入到数据库中失败" /*, zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight)*/, zap.Any("error", err))
			return err
		}
		if h.newTxs {
			go HandleRecord(h.chainName, *client, h.txRecords)

		} else {
			go HandlePendingRecord(h.chainName, *client, h.txRecords)
		}
	}
	return nil
}
