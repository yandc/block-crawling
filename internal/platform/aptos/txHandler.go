package aptos

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
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
	chainHeight uint64
	curHeight   uint64
	now         int64
	newTxs      bool

	txRecords      []*data.AptTransactionRecord
	txHashIndexMap map[string]int
}

func (h *txHandler) OnNewTx(c chain.Clienter, chainBlock *chain.Block, chainTx *chain.Transaction) (err error) {
	block := chainBlock.Raw.(BlockerInfo)
	curHeight := h.curHeight
	height := h.chainHeight

	tx := chainTx.Raw.(TransactionInfo)
	blockType := tx.Type
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

	if tx.Payload.Function == APT_CREATE_ACCOUNT || tx.Payload.Function == APT_REGISTER {
		txType := ""
		var tokenInfo types.TokenInfo
		//var amount, contractAddress string
		var fromAddress, toAddress, fromUid, toUid string
		var fromAddressExist, toAddressExist bool
		fromAddress = tx.Sender

		if tx.Payload.Function == APT_CREATE_ACCOUNT {
			txType = biz.CREATEACCOUNT
			if toAddressStr, ok := tx.Payload.Arguments[0].(string); ok {
				toAddress = toAddressStr
			}
		}
		if tx.Payload.Function == APT_REGISTER {
			txType = biz.REGISTERTOKEN
			if len(tx.Payload.TypeArguments) > 0 {
				tyArgs := tx.Payload.TypeArguments[0]
				mode := strings.Split(tyArgs, "::")
				if len(mode) == 3 {
					toAddress = mode[0]
				}
			}
		}

		if fromAddress != "" {
			fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
			if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
				// redis出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
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
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
				return
			}
		}

		if fromAddressExist || toAddressExist {
			version, _ := strconv.Atoi(tx.Version)
			nonce, _ := strconv.Atoi(tx.SequenceNumber)
			txTime, _ := strconv.ParseInt(tx.Timestamp, 10, 64)
			txTime = txTime / 1000000
			gasUsed, _ := strconv.Atoi(tx.GasUsed)
			gasPrice, _ := strconv.Atoi(tx.GasUnitPrice)
			feeAmount := decimal.NewFromInt(int64(gasUsed * gasPrice))
			payload, _ := utils.JsonEncode(tx.Payload)

			aptosMap := map[string]interface{}{
				"aptos": map[string]string{
					"sequence_number": tx.SequenceNumber,
				},
				"token": tokenInfo,
			}
			parseData, _ := utils.JsonEncode(aptosMap)

			aptTransactionRecord := &data.AptTransactionRecord{
				BlockHash:           block.BlockHash,
				BlockNumber:         int(curHeight),
				Nonce:               int64(nonce),
				TransactionVersion:  version,
				TransactionHash:     tx.Hash,
				FromAddress:         fromAddress,
				ToAddress:           toAddress,
				FromUid:             fromUid,
				ToUid:               toUid,
				FeeAmount:           feeAmount,
				Amount:              decimal.Zero,
				Status:              status,
				TxTime:              txTime,
				ContractAddress:     "",
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
		}
	} else if tx.Payload.Function == APT_ACCOUNT_TRANSFER || tx.Payload.Function == APT_TRANSFER || tx.Payload.Function == APT_MINT {
		txType := biz.NATIVE
		var tokenInfo types.TokenInfo
		var amount, contractAddress string
		var fromAddress, toAddress, fromUid, toUid string
		var fromAddressExist, toAddressExist bool

		fromAddress = tx.Sender
		if toAddressStr, ok := tx.Payload.Arguments[0].(string); ok {
			toAddress = toAddressStr
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

		if fromAddress != "" {
			fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
			if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
				// redis出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
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
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
				return
			}
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

		if fromAddressExist || toAddressExist {
			version, _ := strconv.Atoi(tx.Version)
			nonce, _ := strconv.Atoi(tx.SequenceNumber)
			txTime, _ := strconv.ParseInt(tx.Timestamp, 10, 64)
			txTime = txTime / 1000000
			gasUsed, _ := strconv.Atoi(tx.GasUsed)
			gasPrice, _ := strconv.Atoi(tx.GasUnitPrice)
			feeAmount := decimal.NewFromInt(int64(gasUsed * gasPrice))
			payload, _ := utils.JsonEncode(map[string]interface{}{"changes": changes, "payload": tx.Payload})

			if txType == biz.TRANSFER {
				tokenInfo, err = biz.GetTokenInfo(nil, h.chainName, contractAddress)
				for i := 0; i < 3 && err != nil; i++ {
					time.Sleep(time.Duration(i*1) * time.Second)
					tokenInfo, err = biz.GetTokenInfo(nil, h.chainName, contractAddress)
				}
				if err != nil {
					// nodeProxy出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
				}
				tokenInfo.Amount = amount
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
				TransactionHash:     tx.Hash,
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
		}
	} else {
		flag := false
		var version int
		var nonce int
		var txTime int64
		var gasUsed int
		var gasPrice int
		var feeAmount decimal.Decimal
		var payload string
		var eventLogs []types.EventLog
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
			fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
			if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
				// redis出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
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
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
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

		if fromAddressExist || toAddressExist {
			version, _ = strconv.Atoi(tx.Version)
			nonce, _ = strconv.Atoi(tx.SequenceNumber)
			txTime, _ = strconv.ParseInt(tx.Timestamp, 10, 64)
			txTime = txTime / 1000000
			gasUsed, _ = strconv.Atoi(tx.GasUsed)
			gasPrice, _ = strconv.Atoi(tx.GasUnitPrice)
			feeAmount = decimal.NewFromInt(int64(gasUsed * gasPrice))
			payload, _ = utils.JsonEncode(map[string]interface{}{"changes": changes, "payload": tx.Payload})

			flag = true

			if contractAddress != APT_CODE && contractAddress != "" {
				tokenInfo, err = biz.GetTokenInfo(nil, h.chainName, contractAddress)
				for i := 0; i < 3 && err != nil; i++ {
					time.Sleep(time.Duration(i*1) * time.Second)
					tokenInfo, err = biz.GetTokenInfo(nil, h.chainName, contractAddress)
				}
				if err != nil {
					// nodeProxy出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
				}
				tokenInfo.Amount = amount
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
				TransactionHash:     tx.Hash,
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
			h.txRecords = append(h.txRecords, aptContractRecord)
		}

		txType = biz.EVENTLOG
		index := 0

		for _, event := range tx.Events {
			var tokenInfo types.TokenInfo
			var amount, contractAddress string
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool

			if event.Type == "0x1::coin::WithdrawEvent" ||
				event.Type == "0x1::coin::DepositEvent" {
				amount = event.Data.Amount
				if amount == "0" {
					continue
				}
				mode := strings.Split(tx.Payload.Function, "::")
				if event.Type == "0x1::coin::WithdrawEvent" {
					//fromAddress = "0x" + event.Key[18:]
					fromAddress = event.Guid.AccountAddress
					if len(mode) == 3 {
						toAddress = mode[0]
					}
				} else if event.Type == "0x1::coin::DepositEvent" {
					//toAddress = "0x" + event.Key[18:]
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
			} else {
				continue
			}

			if fromAddress != "" {
				fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
				if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
					// redis出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
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
					log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
					return
				}
			}

			if fromAddressExist || toAddressExist {
				index++
				txHash := tx.Hash + "#result-" + fmt.Sprintf("%v", index)

				if !flag {
					version, _ = strconv.Atoi(tx.Version)
					nonce, _ = strconv.Atoi(tx.SequenceNumber)
					txTime, _ = strconv.ParseInt(tx.Timestamp, 10, 64)
					txTime = txTime / 1000000
					gasUsed, _ = strconv.Atoi(tx.GasUsed)
					gasPrice, _ = strconv.Atoi(tx.GasUnitPrice)
					feeAmount = decimal.NewFromInt(int64(gasUsed * gasPrice))
					payload, _ = utils.JsonEncode(map[string]interface{}{"changes": changes, "payload": tx.Payload})

					flag = true
				}

				if contractAddress != APT_CODE && contractAddress != "" {
					tokenInfo, err = biz.GetTokenInfo(nil, h.chainName, contractAddress)
					for i := 0; i < 3 && err != nil; i++ {
						time.Sleep(time.Duration(i*1) * time.Second)
						tokenInfo, err = biz.GetTokenInfo(nil, h.chainName, contractAddress)
					}
					if err != nil {
						// nodeProxy出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
					}
					tokenInfo.Amount = amount
				}
				aptosMap := map[string]interface{}{
					"aptos": map[string]string{
						"sequence_number": tx.SequenceNumber,
					},
					"token": tokenInfo,
				}
				parseData, _ := utils.JsonEncode(aptosMap)
				amountValue, _ := decimal.NewFromString(amount)
				eventLogInfo := types.EventLog{
					From:   fromAddress,
					To:     toAddress,
					Amount: amountValue.BigInt(),
					Token:  tokenInfo,
				}

				var isContinue bool
				for i, eventLog := range eventLogs {
					if eventLog.From == eventLogInfo.To && eventLog.To == eventLogInfo.From && eventLog.Token.Address == eventLogInfo.Token.Address {
						cmp := eventLog.Amount.Cmp(eventLogInfo.Amount)
						if cmp == 1 {
							isContinue = true
							eventLog.Amount = new(big.Int).Sub(eventLog.Amount, eventLogInfo.Amount)
							aptTransactionRecords[i].Amount = decimal.NewFromBigInt(eventLog.Amount, 0)
						} else if cmp == 0 {
							isContinue = true
							newEventLogs := make([]types.EventLog, 0, len(eventLogs)-1)
							newEventLogs = append(newEventLogs, eventLogs[:i]...)
							newEventLogs = append(newEventLogs, eventLogs[i+1:]...)
							eventLogs = newEventLogs
							aptTransactionRecords[i] = nil
						} else if cmp == -1 {
							eventLogInfo.Amount = new(big.Int).Sub(eventLogInfo.Amount, eventLog.Amount)
							newEventLogs := make([]types.EventLog, 0, len(eventLogs))
							newEventLogs = append(newEventLogs, eventLogs[:i]...)
							newEventLogs = append(newEventLogs, eventLogs[i+1:]...)
							eventLogs = newEventLogs
							aptTransactionRecords[i] = nil
						}
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
		}

		for _, aptTransactionRecord := range aptTransactionRecords {
			if aptTransactionRecord != nil {
				h.txRecords = append(h.txRecords, aptTransactionRecord)
			}
		}
		if aptContractRecord != nil && eventLogs != nil {
			eventLog, _ := utils.JsonEncode(eventLogs)
			aptContractRecord.EventLog = eventLog
		}
	}
	return nil
}

func (h *txHandler) Save(c chain.Clienter) error {
	txRecords := h.txRecords
	if txRecords != nil && len(txRecords) > 0 {
		//保存交易数据
		err := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(h.chainName))
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
	}
	return nil
}

func (h *txHandler) OnSealedTx(c chain.Clienter, txByHash *chain.Transaction) (err error) {
	client := c.(*Client)
	transactionInfo := txByHash.Raw.(TransactionInfo)
	record := txByHash.Record.(*data.AptTransactionRecord)

	transactionVersion, _ := strconv.Atoi(transactionInfo.Version)
	if transactionInfo.ErrorCode == "transaction_not_found" {
		status := biz.CANCEL
		record.Status = status
		record.UpdatedAt = h.now
		h.txRecords = append(h.txRecords, record)
		return nil
	} else if transactionInfo.Type == "pending_transaction" {
		return nil
	}

	block, err := client.GetBlockByVersion(transactionVersion)
	if err != nil {
		log.Error(h.chainName+"扫块，从链上获取区块信息失败", zap.Any("transactionVersion", transactionVersion) /*, zap.Any("new", height)*/, zap.Any("error", err))
		return err
	}
	curHeight, _ := strconv.Atoi(block.BlockHeight)

	for _, tx := range block.Transactions {
		if tx.Hash != record.TransactionHash {
			continue
		}

		blockType := tx.Type
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

		if status == "" || tx.Payload.Type == "module_bundle_payload" {
			continue
		}

		if tx.Payload.Function == APT_CREATE_ACCOUNT || tx.Payload.Function == APT_REGISTER {
			txType := ""
			var tokenInfo types.TokenInfo
			//var amount, contractAddress string
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool
			fromAddress = tx.Sender

			if tx.Payload.Function == APT_CREATE_ACCOUNT {
				txType = biz.CREATEACCOUNT
				if toAddressStr, ok := tx.Payload.Arguments[0].(string); ok {
					toAddress = toAddressStr
				}
			}
			if tx.Payload.Function == APT_REGISTER {
				txType = biz.REGISTERTOKEN
				if len(tx.Payload.TypeArguments) > 0 {
					tyArgs := tx.Payload.TypeArguments[0]
					mode := strings.Split(tyArgs, "::")
					if len(mode) == 3 {
						toAddress = mode[0]
					}
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
				version, _ := strconv.Atoi(tx.Version)
				nonce, _ := strconv.Atoi(tx.SequenceNumber)
				txTime, _ := strconv.ParseInt(tx.Timestamp, 10, 64)
				txTime = txTime / 1000000
				gasUsed, _ := strconv.Atoi(tx.GasUsed)
				gasPrice, _ := strconv.Atoi(tx.GasUnitPrice)
				feeAmount := decimal.NewFromInt(int64(gasUsed * gasPrice))
				payload, _ := utils.JsonEncode(tx.Payload)

				aptosMap := map[string]interface{}{
					"aptos": map[string]string{
						"sequence_number": tx.SequenceNumber,
					},
					"token": tokenInfo,
				}
				parseData, _ := utils.JsonEncode(aptosMap)

				aptTransactionRecord := &data.AptTransactionRecord{
					BlockHash:           block.BlockHash,
					BlockNumber:         curHeight,
					Nonce:               int64(nonce),
					TransactionVersion:  version,
					TransactionHash:     tx.Hash,
					FromAddress:         fromAddress,
					ToAddress:           toAddress,
					FromUid:             fromUid,
					ToUid:               toUid,
					FeeAmount:           feeAmount,
					Amount:              decimal.Zero,
					Status:              status,
					TxTime:              txTime,
					ContractAddress:     "",
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
			}
		} else if tx.Payload.Function == APT_ACCOUNT_TRANSFER || tx.Payload.Function == APT_TRANSFER || tx.Payload.Function == APT_MINT {
			txType := biz.NATIVE
			var tokenInfo types.TokenInfo
			var amount, contractAddress string
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool

			fromAddress = tx.Sender
			if toAddressStr, ok := tx.Payload.Arguments[0].(string); ok {
				toAddress = toAddressStr
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

			if fromAddressExist || toAddressExist {
				version, _ := strconv.Atoi(tx.Version)
				nonce, _ := strconv.Atoi(tx.SequenceNumber)
				txTime, _ := strconv.ParseInt(tx.Timestamp, 10, 64)
				txTime = txTime / 1000000
				gasUsed, _ := strconv.Atoi(tx.GasUsed)
				gasPrice, _ := strconv.Atoi(tx.GasUnitPrice)
				feeAmount := decimal.NewFromInt(int64(gasUsed * gasPrice))
				payload, _ := utils.JsonEncode(map[string]interface{}{"changes": changes, "payload": tx.Payload})

				if txType == biz.TRANSFER {
					tokenInfo, err = biz.GetTokenInfo(nil, h.chainName, contractAddress)
					for i := 0; i < 3 && err != nil; i++ {
						time.Sleep(time.Duration(i*1) * time.Second)
						tokenInfo, err = biz.GetTokenInfo(nil, h.chainName, contractAddress)
					}
					if err != nil {
						// nodeProxy出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
					}
					tokenInfo.Amount = amount
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
					BlockNumber:         curHeight,
					Nonce:               int64(nonce),
					TransactionVersion:  version,
					TransactionHash:     tx.Hash,
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
			}
		} else {
			flag := false
			var version int
			var nonce int
			var txTime int64
			var gasUsed int
			var gasPrice int
			var feeAmount decimal.Decimal
			var payload string
			var eventLogs []types.EventLog
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

			if fromAddressExist || toAddressExist {
				version, _ = strconv.Atoi(tx.Version)
				nonce, _ = strconv.Atoi(tx.SequenceNumber)
				txTime, _ = strconv.ParseInt(tx.Timestamp, 10, 64)
				txTime = txTime / 1000000
				gasUsed, _ = strconv.Atoi(tx.GasUsed)
				gasPrice, _ = strconv.Atoi(tx.GasUnitPrice)
				feeAmount = decimal.NewFromInt(int64(gasUsed * gasPrice))
				payload, _ = utils.JsonEncode(map[string]interface{}{"changes": changes, "payload": tx.Payload})

				flag = true

				if contractAddress != APT_CODE && contractAddress != "" {
					tokenInfo, err = biz.GetTokenInfo(nil, h.chainName, contractAddress)
					for i := 0; i < 3 && err != nil; i++ {
						time.Sleep(time.Duration(i*1) * time.Second)
						tokenInfo, err = biz.GetTokenInfo(nil, h.chainName, contractAddress)
					}
					if err != nil {
						// nodeProxy出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
					}
					tokenInfo.Amount = amount
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
					BlockNumber:         curHeight,
					Nonce:               int64(nonce),
					TransactionVersion:  version,
					TransactionHash:     tx.Hash,
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
				h.txRecords = append(h.txRecords, aptContractRecord)
			}

			txType = biz.EVENTLOG
			index := 0

			for _, event := range tx.Events {
				var tokenInfo types.TokenInfo
				var amount, contractAddress string
				var fromAddress, toAddress, fromUid, toUid string
				var fromAddressExist, toAddressExist bool

				if event.Type == "0x1::coin::WithdrawEvent" ||
					event.Type == "0x1::coin::DepositEvent" {
					amount = event.Data.Amount
					if amount == "0" {
						continue
					}
					mode := strings.Split(tx.Payload.Function, "::")
					if event.Type == "0x1::coin::WithdrawEvent" {
						//fromAddress = "0x" + event.Key[18:]
						fromAddress = event.Guid.AccountAddress
						if len(mode) == 3 {
							toAddress = mode[0]
						}
					} else if event.Type == "0x1::coin::DepositEvent" {
						//toAddress = "0x" + event.Key[18:]
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
				} else {
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
					txHash := tx.Hash + "#result-" + fmt.Sprintf("%v", index)

					if !flag {
						version, _ = strconv.Atoi(tx.Version)
						nonce, _ = strconv.Atoi(tx.SequenceNumber)
						txTime, _ = strconv.ParseInt(tx.Timestamp, 10, 64)
						txTime = txTime / 1000000
						gasUsed, _ = strconv.Atoi(tx.GasUsed)
						gasPrice, _ = strconv.Atoi(tx.GasUnitPrice)
						feeAmount = decimal.NewFromInt(int64(gasUsed * gasPrice))
						payload, _ = utils.JsonEncode(map[string]interface{}{"changes": changes, "payload": tx.Payload})

						flag = true
					}

					if contractAddress != APT_CODE && contractAddress != "" {
						tokenInfo, err = biz.GetTokenInfo(nil, h.chainName, contractAddress)
						for i := 0; i < 3 && err != nil; i++ {
							time.Sleep(time.Duration(i*1) * time.Second)
							tokenInfo, err = biz.GetTokenInfo(nil, h.chainName, contractAddress)
						}
						if err != nil {
							// nodeProxy出错 接入lark报警
							alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
							log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
						}
						tokenInfo.Amount = amount
					}
					aptosMap := map[string]interface{}{
						"aptos": map[string]string{
							"sequence_number": tx.SequenceNumber,
						},
						"token": tokenInfo,
					}
					parseData, _ := utils.JsonEncode(aptosMap)
					amountValue, _ := decimal.NewFromString(amount)
					eventLogInfo := types.EventLog{
						From:   fromAddress,
						To:     toAddress,
						Amount: amountValue.BigInt(),
						Token:  tokenInfo,
					}

					var isContinue bool
					for i, eventLog := range eventLogs {
						if eventLog.From == eventLogInfo.To && eventLog.To == eventLogInfo.From && eventLog.Token.Address == eventLogInfo.Token.Address {
							cmp := eventLog.Amount.Cmp(eventLogInfo.Amount)
							if cmp == 1 {
								isContinue = true
								eventLog.Amount = new(big.Int).Sub(eventLog.Amount, eventLogInfo.Amount)
								aptTransactionRecords[i].Amount = decimal.NewFromBigInt(eventLog.Amount, 0)
							} else if cmp == 0 {
								isContinue = true
								newEventLogs := make([]types.EventLog, 0, len(eventLogs)-1)
								newEventLogs = append(newEventLogs, eventLogs[:i]...)
								newEventLogs = append(newEventLogs, eventLogs[i+1:]...)
								eventLogs = newEventLogs
								aptTransactionRecords[i] = nil
							} else if cmp == -1 {
								eventLogInfo.Amount = new(big.Int).Sub(eventLogInfo.Amount, eventLog.Amount)
								newEventLogs := make([]types.EventLog, 0, len(eventLogs))
								newEventLogs = append(newEventLogs, eventLogs[:i]...)
								newEventLogs = append(newEventLogs, eventLogs[i+1:]...)
								eventLogs = newEventLogs
								aptTransactionRecords[i] = nil
							}
							break
						}
					}
					if isContinue {
						continue
					}
					eventLogs = append(eventLogs, eventLogInfo)

					aptTransactionRecord := &data.AptTransactionRecord{
						BlockHash:           block.BlockHash,
						BlockNumber:         curHeight,
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
			}

			for _, aptTransactionRecord := range aptTransactionRecords {
				if aptTransactionRecord != nil {
					h.txRecords = append(h.txRecords, aptTransactionRecord)
				}
			}
			if aptContractRecord != nil && eventLogs != nil {
				eventLog, _ := utils.JsonEncode(eventLogs)
				aptContractRecord.EventLog = eventLog
			}
		}
	}
	return nil
}

func (h *txHandler) OnDroppedTx(c chain.Clienter, tx *chain.Transaction) error {
	return nil
}
