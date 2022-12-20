package starcoin

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"encoding/hex"
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

	txRecords []*data.StcTransactionRecord
}

func (h *txHandler) OnNewTx(c chain.Clienter, chainBlock *chain.Block, chainTx *chain.Transaction) (err error) {
	client := c.(*Client)
	block := chainBlock.Raw.(*types.Block)
	userTransaction := chainTx.Raw.(types.UserTransaction)
	scriptFunction := userTransaction.RawTransaction.DecodedPayload.ScriptFunction
	if strings.HasPrefix(scriptFunction.Function, "peer_to_peer") {
		txType := biz.NATIVE
		var tokenInfo types.TokenInfo
		var amount, contractAddress string
		var fromAddress, toAddress, fromUid, toUid string
		var fromAddressExist, toAddressExist bool

		fromAddress = userTransaction.RawTransaction.Sender
		if toAddressStr, ok := scriptFunction.Args[0].(string); ok {
			toAddress = toAddressStr
		}
		argsLen := len(scriptFunction.Args)
		if value, ok := scriptFunction.Args[argsLen-1].(float64); ok {
			amount = fmt.Sprintf("%.f", value)
		} else {
			amount = fmt.Sprintf("%v", scriptFunction.Args[argsLen-1])
		}
		if len(scriptFunction.TyArgs) > 0 {
			tyArgs := scriptFunction.TyArgs[0]
			if tyArgs != STC_CODE {
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
			transactionInfo, err := client.GetTransactionInfoByHash(userTransaction.TransactionHash)
			for i := 0; i < 10 && err != nil; i++ {
				time.Sleep(time.Duration(i*5) * time.Second)
				transactionInfo, err = client.GetTransactionInfoByHash(userTransaction.TransactionHash)
			}
			if err != nil {
				return err
			}

			txTime, _ := strconv.ParseInt(block.BlockHeader.TimeStamp, 10, 64)
			txTime = txTime / 1000
			gasUsed, _ := strconv.Atoi(transactionInfo.GasUsed)
			gasPrice, _ := strconv.Atoi(userTransaction.RawTransaction.GasUnitPrice)
			feeAmount := decimal.NewFromInt(int64(gasUsed * gasPrice))
			payload, _ := utils.JsonEncode(userTransaction.RawTransaction.DecodedPayload)

			var status string
			if string(transactionInfo.Status) == "Executed" || string(transactionInfo.Status) == "\"Executed\"" {
				status = biz.SUCCESS
			} else {
				status = biz.FAIL
			}

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
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
				}
				tokenInfo.Amount = amount
			}
			stcMap := map[string]interface{}{
				"stc": map[string]string{
					"sequence_number": userTransaction.RawTransaction.SequenceNumber,
				},
				"token": tokenInfo,
			}
			parseData, _ := utils.JsonEncode(stcMap)
			amountValue, _ := decimal.NewFromString(amount)
			nonce, _ := strconv.Atoi(userTransaction.RawTransaction.SequenceNumber)

			stcRecord := &data.StcTransactionRecord{
				BlockHash:       block.BlockHeader.BlockHash,
				BlockNumber:     int(h.curHeight),
				Nonce:           int64(nonce),
				TransactionHash: userTransaction.TransactionHash,
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
				GasLimit:        userTransaction.RawTransaction.MaxGasAmount,
				GasUsed:         block.BlockHeader.GasUsed,
				GasPrice:        userTransaction.RawTransaction.GasUnitPrice,
				Data:            payload,
				EventLog:        "",
				TransactionType: txType,
				DappData:        "",
				ClientData:      "",
				CreatedAt:       h.now,
				UpdatedAt:       h.now,
			}
			h.txRecords = append(h.txRecords, stcRecord)
		}
	} else {
		flag := false
		var txTime int64
		var gasUsed int
		var gasPrice int
		var feeAmount decimal.Decimal
		var payload string
		var status string
		var eventLogs []types.EventLog
		var stcContractRecord *data.StcTransactionRecord

		events, err := client.GetTransactionEventByHash(userTransaction.TransactionHash)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			events, err = client.GetTransactionEventByHash(userTransaction.TransactionHash)
		}
		if err != nil {
			log.Error(h.chainName+"扫块，从链上获取区块event失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
			return err
		}

		txType := biz.CONTRACT
		var tokenInfo types.TokenInfo
		var amount, contractAddress string
		var fromAddress, toAddress, fromUid, toUid string
		var fromAddressExist, toAddressExist bool

		fromAddress = userTransaction.RawTransaction.Sender
		mode := strings.Split(scriptFunction.Module, "::")
		if len(mode) == 2 {
			toAddress = mode[0]
		}

		if fromAddress != "" {
			fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
			if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
				// redis出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", h.chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
				return err
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
				return err
			}
		}

		if fromAddressExist || toAddressExist {
			transactionInfo, err := client.GetTransactionInfoByHash(userTransaction.TransactionHash)
			for i := 0; i < 10 && err != nil; i++ {
				time.Sleep(time.Duration(i*5) * time.Second)
				transactionInfo, err = client.GetTransactionInfoByHash(userTransaction.TransactionHash)
			}
			if err != nil {
				return err
			}

			txTime, _ = strconv.ParseInt(block.BlockHeader.TimeStamp, 10, 64)
			txTime = txTime / 1000
			gasUsed, _ = strconv.Atoi(transactionInfo.GasUsed)
			gasPrice, _ = strconv.Atoi(userTransaction.RawTransaction.GasUnitPrice)
			feeAmount = decimal.NewFromInt(int64(gasUsed * gasPrice))
			payload, _ = utils.JsonEncode(userTransaction.RawTransaction.DecodedPayload)

			if string(transactionInfo.Status) == "Executed" || string(transactionInfo.Status) == "\"Executed\"" {
				status = biz.SUCCESS
			} else {
				status = biz.FAIL
			}

			flag = true

			if contractAddress != STC_CODE && contractAddress != "" {
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
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
				}
				tokenInfo.Amount = amount
			}
			stcMap := map[string]interface{}{
				"stc": map[string]string{
					"sequence_number": userTransaction.RawTransaction.SequenceNumber,
				},
				"token": tokenInfo,
			}
			parseData, _ := utils.JsonEncode(stcMap)
			amountValue, _ := decimal.NewFromString(amount)
			nonce, _ := strconv.Atoi(userTransaction.RawTransaction.SequenceNumber)

			stcContractRecord = &data.StcTransactionRecord{
				BlockHash:       block.BlockHeader.BlockHash,
				BlockNumber:     int(h.curHeight),
				Nonce:           int64(nonce),
				TransactionHash: userTransaction.TransactionHash,
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
				GasLimit:        userTransaction.RawTransaction.MaxGasAmount,
				GasUsed:         block.BlockHeader.GasUsed,
				GasPrice:        userTransaction.RawTransaction.GasUnitPrice,
				Data:            payload,
				EventLog:        "",
				TransactionType: txType,
				DappData:        "",
				ClientData:      "",
				CreatedAt:       h.now,
				UpdatedAt:       h.now,
			}
			h.txRecords = append(h.txRecords, stcContractRecord)
		}

		txType = biz.EVENTLOG
		index := 0

		for _, event := range events {
			var tokenInfo types.TokenInfo
			var amount *big.Int
			var contractAddress string
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool

			if event.TypeTag == "0x00000000000000000000000000000001::Account::WithdrawEvent" ||
				event.TypeTag == "0x00000000000000000000000000000001::Account::DepositEvent" {
				addrHexString := event.DecodeEventData.TokenCode.Addr
				moduleNameHexString := event.DecodeEventData.TokenCode.ModuleName[2:]
				nameHexString := event.DecodeEventData.TokenCode.Name[2:]
				moduleNameHexBytes, _ := hex.DecodeString(moduleNameHexString)
				nameHexBytes, _ := hex.DecodeString(nameHexString)
				contractAddress = addrHexString + "::" + string(moduleNameHexBytes) + "::" + string(nameHexBytes)

				amount = event.DecodeEventData.Amount
				mode := strings.Split(scriptFunction.Module, "::")
				if event.TypeTag == "0x00000000000000000000000000000001::Account::WithdrawEvent" {
					fromAddress = "0x" + event.EventKey[18:]
					if len(mode) == 2 {
						toAddress = mode[0]
					}
				} else if event.TypeTag == "0x00000000000000000000000000000001::Account::DepositEvent" {
					toAddress = "0x" + event.EventKey[18:]
					if len(mode) == 2 {
						fromAddress = mode[0]
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
					log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
					return err
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
					return err
				}
			}

			if fromAddressExist || toAddressExist {
				index++
				txHash := userTransaction.TransactionHash + "#result-" + fmt.Sprintf("%v", index)

				if !flag {
					transactionInfo, err := client.GetTransactionInfoByHash(userTransaction.TransactionHash)
					for i := 0; i < 10 && err != nil; i++ {
						time.Sleep(time.Duration(i*5) * time.Second)
						transactionInfo, err = client.GetTransactionInfoByHash(userTransaction.TransactionHash)
					}
					if err != nil {
						return err
					}

					txTime, _ = strconv.ParseInt(block.BlockHeader.TimeStamp, 10, 64)
					txTime = txTime / 1000
					gasUsed, _ = strconv.Atoi(transactionInfo.GasUsed)
					gasPrice, _ = strconv.Atoi(userTransaction.RawTransaction.GasUnitPrice)
					feeAmount = decimal.NewFromInt(int64(gasUsed * gasPrice))
					payload, _ = utils.JsonEncode(userTransaction.RawTransaction.DecodedPayload)

					if string(transactionInfo.Status) == "Executed" || string(transactionInfo.Status) == "\"Executed\"" {
						status = biz.SUCCESS
					} else {
						status = biz.FAIL
					}

					flag = true
				}

				if contractAddress != STC_CODE && contractAddress != "" {
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
						log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
					}
					var amountStr string
					if amount != nil {
						amountStr = amount.String()
					}
					tokenInfo.Amount = amountStr
				}
				stcMap := map[string]interface{}{
					"stc": map[string]string{
						"sequence_number": userTransaction.RawTransaction.SequenceNumber,
					},
					"token": tokenInfo,
				}
				parseData, _ := utils.JsonEncode(stcMap)
				amountValue := decimal.NewFromBigInt(amount, 0)
				eventLogInfo := types.EventLog{
					From:   fromAddress,
					To:     toAddress,
					Amount: amount,
					Token:  tokenInfo,
				}
				eventLogs = append(eventLogs, eventLogInfo)
				eventLog, _ := utils.JsonEncode(eventLogInfo)
				nonce, _ := strconv.Atoi(userTransaction.RawTransaction.SequenceNumber)
				stcRecord := &data.StcTransactionRecord{
					BlockHash:       block.BlockHeader.BlockHash,
					BlockNumber:     int(h.curHeight),
					Nonce:           int64(nonce),
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
					GasLimit:        userTransaction.RawTransaction.MaxGasAmount,
					GasUsed:         block.BlockHeader.GasUsed,
					GasPrice:        userTransaction.RawTransaction.GasUnitPrice,
					Data:            payload,
					EventLog:        eventLog,
					TransactionType: txType,
					DappData:        "",
					ClientData:      "",
					CreatedAt:       h.now,
					UpdatedAt:       h.now,
				}
				h.txRecords = append(h.txRecords, stcRecord)
			}
		}

		if stcContractRecord != nil && eventLogs != nil {
			eventLog, _ := utils.JsonEncode(eventLogs)
			stcContractRecord.EventLog = eventLog
		}
	}
	return nil
}

func (h *txHandler) OnSealedTx(c chain.Clienter, tx *chain.Transaction) (err error) {
	client := c.(*Client)
	transactionInfo := tx.Raw.(types.UserTransaction)
	record := tx.Record.(*data.StcTransactionRecord)

	curHeight := tx.BlockNumber
	if transactionInfo.TransactionHash == "" {
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

	block, err := client.GetBlock(curHeight)
	if err != nil {
		log.Error(h.chainName+"扫块，从链上获取区块信息失败", zap.Any("curHeight", curHeight), zap.Any("error", err))
		return err
	}

	err = h.OnNewTx(c, block, tx)

	return err
}

func (h *txHandler) OnDroppedTx(c chain.Clienter, tx *chain.Transaction) error {
	return nil
}

func (h *txHandler) Save(c chain.Clienter) (err error) {
	txRecords := h.txRecords
	client := c.(*Client)

	if txRecords != nil && len(txRecords) > 0 {
		e := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(h.chainName))
		if e != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库库中失败", h.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(h.chainName+"扫块，将数据插入到数据库中失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
			return
		}
		if h.newTxs {
			go HandleRecord(h.chainName, *client, txRecords)
		} else {
			go HandlePendingRecord(h.chainName, *client, txRecords)
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
	return
}
