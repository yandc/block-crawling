package cosmos

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

	txRecords      []*data.AtomTransactionRecord
	txHashIndexMap map[string]int
}

func (h *txHandler) OnNewTx(c chain.Clienter, chainBlock *chain.Block, chainTx *chain.Transaction) (err error) {
	//block := chainBlock.Raw.(BlockerInfo)
	curHeight := chainBlock.Number
	height := h.chainHeight

	tx := chainTx.Raw.(TransactionInfo)

	code := tx.TxResponse.Code
	transactionHash := chainTx.Hash
	var status string
	if code == 0 {
		status = biz.SUCCESS
	} else {
		status = biz.FAIL
	}

	if status == "" || tx.TxResponse.Tx.Type != "/cosmos.tx.v1beta1.Tx" {
		return nil
	}
	messages := tx.TxResponse.Tx.Body.Messages

	var isContract bool
	for _, messageStr := range messages {
		message := messageStr.(map[string]interface{})
		messageType := message["@type"].(string)
		if messageType != "/cosmos.authz.v1beta1.MsgGrant" && messageType != "/cosmos.bank.v1beta1.MsgSend" &&
			messageType != "/ibc.applications.transfer.v1.MsgTransfer" {
			isContract = true
			break
		}
	}

	if !isContract {
		var index int
		for _, messageStr := range messages {
			message := messageStr.(map[string]interface{})
			messageType := message["@type"].(string)

			var msgSendAmountSize int
			if messageType == "/cosmos.bank.v1beta1.MsgSend" {
				amountList := message["amount"].([]interface{})
				msgSendAmountSize = len(amountList)
				if msgSendAmountSize > 1 {
					tokenDenomAmountMap := make(map[string]string)
					for _, amountInterface := range amountList {
						messageAmount := amountInterface.(map[string]interface{})
						tokenDenom := messageAmount["denom"].(string)
						tokenAmount := messageAmount["amount"].(string)

						oldTokenAmount, ok := tokenDenomAmountMap[tokenDenom]
						if !ok {
							tokenDenomAmountMap[tokenDenom] = tokenAmount
						} else {
							oldTokenAmountBig, oldOk := new(big.Int).SetString(oldTokenAmount, 0)
							tokenAmountBig, newOk := new(big.Int).SetString(tokenAmount, 0)
							if oldOk && newOk {
								tokenDenomAmountMap[tokenDenom] = new(big.Int).Add(oldTokenAmountBig, tokenAmountBig).String()
							} else if newOk {
								tokenDenomAmountMap[tokenDenom] = tokenAmount
							}
						}
					}

					tokenDenomAmountSize := len(tokenDenomAmountMap)
					if msgSendAmountSize != tokenDenomAmountSize {
						msgSendAmountSize = tokenDenomAmountSize
						message["amount"] = tokenDenomAmountMap
					}
				}
			}

			if messageType == "/cosmos.authz.v1beta1.MsgGrant" || (messageType == "/cosmos.bank.v1beta1.MsgSend" && msgSendAmountSize <= 1) ||
				messageType == "/ibc.applications.transfer.v1.MsgTransfer" {
				txType := ""
				var tokenInfo types.TokenInfo
				var amount, contractAddress string
				var fromAddress, toAddress, fromUid, toUid string
				var fromAddressExist, toAddressExist bool

				if messageType == "/cosmos.authz.v1beta1.MsgGrant" {
					txType = biz.APPROVE
					fromAddress = message["granter"].(string)
					toAddress = message["grantee"].(string)
					messageAmount := message["grant"].(map[string]interface{})
					if len(messageAmount) > 0 {
						expiration := messageAmount["expiration"].(string)
						local, _ := time.LoadLocation("Asia/Shanghai")
						showTime, _ := time.ParseInLocation("2006-01-02T15:04:05Z", expiration, local)
						amount = strconv.Itoa(int(showTime.Unix()))
					}
				} else if (messageType == "/cosmos.bank.v1beta1.MsgSend" && msgSendAmountSize <= 1) || messageType == "/ibc.applications.transfer.v1.MsgTransfer" {
					txType = biz.NATIVE
					var messageAmount map[string]interface{}
					if messageType == "/cosmos.bank.v1beta1.MsgSend" {
						fromAddress = message["from_address"].(string)
						toAddress = message["to_address"].(string)
						amountList := message["amount"].([]interface{})
						if len(amountList) > 0 {
							messageAmount = amountList[0].(map[string]interface{})
						}
					} else {
						fromAddress = message["sender"].(string)
						toAddress = message["receiver"].(string)
						messageAmount = message["token"].(map[string]interface{})
					}
					if len(messageAmount) > 0 {
						tokenDenom := messageAmount["denom"].(string)
						amount = messageAmount["amount"].(string)
						var denom string
						if platInfo, ok := biz.PlatInfoMap[h.chainName]; ok {
							denom = strings.ToLower(platInfo.NativeCurrency)
						}
						subTokenDenom := tokenDenom[1:]
						if subTokenDenom != denom {
							txType = biz.TRANSFER
							contractAddress = tokenDenom
						}
					}
				}

				if fromAddress != "" {
					fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
					if err != nil {
						log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
						return
					}
				}

				if toAddress != "" {
					toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, toAddress)
					if err != nil {
						log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
						return
					}
				}
				if !fromAddressExist && !toAddressExist {
					continue
				}

				index++
				var txHash string
				if index == 1 {
					txHash = transactionHash
				} else {
					txHash = transactionHash + "#result-" + fmt.Sprintf("%v", index)
				}
				nonce := chainTx.Nonce
				txTime := tx.TxResponse.Timestamp.Unix()
				var feeAmount int
				if len(tx.TxResponse.Tx.AuthInfo.Fee.Amount) > 0 {
					feeAmount, _ = strconv.Atoi(tx.TxResponse.Tx.AuthInfo.Fee.Amount[0].Amount)
				}
				gasUsed, _ := strconv.Atoi(tx.TxResponse.GasUsed)
				gasPrice := strconv.Itoa(feeAmount / gasUsed)
				payload, _ := utils.JsonEncode(map[string]interface{}{"memo": tx.TxResponse.Tx.Body.Memo})

				if txType == biz.TRANSFER {
					tokenInfo, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, contractAddress)
					log.Info("调用token service", zap.Any("response", tokenInfo), zap.Any(h.chainName, contractAddress), zap.Error(err))
					if err != nil {
						log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
					}
					tokenInfo.Amount = amount
					tokenInfo.Address = contractAddress
					if tokenInfo.Symbol == "Unknown Token" {
						tokenInfo.Symbol = contractAddress
					}
				}
				atomosMap := map[string]interface{}{
					"cosmos": map[string]string{
						"sequence_number": strconv.Itoa(int(nonce)),
					},
					"token": tokenInfo,
				}
				parseData, _ := utils.JsonEncode(atomosMap)
				amountValue, _ := decimal.NewFromString(amount)

				atomTransactionRecord := &data.AtomTransactionRecord{
					BlockHash:       chainBlock.Hash,
					BlockNumber:     int(curHeight),
					Nonce:           int64(nonce),
					TransactionHash: txHash,
					FromAddress:     fromAddress,
					ToAddress:       toAddress,
					FromUid:         fromUid,
					ToUid:           toUid,
					FeeAmount:       decimal.NewFromInt(int64(feeAmount)),
					Amount:          amountValue,
					Status:          status,
					TxTime:          txTime,
					ContractAddress: contractAddress,
					ParseData:       parseData,
					GasLimit:        tx.TxResponse.GasWanted,
					GasUsed:         tx.TxResponse.GasUsed,
					GasPrice:        gasPrice,
					Data:            payload,
					EventLog:        "",
					TransactionType: txType,
					DappData:        "",
					ClientData:      "",
					CreatedAt:       h.now,
					UpdatedAt:       h.now,
				}
				h.txRecords = append(h.txRecords, atomTransactionRecord)
			} else if messageType == "/cosmos.bank.v1beta1.MsgSend" {
				var fromAddress, toAddress, fromUid, toUid string
				var fromAddressExist, toAddressExist bool

				fromAddress = message["from_address"].(string)
				toAddress = message["to_address"].(string)

				if fromAddress != "" {
					fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
					if err != nil {
						log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
						return
					}
				}

				if toAddress != "" {
					toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, toAddress)
					if err != nil {
						log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
						return
					}
				}
				if !fromAddressExist && !toAddressExist {
					continue
				}

				nonce := chainTx.Nonce
				txTime := tx.TxResponse.Timestamp.Unix()
				var feeAmount int
				if len(tx.TxResponse.Tx.AuthInfo.Fee.Amount) > 0 {
					feeAmount, _ = strconv.Atoi(tx.TxResponse.Tx.AuthInfo.Fee.Amount[0].Amount)
				}
				gasUsed, _ := strconv.Atoi(tx.TxResponse.GasUsed)
				gasPrice := strconv.Itoa(feeAmount / gasUsed)
				payload, _ := utils.JsonEncode(map[string]interface{}{"memo": tx.TxResponse.Tx.Body.Memo})

				amountList := message["amount"].([]interface{})
				for _, amountInterface := range amountList {
					txType := biz.NATIVE
					var tokenInfo types.TokenInfo
					var amount, contractAddress string

					messageAmount := amountInterface.(map[string]interface{})
					tokenDenom := messageAmount["denom"].(string)
					amount = messageAmount["amount"].(string)
					var denom string
					if platInfo, ok := biz.PlatInfoMap[h.chainName]; ok {
						denom = strings.ToLower(platInfo.NativeCurrency)
					}
					subTokenDenom := tokenDenom[1:]
					if subTokenDenom != denom {
						txType = biz.TRANSFER
						contractAddress = tokenDenom
					}

					index++
					var txHash string
					if index == 1 {
						txHash = transactionHash
					} else {
						txHash = transactionHash + "#result-" + fmt.Sprintf("%v", index)
					}

					if txType == biz.TRANSFER {
						tokenInfo, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, contractAddress)
						if err != nil {
							log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
						}
						tokenInfo.Amount = amount
						tokenInfo.Address = contractAddress
						if tokenInfo.Symbol == "Unknown Token" {
							tokenInfo.Symbol = contractAddress
						}
					}
					atomosMap := map[string]interface{}{
						"cosmos": map[string]string{
							"sequence_number": strconv.Itoa(int(nonce)),
						},
						"token": tokenInfo,
					}
					parseData, _ := utils.JsonEncode(atomosMap)
					amountValue, _ := decimal.NewFromString(amount)

					atomTransactionRecord := &data.AtomTransactionRecord{
						BlockHash:       chainBlock.Hash,
						BlockNumber:     int(curHeight),
						Nonce:           int64(nonce),
						TransactionHash: txHash,
						FromAddress:     fromAddress,
						ToAddress:       toAddress,
						FromUid:         fromUid,
						ToUid:           toUid,
						FeeAmount:       decimal.NewFromInt(int64(feeAmount)),
						Amount:          amountValue,
						Status:          status,
						TxTime:          txTime,
						ContractAddress: contractAddress,
						ParseData:       parseData,
						GasLimit:        tx.TxResponse.GasWanted,
						GasUsed:         tx.TxResponse.GasUsed,
						GasPrice:        gasPrice,
						Data:            payload,
						EventLog:        "",
						TransactionType: txType,
						DappData:        "",
						ClientData:      "",
						CreatedAt:       h.now,
						UpdatedAt:       h.now,
					}
					h.txRecords = append(h.txRecords, atomTransactionRecord)
				}
			}
		}
	} else {
		var nonce uint64
		var txTime int64
		var feeAmount decimal.Decimal
		var payload string
		var eventLogs []*types.EventLog
		var atomTransactionRecords []*data.AtomTransactionRecord
		var atomContractRecord *data.AtomTransactionRecord

		txType := biz.CONTRACT
		var tokenInfo types.TokenInfo
		var amount, contractAddress string
		var fromAddress, toAddress, fromUid, toUid string
		var fromAddressExist, toAddressExist bool

		for _, messageStr := range messages {
			message := messageStr.(map[string]interface{})
			messageType := message["@type"].(string)

			if messageType == "/osmosis.gamm.v1beta1.MsgSwapExactAmountIn" || messageType == "/osmosis.gamm.v1beta1.MsgJoinSwapExternAmountIn" {
				if messageAmount, ok := message["token_in"].(map[string]interface{}); ok {
					tokenDenom := messageAmount["denom"].(string)
					tokenAmount := messageAmount["amount"].(string)
					var denom string
					if platInfo, ok := biz.PlatInfoMap[h.chainName]; ok {
						denom = strings.ToLower(platInfo.NativeCurrency)
					}
					subTokenDenom := tokenDenom[1:]
					if subTokenDenom == denom {
						if amount == "" {
							amount = tokenAmount
						} else {
							amountInt, _ := new(big.Int).SetString(amount, 0)
							tokenAmountInt, _ := new(big.Int).SetString(tokenAmount, 0)
							amount = amountInt.Add(amountInt, tokenAmountInt).String()
						}
					}
				}
			}

			if messageType == "/osmosis.gamm.v1beta1.MsgJoinPool" {
				if tokenInMaxs, ok := message["token_in_maxs"].([]interface{}); ok {
					for _, tokenInMax := range tokenInMaxs {
						if messageAmount, ok := tokenInMax.(map[string]interface{}); ok {
							tokenDenom := messageAmount["denom"].(string)
							tokenAmount := messageAmount["amount"].(string)
							var denom string
							if platInfo, ok := biz.PlatInfoMap[h.chainName]; ok {
								denom = strings.ToLower(platInfo.NativeCurrency)
							}
							subTokenDenom := tokenDenom[1:]
							if subTokenDenom == denom {
								if amount == "" {
									amount = tokenAmount
								} else {
									amountInt, _ := new(big.Int).SetString(amount, 0)
									tokenAmountInt, _ := new(big.Int).SetString(tokenAmount, 0)
									amount = amountInt.Add(amountInt, tokenAmountInt).String()
								}
							}
						}
					}
				}
			}
		}

		for _, messageStr := range messages {
			message := messageStr.(map[string]interface{})
			if sender, ok := message["sender"].(string); ok {
				if fromAddress == "" {
					fromAddress = sender
				} else if fromAddress != sender {
					fromAddress = ""
					break
				}
			}
		}

		if fromAddress != "" {
			fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
			if err != nil {
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
				return
			}
		}

		nonce = chainTx.Nonce
		txTime = tx.TxResponse.Timestamp.Unix()
		if len(tx.TxResponse.Tx.AuthInfo.Fee.Amount) > 0 {
			feeAmount, _ = decimal.NewFromString(tx.TxResponse.Tx.AuthInfo.Fee.Amount[0].Amount)
		}
		gasUsed, _ := decimal.NewFromString(tx.TxResponse.GasUsed)
		gasPrice := feeAmount.Div(gasUsed).String()
		payload, _ = utils.JsonEncode(map[string]interface{}{"memo": tx.TxResponse.Tx.Body.Memo})

		if contractAddress != "" {
			tokenInfo, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, contractAddress)
			if err != nil {
				log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
			}
			tokenInfo.Amount = amount
			tokenInfo.Address = contractAddress
			if tokenInfo.Symbol == "Unknown Token" {
				tokenInfo.Symbol = contractAddress
			}
		}
		atomosMap := map[string]interface{}{
			"cosmos": map[string]string{
				"sequence_number": strconv.Itoa(int(nonce)),
			},
			"token": tokenInfo,
		}
		parseData, _ := utils.JsonEncode(atomosMap)
		amountValue, _ := decimal.NewFromString(amount)

		atomContractRecord = &data.AtomTransactionRecord{
			BlockHash:       chainBlock.Hash,
			BlockNumber:     int(curHeight),
			Nonce:           int64(nonce),
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
			GasLimit:        tx.TxResponse.GasWanted,
			GasUsed:         tx.TxResponse.GasUsed,
			GasPrice:        gasPrice,
			Data:            payload,
			EventLog:        "",
			TransactionType: txType,
			DappData:        "",
			ClientData:      "",
			CreatedAt:       h.now,
			UpdatedAt:       h.now,
		}

		txType = biz.EVENTLOG
		index := 0

		for _, txLog := range tx.TxResponse.Logs {
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool

			for _, event := range txLog.Events {
				if event.Type == "transfer" {
					for _, attribute := range event.Attributes {
						key := attribute.Key
						value := attribute.Value
						if key == "sender" {
							fromAddress = value
						} else if key == "recipient" {
							toAddress = value
						} else if key == "amount" {
							if value == "" {
								//https://www.mintscan.io/osmosis/txs/834574CEC6C645637870D4EE5CC54C5C7523B4B03C29F21E5950367C6C3B17CF
								continue
							}
							values := strings.Split(value, ",")
							for _, tvalue := range values {
								var tokenInfo types.TokenInfo
								var amount, contractAddress string

								var tokenDenomIndex int
								for i, val := range tvalue {
									if val > 57 {
										tokenDenomIndex = i
										break
									}
								}
								amount = tvalue[:tokenDenomIndex]
								tokenDenom := tvalue[tokenDenomIndex:]
								var denom string
								if platInfo, ok := biz.PlatInfoMap[h.chainName]; ok {
									denom = strings.ToLower(platInfo.NativeCurrency)
								}
								subTokenDenom := tokenDenom[1:]
								if subTokenDenom != denom {
									contractAddress = tokenDenom
								}

								if fromAddress != "" {
									fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
									if err != nil {
										log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
										return
									}
								}

								if toAddress != "" {
									toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, toAddress)
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

								if contractAddress != "" {
									tokenInfo, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, contractAddress)
									if err != nil {
										log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("txHash", transactionHash), zap.Any("error", err))
									}
									tokenInfo.Amount = amount
									tokenInfo.Address = contractAddress
									if tokenInfo.Symbol == "Unknown Token" {
										tokenInfo.Symbol = contractAddress
									}
								}
								atomosMap := map[string]interface{}{
									"cosmos": map[string]string{
										"sequence_number": strconv.Itoa(int(nonce)),
									},
									"token": tokenInfo,
								}
								parseData, _ := utils.JsonEncode(atomosMap)
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
											atomTransactionRecords[i].Amount = decimal.NewFromBigInt(subAmount, 0)
										} else if cmp == 0 {
											isContinue = true
											eventLogs[i] = nil
											atomTransactionRecords[i] = nil
										} else if cmp == -1 {
											eventLogs[i] = nil
											atomTransactionRecords[i] = nil
										}
										break
									} else if eventLog.From == eventLogInfo.From && eventLog.To == eventLogInfo.To && eventLog.Token.Address == eventLogInfo.Token.Address &&
										eventLog.Token.TokenId == eventLogInfo.Token.TokenId {
										isContinue = true
										addAmount := new(big.Int).Add(eventLog.Amount, eventLogInfo.Amount)
										eventLogs[i].Amount = addAmount
										atomTransactionRecords[i].Amount = decimal.NewFromBigInt(addAmount, 0)
										break
									}
								}
								if isContinue {
									continue
								}
								eventLogs = append(eventLogs, eventLogInfo)

								atomTransactionRecord := &data.AtomTransactionRecord{
									BlockHash:       chainBlock.Hash,
									BlockNumber:     int(curHeight),
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
									GasLimit:        tx.TxResponse.GasWanted,
									GasUsed:         tx.TxResponse.GasUsed,
									GasPrice:        gasPrice,
									Data:            payload,
									EventLog:        "",
									TransactionType: txType,
									DappData:        "",
									ClientData:      "",
									CreatedAt:       h.now,
									UpdatedAt:       h.now,
								}
								atomTransactionRecords = append(atomTransactionRecords, atomTransactionRecord)
							}
						}
					}
				}
			}
		}

		if fromAddressExist || toAddressExist || len(eventLogs) > 0 {
			h.txRecords = append(h.txRecords, atomContractRecord)
		}
		if len(eventLogs) > 0 {
			for _, atomTransactionRecord := range atomTransactionRecords {
				if atomTransactionRecord != nil {
					h.txRecords = append(h.txRecords, atomTransactionRecord)
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
				atomContractRecord.EventLog = eventLog

				var logAddress datatypes.JSON
				var logFromAddress []string
				var logToAddress []string
				for _, log := range eventLogList {
					logFromAddress = append(logFromAddress, log.From)
					logToAddress = append(logToAddress, log.To)
				}
				logAddressList := [][]string{logFromAddress, logToAddress}
				logAddress, _ = json.Marshal(logAddressList)
				atomContractRecord.LogAddress = logAddress
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

	curHeight := tx.BlockNumber
	block, err := client.GetBlock(curHeight)
	if err != nil {
		log.Error(h.chainName+"扫块，从链上获取区块信息失败", zap.Any("curHeight", curHeight), zap.Any("error", err))
		return err
	}

	err = h.OnNewTx(c, block, tx)

	return err
}

func (h *txHandler) OnDroppedTx(c chain.Clienter, tx *chain.Transaction) error {
	record := tx.Record.(*data.AtomTransactionRecord)

	//判断nonce 是否小于 当前链上的nonce
	result, err := data.AtomTransactionRecordRepoClient.FindLastNonce(nil, biz.GetTableName(h.chainName), record.FromAddress)
	if err != nil {
		return nil
	}
	if result != nil {
		if record.TransactionHash == result.TransactionHash {
			return nil
		}
		nonce := uint64(result.Nonce)
		if uint64(record.Nonce) <= nonce {
			record.Status = biz.DROPPED_REPLACED
			record.UpdatedAt = h.now
			h.txRecords = append(h.txRecords, record)
			log.Info(
				"更新 PENDING txhash对象为丢弃置换状态",
				zap.Any("txId", record.TransactionHash),
				zap.Int64("recordNonce", record.Nonce),
				zap.Uint64("chainNonce", nonce),
			)
			return nil
		}
	}

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
