package aptos

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/subhandle"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"
)

type Platform struct {
	subhandle.CommPlatform
	client    Client
	CoinIndex uint
}

const APT_CODE = "0x1::aptos_coin::AptosCoin"
const APT_CREATE_ACCOUNT = "0x1::account::create_account"
const APT_REGISTER = "0x1::coins::register"

func Init(handler, chain, chainName string, nodeURL []string, height int) *Platform {
	return &Platform{
		CoinIndex:    coins.HandleMap[handler],
		client:       NewClient(nodeURL[0]),
		CommPlatform: subhandle.CommPlatform{Height: height, Chain: chain, ChainName: chainName},
	}
}

func (p *Platform) Coin() coins.Coin {
	return coins.Coins[p.CoinIndex]
}

func (p *Platform) GetTransactions() {
	log.Info("GetTransactions starting, chainName:" + p.ChainName)
	for true {
		p.DoGetTransactions()
		time.Sleep(time.Duration(p.Coin().LiveInterval) * time.Millisecond)
	}
}

func (p *Platform) DoGetTransactions() {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("GetTransactions error, chainName:"+p.ChainName, e)
			} else {
				log.Errore("GetTransactions panic, chainName:"+p.ChainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链爬块失败, error：%s", p.ChainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	//获取最新块高
	height, err := p.client.GetBlockNumber()
	for i := 0; i < 10 && err != nil; i++ {
		time.Sleep(time.Duration(i*5) * time.Second)
		height, err = p.client.GetBlockNumber()
	}
	if err != nil || height <= 0 {
		log.Error(p.ChainName+"扫块，从链上获取最新块高失败", zap.Any("new", height), zap.Any("error", err))
		return
	}

	//获取本地当前块高
	oldHeight := -1
	redisHeight, err := data.RedisClient.Get(biz.BLOCK_HEIGHT_KEY + p.ChainName).Result()
	for i := 0; i < 3 && err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		redisHeight, err = data.RedisClient.Get(biz.BLOCK_HEIGHT_KEY + p.ChainName).Result()
	}
	if err != nil {
		if fmt.Sprintf("%s", err) == biz.REDIS_NIL_KEY {
			// 从数据库中查询最新一条数据
			lastRecord, err := data.AptTransactionRecordRepoClient.FindLast(nil, biz.GetTalbeName(p.ChainName))
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				lastRecord, err = data.AptTransactionRecordRepoClient.FindLast(nil, biz.GetTalbeName(p.ChainName))
			}
			if err != nil {
				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链查询数据库中块高失败", p.ChainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(p.ChainName+"扫块，从数据库中获取块高失败", zap.Any("old", oldHeight), zap.Any("new", height), zap.Any("error", err))
				return
			}
			if lastRecord == nil {
				oldHeight = height - 1
				log.Error(p.ChainName+"扫块，从数据库中获取的块高为空", zap.Any("old", oldHeight), zap.Any("new", height))
				//return
			} else {
				oldHeight = lastRecord.TransactionVersion
			}
		} else {
			// redis出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链查询redis中块高失败", p.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(p.ChainName+"扫块，从redis中获取块高失败", zap.Any("old", oldHeight), zap.Any("new", height), zap.Any("error", err))
			return
		}
	} else {
		if redisHeight != "" {
			oldHeight, err = strconv.Atoi(redisHeight)
			if err != nil {
				log.Error(p.ChainName+"扫块，从redis获取的块高不合法", zap.Any("old", oldHeight), zap.Any("new", height))
				return
			}
		} else {
			log.Error(p.ChainName+"扫块，从redis获取的块高为空", zap.Any("old", oldHeight), zap.Any("new", height))
			return
		}
	}
	log.Info(p.ChainName+"扫块，高度为", zap.Any("old", oldHeight), zap.Any("new", height))
	if oldHeight >= height {
		return
	}

	curHeight := oldHeight
	for curHeight < height {
		curHeight++

		block, err := p.client.GetBlockByNumber(curHeight)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			block, err = p.client.GetBlockByNumber(curHeight)
		}
		if err != nil {
			log.Error(p.ChainName+"扫块，从链上获取区块hash失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
			return
		}

		var txRecords []*data.AptTransactionRecord
		now := time.Now().Unix()
		for _, tx := range block.Transactions {
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

			if status != "" {
				if tx.Payload.Type != "module_bundle_payload" &&
					(tx.Payload.Function == APT_CREATE_ACCOUNT ||
						tx.Payload.Function == APT_REGISTER) {
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
							alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
							log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
							return
						}
					}

					if toAddress != "" {
						toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
						if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
							// redis出错 接入lark报警
							alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
							log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
							return
						}
					}

					if fromAddressExist || toAddressExist {
						version, _ := strconv.Atoi(tx.Version)
						nonce, _ := strconv.Atoi(tx.SequenceNumber)
						txTime, _ := strconv.ParseInt(tx.Timestamp, 10, 64)
						txTime = txTime / 1000
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
							CreatedAt:           now,
							UpdatedAt:           now,
						}
						txRecords = append(txRecords, aptTransactionRecord)
					}
				} else if tx.Payload.Type != "module_bundle_payload" &&
					!strings.HasSuffix(tx.Payload.Function, "::rotate_authentication_key") &&
					!strings.HasSuffix(tx.Payload.Function, "::create_collection_script") &&
					!strings.HasSuffix(tx.Payload.Function, "::init") {
					if strings.HasSuffix(tx.Payload.Function, "::transfer") {
						txType := biz.NATIVE
						var tokenInfo types.TokenInfo
						var amount, contractAddress string
						var fromAddress, toAddress, fromUid, toUid string
						var fromAddressExist, toAddressExist bool

						fromAddress = tx.Sender
						if toAddressStr, ok := tx.Payload.Arguments[0].(string); ok {
							toAddress = toAddressStr
						}
						if amountStr, ok := tx.Payload.Arguments[1].(string); ok {
							amount = amountStr
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
								alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
								alarmOpts := biz.WithMsgLevel("FATAL")
								biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
								log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
								return
							}
						}

						if toAddress != "" {
							toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
							if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
								// redis出错 接入lark报警
								alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
								alarmOpts := biz.WithMsgLevel("FATAL")
								biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
								log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
								return
							}
						}

						if fromAddressExist || toAddressExist {
							version, _ := strconv.Atoi(tx.Version)
							nonce, _ := strconv.Atoi(tx.SequenceNumber)
							txTime, _ := strconv.ParseInt(tx.Timestamp, 10, 64)
							txTime = txTime / 1000
							gasUsed, _ := strconv.Atoi(tx.GasUsed)
							gasPrice, _ := strconv.Atoi(tx.GasUnitPrice)
							feeAmount := decimal.NewFromInt(int64(gasUsed * gasPrice))
							payload, _ := utils.JsonEncode(tx.Payload)

							contractAddressSplit := strings.Split(contractAddress, "::")
							if len(contractAddressSplit) == 3 {
								var decimals int64 = 0
								if txType == biz.TRANSFER {
									getTokenInfo, err := biz.GetTokenInfo(nil, p.ChainName, contractAddress)
									for i := 0; i < 3 && err != nil; i++ {
										time.Sleep(time.Duration(i*1) * time.Second)
										getTokenInfo, err = biz.GetTokenInfo(nil, p.ChainName, contractAddress)
									}
									if err != nil {
										// nodeProxy出错 接入lark报警
										alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", p.ChainName)
										alarmOpts := biz.WithMsgLevel("FATAL")
										biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
										log.Error(p.ChainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
									} else if getTokenInfo.Symbol != "" {
										decimals = getTokenInfo.Decimals
									}
								}
								tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amount}
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
								CreatedAt:           now,
								UpdatedAt:           now,
							}
							txRecords = append(txRecords, aptTransactionRecord)
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
								alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
								alarmOpts := biz.WithMsgLevel("FATAL")
								biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
								log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
								return
							}
						}

						if toAddress != "" {
							toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
							if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
								// redis出错 接入lark报警
								alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
								alarmOpts := biz.WithMsgLevel("FATAL")
								biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
								log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
								return
							}
						}

						if fromAddressExist || toAddressExist {
							version, _ = strconv.Atoi(tx.Version)
							nonce, _ = strconv.Atoi(tx.SequenceNumber)
							txTime, _ = strconv.ParseInt(tx.Timestamp, 10, 64)
							txTime = txTime / 1000
							gasUsed, _ = strconv.Atoi(tx.GasUsed)
							gasPrice, _ = strconv.Atoi(tx.GasUnitPrice)
							feeAmount = decimal.NewFromInt(int64(gasUsed * gasPrice))
							payload, _ = utils.JsonEncode(tx.Payload)

							flag = true

							contractAddressSplit := strings.Split(contractAddress, "::")
							if len(contractAddressSplit) == 3 {
								var decimals int64 = 0
								if contractAddress != APT_CODE && contractAddress != "" {
									getTokenInfo, err := biz.GetTokenInfo(nil, p.ChainName, contractAddress)
									for i := 0; i < 3 && err != nil; i++ {
										time.Sleep(time.Duration(i*1) * time.Second)
										getTokenInfo, err = biz.GetTokenInfo(nil, p.ChainName, contractAddress)
									}
									if err != nil {
										// nodeProxy出错 接入lark报警
										alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", p.ChainName)
										alarmOpts := biz.WithMsgLevel("FATAL")
										biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
										log.Error(p.ChainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
									} else if getTokenInfo.Symbol != "" {
										decimals = getTokenInfo.Decimals
									}
								}
								tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amount}
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
								CreatedAt:           now,
								UpdatedAt:           now,
							}
							txRecords = append(txRecords, aptContractRecord)
						}

						txType = biz.EVENTLOG
						index := 0
						eventIndex := 0
						withdrawEventNum := 0

						for _, event := range tx.Events {
							if event.Type == "0x1::coin::WithdrawEvent" {
								withdrawEventNum++
							}
						}
						for _, event := range tx.Events {
							var tokenInfo types.TokenInfo
							var amount, contractAddress string
							var fromAddress, toAddress, fromUid, toUid string
							var fromAddressExist, toAddressExist bool

							if event.Type == "0x1::coin::WithdrawEvent" ||
								event.Type == "0x1::coin::DepositEvent" {
								amount = event.Data.Amount
								mode := strings.Split(tx.Payload.Function, "::")
								if event.Type == "0x1::coin::WithdrawEvent" {
									if len(tx.Payload.TypeArguments) > 0 {
										if strings.HasSuffix(tx.Payload.Function, "::remove_liquidity") {
											contractAddress = tx.Payload.TypeArguments[2]
										} else {
											if len(tx.Payload.TypeArguments) == 1 {
												contractAddress = tx.Payload.TypeArguments[0]
											} else {
												if eventIndex < len(tx.Payload.TypeArguments) {
													contractAddress = tx.Payload.TypeArguments[eventIndex]
													eventIndex++
												}
											}
										}
									}
									fromAddress = "0x" + event.Key[18:]
									if len(mode) == 3 {
										toAddress = mode[0]
									}
								} else if event.Type == "0x1::coin::DepositEvent" {
									if len(tx.Payload.TypeArguments) > 0 {
										if strings.HasSuffix(tx.Payload.Function, "::remove_liquidity") {
											if eventIndex < len(tx.Payload.TypeArguments) {
												contractAddress = tx.Payload.TypeArguments[eventIndex]
												eventIndex++
											}
										} else {
											if len(tx.Payload.TypeArguments) == 1 {
												contractAddress = tx.Payload.TypeArguments[0]
											} else {
												if withdrawEventNum < len(tx.Payload.TypeArguments) {
													contractAddress = tx.Payload.TypeArguments[withdrawEventNum]
												}
											}
										}
									}
									toAddress = "0x" + event.Key[18:]
									if len(mode) == 3 {
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
									alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
									alarmOpts := biz.WithMsgLevel("FATAL")
									biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
									log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
									return
								}
							}

							if toAddress != "" {
								toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
								if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
									// redis出错 接入lark报警
									alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
									alarmOpts := biz.WithMsgLevel("FATAL")
									biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
									log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
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
									txTime = txTime / 1000
									gasUsed, _ = strconv.Atoi(tx.GasUsed)
									gasPrice, _ = strconv.Atoi(tx.GasUnitPrice)
									feeAmount = decimal.NewFromInt(int64(gasUsed * gasPrice))
									payload, _ = utils.JsonEncode(tx.Payload)

									flag = true
								}

								contractAddressSplit := strings.Split(contractAddress, "::")
								if len(contractAddressSplit) == 3 {
									var decimals int64 = 0
									if contractAddress != APT_CODE && contractAddress != "" {
										getTokenInfo, err := biz.GetTokenInfo(nil, p.ChainName, contractAddress)
										for i := 0; i < 3 && err != nil; i++ {
											time.Sleep(time.Duration(i*1) * time.Second)
											getTokenInfo, err = biz.GetTokenInfo(nil, p.ChainName, contractAddress)
										}
										if err != nil {
											// nodeProxy出错 接入lark报警
											alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", p.ChainName)
											alarmOpts := biz.WithMsgLevel("FATAL")
											biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
											log.Error(p.ChainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
										} else if getTokenInfo.Symbol != "" {
											decimals = getTokenInfo.Decimals
										}
									}
									tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amount}
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
								eventLogs = append(eventLogs, eventLogInfo)
								eventLog, _ := utils.JsonEncode(eventLogInfo)

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
									EventLog:            eventLog,
									TransactionType:     txType,
									DappData:            "",
									ClientData:          "",
									CreatedAt:           now,
									UpdatedAt:           now,
								}
								txRecords = append(txRecords, aptTransactionRecord)
							}
						}

						if aptContractRecord != nil && eventLogs != nil {
							eventLog, _ := utils.JsonEncode(eventLogs)
							aptContractRecord.EventLog = eventLog
						}
					}
				}
			}
		}

		if txRecords != nil && len(txRecords) > 0 {
			//保存交易数据
			err := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(p.ChainName))
			if err != nil {
				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", p.ChainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(p.ChainName+"扫块，将数据插入到数据库中失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
				return
			}
			go HandleRecord(p.ChainName, p.client, txRecords)
		}

		//更新 redis中的块高
		result, err := data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, curHeight, 0).Result()
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			result, err = data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, curHeight, 0).Result()
		}
		if err != nil {
			// redis出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链插入块高到redis中失败", p.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(p.ChainName+"扫块，将块高插入到redis中失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
			return
		}
		if result != "OK" {
			log.Error(p.ChainName+"扫块，将块高插入到redis中失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("result", result))
		}
	}
}

func (p *Platform) GetTransactionResultByTxhash() {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("GetTransactionsResult error, chainName:"+p.ChainName, e)
			} else {
				log.Errore("GetTransactionsResult panic, chainName:"+p.ChainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链处理交易结果失败, error：%s", p.ChainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	records, err := data.AptTransactionRecordRepoClient.FindByStatus(nil, biz.GetTalbeName(p.ChainName), biz.PENDING, biz.NO_STATUS)
	if err != nil {
		log.Error(p.ChainName+"查询数据库失败", zap.Any("error", err))
		return
	}
	log.Info("", zap.Any("records", records))
	var txRecords []*data.AptTransactionRecord
	now := time.Now().Unix()

	for _, record := range records {
		transactionInfo, err := p.client.GetTransactionByHash(record.TransactionHash)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			transactionInfo, err = p.client.GetTransactionByHash(record.TransactionHash)
		}
		if err != nil {
			log.Error(p.ChainName+"查询链上数据失败", zap.Any("error", err))
			continue
		}

		curHeight, _ := strconv.Atoi(transactionInfo.Version)
		if transactionInfo.ErrorCode == "transaction_not_found" {
			status := biz.FAIL
			record.Status = status
			record.UpdatedAt = now
			txRecords = append(txRecords, record)
			continue
		} else if transactionInfo.Type == "pending_transaction" {
			continue
		}

		block, err := p.client.GetBlockByNumber(curHeight)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			block, err = p.client.GetBlockByNumber(curHeight)
		}
		if err != nil {
			log.Error(p.ChainName+"扫块，从链上获取区块hash失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
			return
		}

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

			if status != "" {
				if tx.Payload.Type != "module_bundle_payload" &&
					(tx.Payload.Function == APT_CREATE_ACCOUNT ||
						tx.Payload.Function == APT_REGISTER) {
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
							alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
							log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
							return
						}
					}

					if toAddress != "" {
						toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
						if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
							// redis出错 接入lark报警
							alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
							log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
							return
						}
					}

					if fromAddressExist || toAddressExist {
						version, _ := strconv.Atoi(tx.Version)
						nonce, _ := strconv.Atoi(tx.SequenceNumber)
						txTime, _ := strconv.ParseInt(tx.Timestamp, 10, 64)
						txTime = txTime / 1000
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
							CreatedAt:           now,
							UpdatedAt:           now,
						}
						txRecords = append(txRecords, aptTransactionRecord)
					}
				} else if tx.Payload.Type != "module_bundle_payload" &&
					!strings.HasSuffix(tx.Payload.Function, "::rotate_authentication_key") &&
					!strings.HasSuffix(tx.Payload.Function, "::create_collection_script") &&
					!strings.HasSuffix(tx.Payload.Function, "::init") {
					if strings.HasSuffix(tx.Payload.Function, "::transfer") {
						txType := biz.NATIVE
						var tokenInfo types.TokenInfo
						var amount, contractAddress string
						var fromAddress, toAddress, fromUid, toUid string
						var fromAddressExist, toAddressExist bool

						fromAddress = tx.Sender
						if toAddressStr, ok := tx.Payload.Arguments[0].(string); ok {
							toAddress = toAddressStr
						}
						if amountStr, ok := tx.Payload.Arguments[1].(string); ok {
							amount = amountStr
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
								alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
								alarmOpts := biz.WithMsgLevel("FATAL")
								biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
								log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
								return
							}
						}

						if toAddress != "" {
							toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
							if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
								// redis出错 接入lark报警
								alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
								alarmOpts := biz.WithMsgLevel("FATAL")
								biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
								log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
								return
							}
						}

						if fromAddressExist || toAddressExist {
							version, _ := strconv.Atoi(tx.Version)
							nonce, _ := strconv.Atoi(tx.SequenceNumber)
							txTime, _ := strconv.ParseInt(tx.Timestamp, 10, 64)
							txTime = txTime / 1000
							gasUsed, _ := strconv.Atoi(tx.GasUsed)
							gasPrice, _ := strconv.Atoi(tx.GasUnitPrice)
							feeAmount := decimal.NewFromInt(int64(gasUsed * gasPrice))
							payload, _ := utils.JsonEncode(tx.Payload)

							contractAddressSplit := strings.Split(contractAddress, "::")
							if len(contractAddressSplit) == 3 {
								var decimals int64 = 0
								if txType == biz.TRANSFER {
									getTokenInfo, err := biz.GetTokenInfo(nil, p.ChainName, contractAddress)
									for i := 0; i < 3 && err != nil; i++ {
										time.Sleep(time.Duration(i*1) * time.Second)
										getTokenInfo, err = biz.GetTokenInfo(nil, p.ChainName, contractAddress)
									}
									if err != nil {
										// nodeProxy出错 接入lark报警
										alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", p.ChainName)
										alarmOpts := biz.WithMsgLevel("FATAL")
										biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
										log.Error(p.ChainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
									} else if getTokenInfo.Symbol != "" {
										decimals = getTokenInfo.Decimals
									}
								}
								tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amount}
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
								CreatedAt:           now,
								UpdatedAt:           now,
							}
							txRecords = append(txRecords, aptTransactionRecord)
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
								alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
								alarmOpts := biz.WithMsgLevel("FATAL")
								biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
								log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
								return
							}
						}

						if toAddress != "" {
							toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
							if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
								// redis出错 接入lark报警
								alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
								alarmOpts := biz.WithMsgLevel("FATAL")
								biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
								log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
								return
							}
						}

						if fromAddressExist || toAddressExist {
							version, _ = strconv.Atoi(tx.Version)
							nonce, _ = strconv.Atoi(tx.SequenceNumber)
							txTime, _ = strconv.ParseInt(tx.Timestamp, 10, 64)
							txTime = txTime / 1000
							gasUsed, _ = strconv.Atoi(tx.GasUsed)
							gasPrice, _ = strconv.Atoi(tx.GasUnitPrice)
							feeAmount = decimal.NewFromInt(int64(gasUsed * gasPrice))
							payload, _ = utils.JsonEncode(tx.Payload)

							flag = true

							contractAddressSplit := strings.Split(contractAddress, "::")
							if len(contractAddressSplit) == 3 {
								var decimals int64 = 0
								if contractAddress != APT_CODE && contractAddress != "" {
									getTokenInfo, err := biz.GetTokenInfo(nil, p.ChainName, contractAddress)
									for i := 0; i < 3 && err != nil; i++ {
										time.Sleep(time.Duration(i*1) * time.Second)
										getTokenInfo, err = biz.GetTokenInfo(nil, p.ChainName, contractAddress)
									}
									if err != nil {
										// nodeProxy出错 接入lark报警
										alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", p.ChainName)
										alarmOpts := biz.WithMsgLevel("FATAL")
										biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
										log.Error(p.ChainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
									} else if getTokenInfo.Symbol != "" {
										decimals = getTokenInfo.Decimals
									}
								}
								tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amount}
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
								CreatedAt:           now,
								UpdatedAt:           now,
							}
							txRecords = append(txRecords, aptContractRecord)
						}

						txType = biz.EVENTLOG
						index := 0
						eventIndex := 0
						withdrawEventNum := 0

						for _, event := range tx.Events {
							if event.Type == "0x1::coin::WithdrawEvent" {
								withdrawEventNum++
							}
						}
						for _, event := range tx.Events {
							var tokenInfo types.TokenInfo
							var amount, contractAddress string
							var fromAddress, toAddress, fromUid, toUid string
							var fromAddressExist, toAddressExist bool

							if event.Type == "0x1::coin::WithdrawEvent" {
								if len(tx.Payload.TypeArguments) > 0 {
									if strings.HasSuffix(tx.Payload.Function, "::remove_liquidity") {
										contractAddress = tx.Payload.TypeArguments[2]
									} else {
										if len(tx.Payload.TypeArguments) == 1 {
											contractAddress = tx.Payload.TypeArguments[0]
										} else {
											contractAddress = tx.Payload.TypeArguments[eventIndex]
											eventIndex++
										}
									}
								}
								fromAddress = "0x" + event.Key[18:]
								amount = event.Data.Amount

								mode := strings.Split(tx.Payload.Function, "::")
								if len(mode) == 3 {
									toAddress = mode[0]
								}
							} else if event.Type == "0x1::coin::DepositEvent" {
								if len(tx.Payload.TypeArguments) > 0 {
									if strings.HasSuffix(tx.Payload.Function, "::remove_liquidity") {
										contractAddress = tx.Payload.TypeArguments[eventIndex]
										eventIndex++
									} else {
										if len(tx.Payload.TypeArguments) == 1 {
											contractAddress = tx.Payload.TypeArguments[0]
										} else {
											contractAddress = tx.Payload.TypeArguments[withdrawEventNum]
										}
									}
								}
								toAddress = "0x" + event.Key[18:]
								amount = event.Data.Amount

								mode := strings.Split(tx.Payload.Function, "::")
								if len(mode) == 3 {
									fromAddress = mode[0]
								}
							} else {
								continue
							}

							if fromAddress != "" {
								fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
								if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
									// redis出错 接入lark报警
									alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
									alarmOpts := biz.WithMsgLevel("FATAL")
									biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
									log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
									return
								}
							}

							if toAddress != "" {
								toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
								if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
									// redis出错 接入lark报警
									alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
									alarmOpts := biz.WithMsgLevel("FATAL")
									biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
									log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
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
									txTime = txTime / 1000
									gasUsed, _ = strconv.Atoi(tx.GasUsed)
									gasPrice, _ = strconv.Atoi(tx.GasUnitPrice)
									feeAmount = decimal.NewFromInt(int64(gasUsed * gasPrice))
									payload, _ = utils.JsonEncode(tx.Payload)

									flag = true
								}

								contractAddressSplit := strings.Split(contractAddress, "::")
								if len(contractAddressSplit) == 3 {
									var decimals int64 = 0
									if contractAddress != APT_CODE && contractAddress != "" {
										getTokenInfo, err := biz.GetTokenInfo(nil, p.ChainName, contractAddress)
										for i := 0; i < 3 && err != nil; i++ {
											time.Sleep(time.Duration(i*1) * time.Second)
											getTokenInfo, err = biz.GetTokenInfo(nil, p.ChainName, contractAddress)
										}
										if err != nil {
											// nodeProxy出错 接入lark报警
											alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", p.ChainName)
											alarmOpts := biz.WithMsgLevel("FATAL")
											biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
											log.Error(p.ChainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
										} else if getTokenInfo.Symbol != "" {
											decimals = getTokenInfo.Decimals
										}
									}
									tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amount}
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
								eventLogs = append(eventLogs, eventLogInfo)
								eventLog, _ := utils.JsonEncode(eventLogInfo)

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
									EventLog:            eventLog,
									TransactionType:     txType,
									DappData:            "",
									ClientData:          "",
									CreatedAt:           now,
									UpdatedAt:           now,
								}
								txRecords = append(txRecords, aptTransactionRecord)
							}
						}

						if aptContractRecord != nil && eventLogs != nil {
							eventLog, _ := utils.JsonEncode(eventLogs)
							aptContractRecord.EventLog = eventLog
						}
					}
				}
			}
		}
	}

	if txRecords != nil && len(txRecords) > 0 {
		//保存交易数据
		err := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(p.ChainName))
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", p.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(p.ChainName+"扫块，将数据插入到数据库中失败" /*, zap.Any("current", curHeight), zap.Any("new", height)*/, zap.Any("error", err))
			return
		}
	}
}

func BatchSaveOrUpdate(txRecords []*data.AptTransactionRecord, tableName string) error {
	total := len(txRecords)
	pageSize := biz.PAGE_SIZE
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	for start < stop {
		subTxRecords := txRecords[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}

		_, err := data.AptTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.AptTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		}
		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY) {
			return err
		}
	}
	return nil
}
