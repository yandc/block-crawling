package starcoin

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/subhandle"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"math/big"
	"strconv"
	"strings"
	"time"
)

type Platform struct {
	subhandle.CommPlatform
	client    Client
	CoinIndex uint
}

const STC_CODE = "0x00000000000000000000000000000001::STC::STC"

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
		h := p.IndexBlock()
		if h {
			time.Sleep(time.Duration(p.Coin().LiveInterval) * time.Millisecond)
		}
	}
}

func (p *Platform) IndexBlock() bool {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("IndexBlock error, chainName:"+p.ChainName, e)
			} else {
				log.Errore("IndexBlock panic, chainName:"+p.ChainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链爬块失败, error：%s", p.ChainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	heights, err := p.client.GetBlockHeight()
	if err != nil {
		return true
	}
	height, _ := strconv.Atoi(heights)
	data.RedisClient.Set(biz.BLOCK_NODE_HEIGHT_KEY+p.ChainName, height, 0)

	curHeight := -1
	preDBBlockHash := make(map[int]string)
	redisHeight, _ := data.RedisClient.Get(biz.BLOCK_HEIGHT_KEY + p.ChainName).Result()
	if redisHeight != "" {
		curHeight, _ = strconv.Atoi(redisHeight)
	} else {
		lastRecord, err := data.StcTransactionRecordRepoClient.FindLast(nil, biz.GetTalbeName(p.ChainName))
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			lastRecord, err = data.StcTransactionRecordRepoClient.FindLast(nil, biz.GetTalbeName(p.ChainName))
		}
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链查询数据库中块高失败", p.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(p.ChainName+"扫块，从数据库中获取块高失败", zap.Any("curHeight", curHeight), zap.Any("new", height), zap.Any("error", err))
			return true
		}
		if lastRecord == nil {
			curHeight = height
			log.Error(p.ChainName+"扫块，从数据库中获取的块高为空", zap.Any("curHeight", curHeight), zap.Any("new", height))
			//return true
		} else {
			curHeight = lastRecord.BlockNumber + 1
			preDBBlockHash[lastRecord.BlockNumber] = lastRecord.BlockHash
		}
	}
	if curHeight > height {
		return true
	}

	if curHeight <= height {
		block, err := p.client.GetBlockByNumber(curHeight)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			block, err = p.client.GetBlockByNumber(curHeight)
		}
		if err != nil {
			return true
		}

		if block != nil {
			forked := false
			preHeight := curHeight - 1 // c 5 p 4
			preHash := block.BlockHeader.ParentHash
			curPreBlockHash, _ := data.RedisClient.Get(biz.BLOCK_HASH_KEY + p.ChainName + ":" + strconv.Itoa(preHeight)).Result()

			if curPreBlockHash == "" {
				bh := preDBBlockHash[preHeight]
				if bh != "" {
					curPreBlockHash = bh
				}
			}
			//分叉孤块处理
			for curPreBlockHash != "" && curPreBlockHash != preHash {
				forked = true
				pBlock, _ := p.client.GetBlockByNumber(preHeight)
				preHash = pBlock.BlockHeader.ParentHash
				// c 5 p 4  4 --  3 hahs p 3
				preHeight = preHeight - 1
				curPreBlockHash, _ = data.RedisClient.Get(biz.BLOCK_HASH_KEY + p.ChainName + ":" + strconv.Itoa(preHeight)).Result()
			}

			if forked {
				curHeight = preHeight
				rows, _ := data.StcTransactionRecordRepoClient.DeleteByBlockNumber(nil, biz.GetTalbeName(p.ChainName), preHeight+1)
				log.Info("出现分叉回滚数据", zap.Any("链类型", p.ChainName), zap.Any("共删除数据", rows), zap.Any("回滚到块高", preHeight))
			} else {
				var txRecords []*data.StcTransactionRecord
				now := time.Now().Unix()
				for _, userTransaction := range block.BlockBody.UserTransactions {
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
								alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
								alarmOpts := biz.WithMsgLevel("FATAL")
								biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
								log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
								return true
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
								return true
							}
						}

						if fromAddressExist || toAddressExist {
							transactionInfo, err := p.client.GetTransactionInfoByHash(userTransaction.TransactionHash)
							for i := 0; i < 10 && err != nil; i++ {
								time.Sleep(time.Duration(i*5) * time.Second)
								transactionInfo, err = p.client.GetTransactionInfoByHash(userTransaction.TransactionHash)
							}
							if err != nil {
								return true
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

							contractAddressSplit := strings.Split(contractAddress, "::")
							if len(contractAddressSplit) == 3 {
								var decimals int64 = 9
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
								BlockNumber:     curHeight,
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
								CreatedAt:       now,
								UpdatedAt:       now,
							}
							txRecords = append(txRecords, stcRecord)
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

						events, err := p.client.GetTransactionEventByHash(userTransaction.TransactionHash)
						for i := 0; i < 10 && err != nil; i++ {
							time.Sleep(time.Duration(i*5) * time.Second)
							events, err = p.client.GetTransactionEventByHash(userTransaction.TransactionHash)
						}
						if err != nil {
							log.Error(p.ChainName+"扫块，从链上获取区块event失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
							return true
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
								alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
								alarmOpts := biz.WithMsgLevel("FATAL")
								biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
								log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
								return true
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
								return true
							}
						}

						if fromAddressExist || toAddressExist {
							transactionInfo, err := p.client.GetTransactionInfoByHash(userTransaction.TransactionHash)
							for i := 0; i < 10 && err != nil; i++ {
								time.Sleep(time.Duration(i*5) * time.Second)
								transactionInfo, err = p.client.GetTransactionInfoByHash(userTransaction.TransactionHash)
							}
							if err != nil {
								return true
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

							contractAddressSplit := strings.Split(contractAddress, "::")
							if len(contractAddressSplit) == 3 {
								var decimals int64 = 9
								if contractAddress != STC_CODE && contractAddress != "" {
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
								BlockNumber:     curHeight,
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
								CreatedAt:       now,
								UpdatedAt:       now,
							}
							txRecords = append(txRecords, stcContractRecord)
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
									alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
									alarmOpts := biz.WithMsgLevel("FATAL")
									biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
									log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
									return true
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
									return true
								}
							}

							if fromAddressExist || toAddressExist {
								index++
								txHash := userTransaction.TransactionHash + "#result-" + fmt.Sprintf("%v", index)

								if !flag {
									transactionInfo, err := p.client.GetTransactionInfoByHash(userTransaction.TransactionHash)
									for i := 0; i < 10 && err != nil; i++ {
										time.Sleep(time.Duration(i*5) * time.Second)
										transactionInfo, err = p.client.GetTransactionInfoByHash(userTransaction.TransactionHash)
									}
									if err != nil {
										return true
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

								contractAddressSplit := strings.Split(contractAddress, "::")
								if len(contractAddressSplit) == 3 {
									var decimals int64 = 9
									if contractAddress != STC_CODE && contractAddress != "" {
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
									var amountStr string
									if amount != nil {
										amountStr = amount.String()
									}
									tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amountStr}
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
									BlockNumber:     curHeight,
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
									CreatedAt:       now,
									UpdatedAt:       now,
								}
								txRecords = append(txRecords, stcRecord)
							}
						}

						if stcContractRecord != nil && eventLogs != nil {
							eventLog, _ := utils.JsonEncode(eventLogs)
							stcContractRecord.EventLog = eventLog
						}
					}
				}

				if txRecords != nil && len(txRecords) > 0 {
					e := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(p.ChainName))
					if e != nil {
						// postgres出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库库中失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(p.ChainName+"扫块，将数据插入到数据库中失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
						return true
					}
					go HandleRecord(p.ChainName, p.client, txRecords)
				}
				data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(curHeight), block.BlockHeader.BlockHash, biz.BLOCK_HASH_EXPIRATION_KEY)
			}
		} else {
			return true
		}
	} else {
		return true
	}
	data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, curHeight+1, 0)
	return false
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

	records, err := data.StcTransactionRecordRepoClient.FindByStatus(nil, biz.GetTalbeName(p.ChainName), biz.PENDING, biz.NO_STATUS)
	if err != nil {
		log.Error(p.ChainName+"查询数据库失败", zap.Any("error", err))
		return
	}
	log.Info("", zap.Any("records", records))
	var txRecords []*data.StcTransactionRecord
	now := time.Now().Unix()

	for _, record := range records {
		transactionInfo, err := p.client.GetTransactionInfoByHash(record.TransactionHash)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			transactionInfo, err = p.client.GetTransactionInfoByHash(record.TransactionHash)
		}
		if err != nil {
			log.Error(p.ChainName+"查询链上数据失败", zap.Any("txHash", record.TransactionHash), zap.Any("error", err))
			continue
		}

		curHeight, _ := strconv.Atoi(transactionInfo.BlockNumber)
		block, err := p.client.GetBlockByNumber(curHeight)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			block, err = p.client.GetBlockByNumber(curHeight)
		}
		if err != nil {
			log.Error(p.ChainName+"扫块，从链上获取区块hash失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
			return
		}

		for _, userTransaction := range block.BlockBody.UserTransactions {
			if userTransaction.TransactionHash != record.TransactionHash {
				continue
			}

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
					/*transactionInfo, err := p.client.GetTransactionInfoByHash(userTransaction.TransactionHash)
					for i := 0; i < 10 && err != nil; i++ {
						time.Sleep(time.Duration(i*5) * time.Second)
						transactionInfo, err = p.client.GetTransactionInfoByHash(userTransaction.TransactionHash)
					}
					if err != nil {
						return
					}*/

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

					contractAddressSplit := strings.Split(contractAddress, "::")
					if len(contractAddressSplit) == 3 {
						var decimals int64 = 9
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
						BlockNumber:     curHeight,
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
						CreatedAt:       now,
						UpdatedAt:       now,
					}
					txRecords = append(txRecords, stcRecord)
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

				events, err := p.client.GetTransactionEventByHash(userTransaction.TransactionHash)
				for i := 0; i < 10 && err != nil; i++ {
					time.Sleep(time.Duration(i*5) * time.Second)
					events, err = p.client.GetTransactionEventByHash(userTransaction.TransactionHash)
				}
				if err != nil {
					log.Error(p.ChainName+"扫块，从链上获取区块event失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
					return
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
					/*transactionInfo, err := p.client.GetTransactionInfoByHash(userTransaction.TransactionHash)
					for i := 0; i < 10 && err != nil; i++ {
						time.Sleep(time.Duration(i*5) * time.Second)
						transactionInfo, err = p.client.GetTransactionInfoByHash(userTransaction.TransactionHash)
					}
					if err != nil {
						return
					}*/

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

					contractAddressSplit := strings.Split(contractAddress, "::")
					if len(contractAddressSplit) == 3 {
						var decimals int64 = 9
						if contractAddress != STC_CODE && contractAddress != "" {
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
						BlockNumber:     curHeight,
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
						CreatedAt:       now,
						UpdatedAt:       now,
					}
					txRecords = append(txRecords, stcContractRecord)
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
						txHash := userTransaction.TransactionHash + "#result-" + fmt.Sprintf("%v", index)

						if !flag {
							/*transactionInfo, err := p.client.GetTransactionInfoByHash(userTransaction.TransactionHash)
							for i := 0; i < 10 && err != nil; i++ {
								time.Sleep(time.Duration(i*5) * time.Second)
								transactionInfo, err = p.client.GetTransactionInfoByHash(userTransaction.TransactionHash)
							}
							if err != nil {
								return
							}*/

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

						contractAddressSplit := strings.Split(contractAddress, "::")
						if len(contractAddressSplit) == 3 {
							var decimals int64 = 9
							if contractAddress != STC_CODE && contractAddress != "" {
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
							var amountStr string
							if amount != nil {
								amountStr = amount.String()
							}
							tokenInfo = types.TokenInfo{Address: contractAddress, Symbol: contractAddressSplit[2], Decimals: decimals, Amount: amountStr}
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
							BlockNumber:     curHeight,
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
							CreatedAt:       now,
							UpdatedAt:       now,
						}
						txRecords = append(txRecords, stcRecord)
					}
				}

				if stcContractRecord != nil && eventLogs != nil {
					eventLog, _ := utils.JsonEncode(eventLogs)
					stcContractRecord.EventLog = eventLog
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
		go handleUserNonce(p.ChainName, txRecords)
	}
}

func BatchSaveOrUpdate(txRecords []*data.StcTransactionRecord, table string) error {
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

		_, err := data.StcTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, table, subTxRecords)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.StcTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, table, subTxRecords)
		}
		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY) {
			return err
		}
	}
	return nil
}
