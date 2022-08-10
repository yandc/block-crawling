package ethereum

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/subhandle"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	types2 "github.com/ethereum/go-ethereum/core/types"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
const WITHDRAWAL_TOPIC = "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"


type Platform struct {
	subhandle.CommPlatform
	NodeURL   string
	CoinIndex uint
	UrlList   []string
}

type Config struct {
	ProjectId []string
}

type KVPair struct {
	Key string
	Val int
}

func Init(handler, chain, chainName string, nodeURL []string, height int) *Platform {
	return &Platform{
		CoinIndex:    coins.HandleMap[handler],
		NodeURL:      nodeURL[0],
		CommPlatform: subhandle.CommPlatform{Height: height, Chain: chain, ChainName: chainName},
		UrlList:      nodeURL,
	}
}

func (p *Platform) SetNodeURL(nodeURL string) {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	p.NodeURL = nodeURL
}

func GetPreferentialUrl(rpcURL []string, chainName string) []KVPair {
	//m := make(map[string]int)
	tmpList := make([]KVPair, 0, len(rpcURL))
	lock := new(sync.Mutex)
	var wg sync.WaitGroup
	for i := 0; i < len(rpcURL); i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			url := rpcURL[index]
			client, err := NewClient(url)
			if err != nil {
				log.Error("new client error:", zap.Error(err), zap.Any("url", url))
				return
			}
			defer client.Close()
			ctx := context.Background()
			preTime := time.Now()
			_, err = client.GetBlockNumber(ctx)
			if err != nil {
				utils.UpdateRecordRPCURL(url, false)
				return
			}
			subTime := time.Now().Sub(preTime)
			lock.Lock()
			tmpList = append(tmpList, KVPair{
				Key: rpcURL[index],
				Val: int(subTime),
			})
			lock.Unlock()
			utils.UpdateRecordRPCURL(url, true)
		}(i)
	}
	wg.Wait()
	sort.Slice(tmpList, func(i, j int) bool {
		return tmpList[i].Val < tmpList[j].Val // 升序
	})
	for j := 0; j < len(tmpList); j++ {
		_, err := data.RedisClient.Set(data.CHAINNAME+chainName+":NewRpcUrl:"+strconv.Itoa(j), tmpList[j].Key, 0).Result()
		if err != nil {
			log.Error("redis error", zap.Error(err), zap.Any("key", data.CHAINNAME+chainName+":NewRpcUrl:"+
				strconv.Itoa(j)), zap.Any("value", tmpList[j].Key))
		}
	}
	return tmpList
}

func (p *Platform) Coin() coins.Coin {
	return coins.Coins[p.CoinIndex]
}

func (p *Platform) GetTransactions() {
	for true {
		h := p.IndexBlock()
		if h {
			time.Sleep(time.Duration(p.Coin().LiveInterval) * time.Millisecond)
		}
	}
}

func (p *Platform) IndexBlock() bool {
	url := p.NodeURL
	client, err := NewClient(url)
	if err != nil {
		log.Warn("客户端连接构建失败", zap.Any("nodeUrl", p.NodeURL), zap.Any("error", err))
		return true
	}
	defer client.Close()
	ctx := context.Background()
	height, err := client.GetBlockNumber(ctx)
	for i := 1; err != nil && i <= 5; i++ {
		url_list := GetPreferentialUrl(p.UrlList, p.ChainName)
		if len(url_list) == 0 {
			alarmMsg := fmt.Sprintf("请注意：%s链目前没有可用rpcURL", p.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, p.UrlList, alarmOpts)
		}
		for j := 0; err != nil && j < len(url_list); j++ {
			url = url_list[j].Key
			client, err = NewClient(url)
			if err != nil {
				continue
			}
			height, err = client.GetBlockNumber(ctx)
		}
		time.Sleep(time.Duration(3*i) * time.Second)
	}

	//获取本地当前块高
	curHeight := -1
	preDBBlockHash := make(map[int]string)

	redisHeight, _ := data.RedisClient.Get(biz.BLOCK_HEIGHT_KEY + p.ChainName).Result()
	if redisHeight != "" {
		curHeight, _ = strconv.Atoi(redisHeight)
	} else {
		row, _ := data.EvmTransactionRecordRepoClient.FindLast(ctx, strings.ToLower(p.ChainName) + biz.TABLE_POSTFIX)
		if row != nil && row.BlockNumber != 0 {
			curHeight = row.BlockNumber + 1
			preDBBlockHash[row.BlockNumber] = row.BlockHash
		}
	}
	if curHeight == -1 {
		// redis出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链查询redis块高没有结果", p.ChainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, p.UrlList, alarmOpts)
		return true
	}
	if curHeight <= int(height) {
		block, err := client.GetBlockByNumber(ctx, big.NewInt(int64(curHeight)))
		for i := 1; err != nil && i <= 5; i++ {
			url_list := GetPreferentialUrl(p.UrlList, p.ChainName)
			if len(url_list) == 0 {
				alarmMsg := fmt.Sprintf("请注意：%s链目前没有可用rpcURL", p.ChainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, p.UrlList, alarmOpts)
			}
			for j := 0; err != nil && j < len(url_list); j++ {
				url = url_list[j].Key
				client, err = NewClient(url)
				if err != nil {
					continue
				}
				block, err = client.GetBlockByNumber(ctx, big.NewInt(int64(curHeight)))
			}
			time.Sleep(time.Duration(3*i) * time.Second)
		}
		if err != nil {
			log.Warn("请注意：未查到块高数据", zap.Any("chain", p.ChainName), zap.Any("height", curHeight))
			return true
		}
		if block != nil {
			forked := false
			//3
			preHeight := curHeight - 1
			preHash := block.ParentHash().String()
			curPreBlockHash, err := data.RedisClient.Get(biz.BLOCK_HASH_KEY + p.ChainName + ":" + strconv.Itoa(preHeight)).Result()

			if curPreBlockHash == "" {
				bh := preDBBlockHash[preHeight]
				if bh != "" {
					curPreBlockHash = bh
				} else {
					if err != nil {
						// redis出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链从redis获取区块hash失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						return true
					}
				}
			}

			//分叉孤块处理
			for curPreBlockHash != "" && curPreBlockHash != preHash {
				forked = true
				pBlock, pErr := client.GetBlockByNumber(ctx, big.NewInt(int64(preHeight))) //2 块
				for i := 1; pErr != nil && i <= 5; i++ {
					url_list := GetPreferentialUrl(p.UrlList, p.ChainName)
					if len(url_list) == 0 {
						alarmMsg := fmt.Sprintf("请注意：%s链目前没有可用rpcURL", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, p.UrlList, alarmOpts)
					}
					for j := 0; pErr != nil && j < len(url_list); j++ {
						url = url_list[j].Key
						client, pErr = NewClient(url)
						if pErr != nil {
							continue
						}
						block, pErr = client.GetBlockByNumber(ctx, big.NewInt(int64(curHeight)))
					}
					time.Sleep(time.Duration(3*i) * time.Second)
				}

				preHash = pBlock.ParentHash().String()
				preHeight = preHeight - 1
				curPreBlockHash, _ = data.RedisClient.Get(biz.BLOCK_HASH_KEY + p.ChainName + ":" + strconv.Itoa(preHeight)).Result()
			}
			if forked {
				curHeight = preHeight + 1
				rows, _ := data.EvmTransactionRecordRepoClient.DeleteByBlockNumber(ctx, strings.ToLower(p.ChainName) + biz.TABLE_POSTFIX, preHeight)
				log.Info("出现分叉回滚数据", zap.Any("链类型", p.ChainName), zap.Any("共删除数据", rows), zap.Any("回滚到块高", preHeight))
			} else {
				blockHash := ""
				var txRecords []*data.EvmTransactionRecord
				for _, transaction := range block.Transactions() {
					receipt, err := client.GetTransactionReceipt(ctx, transaction.Hash())
					for i := 1; err != nil && i <= 5; i++ {
						url_list := GetPreferentialUrl(p.UrlList, p.ChainName)
						if len(url_list) == 0 {
							alarmMsg := fmt.Sprintf("请注意：%s链目前没有可用rpcURL", p.ChainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, p.UrlList, alarmOpts)
						}
						for j := 0; err != nil && j < len(url_list); j++ {
							url = url_list[j].Key
							client, err = NewClient(url)
							if err != nil {
								continue
							}
							receipt, err = client.GetTransactionReceipt(ctx, transaction.Hash())
						}
						time.Sleep(time.Duration(3*i) * time.Second)
					}
					if receipt != nil && err == nil && blockHash == "" {
						blockHash = receipt.BlockHash.String()
					}
					var from common.Address
					var toAddress, feeAmount string
					var tokenInfo types.TokenInfo
					feeData := make(map[string]string)
					from, err = types2.Sender(types2.NewLondonSigner(transaction.ChainId()), transaction)
					if err != nil {
						from, err = types2.Sender(types2.HomesteadSigner{}, transaction)
					}
					if transaction.To() != nil {
						toAddress = transaction.To().String()
					}
					fromAddress := from.String()
					value := transaction.Value().String()
					transactionType := "native"
					if len(transaction.Data()) >= 68 && transaction.To() != nil {
						methodId := hex.EncodeToString(transaction.Data()[:4])
						if methodId == "a9059cbb" || methodId == "095ea7b3" {
							toAddress = common.HexToAddress(hex.EncodeToString(transaction.Data()[4:36])).String()
							amount := new(big.Int).SetBytes(transaction.Data()[36:])
							if methodId == "a9059cbb" {
								transactionType = "transfer"
							} else {
								transactionType = "approve"
							}
							value = amount.String()
						} else if methodId == "23b872dd" {
							fromAddress = common.HexToAddress(hex.EncodeToString(transaction.Data()[4:36])).String()
							toAddress = common.HexToAddress(hex.EncodeToString(transaction.Data()[36:68])).String()
							amount := new(big.Int).SetBytes(transaction.Data()[68:])
							transactionType = "transferfrom"
							value = amount.String()
						}
					}
					userFromAddress, fromUid, errr := biz.UserAddressSwitch(fromAddress)
					if errr != nil {
						// redis出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户地址失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						return true
					}
					userToAddress, toUid, errt := biz.UserAddressSwitch(toAddress)
					if errt != nil {
						// redis出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户地址失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						return true
					}

					if userFromAddress || userToAddress {
						if transaction.To() != nil {
							codeAt, err := client.CodeAt(ctx, common.HexToAddress(transaction.To().String()), nil)
							for i := 1; err != nil && i <= 5; i++ {
								url_list := GetPreferentialUrl(p.UrlList, p.ChainName)
								if len(url_list) == 0 {
									alarmMsg := fmt.Sprintf("请注意：%s链目前没有可用rpcURL", p.ChainName)
									alarmOpts := biz.WithMsgLevel("FATAL")
									biz.LarkClient.NotifyLark(alarmMsg, nil, p.UrlList, alarmOpts)
								}
								for j := 0; err != nil && j < len(url_list); j++ {
									url = url_list[j].Key
									client, err = NewClient(url)
									if err != nil {
										continue
									}
									codeAt, err = client.CodeAt(ctx, common.HexToAddress(transaction.To().String()), nil)
								}
								time.Sleep(time.Duration(3*i) * time.Second)
							}
							if len(codeAt) > 0 {
								if transactionType == "native" {
									transactionType = "contract"
								} else {
									decimals, symbol, err := GetTokenDecimals(transaction.To().String(), client)
									if err != nil || decimals == 0 || symbol == "" {
										transactionType = "contract"
									} else {
										tokenInfo = types.TokenInfo{Decimals: decimals, Amount: value, Symbol: symbol}
									}
									tokenInfo.Address = transaction.To().String()
								}
							}
						}
						receipt, err := client.GetTransactionReceipt(ctx, transaction.Hash())

						for i := 1; err != nil && i <= 5; i++ {
							url_list := GetPreferentialUrl(p.UrlList, p.ChainName)
							if len(url_list) == 0 {
								alarmMsg := fmt.Sprintf("请注意：%s链目前没有可用rpcURL", p.ChainName)
								alarmOpts := biz.WithMsgLevel("FATAL")
								biz.LarkClient.NotifyLark(alarmMsg, nil, p.UrlList, alarmOpts)
							}
							for j := 0; err != nil && j < len(url_list); j++ {
								url = url_list[j].Key
								client, err = NewClient(url)
								if err != nil {
									continue
								}
								receipt, err = client.GetTransactionReceipt(ctx, transaction.Hash())
							}
							time.Sleep(time.Duration(3*i) * time.Second)
						}
						if err != nil {
							continue
						}
						var eventLogs []types.EventLog
						eventLogMap := make(map[types.EventLog]struct{})
						if transactionType != "native" {
							for _, log_ := range receipt.Logs {
								if len(log_.Topics) > 1 && (log_.Topics[0].String() == TRANSFER_TOPIC ||
									log_.Topics[0].String() == WITHDRAWAL_TOPIC) {
									var token types.TokenInfo
									amount := big.NewInt(0)
									if len(log_.Data) >= 32 {
										amount = new(big.Int).SetBytes(log_.Data[:32])
									}
									if log_.Address.String() != "" {
										d, s, _ := GetTokenDecimals(log_.Address.String(), client)
										token = types.TokenInfo{
											Address:  log_.Address.String(),
											Decimals: d,
											Symbol:   s,
											Amount:   amount.String(),
										}
									}
									eventFrom := common.HexToAddress(log_.Topics[1].String()).String()
									var to string
									if len(log_.Topics) > 2 && log_.Topics[0].String() == TRANSFER_TOPIC {
										to = common.HexToAddress(log_.Topics[2].String()).String()
									} else if log_.Topics[0].String() == WITHDRAWAL_TOPIC {
										to = fromAddress
										if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
											token.Symbol = token.Symbol[1:]
										}
									}
									eventLog := types.EventLog{
										From:   eventFrom,
										To:     to,
										Amount: amount,
										Token:  token,
									}
									eventLogs = append(eventLogs, eventLog)
									if log_.Topics[0].String() == WITHDRAWAL_TOPIC {
										eventLogMap[eventLog] = struct{}{}
									}
								}
							}
						}
						evmMap := map[string]interface{}{
							"evm": map[string]string{
								"nonce": fmt.Sprintf("%v", transaction.Nonce()),
								"type":  fmt.Sprintf("%v", transaction.Type()),
							},
							"token": tokenInfo,
						}
						parseData, _ := json.Marshal(evmMap)
						feeData["gas_limit"] = fmt.Sprintf("%v", transaction.Gas())
						feeData["gas_used"] = fmt.Sprintf("%v", receipt.GasUsed)
						feeData["gas_price"] = transaction.GasPrice().String()
						feeData["base_fee"] = block.BaseFee().String()
						if transaction.Type() == types2.DynamicFeeTxType {
							feeData["max_fee_per_gas"] = transaction.GasFeeCap().String()
							feeData["max_priority_fee_per_gas"] = transaction.GasFeeCap().String()
						}
						feeAmount = fmt.Sprintf("%v", receipt.GasUsed*transaction.GasPrice().Uint64())
						status := types.STATUSPENDING
						if value, ok := types.Status[receipt.Status]; ok {
							status = value
						}
						bn, _ := strconv.Atoi(receipt.BlockNumber.String())
						fa, _ := decimal.NewFromString(feeAmount)
						at, _ := decimal.NewFromString(value)
						now := time.Now().Unix()
						eventLogJson, _ := json.Marshal(eventLogs)
						evmTransactionRecord := &data.EvmTransactionRecord{
							BlockHash:       blockHash,
							BlockNumber:     bn,
							Nonce:           int64(transaction.Nonce()),
							TransactionHash: transaction.Hash().String(),
							FromAddress:     fromAddress,
							ToAddress:       toAddress,
							FromUid:         fromUid,
							ToUid:           toUid,
							FeeAmount:       fa,
							Amount:          at,
							Status:          status,
							TxTime:          int64(block.Time()),
							ContractAddress: tokenInfo.Address,
							ParseData:       string(parseData),
							Type:            fmt.Sprintf("%v", transaction.Type()),
							GasLimit:        fmt.Sprintf("%v", transaction.Gas()),
							GasUsed:         fmt.Sprintf("%v", receipt.GasUsed),
							GasPrice:        transaction.GasPrice().String(),
							BaseFee:         block.BaseFee().String(),
							Data:            hex.EncodeToString(transaction.Data()),
							EventLog:        string(eventLogJson),
							TransactionType: transactionType,
							DappData:        "",
							ClientData:      "",
							CreatedAt:       now,
							UpdatedAt:       now,
						}

						txRecords = append(txRecords, evmTransactionRecord)
						if len(eventLogs) > 0 && transactionType == "contract" {
							for index, eventLog := range eventLogs {

								eventMap := map[string]interface{}{
									"evm": map[string]string{
										"nonce": fmt.Sprintf("%v", transaction.Nonce()),
										"type":  fmt.Sprintf("%v", transaction.Type()),
									},
									"token": eventLog.Token,
								}
								eventParseData, _ := json.Marshal(eventMap)
								//b, _ := json.Marshal(eventLog)
								txHash := transaction.Hash().String() + "#result-" + fmt.Sprintf("%v", index+1)
								txType := "eventLog"
								contractAddress := eventLog.Token.Address
								if _, ok := eventLogMap[eventLog]; ok {
									txType = "native"
									contractAddress = ""
								}
								evmlogTransactionRecord := &data.EvmTransactionRecord{
									BlockHash:       blockHash,
									BlockNumber:     bn,
									Nonce:           int64(transaction.Nonce()),
									TransactionHash: txHash,
									FromAddress:     eventLog.From,
									ToAddress:       eventLog.To,
									FromUid:         fromUid,
									ToUid:           toUid,
									FeeAmount:       fa,
									Amount:          at,
									Status:          status,
									TxTime:          int64(block.Time()),
									ContractAddress: contractAddress,
									ParseData:       string(eventParseData),
									Type:            fmt.Sprintf("%v", transaction.Type()),
									GasLimit:        fmt.Sprintf("%v", transaction.Gas()),
									GasUsed:         fmt.Sprintf("%v", receipt.GasUsed),
									GasPrice:        transaction.GasPrice().String(),
									BaseFee:         block.BaseFee().String(),
									Data:            hex.EncodeToString(transaction.Data()),
									TransactionType: txType,
									DappData:        "",
									ClientData:      "",
									CreatedAt:       now,
									UpdatedAt:       now,
								}
								txRecords = append(txRecords, evmlogTransactionRecord)

							}
						}
					}
				}

				if txRecords != nil && len(txRecords) > 0 {
					e := BatchSaveOrUpdate(txRecords, strings.ToLower(p.ChainName) + biz.TABLE_POSTFIX)
					if e != nil {
						// postgres出错 接入lark报警
						log.Error("插入数据到数据库库中失败", zap.Any("current", curHeight), zap.Any("chain", p.ChainName))
						alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库库中失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						return true
					}
				}
				//保存对应块高hash
				data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(curHeight), blockHash, biz.BLOCK_HASH_EXPIRATION_KEY)
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
	records, err := data.EvmTransactionRecordRepoClient.FindByStatus(nil, strings.ToLower(p.ChainName) + biz.TABLE_POSTFIX, types.STATUSPENDING)
	if err != nil {
		log.Error(p.ChainName+"查询数据库失败", zap.Any("error", err))
		return
	}
	log.Info(p.ChainName, zap.Any("records", records))
	url := p.NodeURL
	client, err := NewClient(url)
	if err != nil {
		return
	}
	defer client.Close()
	ctx := context.Background()
	for _, record := range records {
		txhash := strings.Split(record.TransactionHash, "#")[0]
		txByhash, _, err1 := client.GetTransactionByHash(ctx, common.HexToHash(txhash))
		for i := 1; err1 != nil && i <= 5; i++ {
			url_list := GetPreferentialUrl(p.UrlList, p.ChainName)
			if len(url_list) == 0 {
				alarmMsg := fmt.Sprintf("请注意：%s链目前没有可用rpcURL", p.ChainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, p.UrlList, alarmOpts)
			}
			for j := 0; err1 != nil && j < len(url_list); j++ {
				url = url_list[j].Key
				client, err1 = NewClient(url)
				if err1 != nil {
					continue
				}
				txByhash, _, err1 = client.GetTransactionByHash(ctx, common.HexToHash(txhash))

			}
			time.Sleep(time.Duration(3*i) * time.Second)
		}
		if err1 != nil || txByhash == nil {
			//判断nonce 是否小于 当前链上的nonce
			nonce, nonceErr := client.NonceAt(ctx, common.HexToAddress(record.FromAddress), nil)
			if nonceErr != nil {
				continue
			}
			if int(record.Nonce) < int(nonce) {
				record.Status = types.STATUSFAIL
				i, _ := data.EvmTransactionRecordRepoClient.Update(nil, strings.ToLower(p.ChainName) + biz.TABLE_POSTFIX, record)
				log.Info("更新txhash对象为终态", zap.Any("txId", record.TransactionHash), zap.Any("result", i))
				continue
			} else {
				now := time.Now().Unix()
				ctime := record.CreatedAt + 21600
				if ctime < now {
					record.Status = types.STATUSFAIL
					i, _ := data.EvmTransactionRecordRepoClient.Update(nil, strings.ToLower(p.ChainName) + biz.TABLE_POSTFIX, record)
					log.Info("更新txhash对象为终态:交易被抛弃", zap.Any("txId", record.TransactionHash), zap.Any("result", i))
					continue
				}
				continue
			}
		}

		receipt, err2 := client.GetTransactionReceipt(ctx, common.HexToHash(txhash))
		if err2 == ethereum.NotFound {
			record.Status = types.STATUSFAIL
			i, _ := data.EvmTransactionRecordRepoClient.Update(nil, strings.ToLower(p.ChainName) + biz.TABLE_POSTFIX, record)
			log.Info("更新txhash对象为终态", zap.Any("txId", record.TransactionHash), zap.Any("result", i))
			continue
		}
		if err2 != nil {
			continue
		}

		if receipt.Status == 1 {
			block, err3 := client.GetBlockByNumber(ctx, receipt.BlockNumber)
			if err3 != nil {
				continue
			}
			tx, isPending1, err11 := client.GetTransactionByHash(ctx, common.HexToHash(txhash))
			if err11 != nil || isPending1 {
				continue
			}
			var from common.Address
			var toAddress, feeAmount string
			var tokenInfo types.TokenInfo
			feeData := make(map[string]string)
			from, err = types2.Sender(types2.NewLondonSigner(tx.ChainId()), tx)
			if err != nil {
				from, err = types2.Sender(types2.HomesteadSigner{}, tx)
			}
			if tx.To() != nil {
				toAddress = tx.To().String()
			}
			fromAddress := from.String()
			value := tx.Value().String()
			transactionType := "native"
			if len(tx.Data()) >= 68 && tx.To() != nil {
				methodId := hex.EncodeToString(tx.Data()[:4])
				if methodId == "a9059cbb" || methodId == "095ea7b3" {
					toAddress = common.HexToAddress(hex.EncodeToString(tx.Data()[4:36])).String()
					amount := new(big.Int).SetBytes(tx.Data()[36:])
					if methodId == "a9059cbb" {
						transactionType = "transfer"
					} else {
						transactionType = "approve"
					}
					value = amount.String()
				} else if methodId == "23b872dd" {
					fromAddress = common.HexToAddress(hex.EncodeToString(tx.Data()[4:36])).String()
					toAddress = common.HexToAddress(hex.EncodeToString(tx.Data()[36:68])).String()
					amount := new(big.Int).SetBytes(tx.Data()[68:])
					transactionType = "transferfrom"
					value = amount.String()
				}

			}

			if tx.To() != nil {
				codeAt, err := client.CodeAt(ctx, common.HexToAddress(tx.To().String()), nil)
				for i := 1; err != nil && i <= 5; i++ {
					url_list := GetPreferentialUrl(p.UrlList, p.ChainName)
					if len(url_list) == 0 {
						alarmMsg := fmt.Sprintf("请注意：%s链目前没有可用rpcURL", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, p.UrlList, alarmOpts)
					}
					for j := 0; err != nil && j < len(url_list); j++ {
						url = url_list[j].Key
						client, err = NewClient(url)
						if err != nil {
							continue
						}
						codeAt, err = client.CodeAt(ctx, common.HexToAddress(tx.To().String()), nil)
					}
					time.Sleep(time.Duration(3*i) * time.Second)
				}
				if len(codeAt) > 0 {
					if transactionType == "native" {
						transactionType = "contract"
					} else {
						decimals, symbol, err := GetTokenDecimals(tx.To().String(), client)
						if err != nil || decimals == 0 || symbol == "" {
							transactionType = "contract"
						} else {
							tokenInfo = types.TokenInfo{Decimals: decimals, Amount: value, Symbol: symbol}
						}
						tokenInfo.Address = tx.To().String()
					}
				}
			}

			var eventLogs []types.EventLog
			eventLogMap := make(map[types.EventLog]struct{})
			if transactionType != "native" {
				for _, log := range receipt.Logs {
					if len(log.Topics) > 1 && (log.Topics[0].String() == TRANSFER_TOPIC ||
						log.Topics[0].String() == WITHDRAWAL_TOPIC) {
						var token types.TokenInfo
						amount := big.NewInt(0)
						if len(log.Data) >= 32 {
							amount = new(big.Int).SetBytes(log.Data[:32])
						}
						if log.Address.String() != "" {
							d, s, _ := GetTokenDecimals(log.Address.String(), client)
							token = types.TokenInfo{
								Address:  log.Address.String(),
								Decimals: d,
								Symbol:   s,
								Amount:   amount.String(),
							}
						}
						eventFrom := common.HexToAddress(log.Topics[1].String()).String()
						var to string
						if len(log.Topics) > 2 && log.Topics[0].String() == TRANSFER_TOPIC {
							to = common.HexToAddress(log.Topics[2].String()).String()
						} else if log.Topics[0].String() == WITHDRAWAL_TOPIC {
							to = fromAddress
							if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
								token.Symbol = token.Symbol[1:]
							}
						}
						eventLog := types.EventLog{
							From:   eventFrom,
							To:     to,
							Amount: amount,
							Token:  token,
						}
						eventLogs = append(eventLogs, eventLog)
						if log.Topics[0].String() == WITHDRAWAL_TOPIC {
							eventLogMap[eventLog] = struct{}{}
						}
					}
				}
			}

			feeData["gas_limit"] = fmt.Sprintf("%v", tx.Gas())
			feeData["gas_used"] = fmt.Sprintf("%v", receipt.GasUsed)
			feeData["gas_price"] = tx.GasPrice().String()
			feeData["base_fee"] = block.BaseFee().String()

			if tx.Type() == types2.DynamicFeeTxType {
				feeData["max_fee_per_gas"] = tx.GasFeeCap().String()
				feeData["max_priority_fee_per_gas"] = tx.GasFeeCap().String()
			}
			feeAmount = fmt.Sprintf("%v", receipt.GasUsed*tx.GasPrice().Uint64())
			status := types.STATUSPENDING
			if value, ok := types.Status[receipt.Status]; ok {
				status = value
			}

			now := time.Now().Unix()
			record.Status = status
			record.BlockHash = block.Hash().String()
			record.UpdatedAt = now
			record.ToAddress = toAddress
			record.FromAddress = feeAmount
			i, _ := data.EvmTransactionRecordRepoClient.Update(nil, strings.ToLower(p.ChainName) + biz.TABLE_POSTFIX, record)
			log.Info("更新txhash对象为终态", zap.Any("txId", record.TransactionHash), zap.Any("result", i))
		}
		if receipt.Status == 0 {
			record.Status = types.STATUSFAIL
			i, _ := data.EvmTransactionRecordRepoClient.Update(nil, strings.ToLower(p.ChainName) + biz.TABLE_POSTFIX, record)
			log.Info("更新txhash对象为终态", zap.Any("txId", record.TransactionHash), zap.Any("result", i))
		}
	}
}

func BatchSaveOrUpdate(txRecords []*data.EvmTransactionRecord, tableName string) error {
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

		_, err := data.EvmTransactionRecordRepoClient.BatchSaveOrUpdate(nil, tableName, subTxRecords)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.EvmTransactionRecordRepoClient.BatchSaveOrUpdate(nil, tableName, subTxRecords)
		}
		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY) {
			return err
		}
	}
	return nil
}
