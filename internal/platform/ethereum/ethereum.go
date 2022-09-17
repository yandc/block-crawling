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
	"errors"
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
const DEPOST_TOPIC = "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"
const BLOCK_NO_TRANSCATION = "server returned empty transaction list but block header indicates transactions"
const BLOCK_NONAL_TRANSCATION = "server returned non-empty transaction list but block header indicates no transactions"

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
			defer func() {
				if err := recover(); err != nil {
					if e, ok := err.(error); ok {
						log.Errore("GetPreferentialUrl error, chainName:"+chainName, e)
					} else {
						log.Errore("GetPreferentialUrl panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
					}
				}
			}()

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
	url := p.NodeURL
	client, err := NewClient(url)
	if err != nil {
		log.Warn("客户端连接构建失败", zap.Any("nodeUrl", p.NodeURL), zap.Any("error", err))
		return true
	}
	defer client.Close()
	ctx := context.Background()
	heightu, err := client.GetBlockNumber(ctx)
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
			heightu, err = client.GetBlockNumber(ctx)
		}
		time.Sleep(time.Duration(3*i) * time.Second)
	}
	height := int(heightu)
	data.RedisClient.Set(biz.BLOCK_NODE_HEIGHT_KEY+p.ChainName, height, 0)

	//获取本地当前块高
	curHeight := -1
	preDBBlockHash := make(map[int]string)
	redisHeight, _ := data.RedisClient.Get(biz.BLOCK_HEIGHT_KEY + p.ChainName).Result()
	if redisHeight != "" {
		curHeight, _ = strconv.Atoi(redisHeight)
	} else {
		lastRecord, err := data.EvmTransactionRecordRepoClient.FindLast(ctx, biz.GetTalbeName(p.ChainName))
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			lastRecord, err = data.EvmTransactionRecordRepoClient.FindLast(ctx, biz.GetTalbeName(p.ChainName))
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
		block, err := client.GetBlockByNumber(ctx, big.NewInt(int64(curHeight)))
		for i := 1; err != nil && fmt.Sprintf("%s", err) != BLOCK_NO_TRANSCATION && fmt.Sprintf("%s", err) != BLOCK_NONAL_TRANSCATION && i <= 5; i++ {
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
		//
		if err != nil && fmt.Sprintf("%s", err) != BLOCK_NO_TRANSCATION && fmt.Sprintf("%s", err) != BLOCK_NONAL_TRANSCATION {
			log.Warn("请注意：未查到块高数据", zap.Any("chain", p.ChainName), zap.Any("height", curHeight), zap.Any("error", err))
		}

		if block != nil {
			data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(curHeight), block.Hash().String(), biz.BLOCK_HASH_EXPIRATION_KEY)
			forked := false
			//3
			preHeight := curHeight - 1
			preHash := block.ParentHash().String()
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
				curHeight = preHeight
				rows, _ := data.EvmTransactionRecordRepoClient.DeleteByBlockNumber(ctx, biz.GetTalbeName(p.ChainName), preHeight+1)
				log.Info("出现分叉回滚数据", zap.Any("链类型", p.ChainName), zap.Any("共删除数据", rows), zap.Any("回滚到块高", preHeight))
			} else {
				blockHash := ""
				var txRecords []*data.EvmTransactionRecord
				now := time.Now().Unix()
				for _, transaction := range block.Transactions() {
					var from common.Address
					var toAddress, feeAmount string
					var tokenInfo types.TokenInfo
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
						log.Info("查询redis缓存报错：用户中心获取", zap.Any(p.ChainName, fromAddress), zap.Any("error", errr))
						return true
					}
					userToAddress, toUid, errt := biz.UserAddressSwitch(toAddress)
					if errt != nil {
						// redis出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户地址失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Info("查询redis缓存报错：用户中心获取", zap.Any(p.ChainName, toAddress), zap.Any("error", errt))
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
						for i := 1; err != nil && err != ethereum.NotFound && i <= 5; i++ {
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
							if err == ethereum.NotFound {
								continue
							} else {
								log.Error(p.ChainName+"扫块，从链上获取交易receipt失败", zap.Any("curHeight", curHeight), zap.Any("new", height), zap.Any("error", err))
								return true
							}
						}
						if blockHash == "" {
							blockHash = receipt.BlockHash
						}

						var eventLogs []types.EventLog
						if transactionType != "native" {
							for _, log_ := range receipt.Logs {
								if len(log_.Topics) > 1 && (log_.Topics[0].String() == TRANSFER_TOPIC ||
									log_.Topics[0].String() == WITHDRAWAL_TOPIC || log_.Topics[0].String() == DEPOST_TOPIC) {
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
									//判断合约 转账， 提现， 兑换。
									if len(log_.Topics) > 2 && log_.Topics[0].String() == TRANSFER_TOPIC {
										to = common.HexToAddress(log_.Topics[2].String()).String()
									} else if log_.Topics[0].String() == WITHDRAWAL_TOPIC {
										//提现，判断 用户无需话费value 判断value是否为0
										if value == "0" {
											to = fromAddress
											if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
												token.Symbol = token.Symbol[1:]
											}
										} else {
											to = toAddress
										}
									} else if log_.Topics[0].String() == DEPOST_TOPIC {
										//兑换时判断 交易金额不能为 0
										//判断 value是否为0 不为 0 则增加记录
										to = common.HexToAddress(log_.Topics[1].String()).String()
										if value != "0" {
											eventFrom = fromAddress
											if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
												token.Symbol = token.Symbol[1:]
											}
										} else {
											eventFrom = toAddress
										}
									}

									eventLog := types.EventLog{
										From:   eventFrom,
										To:     to,
										Amount: amount,
										Token:  token,
									}
									eventLogs = append(eventLogs, eventLog)
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
						gasUsedInt, _ := utils.HexStringToInt(receipt.GasUsed)
						gasUsed := gasUsedInt.String()
						gasPriceInt := transaction.GasPrice()
						if receipt.EffectiveGasPrice != "" {
							gasPriceInt, _ = utils.HexStringToInt(receipt.EffectiveGasPrice)
						}
						gasPrice := gasPriceInt.String()
						var baseFee string
						var maxFeePerGas string
						var maxPriorityFeePerGas string
						if block.BaseFee() != nil {
							baseFee = block.BaseFee().String()
						}
						if transaction.Type() == types2.DynamicFeeTxType {
							if transaction.GasFeeCap() != nil {
								maxFeePerGas = transaction.GasFeeCap().String()
							}
							if transaction.GasTipCap() != nil {
								maxPriorityFeePerGas = transaction.GasTipCap().String()
							}
						}
						feeAmount = new(big.Int).Mul(gasUsedInt, gasPriceInt).String()
						status := biz.PENDING
						if receipt.Status == "0x0" {
							status = biz.FAIL
						} else if receipt.Status == "0x1" {
							status = biz.SUCCESS
						}
						intBlockNumber, _ := utils.HexStringToInt(receipt.BlockNumber)
						bn := int(intBlockNumber.Int64())
						fa, _ := decimal.NewFromString(feeAmount)
						at, _ := decimal.NewFromString(value)
						var eventLog string
						if eventLogs != nil {
							eventLogJson, _ := json.Marshal(eventLogs)
							eventLog = string(eventLogJson)
						}
						evmTransactionRecord := &data.EvmTransactionRecord{
							BlockHash:            blockHash,
							BlockNumber:          bn,
							Nonce:                int64(transaction.Nonce()),
							TransactionHash:      transaction.Hash().String(),
							FromAddress:          fromAddress,
							ToAddress:            toAddress,
							FromUid:              fromUid,
							ToUid:                toUid,
							FeeAmount:            fa,
							Amount:               at,
							Status:               status,
							TxTime:               int64(block.Time()),
							ContractAddress:      tokenInfo.Address,
							ParseData:            string(parseData),
							Type:                 fmt.Sprintf("%v", transaction.Type()),
							GasLimit:             fmt.Sprintf("%v", transaction.Gas()),
							GasUsed:              gasUsed,
							GasPrice:             gasPrice,
							BaseFee:              baseFee,
							MaxFeePerGas:         maxFeePerGas,
							MaxPriorityFeePerGas: maxPriorityFeePerGas,
							Data:                 hex.EncodeToString(transaction.Data()),
							EventLog:             eventLog,
							TransactionType:      transactionType,
							DappData:             "",
							ClientData:           "",
							CreatedAt:            now,
							UpdatedAt:            now,
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
								evmlogTransactionRecord := &data.EvmTransactionRecord{
									BlockHash:            blockHash,
									BlockNumber:          bn,
									Nonce:                int64(transaction.Nonce()),
									TransactionHash:      txHash,
									FromAddress:          eventLog.From,
									ToAddress:            eventLog.To,
									FromUid:              fromUid,
									ToUid:                toUid,
									FeeAmount:            fa,
									Amount:               at,
									Status:               status,
									TxTime:               int64(block.Time()),
									ContractAddress:      contractAddress,
									ParseData:            string(eventParseData),
									Type:                 fmt.Sprintf("%v", transaction.Type()),
									GasLimit:             fmt.Sprintf("%v", transaction.Gas()),
									GasUsed:              gasUsed,
									GasPrice:             gasPrice,
									BaseFee:              baseFee,
									MaxFeePerGas:         maxFeePerGas,
									MaxPriorityFeePerGas: maxPriorityFeePerGas,
									Data:                 hex.EncodeToString(transaction.Data()),
									TransactionType:      txType,
									DappData:             "",
									ClientData:           "",
									CreatedAt:            now,
									UpdatedAt:            now,
								}
								txRecords = append(txRecords, evmlogTransactionRecord)
							}
						}
					}
				}

				if txRecords != nil && len(txRecords) > 0 {
					e := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(p.ChainName))
					if e != nil {
						// postgres出错 接入lark报警
						log.Error("插入数据到数据库库中失败", zap.Any("current", curHeight), zap.Any("chain", p.ChainName))
						alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库库中失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Info("插入数据库报错：", zap.Any(p.ChainName, e))
						return true
					}
					go HandleRecord(p.ChainName, client, txRecords)
				}

				if blockHash == "" {
					if p.ChainName == "OEC" || p.ChainName == "Optimism" || p.ChainName == "Cronos" || p.ChainName == "Polygon" ||
						p.ChainName == "Fantom" || p.ChainName == "Avalanche" || p.ChainName == "Klaytn" || p.ChainName == "xDai" ||
						p.ChainName == "OECTEST" || p.ChainName == "OptimismTEST" || p.ChainName == "CronosTEST" || p.ChainName == "PolygonTEST" ||
						p.ChainName == "FantomTEST" || p.ChainName == "AvalancheTEST" || p.ChainName == "KlaytnTEST" || p.ChainName == "xDaiTEST" {
						if len(block.Transactions()) > 0 {
							receipt, err := client.GetTransactionReceipt(ctx, block.Transactions()[0].Hash())
							for i := 1; err != nil && err != ethereum.NotFound && i <= 5; i++ {
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
									receipt, err = client.GetTransactionReceipt(ctx, block.Transactions()[0].Hash())
								}
								time.Sleep(time.Duration(3*i) * time.Second)
							}
							if err == nil {
								blockHash = receipt.BlockHash
							} else if err != ethereum.NotFound {
								log.Error(p.ChainName+"扫块，从链上获取交易receipt失败", zap.Any("curHeight", curHeight), zap.Any("new", height), zap.Any("error", err))
								return true
							}
						}
					} else {
						blockHash = block.Hash().String()
					}
				}
				//保存对应块高hash
				data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(curHeight), blockHash, biz.BLOCK_HASH_EXPIRATION_KEY)
			}
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

	records, err := data.EvmTransactionRecordRepoClient.FindByStatus(nil, biz.GetTalbeName(p.ChainName), biz.PENDING, biz.NO_STATUS)
	if err != nil {
		log.Error(p.ChainName+"查询数据库失败", zap.Any("error", err))
		return
	}
	log.Info(p.ChainName, zap.Any("records", records))
	var txRecords []*data.EvmTransactionRecord
	now := time.Now().Unix()

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
		for i := 1; err1 != nil && err1 != ethereum.NotFound && i <= 5; i++ {
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
		if err1 != nil && err1 != ethereum.NotFound {
			continue
		}

		if txByhash == nil {
			//判断nonce 是否小于 当前链上的nonce
			nonce, nonceErr := client.NonceAt(ctx, common.HexToAddress(record.FromAddress), nil)
			if nonceErr != nil {
				continue
			}
			if int(record.Nonce) < int(nonce) {
				record.Status = biz.DROPPED_REPLACED
				txRecords = append(txRecords, record)
				log.Info("更新txhash对象为丢弃置换状态", zap.Any("txId", record.TransactionHash))
				continue
			} else {
				now := time.Now().Unix()
				ctime := record.CreatedAt + 21600
				if ctime < now {
					record.Status = biz.FAIL
					txRecords = append(txRecords, record)
					log.Info("更新txhash对象为终态:交易被抛弃", zap.Any("txId", record.TransactionHash))
					continue
				} else {
					record.Status = biz.NO_STATUS
					txRecords = append(txRecords, record)
					log.Info("更新txhash对象无状态", zap.Any("txId", record.TransactionHash))
					continue
				}
			}
		}

		receipt, err := client.GetTransactionReceipt(ctx, common.HexToHash(txhash))
		if err != nil && err != ethereum.NotFound{
			continue
		}

		if receipt == nil {
			//判断nonce 是否小于 当前链上的nonce
			nonce, nonceErr := client.NonceAt(ctx, common.HexToAddress(record.FromAddress), nil)
			if nonceErr != nil {
				continue
			}
			if int(record.Nonce) < int(nonce) {
				record.Status = biz.DROPPED_REPLACED
				txRecords = append(txRecords, record)
				log.Info("更新txhash对象为丢弃置换状态", zap.Any("txId", record.TransactionHash))
				continue
			} else {
				now := time.Now().Unix()
				ctime := record.CreatedAt + 21600
				if ctime < now {
					record.Status = biz.FAIL
					txRecords = append(txRecords, record)
					log.Info("更新txhash对象为终态:交易被抛弃", zap.Any("txId", record.TransactionHash))
					continue
				} else {
					record.Status = biz.NO_STATUS
					txRecords = append(txRecords, record)
					log.Info("更新txhash对象无状态", zap.Any("txId", record.TransactionHash))
					continue
				}
			}
		}



		curHeight, _ := utils.HexStringToInt(receipt.BlockNumber)
		block, err := client.GetBlockByNumber(ctx, curHeight)
		for i := 1; err != nil && fmt.Sprintf("%s", err) != BLOCK_NO_TRANSCATION && fmt.Sprintf("%s", err) != BLOCK_NONAL_TRANSCATION && i <= 5; i++ {
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
				block, err = client.GetBlockByNumber(ctx, curHeight)
			}
			time.Sleep(time.Duration(3*i) * time.Second)
		}

		blockHash := ""
		for _, transaction := range block.Transactions() {
			if transaction.Hash().String() != record.TransactionHash {
				continue
			}

			var from common.Address
			var toAddress, feeAmount string
			var tokenInfo types.TokenInfo
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
				log.Info("查询redis缓存报错：用户中心获取", zap.Any(p.ChainName, fromAddress), zap.Any("error", errr))
				return
			}
			userToAddress, toUid, errt := biz.UserAddressSwitch(toAddress)
			if errt != nil {
				// redis出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户地址失败", p.ChainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Info("查询redis缓存报错：用户中心获取", zap.Any(p.ChainName, toAddress), zap.Any("error", errt))
				return
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

				/*receipt, err := client.GetTransactionReceipt(ctx, transaction.Hash())
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
				}*/
				if blockHash == "" {
					blockHash = receipt.BlockHash
				}

				var eventLogs []types.EventLog
				if transactionType != "native" {
					for _, log_ := range receipt.Logs {
						if len(log_.Topics) > 1 && (log_.Topics[0].String() == TRANSFER_TOPIC ||
							log_.Topics[0].String() == WITHDRAWAL_TOPIC || log_.Topics[0].String() == DEPOST_TOPIC) {
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
							//判断合约 转账， 提现， 兑换。
							if len(log_.Topics) > 2 && log_.Topics[0].String() == TRANSFER_TOPIC {
								to = common.HexToAddress(log_.Topics[2].String()).String()
							} else if log_.Topics[0].String() == WITHDRAWAL_TOPIC {
								//提现，判断 用户无需话费value 判断value是否为0
								if value == "0" {
									to = fromAddress
									if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
										token.Symbol = token.Symbol[1:]
									}
								} else {
									to = toAddress
								}
							} else if log_.Topics[0].String() == DEPOST_TOPIC {
								//兑换时判断 交易金额不能为 0
								//判断 value是否为0 不为 0 则增加记录
								to = common.HexToAddress(log_.Topics[1].String()).String()
								if value != "0" {
									eventFrom = fromAddress
									if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
										token.Symbol = token.Symbol[1:]
									}
								} else {
									eventFrom = toAddress
								}
							}

							eventLog := types.EventLog{
								From:   eventFrom,
								To:     to,
								Amount: amount,
								Token:  token,
							}
							eventLogs = append(eventLogs, eventLog)
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
				gasUsedInt, _ := utils.HexStringToInt(receipt.GasUsed)
				gasUsed := gasUsedInt.String()
				gasPriceInt := transaction.GasPrice()
				if receipt.EffectiveGasPrice != "" {
					gasPriceInt, _ = utils.HexStringToInt(receipt.EffectiveGasPrice)
				}
				gasPrice := gasPriceInt.String()
				var baseFee string
				var maxFeePerGas string
				var maxPriorityFeePerGas string
				if block.BaseFee() != nil {
					baseFee = block.BaseFee().String()
				}
				if transaction.Type() == types2.DynamicFeeTxType {
					if transaction.GasFeeCap() != nil {
						maxFeePerGas = transaction.GasFeeCap().String()
					}
					if transaction.GasTipCap() != nil {
						maxPriorityFeePerGas = transaction.GasTipCap().String()
					}
				}
				feeAmount = new(big.Int).Mul(gasUsedInt, gasPriceInt).String()
				status := biz.PENDING
				if receipt.Status == "0x0" {
					status = biz.FAIL
				} else if receipt.Status == "0x1" {
					status = biz.SUCCESS
				}
				intBlockNumber, _ := utils.HexStringToInt(receipt.BlockNumber)
				bn := int(intBlockNumber.Int64())
				fa, _ := decimal.NewFromString(feeAmount)
				at, _ := decimal.NewFromString(value)
				var eventLog string
				if eventLogs != nil {
					eventLogJson, _ := json.Marshal(eventLogs)
					eventLog = string(eventLogJson)
				}
				evmTransactionRecord := &data.EvmTransactionRecord{
					BlockHash:            blockHash,
					BlockNumber:          bn,
					Nonce:                int64(transaction.Nonce()),
					TransactionHash:      transaction.Hash().String(),
					FromAddress:          fromAddress,
					ToAddress:            toAddress,
					FromUid:              fromUid,
					ToUid:                toUid,
					FeeAmount:            fa,
					Amount:               at,
					Status:               status,
					TxTime:               int64(block.Time()),
					ContractAddress:      tokenInfo.Address,
					ParseData:            string(parseData),
					Type:                 fmt.Sprintf("%v", transaction.Type()),
					GasLimit:             fmt.Sprintf("%v", transaction.Gas()),
					GasUsed:              gasUsed,
					GasPrice:             gasPrice,
					BaseFee:              baseFee,
					MaxFeePerGas:         maxFeePerGas,
					MaxPriorityFeePerGas: maxPriorityFeePerGas,
					Data:                 hex.EncodeToString(transaction.Data()),
					EventLog:             eventLog,
					TransactionType:      transactionType,
					DappData:             "",
					ClientData:           "",
					CreatedAt:            now,
					UpdatedAt:            now,
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
						evmlogTransactionRecord := &data.EvmTransactionRecord{
							BlockHash:            blockHash,
							BlockNumber:          bn,
							Nonce:                int64(transaction.Nonce()),
							TransactionHash:      txHash,
							FromAddress:          eventLog.From,
							ToAddress:            eventLog.To,
							FromUid:              fromUid,
							ToUid:                toUid,
							FeeAmount:            fa,
							Amount:               at,
							Status:               status,
							TxTime:               int64(block.Time()),
							ContractAddress:      contractAddress,
							ParseData:            string(eventParseData),
							Type:                 fmt.Sprintf("%v", transaction.Type()),
							GasLimit:             fmt.Sprintf("%v", transaction.Gas()),
							GasUsed:              gasUsed,
							GasPrice:             gasPrice,
							BaseFee:              baseFee,
							MaxFeePerGas:         maxFeePerGas,
							MaxPriorityFeePerGas: maxPriorityFeePerGas,
							Data:                 hex.EncodeToString(transaction.Data()),
							TransactionType:      txType,
							DappData:             "",
							ClientData:           "",
							CreatedAt:            now,
							UpdatedAt:            now,
						}
						txRecords = append(txRecords, evmlogTransactionRecord)
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
		go handleUserNonce(p.ChainName,txRecords)
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

		_, err := data.EvmTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.EvmTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		}
		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY) {
			return err
		}
	}
	return nil
}
