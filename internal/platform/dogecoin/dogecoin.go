package dogecoin

//import (
//	"block-crawling/internal/biz"
//	coins "block-crawling/internal/common"
//	"block-crawling/internal/data"
//	"block-crawling/internal/log"
//	"block-crawling/internal/subhandle"
//	"block-crawling/internal/types"
//	"context"
//	"errors"
//	"fmt"
//	"github.com/shopspring/decimal"
//	"go.uber.org/zap"
//	"strconv"
//	"strings"
//	"time"
//)
//
//const TO_TYPE = "utxo_output"
//const FROM_TYPE = "utxo_input"
//const FEE_TYPE = "fee"
//const SUCCESS_STATUS = "completed"
//
////TODO 兜底 GetTransactionResultByTxhash + 资产累计
//type Platform struct {
//	subhandle.CommPlatform
//	client    Client
//	CoinIndex uint
//}
//
//func Init(handler, chain, chainName string, nodeURL []string, height int) *Platform {
//	return &Platform{
//		CoinIndex:    coins.HandleMap[handler],
//		client:       NewClient(nodeURL[0]),
//		CommPlatform: subhandle.CommPlatform{Height: height, Chain: chain, ChainName: chainName},
//	}
//}
//
//func (p *Platform) Coin() coins.Coin {
//	return coins.Coins[p.CoinIndex]
//}
//
//func (p *Platform) GetTransactions() {
//	log.Info("GetTransactions starting, chainName:" + p.ChainName)
//	for true {
//		h := p.IndexBlock()
//		if h {
//			time.Sleep(5 * time.Second)
//		}
//	}
//}
//
//func (p *Platform) IndexBlock() bool {
//	defer func() {
//		if err := recover(); err != nil {
//			if e, ok := err.(error); ok {
//				log.Errore("GetTransactions error, chainName:"+p.ChainName, e)
//			} else {
//				log.Errore("GetTransactions panic, chainName:"+p.ChainName, errors.New(fmt.Sprintf("%s", err)))
//			}
//
//			// 程序出错 接入lark报警
//			alarmMsg := fmt.Sprintf("请注意：%s链爬块失败, error：%s", p.ChainName, fmt.Sprintf("%s", err))
//			alarmOpts := biz.WithMsgLevel("FATAL")
//			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
//			return
//		}
//	}()
//
//	ctx := context.Background()
//
//	//获取最新块高
//	height, err := p.client.GetBlockNumber(0)
//	//TODO 判断错误是否 限频
//	for i := 1; i < len(biz.AppConfig.DogeKey) && err != nil; i++ {
//		height, err = p.client.GetBlockNumber(i)
//	}
//	if err != nil || height <= 0 {
//		log.Error("doge主网扫块，从链上获取最新块高失败", zap.Any("new", height), zap.Any("error", err))
//		return true
//	}
//	//获取本地当前块高
//	curHeight := -1
//	preDBBlockHash := make(map[int]string)
//	redisHeight, _ := data.RedisClient.Get(biz.BLOCK_HEIGHT_KEY + p.ChainName).Result()
//	if redisHeight != "" {
//		curHeight, _ = strconv.Atoi(redisHeight)
//	} else {
//		lastRecord, err := data.BtcTransactionRecordRepoClient.FindLast(ctx, biz.GetTalbeName(p.ChainName))
//		for i := 0; i < 3 && err != nil; i++ {
//			time.Sleep(time.Duration(i*1) * time.Second)
//			lastRecord, err = data.BtcTransactionRecordRepoClient.FindLast(ctx, biz.GetTalbeName(p.ChainName))
//		}
//		if err != nil {
//			// postgres出错 接入lark报警
//			alarmMsg := fmt.Sprintf("请注意：%s链查询数据库中块高失败", p.ChainName)
//			alarmOpts := biz.WithMsgLevel("FATAL")
//			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
//			log.Error(p.ChainName+"扫块，从数据库中获取块高失败", zap.Any("curHeight", curHeight), zap.Any("new", height), zap.Any("error", err))
//			return true
//		}
//		if lastRecord == nil {
//			curHeight = height
//			log.Info(p.ChainName+"扫块，从数据库中获取的块高为空,默认从当前块爬取", zap.Any("curHeight", curHeight), zap.Any("new", height))
//		} else {
//			curHeight = lastRecord.BlockNumber + 1
//			preDBBlockHash[lastRecord.BlockNumber] = lastRecord.BlockHash
//		}
//	}
//	if curHeight > height {
//		return true
//	}
//	if curHeight <= height {
//		block, err := p.client.GetBlockByNumber(curHeight, 0)
//		for i := 1; i < len(biz.AppConfig.DogeKey) && err != nil; i++ {
//			block, err = p.client.GetBlockByNumber(curHeight, i)
//		}
//
//		forked := false
//		preHeight := curHeight - 1
//		//块间距小于安全区块距离
//		if height-curHeight <= 12 {
//			preHash := block.ParentId
//			curPreBlockHash, _ := data.RedisClient.Get(biz.BLOCK_HASH_KEY + p.ChainName + ":" + strconv.Itoa(preHeight)).Result()
//			if curPreBlockHash == "" {
//				bh := preDBBlockHash[preHeight]
//				if bh != "" {
//					curPreBlockHash = bh
//				}
//			}
//			for curPreBlockHash != "" && curPreBlockHash != preHash {
//				forked = true
//				pBlock, pErr := p.client.GetBlockByNumber(preHeight, 0)
//				for i := 1; i < len(biz.AppConfig.DogeKey) && pErr != nil; i++ {
//					block, pErr = p.client.GetBlockByNumber(curHeight, i)
//				}
//				preHash = pBlock.ParentId
//				preHeight = preHeight - 1
//				curPreBlockHash, _ = data.RedisClient.Get(biz.BLOCK_HASH_KEY + p.ChainName + ":" + strconv.Itoa(preHeight)).Result()
//			}
//		}
//		if forked {
//			curHeight = preHeight
//			rows, _ := data.BtcTransactionRecordRepoClient.DeleteByBlockNumber(ctx, biz.GetTalbeName(p.ChainName), preHeight+1)
//			log.Info("出现分叉回滚数据", zap.Any("链类型", p.ChainName), zap.Any("共删除数据", rows), zap.Any("回滚到块高", preHeight))
//		} else {
//			data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(curHeight), block.Id, biz.BLOCK_HASH_EXPIRATION_KEY)
//			var txRecords []*data.BtcTransactionRecord
//			now := time.Now().Unix()
//			for _, transaction := range block.Txs {
//				var fromAddress, toAddress, fromUid, toUid string
//				var toAmout, feeAmount int64
//				status := types.STATUSFAIL
//				if transaction.Status == SUCCESS_STATUS {
//					status = types.STATUSSUCCESS
//				}
//				txTime := transaction.Date
//				fromFlag := false
//				toFlag := false
//				addressExist:= false
//				input := make(map[types.Events]string)
//				output := make(map[types.Events]string)
//
//				for _, puts := range transaction.Events {
//					if puts.Type == FROM_TYPE {
//						_, uid, _ := biz.UserAddressSwitch(puts.Source)
//						//uid 判断是否是"" ,是的话就无需 处理
//						input[puts] = uid
//					}
//					if puts.Type == TO_TYPE {
//						_, uid, _ := biz.UserAddressSwitch(puts.Destination)
//						output[puts] = uid
//					}
//					if puts.Type == FEE_TYPE {
//						feeAmount = puts.Amount
//					}
//				}
//
//				for k, v := range input {
//					if v != "" {
//						fromAddress = k.Source
//						fromUid = v
//						fromFlag = true
//						addressExist = true
//					}
//					if fromFlag {
//						for outKey,_ := range  output{
//							if outKey.Destination != fromAddress{
//								toAddress = outKey.Destination
//								toAmout = outKey.Amount
//								break
//							}
//							//给自己转账
//							if toAddress == "" {
//								toAddress = outKey.Destination
//								toAmout = outKey.Amount
//								toUid = v
//							}
//						}
//						break
//					}
//				}
//				//vin 没有 ob 用户地址
//				if fromAddress == ""{
//					for k,v := range output{
//						if v != ""{
//							toAddress = k.Destination
//							toAmout = k.Amount
//							toUid = v
//							toFlag = true
//							addressExist = true
//
//						}
//						if toFlag {
//							for inputKey,_ := range input{
//								fromAddress = inputKey.Source
//								break
//							}
//							break
//						}
//					}
//				}
//				if addressExist {
//					btcTransactionRecord := &data.BtcTransactionRecord{
//						BlockHash:       block.Id,
//						BlockNumber:     block.Number,
//						TransactionHash: transaction.Id,
//						FromAddress:     fromAddress,
//						ToAddress:       toAddress,
//						FromUid:         fromUid,
//						ToUid:           toUid,
//						FeeAmount:       decimal.NewFromInt(feeAmount),
//						Amount:          decimal.NewFromInt(toAmout),
//						Status:          status,
//						TxTime:          int64(txTime),
//						ConfirmCount:    0,
//						CreatedAt:       now,
//						UpdatedAt:       now,
//					}
//					txRecords = append(txRecords, btcTransactionRecord)
//				}
//
//			}
//			if txRecords != nil && len(txRecords) > 0 {
//				//批量存储
//				err := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(p.ChainName))
//				if err != nil {
//
//					// postgres出错 接入lark报警
//					alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", p.ChainName)
//					alarmOpts := biz.WithMsgLevel("FATAL")
//					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
//					log.Error("主网扫块，将数据插入到数据库中失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
//					return true
//				}
//				go HandleRecord(p.ChainName, p.client, txRecords)
//			}
//
//		}
//
//	} else {
//		return true
//	}
//	data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, curHeight+1, 0)
//	return false
//}
//func (p *Platform) GetTransactionResultByTxhash() {
//	defer func() {
//		if err := recover(); err != nil {
//			if e, ok := err.(error); ok {
//				log.Errore("GetTransactionsResult error, chainName:"+p.ChainName, e)
//			} else {
//				log.Errore("GetTransactionsResult panic, chainName:"+p.ChainName, errors.New(fmt.Sprintf("%s", err)))
//			}
//
//			// 程序出错 接入lark报警
//			alarmMsg := fmt.Sprintf("请注意：%s链处理交易结果失败, error：%s", p.ChainName, fmt.Sprintf("%s", err))
//			alarmOpts := biz.WithMsgLevel("FATAL")
//			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
//			return
//		}
//	}()
//
//	//records, err := biz.GetTransactionFromDB("BTC", types.STATUSPENDING)
//	records, err := data.BtcTransactionRecordRepoClient.FindByStatus(nil, biz.GetTalbeName(p.ChainName), types.STATUSPENDING, types.NOSTATUS)
//	if err != nil {
//		log.Error("BTC查询数据库失败", zap.Any("error", err))
//		return
//	}
//
//	pointsDataLimit := biz.PAGE_SIZE
//
//	if pointsDataLimit < len(records) {
//		part := len(records) / pointsDataLimit
//		curr := len(records) % pointsDataLimit
//		if curr > 0 {
//			part += 1
//		}
//		var txBatch []*data.BtcTransactionRecord
//		for i := 0; i < part; i++ {
//			if i == part-1 {
//				txBatch = records[0:curr]
//				p.HandlerResult(txBatch)
//
//			} else {
//				txBatch = records[0:pointsDataLimit]
//				p.HandlerResult(txBatch)
//				records = records[pointsDataLimit:]
//			}
//		}
//	} else {
//		p.HandlerResult(records)
//	}
//}
//func (p *Platform) HandlerResult(param []*data.BtcTransactionRecord) {
//	var txRecords []*data.BtcTransactionRecord
//
//	for _, record := range param {
//		btcTx, err1 := p.client.GetTransactionsByTXHash(record.TransactionHash)
//		if err1 != nil {
//			continue
//		}
//		result := btcTx.Status
//		if result == SUCCESS_STATUS {
//			//未查出
//			record.Status = types.STATUSSUCCESS
//			record.BlockNumber = btcTx.BlockNumber
//			txRecords = append(txRecords, record)
//		}else {
//			record.Status = types.STATUSFAIL
//			txRecords = append(txRecords, record)
//		}
//	}
//
//	if txRecords != nil && len(txRecords) > 0 {
//		//保存交易数据
//		err := BatchSaveOrUpdate(txRecords, strings.ToLower(p.ChainName)+biz.TABLE_POSTFIX)
//		if err != nil {
//			// postgres出错 接入lark报警
//			log.Error("btc主网扫块，插入数据到数据库中失败", zap.Any("size", len(txRecords)))
//			alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", p.ChainName)
//			alarmOpts := biz.WithMsgLevel("FATAL")
//			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
//			return
//		}
//	}
//}
//
//func BatchSaveOrUpdate(txRecords []*data.BtcTransactionRecord, table string) error {
//	total := len(txRecords)
//	pageSize := biz.PAGE_SIZE
//	start := 0
//	stop := pageSize
//	if stop > total {
//		stop = total
//	}
//	for start < stop {
//		subTxRecords := txRecords[start:stop]
//		start = stop
//		stop += pageSize
//		if stop > total {
//			stop = total
//		}
//
//		_, err := data.BtcTransactionRecordRepoClient.BatchSaveOrUpdate(nil, table, subTxRecords)
//		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
//			time.Sleep(time.Duration(i*1) * time.Second)
//			_, err = data.BtcTransactionRecordRepoClient.BatchSaveOrUpdate(nil, table, subTxRecords)
//		}
//		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY) {
//			return err
//		}
//	}
//	return nil
//}