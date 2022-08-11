package starcoin

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/subhandle"
	"block-crawling/internal/types"
	"encoding/json"
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
				log.Errore("IndexBlock error, chainName:{}"+p.ChainName, e)
			} else {
				log.Errore("IndexBlock panic, chainName:{}"+p.ChainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链爬块失败, error：%s", p.ChainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	height, err := p.client.GetBlockHeight()
	if err != nil {
		return true
	}
	curHeight := -1
	preDBBlockHash := make(map[int]string)
	redisHeight, _ := data.RedisClient.Get(biz.BLOCK_HEIGHT_KEY + p.ChainName).Result()
	if redisHeight != "" {
		curHeight, _ = strconv.Atoi(redisHeight)
	} else {
		row, _ := data.StcTransactionRecordRepoClient.FindLast(nil,strings.ToLower(p.ChainName) + biz.TABLE_POSTFIX)
		if row != nil && row.BlockNumber != 0 {
			curHeight = row.BlockNumber + 1
			preDBBlockHash[row.BlockNumber] = row.BlockHash
		}
	}
	if curHeight == -1 {
		return true
	}
	heightNum, _ := strconv.Atoi(height)
	if curHeight <= heightNum {
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
				rows, _ := data.StcTransactionRecordRepoClient.DeleteByBlockNumber(nil, strings.ToLower(p.ChainName) + biz.TABLE_POSTFIX, preHeight + 1)
				log.Info("出现分叉回滚数据", zap.Any("链类型", p.ChainName), zap.Any("共删除数据", rows), zap.Any("回滚到块高", preHeight))
			} else {
				var txRecords []*data.StcTransactionRecord
				for _, userTransaction := range block.BlockBody.UserTransactions {
					var tokenInfo types.TokenInfo
					var contractAddress string
					txType := "native"
					var toAddress string
					var amount string
					argsLen := len(userTransaction.RawTransaction.DecodedPayload.ScriptFunction.Args)
					if argsLen > 0 {
						if strings.HasPrefix(userTransaction.RawTransaction.DecodedPayload.ScriptFunction.Function, "peer_to_peer") {
							toAddress = fmt.Sprintf("%v", userTransaction.RawTransaction.DecodedPayload.ScriptFunction.Args[0])
							amount = fmt.Sprintf("%v", userTransaction.RawTransaction.DecodedPayload.ScriptFunction.Args[argsLen-1])
							if value, ok := userTransaction.RawTransaction.DecodedPayload.ScriptFunction.Args[argsLen-1].(float64); ok {
								amount = fmt.Sprintf("%.f", value)
							}
						}
					}
					if len(userTransaction.RawTransaction.DecodedPayload.ScriptFunction.TyArgs) > 0 {
						tyArgs := userTransaction.RawTransaction.DecodedPayload.ScriptFunction.TyArgs[0]
						if tyArgs != STC_CODE {
							mode := strings.Split(tyArgs, "::")
							if len(mode) == 3 {
								tokenInfo = types.TokenInfo{Address: tyArgs, Symbol: mode[2], Decimals: 9, Amount: amount}
								contractAddress = tyArgs
							}
						}
					}

					userFromAddress, fromUid, errr := biz.UserAddressSwitch(userTransaction.RawTransaction.Sender)
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
						transactionInfo, err := p.client.GetTransactionInfoByHash(userTransaction.TransactionHash)
						for i := 0; i < 10 && err != nil; i++ {
							time.Sleep(time.Duration(i*5) * time.Second)
							transactionInfo, err = p.client.GetTransactionInfoByHash(userTransaction.TransactionHash)
						}
						if err != nil {
							return true
						}
						t, _ := strconv.ParseInt(block.BlockHeader.TimeStamp, 10, 64)
						gasUser, _ := strconv.Atoi(transactionInfo.GasUsed)
						gasPrice, _ := strconv.Atoi(userTransaction.RawTransaction.GasUnitPrice)
						stcMap := map[string]interface{}{
							"stc": map[string]string{
								"sequence_number": userTransaction.RawTransaction.SequenceNumber,
							},
							"token": tokenInfo,
						}
						parseData, _ := json.Marshal(stcMap)
						status := types.STATUSPENDING
						if string(transactionInfo.Status) == "Executed" || string(transactionInfo.Status) == "\"Executed\"" {
							status = types.STATUSSUCCESS
						} else {
							status = types.STATUSFAIL
						}
						amountValue, _ := strconv.Atoi(amount)
						now := time.Now().Unix()
						stcRecord := &data.StcTransactionRecord{
							BlockHash:       block.BlockHeader.BlockHash,
							BlockNumber:     curHeight,
							TransactionHash: userTransaction.TransactionHash,
							FromAddress:     userTransaction.RawTransaction.Sender,
							ToAddress:       toAddress,
							FromUid:         fromUid,
							ToUid:           toUid,
							FeeAmount:       decimal.NewFromInt(int64(gasUser * gasPrice)),
							Amount:          decimal.NewFromInt(int64(amountValue)),
							Status:          status,
							TxTime:          t / 1000,
							ContractAddress: contractAddress,
							ParseData:       string(parseData),
							GasLimit:        userTransaction.RawTransaction.MaxGasAmount,
							GasUsed:         transactionInfo.GasUsed,
							GasPrice:        userTransaction.RawTransaction.GasUnitPrice,
							Data:            "",
							TransactionType: txType,
							DappData:        "",
							ClientData:      "",
							CreatedAt:       now,
							UpdatedAt:       now,
						}
						txRecords = append(txRecords, stcRecord)
					}
				}
				if txRecords != nil && len(txRecords) > 0 {
					e := BatchSaveOrUpdate(txRecords,strings.ToLower(p.ChainName) + biz.TABLE_POSTFIX)
					if e != nil {
						// postgres出错 接入lark报警
						log.Error("插入数据到数据库库中失败", zap.Any("current", curHeight), zap.Any("chain", p.ChainName))
						alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库库中失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						return true
					}
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

	records, err := data.StcTransactionRecordRepoClient.FindByStatus(nil, strings.ToLower(p.ChainName) + biz.TABLE_POSTFIX,types.STATUSPENDING)
	if err != nil {
		log.Error("STC查询数据库失败", zap.Any("error", err))
		return
	}
	log.Info("", zap.Any("records", records))
	for _, record := range records {
		transactionInfo, err1 := p.client.GetTransactionInfoByHash(record.TransactionHash)
		if err1 != nil {
			continue
		}
		if string(transactionInfo.Status) == "Executed" || string(transactionInfo.Status) == "\"Executed\"" {
			record.Status = types.STATUSSUCCESS
			bn, _ := strconv.Atoi(transactionInfo.BlockNumber)
			record.BlockNumber = bn
			record.BlockHash = transactionInfo.BlockHash
			i, _ := data.StcTransactionRecordRepoClient.Update(nil, strings.ToLower(p.ChainName) + biz.TABLE_POSTFIX, record)
			log.Info("更新txhash对象为终态", zap.Any("txId", record.TransactionHash), zap.Any("result", i))

		} else {
			record.Status = types.STATUSFAIL
			i, _ := data.StcTransactionRecordRepoClient.Update(nil, strings.ToLower(p.ChainName) + biz.TABLE_POSTFIX, record)
			log.Info("更新txhash对象为终态", zap.Any("txId", record.TransactionHash), zap.Any("result", i))
		}
	}
}

func BatchSaveOrUpdate(txRecords []*data.StcTransactionRecord,table string) error {
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

		_, err := data.StcTransactionRecordRepoClient.BatchSaveOrUpdate(nil, table,subTxRecords)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.StcTransactionRecordRepoClient.BatchSaveOrUpdate(nil, table,subTxRecords)
		}
		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY) {
			return err
		}
	}
	return nil
}
