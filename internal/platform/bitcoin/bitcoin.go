package bitcoin

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/model"
	"block-crawling/internal/platform/common"
	ncommon "gitlab.bixin.com/mili/node-driver/common"

	in "block-crawling/internal/types"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type Platform struct {
	biz.CommPlatform
	client    Client
	CoinIndex uint
	conf      *conf.PlatInfo
	spider    *chain.BlockSpider
}

func Init(handler string, value *conf.PlatInfo, nodeURL []string, height int) *Platform {
	chainType := value.Handler
	chainName := value.Chain

	clients := make([]chain.Clienter, 0, len(nodeURL))
	for _, url := range nodeURL {
		c := NewClient(url, chainName)
		clients = append(clients, &c)
	}
	spider := chain.NewBlockSpider(newStateStore(chainName), clients...)
	if len(value.StandbyRPCURL) > 0 {
		standby := make([]chain.Clienter, 0, len(value.StandbyRPCURL))
		for _, url := range value.StandbyRPCURL {
			c := NewClient(url, chainName)
			standby = append(standby, &c)
		}
		spider.AddStandby(standby...)
	}
	spider.Watch(common.NewDectorZapWatcher(chainName))

	return &Platform{
		CoinIndex: coins.HandleMap[handler],
		client:    NewClient(nodeURL[0], chainName),
		spider:    spider,
		conf:      value,
		CommPlatform: biz.CommPlatform{
			Height:         height,
			Chain:          chainType,
			ChainName:      chainName,
			HeightAlarmThr: int(value.GetMonitorHeightAlarmThr()),
		},
	}
}

func (p *Platform) Coin() coins.Coin {
	return coins.Coins[p.CoinIndex]
}

func (p *Platform) GetUTXOByHash(txHash string) (tx in.TX, err error) {
	p.spider.WithRetry(func(client chain.Clienter) error {
		tx, err = client.(*Client).GetTransactionByHash(txHash)
		if err != nil && err.Error() == "The requested resource has not been found" {
			return chain.RetryStandby(err)
		}
		return ncommon.Retry(err)
	})
	return
}

func (p *Platform) GetTransactions() {
	liveInterval := time.Duration(p.Coin().LiveInterval) * time.Millisecond
	log.Info(
		"GetTransactions starting, chainName:"+p.ChainName,
		zap.String("liveInterval", liveInterval.String()),
		zap.Bool("roundRobinConcurrent", p.conf.GetRoundRobinConcurrent()),
	)
	if p.conf.GetRoundRobinConcurrent() {
		p.spider.EnableRoundRobin()
	}
	p.spider.StartIndexBlock(
		newHandler(p.ChainName, liveInterval),
		int(p.conf.GetSafelyConcurrentBlockDelta()),
		int(p.conf.GetMaxConcurrency()),
	)
}

func (p *Platform) GetPendingTransactionsByInnerNode() {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("GetPendingTransactions error, chainName:"+p.ChainName, e)
			} else {
				log.Errore("GetPendingTransactions panic, chainName:"+p.ChainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链处理pending状态失败, error：%s", p.ChainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	pointsDataLimit := biz.PAGE_SIZE
	result, err := p.client.DispatchClient.GetMemoryPoolTXByNode()

	if err != nil || result.Error != nil {
		log.Error(p.ChainName+"获取为上链交易hash报错", zap.Any("error", err))
		return
	}
	if len(result.Result) == 0 {
		return
	}

	txIds := result.Result

	queryCount := 0
	start := time.Now()
	if pointsDataLimit < len(txIds) {
		part := len(txIds) / pointsDataLimit
		curr := len(txIds) % pointsDataLimit
		if curr > 0 {
			part += 1
		}
		for i := 0; i < part; i++ {
			var txBatchIds []string
			if i == part-1 {
				txBatchIds = txIds[0:curr]
				queryCount += p.SendMempoolTXIds(txBatchIds)

			} else {
				txBatchIds = txIds[0:pointsDataLimit]
				queryCount += p.SendMempoolTXIds(txBatchIds)
				txIds = txIds[pointsDataLimit:]
			}
		}
	} else {
		queryCount += p.SendMempoolTXIds(txIds)
	}
	log.Info(
		"HANDLED MEMPOOL FROM INNER NODE",
		zap.String("chainName", p.ChainName),
		zap.Int("numOfTxs", len(txIds)),
		zap.Int("numOfQueryTx", queryCount),
		zap.String("elapsed", time.Now().Sub(start).String()),
	)
}

func (p *Platform) SendMempoolTXIds(txIds []string) (queryCount int) {
	var txRecords []*data.BtcTransactionRecord
	now := time.Now().Unix()
	for _, txid := range txIds {
		redisTxid, _ := data.RedisClient.Get(data.PENDINGTX + p.ChainName + ":" + txid).Result()
		if len(redisTxid) != 0 {
			continue
		}

		queryCount++

		btcTx, err := p.client.DispatchClient.GetTransactionsByTXHash(txid)
		transactionHash := btcTx.Result.Txid

		if err != nil || btcTx.Error != nil {
			continue
		}
		//获取 vin 和vout地址
		var txddressList []string
		txddressList = make([]string, 0)

		voutMap := make(map[string]float64)

		var outAddressList []string
		outAddressList = make([]string, 0)
		for _, vout := range btcTx.Result.Vout {
			if vout.ScriptPubKey.Address != "" {
				txddressList = append(txddressList, vout.ScriptPubKey.Address)
				outAddressList = append(outAddressList, vout.ScriptPubKey.Address)
				voutMap[vout.ScriptPubKey.Address] = vout.Value
			}
		}

		vinMap := make(map[string]model.BTCTX)
		var vinAddressList []string
		vinAddressList = make([]string, 0)
		for _, vin := range btcTx.Result.Vin {
			preTxid := vin.Txid
			if preTxid != "" {
				voutIndex := vin.Vout
				queryCount++
				btcPreTx, err1 := p.client.DispatchClient.GetTransactionsByTXHash(preTxid)
				if err1 != nil || btcPreTx.Error != nil {
					log.Error(p.ChainName+"获取vin交易报错", zap.Any("error", err1))
					return
				}
				pvout := btcPreTx.Result.Vout[voutIndex]
				txddressList = append(txddressList, pvout.ScriptPubKey.Address)
				vinAddressList = append(vinAddressList, pvout.ScriptPubKey.Address)

				vinMap[pvout.ScriptPubKey.Address] = btcPreTx
			} else {
				// Coinbase单独在一笔交易里，转账金额随时间变化，该笔交易无矿工费
				txddressList = append(txddressList, "Coinbase")
				vinAddressList = append(vinAddressList, "Coinbase")
			}
		}

		addressExist := false
		var amount decimal.Decimal
		var fromAddress, toAddress, fromUid, toUid string

		for _, vin := range vinAddressList {
			exist, uid, err := biz.UserAddressSwitchRetryAlert(p.ChainName, vin)
			if err != nil {
				log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("txHash", transactionHash), zap.Any("error", err))
				return
			}
			if exist {
				//map中获取vin的 交易对象
				fromAddress = vin
				fromUid = uid
				addressExist = true

				for _, out := range outAddressList {
					if out != "" && fromAddress != out {
						toAddress = out
						value := voutMap[out]
						valueNum := decimal.NewFromFloat(value)
						// 需要乘以100000000(10的8次方)转成整型
						utxo := decimal.NewFromInt(100000000)
						amount = valueNum.Mul(utxo)

						break
					}
					break
				}
			}
		}

		if fromAddress == "" {
			for _, vout := range btcTx.Result.Vout {
				if vout.ScriptPubKey.Address != "" {
					exist, uid, err := biz.UserAddressSwitchRetryAlert(p.ChainName, vout.ScriptPubKey.Address)
					if err != nil {
						log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("txHash", transactionHash), zap.Any("error", err))
						return
					}
					if exist {
						toAddress = vout.ScriptPubKey.Address
						toUid = uid
						addressExist = true
						valueNum := decimal.NewFromFloat(vout.Value)
						// 需要乘以100000000(10的8次方)转成整型
						utxo := decimal.NewFromInt(100000000)
						amount = valueNum.Mul(utxo)
						for _, input := range vinAddressList {
							if input != "" {
								fromAddress = input
								break
							}
						}
						break
					}
				}
			}
		}

		if addressExist {
			btcTransactionRecord := &data.BtcTransactionRecord{
				BlockHash:       btcTx.Result.Blockhash,
				BlockNumber:     -1,
				TransactionHash: transactionHash,
				FromAddress:     fromAddress,
				ToAddress:       toAddress,
				FromUid:         fromUid,
				ToUid:           toUid,
				FeeAmount:       decimal.Zero,
				Amount:          amount,
				Status:          biz.PENDING,
				TxTime:          now,
				ConfirmCount:    0,
				DappData:        "",
				ClientData:      "",
				CreatedAt:       now,
				UpdatedAt:       now,
			}
			txRecords = append(txRecords, btcTransactionRecord)
		}
		data.RedisClient.Set(data.PENDINGTX+p.ChainName+":"+txid, txid, 3*time.Hour)
	}
	if txRecords != nil && len(txRecords) > 0 {
		//保存交易数据
		err := BatchSaveOrUpdate(txRecords, biz.GetTableName(p.ChainName))
		if err != nil {
			// postgres出错 接入lark报警
			log.Error(p.ChainName+"扫块，插入数据到数据库中失败", zap.Any("size", len(txRecords)))
			alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", p.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}
	return
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

	liveInterval := time.Duration(p.Coin().LiveInterval) * time.Millisecond
	p.spider.SealPendingTransactions(newHandler(p.ChainName, liveInterval))
}

func (p *Platform) GetBlockSpider() *chain.BlockSpider {
	return p.spider
}

func BatchSaveOrUpdate(txRecords []*data.BtcTransactionRecord, table string) error {
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

		_, err := data.BtcTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, table, subTxRecords)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.BtcTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, table, subTxRecords)
		}
		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY) {
			return err
		}
	}
	return nil
}
