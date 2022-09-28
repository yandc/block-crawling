package bitcoin

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/model"
	"block-crawling/internal/subhandle"
	"block-crawling/internal/types"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

type Platform struct {
	subhandle.CommPlatform
	client    Client
	CoinIndex uint
}

func Init(handler string, value *conf.PlatInfo, nodeURL []string, height int) *Platform {
	chain := value.Handler
	chainName := value.Chain

	return &Platform{
		CoinIndex: coins.HandleMap[handler],
		client:    NewClient(nodeURL[0], chainName),
		CommPlatform: subhandle.CommPlatform{
			Height:         height,
			Chain:          chain,
			ChainName:      chainName,
			HeightAlarmThr: int(value.GetMonitorHeightAlarmThr()),
		},
	}
}

func (p *Platform) Coin() coins.Coin {
	return coins.Coins[p.CoinIndex]
}

func (p *Platform) GetTransactions() {
	log.Info("GetTransactions starting, chainName:" + p.ChainName)
	for true {
		if p.ChainName == "BTCTEST" {
			p.getBTCTestTransactions()
		}
		if p.ChainName == "BTC" || p.ChainName == "LTC" || p.ChainName == "DOGE" {
			p.getBTCTransactions()
		}
		time.Sleep(time.Duration(p.Coin().LiveInterval) * time.Millisecond)
	}
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

	memoryTxId := model.JsonRpcRequest{
		Jsonrpc: "1.0",
		Id:      "curltest",
		Method:  "getrawmempool",
	}
	var pa = make([]interface{}, 0, 0)

	memoryTxId.Params = pa

	result, err := p.client.GetMemoryPoolTXByNode(memoryTxId)
	if err != nil || result.Error != nil {
		log.Error(p.ChainName+"获取为上链交易hash报错", zap.Any("error", err))
		return
	}
	if len(result.Result) == 0 {
		return
	}

	txIds := result.Result

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
				p.SendMempoolTXIds(txBatchIds)

			} else {
				txBatchIds = txIds[0:pointsDataLimit]
				p.SendMempoolTXIds(txBatchIds)
				txIds = txIds[pointsDataLimit:]
			}
		}
	} else {
		p.SendMempoolTXIds(txIds)
	}
}

func (p *Platform) GetTransactionsByTXHash(txid string) (tx model.BTCTX, err error) {
	param := model.JsonRpcRequest{
		Jsonrpc: "1.0",
		Id:      "curltest",
		Method:  "getrawtransaction",
	}
	var pa = make([]interface{}, 0, 2)
	pa = append(pa, txid)
	pa = append(pa, 1)
	param.Params = pa
	return p.client.GetTransactionByPendingHashByNode(param)
}

func (p *Platform) GetBlockCount() (count model.BTCCount, err error) {
	countParam := model.JsonRpcRequest{
		Jsonrpc: "1.0",
		Id:      "curltest",
		Method:  "getblockcount",
	}
	var pa = make([]interface{}, 0, 0)
	countParam.Params = pa
	return p.client.GetBlockCount(countParam)
}

func (p *Platform) SendMempoolTXIds(txIds []string) {
	var txRecords []*data.BtcTransactionRecord
	now := time.Now().Unix()
	for _, txid := range txIds {
		redisTxid, _ := data.RedisClient.Get(data.PENDINGTX + p.ChainName + ":" + txid).Result()
		if len(redisTxid) != 0 {
			continue
		}

		btcTx, err := p.GetTransactionsByTXHash(txid)

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
				btcPreTx, err1 := p.GetTransactionsByTXHash(preTxid)
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
			exist, uid, err := biz.UserAddressSwitch(vin)
			if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
				// redis出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户地址失败", p.ChainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
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
					exist, uid, err := biz.UserAddressSwitch(vout.ScriptPubKey.Address)
					if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
						// redis出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户地址失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
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
				TransactionHash: btcTx.Result.Txid,
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
			data.RedisClient.Set(data.PENDINGTX+p.ChainName+":"+txid, txid, 60*time.Minute)
		}
	}
	if txRecords != nil && len(txRecords) > 0 {
		//保存交易数据
		err := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(p.ChainName))
		if err != nil {
			// postgres出错 接入lark报警
			log.Error(p.ChainName+"扫块，插入数据到数据库中失败", zap.Any("size", len(txRecords)))
			alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", p.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}
}

func (p *Platform) GetPendingTransactions() {
	//测试网走 公共节点
	pointsDataLimit := 50
	//获取
	txIds, err := p.client.GetMempoolTxIds()

	for i := 0; i < 10 && err != nil; i++ {
		time.Sleep(time.Duration(i*5) * time.Second)
		txIds, err = p.client.GetMempoolTxIds()
	}
	//err 判断
	if err != nil {
		log.Error(p.ChainName+"GetMempoolTxIdsRPC", zap.Any("error", err))
		return
	}
	//进行分批处理
	if pointsDataLimit < len(txIds) {
		part := len(txIds) / pointsDataLimit
		for i := 0; i < part; i++ {
			txBatchIds := txIds[0:pointsDataLimit]
			p.SendTXIds(txBatchIds)
			txIds = txIds[pointsDataLimit:]
		}
	} else {
		p.SendTXIds(txIds)
	}
}

func (p *Platform) SendTXIds(txIds []string) {
	var txRecords []*data.BtcTransactionRecord
	now := time.Now().Unix()
	for _, txid := range txIds {
		tx, err1 := p.client.GetTransactionByPendingHash(txid)
		for i := 0; i < 10 && err1 != nil; i++ {
			log.Warn(p.ChainName+"节点报错", zap.Any("chainName", p.ChainName), zap.Any("chainUrl", p.client.DispatchClient.StreamURL), zap.Any("err", err1))
			time.Sleep(time.Duration(i*5) * time.Second)
			tx, err1 = p.client.GetTransactionByPendingHash(txid)
		}
		if err1 != nil {
			continue
		}
		if tx.BlockHeight > 0 {
			continue
		}
		redisTxid, _ := data.RedisClient.Get(data.PENDINGTX + p.ChainName + ":" + txid).Result()
		if len(redisTxid) != 0 {
			continue
		}
		//判断地址是否是白名单地址
		var txs []types.TXByHash
		txs = make([]types.TXByHash, 0, 1)
		txs = append(txs, tx)

		//组装 transaction
		var addressList []string
		addressList = make([]string, 0)

		addressList = append(addressList, tx.Addresses...)
		addressExist := false
		var amount decimal.Decimal
		var fromAddress, toAddress, fromUid, toUid string

		for _, tXPendingInputs := range tx.Inputs {
			exist, uid, err := biz.UserAddressSwitch(tXPendingInputs.Addresses[0])
			if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
				// redis出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户地址失败", p.ChainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				return
			}
			if exist {
				fromAddress = tXPendingInputs.Addresses[0]
				fromUid = uid
				addressExist = true

				for _, tXPendingOutputs := range tx.Outputs {
					if tXPendingOutputs.Addresses != nil && tXPendingOutputs.Addresses[0] != "" && fromAddress != tXPendingOutputs.Addresses[0] {

						toAddress = tXPendingOutputs.Addresses[0]
						amount = decimal.NewFromInt(int64(tXPendingOutputs.Value))
						break
					}

					break
				}
			}
		}

		if fromAddress == "" {
			for _, tXPendingOutputs := range tx.Outputs {
				exist, uid, err := biz.UserAddressSwitch(tXPendingOutputs.Addresses[0])
				if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
					// redis出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户地址失败", p.ChainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					return
				}
				if exist {
					toAddress = tXPendingOutputs.Addresses[0]
					toUid = uid
					addressExist = true
					amount = decimal.NewFromInt(int64(tXPendingOutputs.Value))
					for _, input := range tx.Inputs {
						if input.Addresses[0] != "" {
							fromAddress = input.Addresses[0]
							break
						}
					}
					break
				}
			}
		}

		if addressExist {
			local, _ := time.LoadLocation("Asia/Shanghai")
			showTime, _ := time.ParseInLocation("2006-01-02T15:04:05.000Z", tx.Received, local)
			txTime := showTime.Unix()
			if txTime < 0 {
				txTime = time.Now().Unix()
			}

			btcTransactionRecord := &data.BtcTransactionRecord{
				BlockHash:       fmt.Sprintf("%d", -1),
				BlockNumber:     tx.BlockHeight,
				TransactionHash: tx.Hash,
				FromAddress:     fromAddress,
				ToAddress:       toAddress,
				FromUid:         fromUid,
				ToUid:           toUid,
				FeeAmount:       decimal.NewFromInt(int64(tx.Fees)),
				Amount:          amount,
				Status:          biz.PENDING,
				TxTime:          txTime,
				ConfirmCount:    0,
				DappData:        "",
				ClientData:      "",
				CreatedAt:       now,
				UpdatedAt:       now,
			}
			txRecords = append(txRecords, btcTransactionRecord)
		}

		//处理 数据 并存储到redia里面
		data.RedisClient.Set(data.PENDINGTX+p.ChainName+":"+txid, txid, 60*time.Minute)

	}
	if txRecords != nil && len(txRecords) > 0 {
		//保存交易数据
		err := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(p.ChainName))
		if err != nil {
			// postgres出错 接入lark报警
			log.Error(p.ChainName+"扫块，插入数据到数据库中失败", zap.Any("size", len(txRecords)))
			alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", p.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}
}

func (p *Platform) getBTCTransactions() {
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

	data.RedisClient.Set(biz.BLOCK_NODE_HEIGHT_KEY+p.ChainName, height, 0)
	//获取本地当前块高
	oldHeight := -1
	redisHeight, err := data.RedisClient.Get(biz.BLOCK_HEIGHT_KEY + p.ChainName).Result()
	for i := 0; i < 3 && err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		redisHeight, err = data.RedisClient.Get(biz.BLOCK_HEIGHT_KEY + p.ChainName).Result()
	}
	if err != nil {
		if fmt.Sprintf("%s", err) == biz.REDIS_NIL_KEY {
			// 从数据库中查询最新一条的数据
			lastRecord, err := data.BtcTransactionRecordRepoClient.FindLast(nil, biz.GetTalbeName(p.ChainName))
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				lastRecord, err = data.BtcTransactionRecordRepoClient.FindLast(nil, biz.GetTalbeName(p.ChainName))
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
				oldHeight = lastRecord.BlockNumber
				data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(oldHeight), lastRecord.BlockHash, biz.BLOCK_HASH_EXPIRATION_KEY)
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

		block, err := p.client.GetBTCBlockByNumber(curHeight)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			block, err = p.client.GetBTCBlockByNumber(curHeight)
		}
		if err != nil {
			log.Error(p.ChainName+"扫块，从链上获取区块信息失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
			return
		}

		// 处理分叉开始
		forked := false
		preBlockHash := block.PrevBlock
		preHeight := curHeight - 1
		redisPreBlockHash, err := data.RedisClient.Get(biz.BLOCK_HASH_KEY + p.ChainName + ":" + strconv.Itoa(preHeight)).Result()
		for i := 0; i < 3 && err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			redisPreBlockHash, err = data.RedisClient.Get(biz.BLOCK_HASH_KEY + p.ChainName + ":" + strconv.Itoa(preHeight)).Result()
		}
		if err != nil {
			if fmt.Sprintf("%s", err) == biz.REDIS_NIL_KEY {
				// 从数据库中查询指定块高的数据
				lastRecord, err := data.BtcTransactionRecordRepoClient.FindOneByBlockNumber(nil, biz.GetTalbeName(p.ChainName), preHeight)
				for i := 0; i < 3 && err != nil; i++ {
					time.Sleep(time.Duration(i*1) * time.Second)
					lastRecord, err = data.BtcTransactionRecordRepoClient.FindOneByBlockNumber(nil, biz.GetTalbeName(p.ChainName), preHeight)
				}
				if err != nil {
					// postgres出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询数据库中区块hash失败", p.ChainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(p.ChainName+"扫块，从数据库中获取区块hash失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
					return
				}
				if lastRecord == nil {
					log.Error(p.ChainName+"扫块，从数据库中获取的区块hash为空", zap.Any("current", curHeight), zap.Any("new", height))
					//return
				} else {
					redisPreBlockHash = lastRecord.BlockHash
				}
			} else {
				// redis出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链查询redis中区块hash失败", p.ChainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(p.ChainName+"扫块，从redis中获取区块hash失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
				return
			}
		}

		//判断是否产生了分叉
		for redisPreBlockHash != "" && redisPreBlockHash != preBlockHash {
			forked = true
			block, err = p.client.GetBTCBlockByNumber(preHeight)
			for i := 0; i < 10 && err != nil; i++ {
				time.Sleep(time.Duration(i*5) * time.Second)
				block, err = p.client.GetBTCBlockByNumber(preHeight)
			}
			if err != nil {
				//更新 redis中的块高和区块hash
				data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, preHeight, 0)
				data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(preHeight), preBlockHash, biz.BLOCK_HASH_EXPIRATION_KEY)

				log.Error(p.ChainName+"扫块，从链上获取区块信息失败", zap.Any("prevent", preHeight), zap.Any("new", height), zap.Any("error", err))
				return
			}

			preBlockHash = block.PrevBlock
			preHeight = preHeight - 1
			redisPreBlockHash, err = data.RedisClient.Get(biz.BLOCK_HASH_KEY + p.ChainName + ":" + strconv.Itoa(preHeight)).Result()
			for i := 0; i < 3 && err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				redisPreBlockHash, err = data.RedisClient.Get(biz.BLOCK_HASH_KEY + p.ChainName + ":" + strconv.Itoa(preHeight)).Result()
			}
			if err != nil {
				if fmt.Sprintf("%s", err) == biz.REDIS_NIL_KEY {
					// 从数据库中查询指定块高的数据
					lastRecord, err := data.BtcTransactionRecordRepoClient.FindOneByBlockNumber(nil, biz.GetTalbeName(p.ChainName), preHeight)
					for i := 0; i < 3 && err != nil; i++ {
						time.Sleep(time.Duration(i*1) * time.Second)
						lastRecord, err = data.BtcTransactionRecordRepoClient.FindOneByBlockNumber(nil, biz.GetTalbeName(p.ChainName), preHeight)
					}
					if err != nil {
						//更新 redis中的块高和区块hash
						data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, preHeight, 0)
						data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(preHeight), preBlockHash, biz.BLOCK_HASH_EXPIRATION_KEY)

						// postgres出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询数据库中区块hash失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(p.ChainName+"扫块，从数据库中获取区块hash失败", zap.Any("prevent", preHeight), zap.Any("new", height), zap.Any("error", err))
						return
					}
					if lastRecord == nil {
						log.Error(p.ChainName+"扫块，从数据库中获取的区块hash为空", zap.Any("prevent", preHeight), zap.Any("new", height))
						//return
					} else {
						redisPreBlockHash = lastRecord.BlockHash
					}
				} else {
					//更新 redis中的块高和区块hash
					data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, preHeight, 0)
					data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(preHeight), preBlockHash, biz.BLOCK_HASH_EXPIRATION_KEY)

					// redis出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询redis中区块hash失败", p.ChainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(p.ChainName+"扫块，从redis中获取区块hash失败", zap.Any("prevent", preHeight), zap.Any("new", height), zap.Any("error", err))
					return
				}
			}
		}

		curBlockHash := block.Hash
		curHeight = preHeight + 1
		//处理分叉上的孤块
		if forked {
			//删除 DB中 比curHeight 高的 块交易数据
			_, err := data.BtcTransactionRecordRepoClient.DeleteByBlockNumber(nil, biz.GetTalbeName(p.ChainName), curHeight)
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				_, err = data.BtcTransactionRecordRepoClient.DeleteByBlockNumber(nil, biz.GetTalbeName(p.ChainName), curHeight)
			}
			if err != nil {
				//更新 redis中的块高和区块hash
				data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, preHeight, 0)
				data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(preHeight), preBlockHash, biz.BLOCK_HASH_EXPIRATION_KEY)

				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链删除数据库中分叉孤块数据失败", p.ChainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(p.ChainName+"扫块，从数据库中删除分叉孤块数据失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
				return
			}
		}
		// 处理分叉结束

		var txRecords []*data.BtcTransactionRecord
		now := time.Now().Unix()
		for _, tx := range block.Tx {
			status := biz.PENDING
			if tx.BlockHeight > 0 {
				status = biz.SUCCESS
			}
			var amount decimal.Decimal
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool

			//openblock钱包转账中tx.Inputs只会有一笔
			if len(tx.Inputs) > 0{
				fromAddress = tx.Inputs[0].PrevOut.Addr
			}else {
				log.Error(p.ChainName+"扫块，txInputs为0", zap.Any("current", curHeight), zap.Any("new", height))
			}
			if fromAddress != "" {
				fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
				if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
					//更新 redis中的块高和区块hash
					data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, preHeight, 0)
					data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(preHeight), preBlockHash, biz.BLOCK_HASH_EXPIRATION_KEY)

					// redis出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
					return
				}
			}

			index := 0
			for _, out := range tx.Out {
				toAddress = out.Addr
				if fromAddress == toAddress {
					continue
				}
				if toAddress != "" {
					toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
					if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
						//更新 redis中的块高和区块hash
						data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, preHeight, 0)
						data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(preHeight), preBlockHash, biz.BLOCK_HASH_EXPIRATION_KEY)

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
					var txHash string
					if index == 1 {
						txHash = tx.Hash
					} else {
						txHash = tx.Hash + "#result-" + fmt.Sprintf("%v", index)
					}
					amount = decimal.NewFromInt(int64(out.Value))
					btcTransactionRecord := &data.BtcTransactionRecord{
						BlockHash:       curBlockHash,
						BlockNumber:     curHeight,
						TransactionHash: txHash,
						FromAddress:     fromAddress,
						ToAddress:       toAddress,
						FromUid:         fromUid,
						ToUid:           toUid,
						FeeAmount:       decimal.NewFromInt(int64(tx.Fee)),
						Amount:          amount,
						Status:          status,
						TxTime:          int64(tx.Time),
						ConfirmCount:    int32(height - curHeight),
						DappData:        "",
						ClientData:      "",
						CreatedAt:       now,
						UpdatedAt:       now,
					}
					txRecords = append(txRecords, btcTransactionRecord)
				}
			}
		}

		if txRecords != nil && len(txRecords) > 0 {
			//保存交易数据
			err := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(p.ChainName))
			if err != nil {
				//更新 redis中的块高和区块hash
				data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, preHeight, 0)
				data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(preHeight), preBlockHash, biz.BLOCK_HASH_EXPIRATION_KEY)

				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", p.ChainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error("btc主网扫块，将数据插入到数据库中失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
				return
			}
			go HandleRecord(p.ChainName, p.client, txRecords)
		}

		//更新 redis中的块高和区块hash
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
		result, err = data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(curHeight), curBlockHash, biz.BLOCK_HASH_EXPIRATION_KEY).Result()
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			result, err = data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(curHeight), curBlockHash, biz.BLOCK_HASH_EXPIRATION_KEY).Result()
		}
		if err != nil {
			// redis出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链插入区块hash到redis中失败", p.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(p.ChainName+"扫块，将区块hash插入到redis中失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("error", err))
			return
		}
		if result != "OK" {
			log.Error(p.ChainName+"扫块，将区块hash插入到redis中失败", zap.Any("current", curHeight), zap.Any("new", height), zap.Any("result", result))
		}
	}
}

func (p *Platform) getBTCTestTransactions() {
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

	height, err := p.client.GetBlockNumber()
	if err != nil || height <= 0 {
		return
	}
	oldHeight := p.HandlerHeight(height)
	if oldHeight == height || oldHeight == -1 {
		return
	}
	for ; oldHeight < height; oldHeight++ {
		block, err := p.client.GetTestBlockByHeight(oldHeight)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			block, err = p.client.GetTestBlockByHeight(oldHeight)
		}
		if err != nil {
			continue
		}
		var txRecords []*data.BtcTransactionRecord
		now := time.Now().Unix()
		for _, tx := range block {
			status := biz.PENDING
			if tx.Status.BlockHeight > 0 {
				status = biz.SUCCESS
			}
			addressExist := false
			var amount decimal.Decimal
			var fromAddress, toAddress, fromUid, toUid string
			for _, input := range tx.Vin {
				exist, uid, err := biz.UserAddressSwitch(input.Prevout.ScriptpubkeyAddress)
				if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
					//更新 redis中的块高和区块hash
					//data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, preHeight, 0)
					//data.RedisClient.Set(biz.BLOCK_HASH_KEY + p.ChainName+":"+strconv.Itoa(preHeight), preBlockHash, biz.BLOCK_HASH_EXPIRATION_KEY)

					// redis出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户地址失败", p.ChainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					return
				}
				if exist {
					fromAddress = input.Prevout.ScriptpubkeyAddress
					fromUid = uid
					addressExist = true
					for _, out := range tx.Vout {
						if out.ScriptpubkeyAddress != "" && fromAddress != out.ScriptpubkeyAddress {
							toAddress = out.ScriptpubkeyAddress
							amount = decimal.NewFromInt(int64(out.Value))
							break
						}
					}
					break
				}
			}
			if fromAddress == "" {
				for _, out := range tx.Vout {
					exist, uid, err := biz.UserAddressSwitch(out.ScriptpubkeyAddress)
					if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
						//更新 redis中的块高和区块hash
						//data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, preHeight, 0)
						//data.RedisClient.Set(biz.BLOCK_HASH_KEY + p.ChainName+":"+strconv.Itoa(preHeight), preBlockHash, biz.BLOCK_HASH_EXPIRATION_KEY)

						// redis出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户地址失败", p.ChainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						return
					}
					if exist {
						toAddress = out.ScriptpubkeyAddress
						toUid = uid
						addressExist = true
						amount = decimal.NewFromInt(int64(out.Value))
						for _, input := range tx.Vin {
							if input.Prevout.ScriptpubkeyAddress != "" {
								fromAddress = input.Prevout.ScriptpubkeyAddress
								break
							}
						}
						break
					}
				}
			}
			if addressExist {
				btcTransactionRecord := &data.BtcTransactionRecord{
					BlockHash:       tx.Status.BlockHash,
					BlockNumber:     tx.Status.BlockHeight,
					TransactionHash: tx.Txid,
					FromAddress:     fromAddress,
					ToAddress:       toAddress,
					FromUid:         fromUid,
					ToUid:           toUid,
					FeeAmount:       decimal.NewFromInt(int64(tx.Fee)),
					Amount:          amount,
					Status:          status,
					TxTime:          int64(tx.Status.BlockTime),
					ConfirmCount:    0,
					DappData:        "",
					ClientData:      "",
					CreatedAt:       now,
					UpdatedAt:       now,
				}
				txRecords = append(txRecords, btcTransactionRecord)
			}
		}

		if txRecords != nil && len(txRecords) > 0 {
			//保存交易数据
			err := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(p.ChainName))
			if err != nil {
				// postgres出错 接入lark报警
				log.Error(p.ChainName+"扫块，插入数据到数据库中失败", zap.Any("size", len(txRecords)))
				alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", p.ChainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				return
			}
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

	records, err := data.BtcTransactionRecordRepoClient.FindByStatus(nil, biz.GetTalbeName(p.ChainName), biz.PENDING, biz.NO_STATUS)
	if err != nil {
		log.Error(p.ChainName+"查询数据库失败", zap.Any("error", err))
		return
	}
	log.Info("", zap.Any("records", records))
	var txRecords []*data.BtcTransactionRecord
	now := time.Now().Unix()

	for _, record := range records {
		tx, err := p.client.GetTransactionByHash(record.TransactionHash)
		for i := 0; i < 10 && err != nil; i++ {
			time.Sleep(time.Duration(i*5) * time.Second)
			tx, err = p.client.GetTransactionByHash(record.TransactionHash)
		}
		if err != nil {
			log.Error(p.ChainName+"查询链上数据失败", zap.Any("txHash", record.TransactionHash), zap.Any("error", err))
			continue
		}

		if strings.HasSuffix(tx.Error, " not found.") {
			nowTime := time.Now().Unix()
			if record.CreatedAt+300 > nowTime {
				status := biz.NO_STATUS
				record.Status = status
				record.UpdatedAt = now
				txRecords = append(txRecords, record)
			} else {
				status := biz.FAIL
				record.Status = status
				record.UpdatedAt = now
				txRecords = append(txRecords, record)
			}
			continue
		}

		isPending := tx.BlockHeight <= 0
		if isPending {
			continue
		}

		curHeight := tx.BlockHeight
		status := biz.PENDING
		if tx.BlockHeight > 0 {
			status = biz.SUCCESS
		}
		var amount decimal.Decimal
		var fromAddress, toAddress, fromUid, toUid string
		var fromAddressExist, toAddressExist bool

		//openblock钱包转账中tx.Inputs只会有一笔
		fromAddress = tx.Inputs[0].Addresses[0]
		if fromAddress != "" {
			fromAddressExist, fromUid, err = biz.UserAddressSwitch(fromAddress)
			if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
				/*//更新 redis中的块高和区块hash
				data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, preHeight, 0)
				data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(preHeight), preBlockHash, biz.BLOCK_HASH_EXPIRATION_KEY)*/

				// redis出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败", p.ChainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(p.ChainName+"扫块，从redis中获取用户地址失败", zap.Any("current", curHeight) /*, zap.Any("new", height)*/, zap.Any("error", err))
				return
			}
		}

		index := 0
		for _, out := range tx.Outputs {
			toAddress = out.Addresses[0]
			if fromAddress == toAddress {
				continue
			}
			if toAddress != "" {
				toAddressExist, toUid, err = biz.UserAddressSwitch(toAddress)
				if err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY {
					/*//更新 redis中的块高和区块hash
					data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+p.ChainName, preHeight, 0)
					data.RedisClient.Set(biz.BLOCK_HASH_KEY+p.ChainName+":"+strconv.Itoa(preHeight), preBlockHash, biz.BLOCK_HASH_EXPIRATION_KEY)*/

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
				var txHash string
				if index == 1 {
					txHash = tx.Hash
				} else {
					txHash = tx.Hash + "#result-" + fmt.Sprintf("%v", index)
				}
				amount = decimal.NewFromBigInt(&out.Value, 0)
				btcTransactionRecord := &data.BtcTransactionRecord{
					BlockHash:       tx.BlockHash,
					BlockNumber:     curHeight,
					TransactionHash: txHash,
					FromAddress:     fromAddress,
					ToAddress:       toAddress,
					FromUid:         fromUid,
					ToUid:           toUid,
					FeeAmount:       decimal.NewFromInt(tx.Fees.Int64()),
					Amount:          amount,
					Status:          status,
					TxTime:          tx.Confirmed.Unix(),
					//ConfirmCount:    int32(height - curHeight),
					DappData:   "",
					ClientData: "",
					CreatedAt:  now,
					UpdatedAt:  now,
				}
				txRecords = append(txRecords, btcTransactionRecord)
			}
		}
	}

	if txRecords != nil && len(txRecords) > 0 {
		//保存交易数据
		err := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(p.ChainName))
		if err != nil {
			// postgres出错 接入lark报警
			log.Error(p.ChainName+"扫块，插入数据到数据库中失败", zap.Any("size", len(txRecords)))
			alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", p.ChainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}
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
