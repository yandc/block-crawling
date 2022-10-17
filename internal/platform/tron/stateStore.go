package tron

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"fmt"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type stateStore struct {
	common.StateStore

	chainName    string
	dbBlockHashs map[uint64]string
}

func newStateStore(chainName string) chain.StateStore {
	return &stateStore{
		StateStore: *common.NewStateStore(chainName, loadHeightFromDB),
		chainName:  chainName,
	}
}

func loadHeightFromDB(chainName string) (*common.DBBlockRecord, error) {
	lastRecord, err := data.TrxTransactionRecordRepoClient.FindLast(nil, biz.GetTalbeName(chainName))
	if err != nil {
		return nil, err
	}
	if lastRecord == nil {
		return nil, nil
	}
	return &common.DBBlockRecord{
		BlockNumber: uint64(lastRecord.BlockNumber),
		BlockHash:   lastRecord.BlockHash,
	}, nil
}

func (store *stateStore) LoadPendingTxs() (txs []*chain.Transaction, err error) {
	records, err := data.TrxTransactionRecordRepoClient.FindByStatus(nil, biz.GetTalbeName(store.chainName), biz.PENDING, biz.NO_STATUS)
	if err != nil {
		log.Error(store.chainName+"查询数据库失败", zap.Any("error", err))
		return
	}
	txs = make([]*chain.Transaction, 0, len(records))
	for _, r := range records {
		txs = append(txs, &chain.Transaction{
			Hash: r.TransactionHash,

			// The value may be zero if the tx was created from wallet.
			// More details please check `CreateRecordFromWallet`.
			BlockNumber: uint64(r.BlockNumber),

			FromAddress: r.FromAddress,
			ToAddress:   r.ToAddress,
			Value:       r.Amount.String(),
			Raw:         nil,
			Record:      r,
		})
	}
	log.Info("LOAD PENDING TXs", zap.String("chainName", store.chainName), zap.Any("records", records))
	return txs, nil
}

func (store *stateStore) LoadBlockHash(height uint64) (string, error) {
	redisPreBlockHash, err := store.StateStore.LoadBlockHash(height)
	for i := 0; i < 3 && err != nil && fmt.Sprintf("%s", err) != biz.REDIS_NIL_KEY; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		redisPreBlockHash, err = store.StateStore.LoadBlockHash(height)
	}
	if err != nil {
		if fmt.Sprintf("%s", err) == biz.REDIS_NIL_KEY {
			// 从数据库中查询指定块高的数据
			lastRecord, err := data.TrxTransactionRecordRepoClient.FindLast(nil, biz.GetTalbeName(store.chainName))
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				lastRecord, err = data.TrxTransactionRecordRepoClient.FindLast(nil, biz.GetTalbeName(store.chainName))
			}
			if err != nil {
				// postgres出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链查询数据库中区块hash失败", store.chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(store.chainName+"扫块，从数据库中获取区块hash失败", zap.Any("prevent", height), zap.Any("new", height), zap.Any("error", err))
				return "", err
			}
			if lastRecord == nil {
				log.Error(store.chainName+"扫块，从数据库中获取的区块hash为空", zap.Any("prevent", height), zap.Any("new", height))
				//return
			} else {
				redisPreBlockHash = lastRecord.BlockHash
			}
		} else {
			// redis出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链查询redis中区块hash失败", store.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(store.chainName+"扫块，从redis中获取区块hash失败", zap.Any("prevent", height), zap.Any("new", height), zap.Any("error", err))
			return "", err
		}
	}
	return redisPreBlockHash, nil
}
