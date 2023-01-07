package solana

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"context"
	"fmt"
	"strconv"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type stateStore struct {
	chainName    string
	dbBlockHashs map[uint64]string
}

func newStateStore(chainName string) chain.StateStore {
	return &stateStore{
		chainName:    chainName,
		dbBlockHashs: make(map[uint64]string),
	}
}

func (store *stateStore) LoadHeight() (uint64, error) {
	redisHeight, _ := data.RedisClient.Get(biz.BLOCK_HEIGHT_KEY + store.chainName).Result()
	if redisHeight != "" {
		curHeight, err := strconv.Atoi(redisHeight)
		if err != nil {
			return 0, err
		}
		log.Debug(
			"LOADED CURRENT HEIGHT FROM CACHE",
			zap.String("chainName", store.chainName),
			zap.Int("height", curHeight),
		)
		return uint64(curHeight), nil
	}
	return store.loadHeightFromDB()
}

func (store *stateStore) loadHeightFromDB() (uint64, error) {
	ctx := context.Background()
	lastRecord, err := data.SolTransactionRecordRepoClient.FindLast(ctx, biz.GetTalbeName(store.chainName))

	// NOTE: can we remove this retry logic?
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		lastRecord, err = data.SolTransactionRecordRepoClient.FindLast(ctx, biz.GetTalbeName(store.chainName))
	}

	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链查询数据库中块高失败", store.chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error(store.chainName+"扫块，从数据库中获取块高失败", zap.Any("error", err))
		return 0, err
	}

	if lastRecord == nil {
		return 0, chain.ErrNoCurrentHeight
	}
	store.dbBlockHashs[uint64(lastRecord.SlotNumber)] = lastRecord.BlockHash
	curHeight := lastRecord.SlotNumber + 1
	log.Debug(
		"LOAD CURRENT HEIGHT FROM DB",
		zap.String("chainName", store.chainName),
		zap.Int("height", curHeight),
	)
	return uint64(curHeight), nil
}

func (store *stateStore) StoreHeight(height uint64) error {
	return data.RedisClient.Set(biz.BLOCK_HEIGHT_KEY+store.chainName, height, 0).Err()
}

func (store *stateStore) StoreNodeHeight(height uint64) error {
	return data.RedisClient.Set(biz.BLOCK_NODE_HEIGHT_KEY+store.chainName, height, 0).Err()
}

func (store *stateStore) LoadBlockHash(height uint64) (string, error) {
	curPreBlockHash, _ := data.RedisClient.Get(biz.BLOCK_HASH_KEY + store.chainName + ":" + strconv.Itoa(int(height))).Result()

	if curPreBlockHash == "" {
		bh := store.dbBlockHashs[height]
		if bh != "" {
			curPreBlockHash = bh
		}
	}
	return curPreBlockHash, nil
}

func (store *stateStore) StoreBlockHash(height uint64, blockHash string) error {
	return data.RedisClient.Set(biz.BLOCK_HASH_KEY+store.chainName+":"+strconv.Itoa(int(height)), blockHash, biz.BLOCK_HASH_EXPIRATION_KEY).Err()
}

func (store *stateStore) LoadPendingTxs() (txs []*chain.Transaction, err error) {
	records, err := data.SolTransactionRecordRepoClient.FindByStatus(nil, biz.GetTalbeName(store.chainName), biz.PENDING, biz.NO_STATUS)
	if err != nil {
		log.Error(store.chainName+"查询数据库失败", zap.Any("error", err))
		return nil, err
	}
	txs = make([]*chain.Transaction, 0, len(records))
	for _, r := range records {
		txs = append(txs, &chain.Transaction{
			Hash:  r.TransactionHash,
			Nonce: 0,

			// The value may be zero if the tx was created from wallet.
			// More details please check `CreateRecordFromWallet`.
			BlockNumber: uint64(r.SlotNumber),

			TxType:      chain.TxType(r.TransactionType),
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
