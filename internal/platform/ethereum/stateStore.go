package ethereum

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"context"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type StateStore struct {
	common.StateStore

	chainName    string
	dbBlockHashs map[uint64]string
}

func NewStateStore(chainName string) chain.StateStore {
	return &StateStore{
		StateStore:   *common.NewStateStore(chainName, loadHeightFromDB),
		chainName:    chainName,
		dbBlockHashs: make(map[uint64]string),
	}
}

func loadHeightFromDB(chainName string) (*common.DBBlockRecord, error) {
	ctx := context.Background()
	lastRecord, err := data.EvmTransactionRecordRepoClient.FindLast(ctx, biz.GetTalbeName(chainName))
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

func (store *StateStore) LoadPendingTxs() (txs []*chain.Transaction, err error) {
	records, err := data.EvmTransactionRecordRepoClient.FindByStatus(nil, biz.GetTalbeName(store.chainName), biz.PENDING, biz.NO_STATUS)
	if err != nil {
		log.Error(store.chainName+"查询数据库失败", zap.Any("error", err))
		return nil, err
	}
	txs = make([]*chain.Transaction, 0, len(records))
	for _, r := range records {
		txs = append(txs, &chain.Transaction{
			Hash:  r.TransactionHash,
			Nonce: uint64(r.Nonce),

			// The value may be zero if the tx was created from wallet.
			// More details please check `CreateRecordFromWallet`.
			BlockNumber: uint64(r.BlockNumber),

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
