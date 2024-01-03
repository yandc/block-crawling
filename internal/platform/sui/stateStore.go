package sui

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"context"
	"fmt"
	"strings"
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
		StateStore:   *common.NewStateStore(chainName, loadHeightFromDB),
		chainName:    chainName,
		dbBlockHashs: make(map[uint64]string),
	}
}

func loadHeightFromDB(chainName string) (*common.DBBlockRecord, error) {
	lastRecord, err := data.SuiTransactionRecordRepoClient.FindLast(nil, biz.GetTableName(chainName))
	if err != nil {
		return nil, err
	}
	if lastRecord == nil {
		return nil, nil
	}
	return &common.DBBlockRecord{
		BlockNumber: uint64(lastRecord.BlockNumber),
		BlockHash:   lastRecord.TransactionHash,
	}, nil
}

func (store *stateStore) LoadPendingTxs() (txs []*chain.Transaction, err error) {
	records, err := data.SuiTransactionRecordRepoClient.FindByStatus(nil, biz.GetTableName(store.chainName), biz.PENDING, biz.NO_STATUS)
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
	if biz.IsBenfenNet(store.chainName) {
		go store.notifyTimeoutStationTxns()
	}
	return txs, nil
}

func (store *stateStore) notifyTimeoutStationTxns() {
	records, err := data.BFCStationRepoIns.LoadTimeoutRecords(context.Background(), store.chainName, 15*time.Minute)
	if err != nil {
		log.Error("LOAD TIMEOUT RECORDS", zap.String("chainName", store.chainName), zap.Error(err))
	}
	if len(records) == 0 {
		return
	}
	var msgBody strings.Builder
	for _, r := range records {
		msgBody.WriteString(fmt.Sprintf(`
ID: %d
Type: %s
TxHash: %s
Wallet: %s
TxTime: %d
`,

			r.Id,
			r.Type,
			r.TransactionHash,
			r.WalletAddress,
			r.TxTime,
		))
	}

	alarmMsg := fmt.Sprintf(`%s Station 订单已超时。\n%s`,
		store.chainName,
		msgBody.String(),
	)
	biz.LarkClient.NotifyLark(alarmMsg, nil, nil, biz.WithStationBot())
}
