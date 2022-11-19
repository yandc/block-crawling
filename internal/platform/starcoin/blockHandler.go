package starcoin

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"strings"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"gitlab.bixin.com/mili/node-driver/common"
	"go.uber.org/zap"
)

type handler struct {
	chainName         string
	liveBlockInterval time.Duration
}

func newHandler(chainName string, liveBlockInterval time.Duration) chain.BlockHandler {
	return &handler{
		chainName:         chainName,
		liveBlockInterval: liveBlockInterval,
	}
}

func (h *handler) BlockInterval() time.Duration {
	return h.liveBlockInterval
}

func (h *handler) BlockMayFork() bool {
	return true
}

func (h *handler) OnNewBlock(client chain.Clienter, chainHeight uint64, block *chain.Block) (chain.TxHandler, error) {
	decoder := &txHandler{
		chainName:   h.chainName,
		block:       block,
		chainHeight: chainHeight,
		curHeight:   block.Number,
		newTxs:      true,
		now:         time.Now().Unix(),
	}
	log.Debug(
		"GOT NEW BLOCK",
		zap.String("chainName", h.chainName),
		zap.Uint64("chainHeight", chainHeight),
		zap.Uint64("curHeight", block.Number),
		zap.String("blockHash", block.Hash),
		zap.Int("txLen", len(block.Transactions)),
		zap.String("nodeUrl", client.URL()),
	)
	return decoder, nil
}

func (h *handler) CreateTxHandler(client chain.Clienter, tx *chain.Transaction) (chain.TxHandler, error) {
	log.Debug(
		"CREATE TX HANDLER TO ATTEMPT TO SEAL PENDING TX",
		zap.String("chainName", h.chainName),
		zap.String("txHash", tx.Hash),
		zap.String("nodeUrl", client.URL()),
	)
	decoder := &txHandler{
		chainName: h.chainName,
		block:     nil,
		curHeight: tx.BlockNumber,
		newTxs:    false,
		now:       time.Now().Unix(),
	}
	return decoder, nil
}

func (h *handler) OnForkedBlock(client chain.Clienter, block *chain.Block) error {
	preHeight := int(block.Number) - 1
	rows, _ := data.StcTransactionRecordRepoClient.DeleteByBlockNumber(nil, biz.GetTalbeName(h.chainName), preHeight+1)
	log.Info("出现分叉回滚数据", zap.Any("链类型", h.chainName), zap.Any("共删除数据", rows), zap.Any("回滚到块高", preHeight))
	return nil
}

func (h *handler) WrapsError(client chain.Clienter, err error) error {
	return common.Retry(err)
}

func (h *handler) OnError(err error, optHeights ...chain.HeightInfo) (incrHeight bool) {
	fields := make([]zap.Field, 0, 4)
	fields = append(
		fields,
		zap.String("chainName", h.chainName),
		zap.Error(err),
	)
	if len(optHeights) > 0 {
		fields = append(
			fields,
			zap.Uint64("curHeight", optHeights[0].CurHeight),
			zap.Uint64("chainHeight", optHeights[0].ChainHeight),
		)
	}

	if !strings.HasSuffix(h.chainName, "TEST") {
		log.Error(
			"ERROR OCCURRED WHILE HANDLING BLOCK, WILL TRY LATER",
			fields...,
		)
	}
	return false
}
