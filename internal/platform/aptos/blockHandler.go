package aptos

import (
	"block-crawling/internal/log"
	pcommon "block-crawling/internal/platform/common"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"gitlab.bixin.com/mili/node-driver/common"
	"go.uber.org/zap"
)

type handler struct {
	chain.DefaultTxDroppedIn

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
	return false
}

func (h *handler) OnNewBlock(client chain.Clienter, chainHeight uint64, block *chain.Block) (chain.TxHandler, error) {
	decoder := &txHandler{
		chainName:      h.chainName,
		block:          block,
		chainHeight:    chainHeight,
		curHeight:      block.Number,
		newTxs:         true,
		txHashIndexMap: make(map[string]int),
		now:            time.Now().Unix(),
	}
	/*log.InfoS(
		"GOT NEW BLOCK",
		zap.String("chainName", h.chainName),
		zap.Uint64("chainHeight", chainHeight),
		zap.Uint64("curHeight", block.Number),
		zap.String("blockHash", block.Hash),
		zap.Int("txLen", len(block.Transactions)),
		zap.String("nodeUrl", client.URL()),
	)*/
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
		chainName:      h.chainName,
		block:          nil,
		txByHash:       tx,
		txHashIndexMap: make(map[string]int),
		curHeight:      tx.BlockNumber,
		newTxs:         false,
		now:            time.Now().Unix(),
	}
	return decoder, nil
}

func (h *handler) OnForkedBlock(client chain.Clienter, block *chain.Block) error {
	return nil
}

func (h *handler) WrapsError(client chain.Clienter, err error) error {
	if err == nil {
		return err
	}
	return common.Retry(err)
}

func (h *handler) OnError(err error, optHeights ...chain.HeightInfo) (incrHeight bool) {
	pcommon.LogBlockError(h.chainName, err, optHeights...)
	return false
}
