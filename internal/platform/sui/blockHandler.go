package sui

import (
	"block-crawling/internal/log"
	pcommon "block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"strings"
	"time"

	"gitlab.bixin.com/mili/node-driver/common"

	"gitlab.bixin.com/mili/node-driver/chain"
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
	return false
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
	/*log.Info(
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
		chainName:   h.chainName,
		block:       nil,
		txByHash:    tx,
		curHeight:   tx.BlockNumber,
		chainHeight: tx.BlockNumber,
		newTxs:      false,
		now:         time.Now().Unix(),
	}
	return decoder, nil
}

func (h *handler) OnForkedBlock(client chain.Clienter, block *chain.Block) error {
	return nil
}

func (h *handler) WrapsError(client chain.Clienter, err error) error {
	// DO NOT RETRY
	if err == nil {
		return err
	}
	if _, ok := err.(*types.ErrorObject); ok {
		return err
	}
	return common.Retry(err)
}

func (h *handler) OnError(err error, heights ...chain.HeightInfo) (incrHeight bool) {
	if err == nil || err.Error() == pcommon.NotFound.Error() {
		return true
	}
	if _, ok := err.(*types.ErrorObject); ok {
		return true
	}

	errStr := err.Error()
	errList := strings.Split(errStr, "\n")
	errListLen := len(errList)
	if errListLen >= 3 && errList[0] == "HTTP 200 OK" && errList[errListLen-1] == "context deadline exceeded (Client.Timeout or context cancellation while reading body)" && len(errStr) > 1048576 {
		incrHeight = true
	}

	nerr := utils.SubError(err)
	fields := make([]zap.Field, 0, 4)
	fields = append(
		fields,
		zap.String("chainName", h.chainName),
		zap.Error(nerr),
	)
	if len(heights) > 0 {
		fields = append(
			fields,
			zap.Uint64("curHeight", heights[0].CurHeight),
			zap.Uint64("chainHeight", heights[0].ChainHeight),
		)
	}

	log.Warn(
		"ERROR OCCURRED WHILE HANDLING BLOCK",
		fields...,
	)
	return
}
