package solana

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	pcommon "block-crawling/internal/platform/common"
	"context"
	"errors"
	"strings"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"gitlab.bixin.com/mili/node-driver/common"
	"go.uber.org/zap"
)

type handler struct {
	ChainName         string
	liveBlockInterval time.Duration
}

func newHandler(chainName string, liveBlockInterval time.Duration) chain.BlockHandler {
	return &handler{
		ChainName:         chainName,
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
	decoder := &txDecoder{
		ChainName: h.ChainName,
		block:     block,
		newTxs:    true,
		now:       time.Now(),
	}
	log.Info(
		"GOT NEW BLOCK",
		zap.String("chainName", h.ChainName),
		zap.Uint64("height", block.Number),
		zap.String("blockHash", block.Hash),
		zap.Int("txLen", len(block.Transactions)),
		zap.String("nodeUrl", client.URL()),
	)
	return decoder, nil
}

func (h *handler) CreateTxHandler(client chain.Clienter, tx *chain.Transaction) (chain.TxHandler, error) {
	log.Debug(
		"CREATE TX HANDLER TO ATTEMPT TO SEAL PENDING TX",
		zap.String("chainName", h.ChainName),
		zap.String("txHash", tx.Hash),
		zap.String("nodeUrl", client.URL()),
	)
	decoder := &txDecoder{
		ChainName: h.ChainName,
		block:     nil,
		txByHash:  tx,
		newTxs:    false,
		now:       time.Now(),
	}
	return decoder, nil
}

func (h *handler) OnForkedBlock(client chain.Clienter, block *chain.Block) error {
	rows, _ := data.SolTransactionRecordRepoClient.DeleteByBlockNumber(
		context.Background(),
		biz.GetTableName(h.ChainName),
		int(block.Number),
	)
	pcommon.NotifyForkedDelete(h.ChainName, block.Number, rows)
	log.Info(
		"出现分叉回滚数据",
		zap.Any("链类型", h.ChainName),
		zap.Any("共删除数据", rows),
		zap.Any("回滚到块高", block.Number-1),
		zap.String("nodeUrl", client.URL()),
	)
	return nil
}

func (h *handler) WrapsError(client chain.Clienter, err error) error {
	// DO NOT RETRY
	if err == nil || err == pcommon.NotFound {
		return err
	}

	if !strings.HasSuffix(h.ChainName, "TEST") {
		log.Error(
			"error occurred then retry",
			zap.String("chainName", h.ChainName),
			zap.String("nodeUrl", client.URL()),
			zap.Error(err),
		)
	}
	pcommon.NotifyForkedError(h.ChainName, err)
	return common.Retry(err)
}

func (h *handler) OnError(err error, optHeight ...chain.HeightInfo) (incrHeight bool) {
	if err == nil || err == pcommon.NotFound {
		return true
	}

	errStr := err.Error()
	errList := strings.Split(errStr, "\n")
	errStrLen := len(errStr)
	errListLen := len(errList)
	/*if errListLen >= 3 && errList[0] == "HTTP 200 OK" && errList[errListLen-1] == "context deadline exceeded (Client.Timeout or context cancellation while reading body)" && errStrLen > 1048576 {
		incrHeight = true
	}*/
	if errStrLen > 10240 {
		nerrStr := errList[0] + errStr[0:10240] + errList[errListLen-1]
		err = errors.New(nerrStr)
	}
	if !strings.HasSuffix(h.ChainName, "TEST") {
		log.Error(
			"ERROR OCCURRED WHILE HANDLING BLOCK",
			zap.String("chainName", h.ChainName),
			zap.Error(err),
		)
	}
	return
}
