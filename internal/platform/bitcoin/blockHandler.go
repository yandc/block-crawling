package bitcoin

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	pcommon "block-crawling/internal/platform/common"
	"block-crawling/internal/utils"
	"fmt"
	"strings"
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
	return true
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
	log.InfoS(
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

	curHeight := int(block.Number)
	//删除 DB中 比curHeight 高的 块交易数据
	rows, err := data.BtcTransactionRecordRepoClient.DeleteByBlockNumber(nil, biz.GetTableName(h.chainName), curHeight)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		rows, err = data.BtcTransactionRecordRepoClient.DeleteByBlockNumber(nil, biz.GetTableName(h.chainName), curHeight)
	}
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链扫块，删除数据库中分叉孤块数据失败", h.chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("扫块，从数据库中删除分叉孤块数据失败", zap.Any("chainName", h.chainName), zap.Any("current", curHeight), zap.Any("error", err))
		return err
	}
	pcommon.NotifyForkedDelete(h.chainName, block.Number, rows)
	log.Info(
		"出现分叉回滚数据",
		zap.Any("链类型", h.chainName),
		zap.Any("共删除数据", rows),
		zap.Any("回滚到块高", block.Number-1),
		zap.String("nodeUrl", client.URL()),
	)
	return nil
}

func (h *handler) WrapsError(client chain.Clienter, err error) error {
	if err != nil && err.Error() == "The requested resource has not been found" {
		return chain.RetryStandby(err)
	}
	pcommon.NotifyForkedError(h.chainName, err)
	return common.Retry(err)
}

func (h *handler) OnError(err error, optHeights ...chain.HeightInfo) (incrHeight bool) {
	nerr := utils.SubError(err)
	fields := make([]zap.Field, 0, 4)
	fields = append(
		fields,
		zap.String("chainName", h.chainName),
		zap.Error(nerr),
	)
	if len(optHeights) > 0 {
		fields = append(
			fields,
			zap.Uint64("curHeight", optHeights[0].CurHeight),
			zap.Uint64("chainHeight", optHeights[0].ChainHeight),
		)
	}

	if strings.Contains(fmt.Sprintf("%s", err), "504") {
		log.Warn(
			"IGNORE CURRENT BLOCK AS ALL NODES HAVE BEEN TRIED AND STILL GOT 504",
			fields...,
		)
		return true
	}

	pcommon.LogBlockError(h.chainName, err, optHeights...)
	return false

}
