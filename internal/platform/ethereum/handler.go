package ethereum

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum"
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

func (h *handler) OnNewBlock(client chain.Clienter, block *chain.Block) (chain.TxHandler, error) {
	decoder := &txDecoder{
		chainName: h.chainName,
		block:     block,
		blockHash: "",
		now:       time.Now(),
	}
	log.Info(
		"GOT NEW BLOCK",
		zap.String("chainName", h.chainName),
		zap.Uint64("height", block.Number),
		zap.String("blockHash", block.Hash),
		zap.Int("txLen", len(block.Transactions)),
		zap.String("nodeUrl", client.URL()),
	)
	if isNonstandardEVM(h.chainName) && len(block.Transactions) == 0 {
		// OEC 等链的区块哈希都是从 Receipt 取到的，如果一个区块没有交易记录，就没办法获取到真正的区块哈希，
		// 比如 https://www.oklink.com/zh-cn/okc/block/14124520
		// 那么在处理下一个区块的时候就是判定为分叉，接下来就会卡在这个区块上。
		// 将区块 Hash 存储为空防止下一个区块进行分叉判断。
		log.Info(
			"MAKE BLOCK HASH TO EMPTY AS THERE IS NO TX IN IT",
			zap.String("chainName", h.chainName),
			zap.Uint64("height", block.Number),
			zap.String("blockHash", block.Hash),
			zap.Int("txLen", len(block.Transactions)),
			zap.String("nodeUrl", client.URL()),
		)
		block.Hash = ""
	}
	return decoder, nil
}

func (h *handler) CreateTxHandler(client chain.Clienter, tx *chain.Transaction) (chain.TxHandler, error) {
	log.Debug(
		"CREATE TX HANDLER TO ATTEMPT TO SEAL PENDING TX",
		zap.String("chainName", h.chainName),
		zap.String("txHash", tx.Hash),
		zap.String("nodeUrl", client.URL()),
	)
	decoder := &txDecoder{
		chainName: h.chainName,
		block:     nil,
		blockHash: "",
		now:       time.Now(),
	}
	return decoder, nil
}

func (h *handler) OnForkedBlock(client chain.Clienter, block *chain.Block) error {
	// XXX: is the removal correct?
	rows, _ := data.EvmTransactionRecordRepoClient.DeleteByBlockNumber(
		context.Background(),
		biz.GetTalbeName(h.chainName),
		int(block.Number),
	)
	log.Info(
		"出现分叉回滚数据",
		zap.Any("链类型", h.chainName),
		zap.Any("共删除数据", rows),
		zap.Any("回滚到块高", block.Number-1),
		zap.String("nodeUrl", client.URL()),
	)
	return nil
}

func (h *handler) WrapsError(err error) error {
	// DO NOT RETRY
	if err == ethereum.NotFound {
		return err
	}

	if err != nil {
		if fmt.Sprintf("%s", err) != BLOCK_NO_TRANSCATION && fmt.Sprintf("%s", err) != BLOCK_NONAL_TRANSCATION {
			return common.Retry(err)
		}
	}
	return err
}

func (h *handler) OnError(err error) (incrHeight bool) {
	if fmt.Sprintf("%s", err) != BLOCK_NO_TRANSCATION && fmt.Sprintf("%s", err) != BLOCK_NONAL_TRANSCATION {
		log.Warn(
			"ERROR OCCURRED WHILE HANDLING BLOCK",
			zap.String("chainName", h.chainName),
			zap.Error(err),
		)
		return false
	}
	log.Warn(
		"IGNORE CURRENT BLOCK AS AN UNRESOLVABLE ERROR OCCURRED",
		zap.String("chainName", h.chainName),
		zap.Error(err),
	)
	return true
}