package ethereum

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	pcommon "block-crawling/internal/platform/common"
	"block-crawling/internal/platform/ethereum/rtypes"
	"block-crawling/internal/utils"
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"gitlab.bixin.com/mili/node-driver/chain"
	"gitlab.bixin.com/mili/node-driver/common"
	"go.uber.org/zap"
)

type handler struct {
	chain.DefaultTxDroppedIn

	chainName         string
	liveBlockInterval time.Duration
	blocksStore       map[uint64]*chain.Block
	blockEventLogMap  sync.Map
}

func newHandler(chainName string, liveBlockInterval time.Duration) chain.BlockHandler {
	return &handler{
		chainName:         chainName,
		liveBlockInterval: liveBlockInterval,
		blocksStore:       make(map[uint64]*chain.Block),
		//blockEventLogMap:  make(map[uint64][]*rtypes.Log),
	}
}

func (h *handler) BlockInterval() time.Duration {
	return h.liveBlockInterval
}

func (h *handler) BlockMayFork() bool {
	if h.chainName == "zkSyncTEST" {
		return false
	}
	return true
}

func getToBlock(chainName string, chainHeight uint64) *big.Int {
	switch chainName {
	case "ArbitrumNova", "Arbitrum":
		chainHeight += 10
	}
	return new(big.Int).SetUint64(chainHeight)
}

func (h *handler) OnNewBlock(client chain.Clienter, chainHeight uint64, block *chain.Block) (chain.TxHandler, error) {
	//var chainEvnetLogs []*rtypes.Log
	decoder := &txDecoder{
		chainName: h.chainName,
		block:     block,
		newTxs:    true,
		blockHash: "",
		now:       time.Now().Unix(),
	}
	//TODO
	if true {
		return decoder, nil
	}
	if _, ok := h.blockEventLogMap.Load(chainHeight); !ok {
		//获取整个区块的 event logs
		ethClient, err := getETHClient(client.URL())
		if err != nil {
			return decoder, nil
		}
		fromBlock := new(big.Int).SetUint64(chainHeight)
		toBlock := getToBlock(h.chainName, chainHeight)
		logs, err := ethClient.FilterLogs(context.Background(), ethereum.FilterQuery{
			FromBlock: fromBlock,
			ToBlock:   toBlock,
		})
		log.Info("feature_block_eventLog logs length:", zap.Any("logLength", len(logs)))
		if err != nil {
			return decoder, nil
		}
		var tempChainEvnetLogs []*rtypes.Log
		tempBlcokEventLogMap := make(map[uint64][]*rtypes.Log)
		utils.CopyProperties(logs, &tempChainEvnetLogs)
		for _, log := range tempChainEvnetLogs {
			if _, blockOk := tempBlcokEventLogMap[log.BlockNumber]; blockOk {
				tempBlcokEventLogMap[log.BlockNumber] = append(tempBlcokEventLogMap[log.BlockNumber], log)
			} else {
				tempBlcokEventLogMap[log.BlockNumber] = []*rtypes.Log{log}
			}
		}
		for key, value := range tempBlcokEventLogMap {
			h.blockEventLogMap.Store(key, value)
		}
	}
	var chainEvnetLogs []*rtypes.Log
	if value, ok := h.blockEventLogMap.Load(chainHeight); ok && value != nil {
		chainEvnetLogs = value.([]*rtypes.Log)
	}
	//按照tx hash 分组
	txEventLogMap := map[string][]*rtypes.Log{}
	for _, eventLog := range chainEvnetLogs {
		txHash := eventLog.TxHash.String()
		if _, ok := txEventLogMap[txHash]; !ok {
			txEventLogMap[txHash] = []*rtypes.Log{}
		}
		txEventLogMap[txHash] = append(txEventLogMap[txHash], eventLog)
	}
	log.Info("feature_block_eventLog txEventLogMap length:", zap.Any("txEventLogMap", len(txEventLogMap)))
	decoder.txEventLogMap = txEventLogMap
	h.blockEventLogMap.Delete(chainHeight - 1)
	//delete(h.blockEventLogMap, chainHeight)
	/*log.Info(
		"GOT NEW BLOCK",
		zap.String("chainName", h.chainName),
		zap.Uint64("chainHeight", chainHeight),
		zap.Uint64("curHeight", block.Number),
		zap.String("blockHash", block.Hash),
		zap.Int("txLen", len(block.Transactions)),
		zap.String("nodeUrl", client.URL()),
	)*/
	/*if len(block.Transactions) == 0 {
		// OEC 等链的区块哈希都是从 Receipt 取到的，如果一个区块没有交易记录，就没办法获取到真正的区块哈希，
		// 比如 https://www.oklink.com/zh-cn/okc/block/14124520
		// 那么在处理下一个区块的时候就是判定为分叉，接下来就会卡在这个区块上。
		// 将区块 Hash 存储为空防止下一个区块进行分叉判断。
		log.Info(
			"MAKE BLOCK HASH TO EMPTY AS THERE IS NO TX IN IT",
			zap.String("chainName", h.chainName),
			zap.Uint64("chainHeight", chainHeight),
			zap.Uint64("curHeight", block.Number),
			zap.String("blockHash", block.Hash),
			zap.Int("txLen", len(block.Transactions)),
			zap.String("nodeUrl", client.URL()),
		)
		block.Hash = ""
	}*/
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
		chainName:   h.chainName,
		block:       nil,
		txByHash:    tx,
		blockHash:   "",
		newTxs:      false,
		now:         time.Now().Unix(),
		blocksStore: h.blocksStore,
	}
	return decoder, nil
}

func (h *handler) OnForkedBlock(client chain.Clienter, block *chain.Block) error {
	// XXX: is the removal correct?
	rows, _ := data.EvmTransactionRecordRepoClient.DeleteByBlockNumber(
		context.Background(),
		biz.GetTableName(h.chainName),
		int(block.Number),
	)
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
	// DO NOT RETRY
	if err == nil || fmt.Sprintf("%s", err) == BLOCK_NO_TRANSCATION || fmt.Sprintf("%s", err) == FILE_BLOCK_NULL || fmt.Sprintf("%s", err) == BLOCK_NONAL_TRANSCATION || err == ethereum.NotFound || err == pcommon.NotFound {
		return err
	}
	pcommon.NotifyForkedError(h.chainName, err)
	return common.Retry(err)
}

func (h *handler) OnError(err error, optHeights ...chain.HeightInfo) (incrHeight bool) {
	if err == nil || fmt.Sprintf("%s", err) == BLOCK_NO_TRANSCATION || fmt.Sprintf("%s", err) == BLOCK_NONAL_TRANSCATION || err == ethereum.NotFound || err == pcommon.NotFound || err == pcommon.BlockNotFound || err == pcommon.TransactionNotFound || fmt.Sprintf("%s", err) == FILE_BLOCK_NULL || err == pcommon.TransactionStillPending {
		if fmt.Sprintf("%s", err) != BLOCK_NO_TRANSCATION && fmt.Sprintf("%s", err) != BLOCK_NONAL_TRANSCATION {
			pcommon.LogBlockWarn(h.chainName, err, optHeights...)
		}
		return true
	}

	pcommon.LogBlockError(h.chainName, err, optHeights...)
	return
}

func (h *handler) IsDroppedTx(txByHash *chain.Transaction, err error) (isDropped bool) {
	if txByHash == nil && (err == nil || err == ethereum.NotFound || err == pcommon.NotFound || err == pcommon.TransactionNotFound) {
		return true
	}

	return
}
