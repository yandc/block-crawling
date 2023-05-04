package common

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"errors"
	"fmt"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

// NotFound is returned by API methods if the requested item does not exist.
var NotFound = errors.New("not found")
var TransactionNotFound = errors.New("transaction not found")
var BlockNotFound = errors.New("block not found")

// NotifyForkedDelete notify lark when delete rows when forked.
func NotifyForkedDelete(chainName string, blockNumber uint64, nRows int64) {
	if nRows <= 0 {
		return
	}
	alarmMsg := fmt.Sprintf("请注意：%s 链产出分叉，回滚到块高 %d，删除 %d 条数据", chainName, blockNumber, nRows)
	alarmOpts := biz.WithMsgLevel("FATAL")
	alarmOpts = biz.WithAlarmChainName(chainName)
	biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
}

func NotifyForkedError(chainName string, err error) bool {
	if err == nil {
		return false
	}

	alarmOpts := biz.WithMsgLevel("FATAL")
	alarmOpts = biz.WithAlarmChainName(chainName)
	var alarmMsg string
	if err == chain.ErrForkedZeroBlockNumber {
		alarmMsg = fmt.Sprintf("请注意： %s 链产生分叉，但是获取块高为 0", chainName)
	} else if err, ok := err.(*chain.ForkDeltaOverflow); ok {
		alarmMsg = fmt.Sprintf("请注意： %s 链产生分叉，但是回滚到了安全块高以外，链上块高：%d，回滚到块高：%d，安全块高差：%d", chainName, err.ChainHeight, err.BlockNumber, err.SafelyDelta)
	} else {
		return false
	}
	biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	return true
}

type txResult struct {
	hash          string
	matchedFrom   bool
	matchedTo     bool
	txType        chain.TxType
	failedOnChain bool
	fromUID       string
}

func SetResultOfTxs(block *chain.Block, records []interface{}) {
	txs := make(map[string]*chain.Transaction)
	for _, tx := range block.Transactions {
		txs[tx.Hash] = tx
	}

	for _, r := range records {
		result := recordToTxResult(r)
		if tx, ok := txs[result.hash]; ok {
			applyTxResult(tx, result)
			tx.TxType = result.txType
		} else {
			tx := &chain.Transaction{
				Hash:   result.hash,
				TxType: result.txType,
			}
			applyTxResult(tx, result)
			block.ExtraTxs = append(block.ExtraTxs)
		}
	}
}

func SetTxResult(tx *chain.Transaction, record interface{}) {
	applyTxResult(tx, recordToTxResult(record))
}

func applyTxResult(tx *chain.Transaction, result *txResult) {
	tx.SetResult(result.matchedFrom, result.matchedTo, result.failedOnChain, result.fromUID)
	tx.TxType = result.txType
}

func recordToTxResult(record interface{}) *txResult {
	switch v := record.(type) {
	case *data.AptTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(v.TransactionType),
			failedOnChain: failedOnChain(v.Status),
			fromUID:       v.FromUid,
		}
	case *data.AtomTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(v.TransactionType),
			failedOnChain: failedOnChain(v.Status),
			fromUID:       v.FromUid,
		}
	case *data.BtcTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(biz.NATIVE),
			failedOnChain: failedOnChain(v.Status),
			fromUID:       v.FromUid,
		}
	case *data.CkbTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(v.TransactionType),
			failedOnChain: failedOnChain(v.Status),
			fromUID:       v.FromUid,
		}
	case *data.CsprTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(v.TransactionType),
			failedOnChain: failedOnChain(v.Status),
			fromUID:       v.FromUid,
		}
	case *data.EvmTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(v.TransactionType),
			failedOnChain: failedOnChain(v.Status),
			fromUID:       v.FromUid,
		}
	case *data.SolTransactionRecord:

		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(v.TransactionType),
			failedOnChain: failedOnChain(v.Status),
			fromUID:       v.FromUid,
		}
	case *data.StcTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(v.TransactionType),
			failedOnChain: failedOnChain(v.Status),
			fromUID:       v.FromUid,
		}
	case *data.SuiTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(v.TransactionType),
			failedOnChain: failedOnChain(v.Status),
			fromUID:       v.FromUid,
		}
	case *data.TrxTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(v.TransactionType),
			failedOnChain: failedOnChain(v.Status),
			fromUID:       v.FromUid,
		}
	default:
		panic("unsupport record")
	}
}

func failedOnChain(status string) bool {
	return status == biz.FAIL || status == biz.DROPPED || status == biz.DROPPED_REPLACED
}

func LogBlockError(chainName string, err error, optHeights ...chain.HeightInfo) {
	LogBlock(chainName, "error", err, optHeights...)
}

func LogBlockWarn(chainName string, err error, optHeights ...chain.HeightInfo) {
	LogBlock(chainName, "warn", err, optHeights...)
}

func LogBlock(chainName, level string, err error, optHeights ...chain.HeightInfo) {
	if err == nil {
		return
	}
	nerr := utils.SubError(err)
	fields := make([]zap.Field, 0, 4)
	fields = append(
		fields,
		zap.String("chainName", chainName),
		zap.Error(nerr),
	)
	if len(optHeights) > 0 {
		fields = append(
			fields,
			zap.Uint64("curHeight", optHeights[0].CurHeight),
			zap.Uint64("chainHeight", optHeights[0].ChainHeight),
		)
	}

	var showMsg func(msg string, args ...zap.Field)
	if errors.Is(err, chain.ErrSlowBlockHandling) {
		if level == "error" {
			showMsg = log.ErrorS
		} else if level == "warn" {
			showMsg = log.WarnS
		}
	} else {
		if level == "error" {
			showMsg = log.Error
		} else if level == "warn" {
			showMsg = log.Warn
		}
	}
	showMsg(
		"error occurred while handling block",
		fields...,
	)
}
