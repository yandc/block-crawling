package common

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"errors"
	"fmt"

	"gitlab.bixin.com/mili/node-driver/chain"
)

// NotFound is returned by API methods if the requested item does not exist.
var NotFound = errors.New("not found")

// NotifyForkedDelete notify lark when delete rows when forked.
func NotifyForkedDelete(chainName string, blockNumber uint64, nRows int64) {
	if nRows <= 0 {
		return
	}
	alarmMsg := fmt.Sprintf("请注意：%s 链产出分叉，回滚到块高 %d，删除 %d 条数据", chainName, blockNumber, nRows)
	alarmOpts := biz.WithMsgLevel("FATAL")
	biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
}

func NotifyForkedError(chainName string, err error) bool {
	if err == nil {
		return false
	}

	alarmOpts := biz.WithMsgLevel("FATAL")
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
}

func SetResultOfTxs(block *chain.Block, records []interface{}) {
	txs := make(map[string]*chain.Transaction)
	for _, tx := range block.Transactions {
		txs[tx.Hash] = tx
	}

	for _, r := range records {
		result := recordToTxResult(r)
		if tx, ok := txs[result.hash]; ok {
			tx.SetResult(result.matchedFrom, result.matchedTo, result.failedOnChain)
		} else {
			block.ExtraTxs = append(block.ExtraTxs, &chain.Transaction{
				Hash:   result.hash,
				TxType: result.txType,
				Result: &chain.TxResult{
					MatchedFrom:   result.matchedFrom,
					MatchedTo:     result.matchedTo,
					FailedOnChain: result.failedOnChain,
				},
			})
		}
	}
}

func SetTxResult(tx *chain.Transaction, record interface{}) {
	result := recordToTxResult(record)
	tx.SetResult(result.matchedFrom, result.matchedTo, result.failedOnChain)
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
		}
	case *data.AtomTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(v.TransactionType),
			failedOnChain: failedOnChain(v.Status),
		}
	case *data.BtcTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(biz.NATIVE),
			failedOnChain: failedOnChain(v.Status),
		}
	case *data.CkbTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(v.TransactionType),
			failedOnChain: failedOnChain(v.Status),
		}
	case *data.CsprTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(v.TransactionType),
			failedOnChain: failedOnChain(v.Status),
		}
	case *data.EvmTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(v.TransactionType),
			failedOnChain: failedOnChain(v.Status),
		}
	case *data.SolTransactionRecord:

		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(v.TransactionType),
			failedOnChain: failedOnChain(v.Status),
		}
	case *data.StcTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(v.TransactionType),
			failedOnChain: failedOnChain(v.Status),
		}
	case *data.SuiTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(v.TransactionType),
			failedOnChain: failedOnChain(v.Status),
		}
	case *data.TrxTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(v.TransactionType),
			failedOnChain: failedOnChain(v.Status),
		}
	default:
		panic("unsupport record")
	}
}

func failedOnChain(status string) bool {
	return status == biz.FAIL || status == biz.DROPPED || status == biz.DROPPED_REPLACED
}
