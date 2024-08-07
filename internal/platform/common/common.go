package common

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"errors"
	"fmt"
	"sync"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NotFound is returned by API methods if the requested item does not exist.
var NotFound = errors.New("not found")
var TransactionNotFound = errors.New("transaction not found")
var TransactionStillPending = errors.New("transaction is still pending")
var BlockNotFound = errors.New("block not found")

var forkedDelNotifiedAt = &sync.Map{}

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
		if err.SafelyDelta == 0 {
			return true
		}
		if v, ok := forkedDelNotifiedAt.Load(chainName); ok {
			notifiedAt := v.(int64)
			if time.Since(time.Unix(notifiedAt, 0)) < time.Hour {
				return true
			}
		}

		forkedDelNotifiedAt.Store(chainName, time.Now().Unix())

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
	case *data.KasTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(biz.NATIVE),
			failedOnChain: failedOnChain(v.Status),
			fromUID:       v.FromUid,
		}
	case *data.TonTransactionRecord:
		return &txResult{
			hash:          v.TransactionHash,
			matchedFrom:   v.FromUid != "",
			matchedTo:     v.ToUid != "",
			txType:        chain.TxType(biz.NATIVE),
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
	// Ignore the custom chain that's not featured.
	if biz.IsCustomChain(chainName) && !biz.IsCustomChainFeatured(chainName) {
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

func RetryWithAlarm10(chainName string, fn func() error, scope string, fields ...zap.Field) error {
	err := fn()
	for i := 0; i < 10 && err != nil; i++ {
		time.Sleep(time.Duration(i*5) * time.Second)
		err = fn()
	}
	if err != nil {
		// 更新用户资产出错 接入lark报警
		message := fmt.Sprintf("请注意：%s链%s失败", chainName, scope)
		AlarmWithFields(chainName, message, err, fields...)
		return err
	}
	return nil
}

func AlarmWithFields(chainName string, message string, err error, fields ...zap.Field) {
	fields = append(fields, zap.Any("chainName", chainName), zap.Any("error", err))
	alarmMsg := fmt.Sprintf("%s：\n%s", message, zapFieldsToJSON(fields...))
	alarmOpts := biz.WithMsgLevel("FATAL")
	biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	log.Error(message, fields...)
}

var defaultZapEnc = zapcore.NewJSONEncoder(zapcore.EncoderConfig{})
var defaultZapEnt = zapcore.Entry{
	Level:      zap.ErrorLevel,
	Time:       time.Now(),
	LoggerName: "",
	Message:    "",
	Caller:     zapcore.EntryCaller{},
	Stack:      "",
}

func zapFieldsToJSON(fields ...zap.Field) string {
	buf, _ := defaultZapEnc.EncodeEntry(
		defaultZapEnt,
		fields,
	)
	if buf != nil {
		return buf.String()
	}
	return ""
}

type TxHashSuffixer interface {
	WithSuffix() string
	Hash() string
}

func NewTxHashSuffixer(txHash string) TxHashSuffixer {
	return &txHashSuffixer{
		txHash:  txHash,
		counter: 0,
	}
}

type txHashSuffixer struct {
	txHash  string
	counter int
}

func (s *txHashSuffixer) WithSuffix() string {
	s.counter++
	if s.counter-1 == 0 {
		return s.txHash
	}
	return fmt.Sprintf("%s#result-%d", s.txHash, s.counter-1)
}

func (s *txHashSuffixer) Hash() string {
	return s.txHash
}
