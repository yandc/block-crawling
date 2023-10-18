package swap

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/log"
	"errors"
	"fmt"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type Pair = biz.SwapPair
type PairItem = biz.SwapPairItem

type SwapContract interface {
	// Name returns the name of the swap contract.
	Name() string

	// Is returns true if the toAddress is a swap contract.
	Is(chainName string, tx *chain.Transaction) (bool, error)

	// ExtractParis extract pairs from the transaction receipt.
	ExtractPairs(tx *chain.Transaction, args ...interface{}) ([]*Pair, error)
}

var gSwapContracts = make(map[string][]SwapContract)

func RegisterSwapContract(chainType string, contracts ...SwapContract) {
	container := gSwapContracts[chainType]
	for _, c := range contracts {
		container = append(container, c)
	}
	gSwapContracts[chainType] = container
}

func AttemptToExtractSwapPairs(chainName, contract string, block *chain.Block, tx *chain.Transaction, args ...interface{}) (int, error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("Extract Swap error, chainName:"+chainName, e)
			} else {
				log.Errore("Extract Swap panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链解析 SWAP 失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithAlarmChannel("kanban")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()
	chainType, _ := biz.GetChainNameType(chainName)
	swapContracts := gSwapContracts[chainType]

	results := make([]*Pair, 0, 2)
	errs := make([]error, 0, 2)
	if tx.ToAddress == "" {
		tx.ToAddress = contract
	}
	for _, s := range swapContracts {
		matched, err := delegateMatchingSwapContract(chainName, tx, s)
		if !matched {
			continue
		}
		paris, err := s.ExtractPairs(tx, args...)
		if err != nil {
			log.Warn(
				"ExtractPairs FROM SWAP CONTRACT FAILED WITH ERROR",
				zap.String("name", s.Name()),
				zap.String("contract", contract),
				zap.String("txHash", tx.Hash),
				zap.Error(err),
			)
			errs = append(errs, err)
			continue
		}
		for _, p := range paris {
			p.Chain = chainName
			p.Dex = s.Name()
		}
		results = append(results, paris...)
	}
	if len(results) > 0 {
		if len(results) > 0 {
			for _, p := range results {
				p.TxTime = int(block.Time)
				p.BlockNumber = int(block.Number)
				p.FromAddress = tx.FromAddress
			}
			if err := biz.BulkPushSwapPairs(chainName, results); err != nil {
				return 0, nil
			}
		}
		return len(results), nil
	}
	if len(errs) > 0 {
		return 0, errors.New("encounter error")
	}
	return 0, nil
}

func Is(chainName string, tx *chain.Transaction) bool {
	chainType, _ := biz.GetChainNameType(chainName)
	swapContracts := gSwapContracts[chainType]
	for _, s := range swapContracts {
		matched, _ := delegateMatchingSwapContract(chainType, tx, s)
		if !matched {
			continue
		}
		return true
	}
	return false
}

// delegateMatchingSwapContract to cache the result of matching swap contract to redis.
func delegateMatchingSwapContract(chainName string, tx *chain.Transaction, s SwapContract) (bool, error) {
	matched, err := s.Is(chainName, tx)

	if err != nil {
		log.Warn(
			"MATCH SWAP CONTRACT FAILED WITH ERROR",
			zap.String("name", s.Name()),
			zap.String("contract", tx.ToAddress),
			zap.String("txHash", tx.Hash),
			zap.Error(err),
		)
	}
	return matched, err
}
