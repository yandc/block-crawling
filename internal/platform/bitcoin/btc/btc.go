package btc

import "block-crawling/internal/types"

type Noder interface {
	GetBalance(address string) (string, error)
	GetBlockNumber() (int, error)
	GetTransactionByHash(hash string) (tx types.TX, err error)
}

type Streamer interface {
	GetBlockHeight() (int, error)
	GetMempoolTxIds() ([]string, error)
	GetBlockHashByNumber(number int) (string, error)
	GetTestBlockByHeight(height int) (result types.BTCTestBlockerInfo, err error)
}
