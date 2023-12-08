package btc

import (
	"block-crawling/internal/httpclient"
	"block-crawling/internal/types"
	"fmt"
	"time"
)

type btcStream struct {
	streamURL string
}

// GetBlockHashByNumber implements Streamer
func (s *btcStream) GetBlockHashByNumber(number int) (string, error) {
	url := s.streamURL + "/block-height/" + fmt.Sprintf("%d", number)
	return httpclient.HttpsGetFormString(url, nil)
}

// GetBlockHeight implements Streamer
func (s *btcStream) GetBlockHeight() (int, error) {
	url := s.streamURL + "/blocks/tip/height"
	var height int
	timeoutMS := 5_000 * time.Millisecond
	err := httpclient.HttpsGetForm(url, nil, &height, &timeoutMS)
	return height, err
}

// GetMempoolTxIds implements Streamer
func (s *btcStream) GetMempoolTxIds() ([]string, error) {
	url := s.streamURL + "/mempool/txids"
	var txIds []string
	timeoutMS := 5_000 * time.Millisecond
	err := httpclient.HttpsGetForm(url, nil, &txIds, &timeoutMS)
	return txIds, err
}

// GetTestBlockByHeight implements Streamer
func (s *btcStream) GetTestBlockByHeight(height int) (result []types.BTCTestBlock, err error) {
	//get block hash
	hash, err := s.GetBlockHashByNumber(height)
	if err != nil {
		return result, err
	}
	starIndex := 0
	pageSize := 25
	timeoutMS := 10_000 * time.Millisecond
	for {
		var block types.BTCTestBlockerInfo
		url := s.streamURL + "/block/" + hash + "/txs/" + fmt.Sprintf("%d", starIndex*pageSize)
		err = httpclient.HttpsGetForm(url, nil, &block, &timeoutMS)
		starIndex++
		result = append(result, block...)
		if len(block) < pageSize {
			break
		}
	}
	return
}

func newBTCStream(streamURL string) *btcStream {
	return &btcStream{
		streamURL: streamURL,
	}
}
