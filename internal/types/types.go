package types

import (
	"sync"
	"time"
)

type SessionInfo struct {
	Session   string
	Timestamp int64
}

type TransactionResponse struct {
	BlockHash               string    `json:"block_hash"`
	BlockNumber             string    `json:"block_number"`
	TransactionHash         string    `json:"transaction_hash"`
	Status                  string    `json:"status"`
	ExpirationTimestampSecs string    `json:"expiration_timestamp_secs"`
	From                    string    `json:"from"`
	To                      string    `json:"to"`
	Amount                  string    `json:"amount"`
	Nonce                   string    `json:"nonce"`
	GasUsed                 string    `json:"gas_used"`
	GasPrice                string    `json:"gas_price"`
	GasLimit                string    `json:"gas_limit"`
	SequenceNumber          string    `json:"sequence_number"`
	BaseFeePerGas           string    `json:"base_fee_per_gas"`
	Token                   TokenInfo `json:"token"`
}

type TokenInfo struct {
	Address        string      `json:"address"`
	Amount         string      `json:"amount"`
	Decimals       int64       `json:"decimals"`
	Symbol         string      `json:"symbol"`
	TokenUri       string      `json:"token_uri,omitempty"`
	CollectionName string      `json:"collection_name,omitempty"`
	TokenType      string      `json:"token_type,omitempty"`
	TokenId        string      `json:"token_id,omitempty"`
	ItemName       string      `json:"item_name,omitempty"`
	ItemUri        string      `json:"item_uri,omitempty"`
	Tokens         []TokenInfo `json:"tokens,omitempty"`
}

const (
	nRecordRPCURLInfoBuckets = 30 // 30 buckets for 30 minutes
	nEachBucketSeconds       = 60 // each bucket for 1 minute.
)

type RecordRPCURLInfo struct {
	buckets []RecordRPCURLBucket
	lock    *sync.RWMutex
}

type RecordRPCURLBucket struct {
	SumCount     int64
	SuccessCount int64

	period string
}

func NewRecordRPCURLInfo() *RecordRPCURLInfo {
	return &RecordRPCURLInfo{
		buckets: make([]RecordRPCURLBucket, nRecordRPCURLInfoBuckets),
		lock:    &sync.RWMutex{},
	}
}

func (r *RecordRPCURLInfo) Incr(success bool) {
	r.lock.Lock()
	defer r.lock.Unlock()
	bucket := r.getBucket(time.Now().Unix())
	r.buckets[bucket].Incr(success)
}

func (r *RecordRPCURLInfo) FailRate() int {
	nowSecs := time.Now().Unix()
	var sum, success int64
	r.lock.RLock()
	defer r.lock.RUnlock()
	for i := 0; i < 30; i++ {
		idx := r.getBucket(nowSecs)
		bucket := r.buckets[idx]

		if bucket.period != bucket.getPeriod(nowSecs) {
			continue
		}
		sum += bucket.SumCount
		success += bucket.SuccessCount
		nowSecs -= 60 // previous bucket
	}
	if sum == 0 {
		return 0
	}
	fail := sum - success
	failRate := int((fail * 100) / sum)
	return failRate
}

func (r *RecordRPCURLInfo) getBucket(nowSecs int64) int64 {
	return nowSecs / nEachBucketSeconds % nRecordRPCURLInfoBuckets
}

func (bucket *RecordRPCURLBucket) Incr(success bool) {
	period := bucket.getPeriod(time.Now().Unix())

	if period != bucket.period {
		bucket.period = period
		bucket.SuccessCount = 0
		bucket.SumCount = 0
	}

	bucket.SumCount++
	if success {
		bucket.SuccessCount++
	}
}

func (bucket *RecordRPCURLBucket) getPeriod(nowSecs int64) string {
	return time.Unix(nowSecs, 0).Format("2006-01-02T15:04")
}

type UbiquityUtxo struct {
	Total int `json:"total"`
	Data  []struct {
		Status  string `json:"status"`
		IsSpent bool   `json:"is_spent"`
		Value   int    `json:"value"`
		Mined   struct {
			Index         int    `json:"index"`
			TxId          string `json:"tx_id"`
			Date          int    `json:"date"`
			BlockId       string `json:"block_id"`
			BlockNumber   int    `json:"block_number"`
			Confirmations int    `json:"confirmations"`
			Meta          struct {
				Addresses  []string `json:"addresses"`
				Index      int      `json:"index"`
				Script     string   `json:"script"`
				ScriptType string   `json:"script_type"`
			} `json:"meta"`
		} `json:"mined"`
	} `json:"data"`
}

type BlockcypherUtxo struct {
	Address            string `json:"address"`
	TotalReceived      int    `json:"total_received"`
	TotalSent          int    `json:"total_sent"`
	Balance            int    `json:"balance"`
	UnconfirmedBalance int    `json:"unconfirmed_balance"`
	FinalBalance       int    `json:"final_balance"`
	NTx                int    `json:"n_tx"`
	UnconfirmedNTx     int    `json:"unconfirmed_n_tx"`
	FinalNTx           int    `json:"final_n_tx"`
	Txrefs             []struct {
		TxHash        string    `json:"tx_hash"`
		BlockHeight   int       `json:"block_height"`
		TxInputN      int       `json:"tx_input_n"`
		TxOutputN     int       `json:"tx_output_n"`
		Value         int       `json:"value"`
		RefBalance    int       `json:"ref_balance"`
		Confirmations int       `json:"confirmations"`
		Confirmed     time.Time `json:"confirmed"`
		DoubleSpend   bool      `json:"double_spend"`
		Spent         bool      `json:"spent"`
		SpentBy       string    `json:"spent_by,omitempty"`
	} `json:"txrefs"`
}