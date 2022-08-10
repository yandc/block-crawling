package types

const (
	STATUSSUCCESS = "success"
	STATUSFAIL    = "fail"
	STATUSPENDING = "pending"
	NOSTATUS      = "no_status"
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
	Address  string `json:"address"`
	Amount   string `json:"amount"`
	Decimals int64  `json:"decimals"`
	Symbol   string `json:"symbol"`
}

type RecordRPCURLInfo struct {
	SumCount     int64
	SuccessCount int64
}
