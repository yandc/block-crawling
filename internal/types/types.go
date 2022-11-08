package types

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
	CollectionName string      `json:"collection_name,omitempty"`
	TokenType      string      `json:"token_type,omitempty"`
	TokenId        string      `json:"token_id,omitempty"`
	ItemName       string      `json:"item_name,omitempty"`
	ItemUri        string      `json:"item_uri,omitempty"`
	Tokens         []TokenInfo `json:"tokens,omitempty"`
}

type RecordRPCURLInfo struct {
	SumCount     int64
	SuccessCount int64
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