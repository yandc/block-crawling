package types

import "time"

type Blockspan struct {
	Chain   string      `json:"chain"`
	Total   int         `json:"total"`
	Cursor  interface{} `json:"cursor"`
	PerPage string      `json:"per_page"`
	Results []struct {
		TokenType       string      `json:"token_type"`
		ContractAddress string      `json:"contract_address"`
		Id              string      `json:"id"`
		Operator        interface{} `json:"operator"`
		FromAddress     string      `json:"from_address"`
		ToAddress       string      `json:"to_address"`
		TransferType    string      `json:"transfer_type"`
		BlockTimestamp  time.Time   `json:"block_timestamp"`
		BlockNumber     string      `json:"block_number"`
		LogIndex        int         `json:"log_index"`
		TransactionHash string      `json:"transaction_hash"`
		BatchIndex      int         `json:"batch_index"`
		Quantity        string      `json:"quantity"`
		Price           []struct {
			ContractAddress string    `json:"contract_address"`
			Id              string    `json:"id"`
			Hash            string    `json:"hash"`
			FromAddress     string    `json:"from_address"`
			ToAddress       string    `json:"to_address"`
			Date            time.Time `json:"date"`
			Quantity        string    `json:"quantity"`
			Price           string    `json:"price"`
		} `json:"price"`
	} `json:"results"`
}