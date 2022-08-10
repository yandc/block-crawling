package data

import "time"

type TransactionResponse struct {
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	BlockHash       string    `json:"block_hash"`
	BlockNumber     string    `json:"block_number"`
	TransactionHash string    `json:"transaction_hash"`
	Status          string    `json:"status"`
	TxTime          int64     `json:"tx_time"`
	Chain         string         `json:"chain"`
	ChainName string    `json:"chain_name"`
	TransactionType string    `json:"transaction_type"`
	TransactionSource string    `json:"transaction_source"`
	FromObj string    `json:"from_obj"`
	ToObj string    `json:"to_obj"`
	Data string    `json:"data"`
	Amount string    `json:"amount"`
	FeeAmount string    `json:"fee_amount"`
	FeeData string    `json:"fee_data"`
	ContractAddress string    `json:"contract_address"`
	ParseData string    `json:"parse_data"`
	DappData string    `json:"dapp_data"`
	EventLog string    `json:"event_log"`
	ClientData string    `json:"client_data"`
	ApproveLogicData string    `json:"approve_logic_data"`
}

type TBTransactionRecord struct {
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	BlockHash       string    `json:"block_hash"`
	BlockNumber     string    `json:"block_number"`
	TransactionHash string    `json:"transaction_hash"`
	Status          string    `json:"status"`
	TxTime          int64     `json:"tx_time"`
	Chain         string         `json:"chain"`
	ChainName string    `json:"chain_name"`
	TransactionType string    `json:"transaction_type"`
	TransactionSource string    `json:"transaction_source"`
	FromObj string    `json:"from_obj"`
	ToObj string    `json:"to_obj"`
	Data string    `json:"data"`
	Amount string    `json:"amount"`
	FeeAmount string    `json:"fee_amount"`
	FeeData string    `json:"fee_data"`
	ContractAddress string    `json:"contract_address"`
	ParseData string    `json:"parse_data"`
	DappData string    `json:"dapp_data"`
	EventLog string    `json:"event_log"`
	ClientData string    `json:"client_data"`
	ApproveLogicData string    `json:"approve_logic_data"`
}
func (tbTransactionRecord TBTransactionRecord) TableName() string {
	return "tb_transaction_record"
}
type TokenInfo struct {
	Address  string `json:"address"`
	Amount   string `json:"amount"`
	Decimals int64  `json:"decimals"`
	Symbol   string `json:"symbol"`
}
