package types

import (
	"encoding/json"
	"fmt"
)

//Request is a jsonrpc request
type Request struct {
	ID      int           `json:"id"`
	Jsonrpc string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

// ErrorObject is a jsonrpc error
type ErrorObject struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// Response is a jsonrpc response
type Response struct {
	ID     uint64          `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *ErrorObject    `json:"error,omitempty"`
}

// Error implements error interface
func (e *ErrorObject) Error() string {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Sprintf("jsonrpc.internal marshal error: %v", err)
	}
	return string(data)
}

type Block struct {
	BlockHeader BlockHeader `json:"header"`
	BlockBody   BlockBody   `json:"body"`
}

type BlockBody struct {
	UserTransactions []UserTransaction `json:"Full"`
}

type BlockHeader struct {
	BlockHash string `json:"block_hash"`
	ParentHash string `json:"parent_hash"`
	TimeStamp string `json:"timestamp"`
	Height    string `json:"number"`
}

type Chain struct {
	ChainID     int         `json:"chain_id"`
	GenesisHash string      `json:"genesis_hash"`
	Header      BlockHeader `json:"head"`
}

type BlockTxnInfos struct {
	TransactionHash string `json:"transaction_hash"`
	GasUsed         string `json:"gas_used"`
	Status          string `json:"status"`
}

type Transaction struct {
	BlockHash        string          `json:"block_hash"`
	BlockNumber      string          `json:"block_number"`
	TransactionHash  string          `json:"transaction_hash"`
	TransactionIndex int             `json:"transaction_index"`
	BlockMetadata    BlockMetadata   `json:"block_metadata"`
	UserTransaction  UserTransaction `json:"user_transaction"`
}

type BlockMetadata struct {
	Author        string `json:"author"`
	ChainID       string `json:"chain_id"`
	Number        string `json:"number"`
	ParentGasUsed int    `json:"parent_gas_used"`
	ParentHash    string `json:"parent_hash"`
	Timestamp     int64  `json:"timestamp"`
	Uncles        string `json:"uncles"`
}

type UserTransaction struct {
	TransactionHash string         `json:"transaction_hash"`
	RawTransaction  RawTransaction `json:"raw_txn"`
	Authenticator   Authenticator  `json:"authenticator"`
}

type RawTransaction struct {
	Sender                  string         `json:"sender"`
	SequenceNumber          string         `json:"sequence_number"`
	Payload                 string         `json:"payload"`
	DecodedPayload          DecodedPayload `json:"decoded_payload"`
	MaxGasAmount            string         `json:"max_gas_amount"`
	GasUnitPrice            string         `json:"gas_unit_price"`
	GasTokenCode            string         `json:"gas_token_code"`
	ExpirationTimestampSecs string         `json:"expiration_timestamp_secs"`
	ChainID                 int            `json:"chain_id"`
}

type DecodedPayload struct {
	ScriptFunction ScriptFunction `json:"ScriptFunction"`
}

type ScriptFunction struct {
	Module   string        `json:"module"`
	Function string        `json:"function"`
	TyArgs   []string      `json:"ty_args"`
	Args     []interface{} `json:"args"`
}

type Authenticator struct {
	Ed25519 Ed25519 `json:"Ed25519"`
}

type Ed25519 struct {
	PublicKey string `json:"public_key"`
	Signature string `json:"signature"`
}

type NodeInfo struct {
	PeerInfo PeerInfo `json:"peer_info"`
}

type PeerInfo struct {
	ChainInfo ChainInfo `json:"chain_info"`
}

type ChainInfo struct {
	Header struct {
		Height string `json:"number"`
	} `json:"head"`
}

type STCTXResponse struct {
	BlockHash               string    `json:"block_hash"`
	BlockNumber             string    `json:"block_number"`
	TransactionHash         string    `json:"transaction_hash"`
	Status                  string    `json:"status"`
	ExpirationTimestampSecs string    `json:"expiration_timestamp_secs"`
	From                    string    `json:"from"`
	To                      string    `json:"to"`
	Amount                  string    `json:"amount"`
	GasUsed                 string    `json:"gas_used"`
	GasPrice                string    `json:"gas_price"`
	GasLimit                string    `json:"gas_limit"`
	SequenceNumber          string    `json:"sequence_number"`
	ChainName               string    `json:"chain_name"`
	TransactionType         string    `json:"transaction_type"`
	Token                   TokenInfo `json:"token"`
}
