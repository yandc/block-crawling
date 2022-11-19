package types

import (
	"math/big"
	"time"

	"github.com/blockcypher/gobcy"
)

type BTCTestBlockerInfo = []BTCTestBlock

type BTCTestBlock struct {
	Txid     string `json:"txid"`
	Version  int    `json:"version"`
	Locktime int    `json:"locktime"`
	Vin      []struct {
		Txid    string `json:"txid"`
		Vout    int    `json:"vout"`
		Prevout struct {
			Scriptpubkey        string `json:"scriptpubkey"`
			ScriptpubkeyAsm     string `json:"scriptpubkey_asm"`
			ScriptpubkeyType    string `json:"scriptpubkey_type"`
			ScriptpubkeyAddress string `json:"scriptpubkey_address"`
			Value               int    `json:"value"`
		} `json:"prevout"`
		Scriptsig    string   `json:"scriptsig"`
		ScriptsigAsm string   `json:"scriptsig_asm"`
		Witness      []string `json:"witness"`
		IsCoinbase   bool     `json:"is_coinbase"`
		Sequence     int64    `json:"sequence"`
	} `json:"vin"`
	Vout []struct {
		Scriptpubkey        string `json:"scriptpubkey"`
		ScriptpubkeyAsm     string `json:"scriptpubkey_asm"`
		ScriptpubkeyType    string `json:"scriptpubkey_type"`
		ScriptpubkeyAddress string `json:"scriptpubkey_address"`
		Value               int    `json:"value"`
	} `json:"vout"`
	Size   int `json:"size"`
	Weight int `json:"weight"`
	Fee    int `json:"fee"`
	Status struct {
		Confirmed   bool   `json:"confirmed"`
		BlockHeight int    `json:"block_height"`
		BlockHash   string `json:"block_hash"`
		BlockTime   int    `json:"block_time"`
	} `json:"status"`
}

type BTCBlockerInfo struct {
	Hash       string   `json:"hash"`
	Ver        int      `json:"ver"`
	PrevBlock  string   `json:"prev_block"`
	MrklRoot   string   `json:"mrkl_root"`
	Time       int      `json:"time"`
	Bits       int      `json:"bits"`
	NextBlock  []string `json:"next_block"`
	Fee        int      `json:"fee"`
	Nonce      int      `json:"nonce"`
	NTx        int      `json:"n_tx"`
	Size       int      `json:"size"`
	BlockIndex int      `json:"block_index"`
	MainChain  bool     `json:"main_chain"`
	Height     int      `json:"height"`
	Weight     int      `json:"weight"`
	Tx         []Tx     `json:"tx"`
}
type SpendingOutpoints struct {
	TxIndex int64 `json:"tx_index"`
	N       int   `json:"n"`
}
type PrevOut struct {
	Spent             bool                `json:"spent"`
	Script            string              `json:"script"`
	SpendingOutpoints []SpendingOutpoints `json:"spending_outpoints"`
	TxIndex           int                 `json:"tx_index"`
	Value             int                 `json:"value"`
	N                 int64               `json:"n"`
	Type              int                 `json:"type"`
	Addr              string              `json:"addr,omitempty"`
}
type Inputs struct {
	Sequence int64   `json:"sequence"`
	Witness  string  `json:"witness"`
	Script   string  `json:"script"`
	Index    int     `json:"index"`
	PrevOut  PrevOut `json:"prev_out"`
}
type Out struct {
	Type              int           `json:"type"`
	Spent             bool          `json:"spent"`
	Value             int           `json:"value"`
	SpendingOutpoints []interface{} `json:"spending_outpoints"`
	N                 int           `json:"n"`
	TxIndex           int64         `json:"tx_index"`
	Script            string        `json:"script"`
	Addr              string        `json:"addr,omitempty"`
}
type Tx struct {
	Hash        string   `json:"hash"`
	Ver         int      `json:"ver"`
	VinSz       int      `json:"vin_sz"`
	VoutSz      int      `json:"vout_sz"`
	Size        int      `json:"size"`
	Weight      int      `json:"weight"`
	Fee         int      `json:"fee"`
	RelayedBy   string   `json:"relayed_by"`
	LockTime    int      `json:"lock_time"`
	TxIndex     int64    `json:"tx_index"`
	DoubleSpend bool     `json:"double_spend"`
	Time        int      `json:"time"`
	BlockIndex  int      `json:"block_index"`
	BlockHeight int      `json:"block_height"`
	Inputs      []Inputs `json:"inputs"`
	Out         []Out    `json:"out"`
}

type TXResponse struct {
	BlockHash               string `json:"block_hash"`
	BlockNumber             string `json:"block_number"`
	TransactionHash         string `json:"transaction_hash"`
	Status                  string `json:"status"`
	ExpirationTimestampSecs string `json:"expiration_timestamp_secs"`
	From                    string `json:"from"`
	To                      string `json:"to"`
	Fee                     string `json:"fee"`
	Size                    string `json:"size"`
	Amount                  string `json:"amount"`
	ChainName               string `json:"chain_name"`
	TransactionType         string `json:"transaction_type"`
}

type TX struct {
	BlockHash     string           `json:"block_hash,omitempty"`
	BlockHeight   int              `json:"block_height,omitempty"`
	Hash          string           `json:"hash,omitempty"`
	Addresses     []string         `json:"addresses,omitempty"`
	Total         big.Int          `json:"total,omitempty"`
	Fees          big.Int          `json:"fees,omitempty"`
	Size          int              `json:"size"`
	Preference    string           `json:"preference,omitempty"`
	RelayedBy     string           `json:"relayed_by,omitempty"`
	Received      time.Time        `json:"received,omitempty"`
	Confirmed     time.Time        `json:"confirmed,omitempty"`
	Confirmations int              `json:"confirmations,omitempty"`
	Confidence    float64          `json:"confidence,omitempty"`
	Ver           int              `json:"ver,omitempty"`
	LockTime      int              `json:"lock_time,omitempty"`
	DoubleSpend   bool             `json:"double_spend,omitempty"`
	DoubleOf      string           `json:"double_of,omitempty"`
	ReceiveCount  int              `json:"receive_count,omitempty"`
	VinSize       int              `json:"vin_sz,omitempty"`
	VoutSize      int              `json:"vout_sz,omitempty"`
	Hex           string           `json:"hex,omitempty"`
	DataProtocol  string           `json:"data_protocol,omitempty"`
	ChangeAddress string           `json:"change_address,omitempty"`
	NextInputs    string           `json:"next_inputs,omitempty"`
	NextOutputs   string           `json:"next_outputs,omitempty"`
	Inputs        []gobcy.TXInput  `json:"inputs"`
	Outputs       []gobcy.TXOutput `json:"outputs"`
	Error         string           `json:"error"`
}

type TXByHash struct {
	BlockHeight   int               `json:"block_height"`
	BlockIndex    int               `json:"block_index"`
	Hash          string            `json:"hash"`
	Addresses     []string          `json:"addresses"`
	Total         int               `json:"total"`
	Fees          int               `json:"fees"`
	Size          int               `json:"size"`
	Vsize         int               `json:"vsize"`
	Preference    string            `json:"preference"`
	RelayedBy     string            `json:"relayed_by"`
	Received      string            `json:"received"`
	Ver           int               `json:"ver"`
	DoubleSpend   bool              `json:"double_spend"`
	VinSz         int               `json:"vin_sz"`
	VoutSz        int               `json:"vout_sz"`
	Confirmations int               `json:"confirmations"`
	Inputs        []TXPendingInputs `json:"inputs"`
	Outputs       []TXPendingOut    `json:"outputs"`
}

type TXPendingInputs struct {
	PrevHash    string   `json:"prev_hash"`
	OutputIndex int      `json:"output_index"`
	OutputValue int      `json:"output_value"`
	Sequence    int64    `json:"sequence"`
	Addresses   []string `json:"addresses"`
	ScriptType  string   `json:"script_type"`
	Age         int      `json:"age"`
	Witness     []string `json:"witness"`
}

type TXPendingOut struct {
	Value      int      `json:"value"`
	Script     string   `json:"script"`
	Addresses  []string `json:"addresses"`
	ScriptType string   `json:"script_type"`
}
type ChainSOTX struct {
	Status string `json:"status"`
	Data   struct {
		Network       string `json:"network"`
		Txid          string `json:"txid"`
		Blockhash     string `json:"blockhash"`
		Confirmations int    `json:"confirmations"`
		Time          int    `json:"time"`
		Inputs        []struct {
			InputNo    int      `json:"input_no"`
			Value      string   `json:"value"`
			Address    string   `json:"address"`
			Type       string   `json:"type"`
			Script     string   `json:"script"`
			Sequence   int64    `json:"sequence"`
			Witness    []string `json:"witness"`
			FromOutput struct {
				Txid     string `json:"txid"`
				OutputNo int    `json:"output_no"`
			} `json:"from_output"`
		} `json:"inputs"`
		Outputs []struct {
			OutputNo int    `json:"output_no"`
			Value    string `json:"value"`
			Address  string `json:"address"`
			Type     string `json:"type"`
			Script   string `json:"script"`
		} `json:"outputs"`
		TxHex      string `json:"tx_hex"`
		NetworkFee string `json:"network_fee"`
		Size       int    `json:"size"`
		Vsize      int    `json:"vsize"`
		Version    int    `json:"version"`
		Locktime   int    `json:"locktime"`
	} `json:"data"`
}

