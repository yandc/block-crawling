package model

type TransactionRecords struct {
	BlockHash         string `json:"block_hash"`
	BlockNumber       string `json:"block_number"`
	TransactionHash   string `json:"transaction_hash"`
	Status            string `json:"status"`
	TxTime            string `json:"tx_time"`
	Chain             string `json:"chain"`
	ChainName         string `json:"chain_name"`
	TransactionType   string `json:"transaction_type"`
	From              string `json:"from"`
	To                string `json:"to"`
	Data              string `json:"data"`
	Amount            string `json:"amount"`
	ContractAddress   string `json:"contract_address"`
	TransactionSource string `json:"transaction_source"`
	ParseData         string `json:"parse_data"`
	FeeAmount         string `json:"fee_amount"`
	FeeData           string `json:"fee_data"`
	EventLog          string `json:"event_log"`
}

type BTCTX struct {
	Result struct {
		Txid     string `json:"txid"`
		Hash     string `json:"hash"`
		Version  int    `json:"version"`
		Size     int    `json:"size"`
		Vsize    int    `json:"vsize"`
		Weight   int    `json:"weight"`
		Locktime int    `json:"locktime"`
		Vin      []struct {
			Txid      string `json:"txid"`
			Vout      int    `json:"vout"`
			ScriptSig struct {
				Asm string `json:"asm"`
				Hex string `json:"hex"`
			} `json:"scriptSig"`
			Txinwitness []string `json:"txinwitness"`
			Sequence    int64    `json:"sequence"`
		} `json:"vin"`
		Vout []struct {
			Value        float64 `json:"value"`
			N            int     `json:"n"`
			ScriptPubKey struct {
				Asm     string `json:"asm"`
				Hex     string `json:"hex"`
				Address string `json:"address"`
				Type    string `json:"type"`
			} `json:"scriptPubKey"`
		} `json:"vout"`
		Hex           string `json:"hex"`
		Blockhash     string `json:"blockhash"`
		Confirmations int    `json:"confirmations"`
		Time          int    `json:"time"`
		Blocktime     int    `json:"blocktime"`
	} `json:"result"`
	Error interface{} `json:"error"`
	Id    string      `json:"id"`
}

type JsonRpcRequest struct {
	Jsonrpc string      `json:"jsonrpc"`
	Id      string      `json:"id"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
}
type BTCCount struct {
	Result int         `json:"result"`
	Error  interface{} `json:"error"`
	Id     string      `json:"id"`
}


type MemoryPoolTX struct {
	Result []string    `json:"result"`
	Error  interface{} `json:"error"`
	Id     string      `json:"id"`
}

type UTXOBlock struct {
	Result struct {
		Hash              string  `json:"hash"`
		Confirmations     int     `json:"confirmations"`
		Height            int     `json:"height"`
		Version           int     `json:"version"`
		VersionHex        string  `json:"versionHex"`
		Merkleroot        string  `json:"merkleroot"`
		Time              int     `json:"time"`
		Mediantime        int     `json:"mediantime"`
		Nonce             int     `json:"nonce"`
		Bits              string  `json:"bits"`
		Difficulty        float64 `json:"difficulty"`
		Chainwork         string  `json:"chainwork"`
		NTx               int     `json:"nTx"`
		Previousblockhash string  `json:"previousblockhash"`
		Nextblockhash     string  `json:"nextblockhash"`
		Strippedsize      int     `json:"strippedsize"`
		Size              int     `json:"size"`
		Weight            int     `json:"weight"`
		Tx                []struct {
			Txid     string `json:"txid"`
			Hash     string `json:"hash"`
			Version  int    `json:"version"`
			Size     int    `json:"size"`
			Vsize    int    `json:"vsize"`
			Weight   int    `json:"weight"`
			Locktime int    `json:"locktime"`
			Vin      []struct {
				Coinbase    string   `json:"coinbase,omitempty"`
				Txinwitness []string `json:"txinwitness,omitempty"`
				Sequence    int64    `json:"sequence"`
				Txid        string   `json:"txid,omitempty"`
				Vout        int      `json:"vout,omitempty"`
				ScriptSig   struct {
					Asm string `json:"asm"`
					Hex string `json:"hex"`
				} `json:"scriptSig,omitempty"`
			} `json:"vin"`
			Vout []struct {
				Value        float64 `json:"value"`
				N            int     `json:"n"`
				ScriptPubKey struct {
					Asm     string `json:"asm"`
					Hex     string `json:"hex"`
					Address string `json:"address,omitempty"`
					Type    string `json:"type"`
				} `json:"scriptPubKey"`
			} `json:"vout"`
			Hex string  `json:"hex"`
			Fee float64 `json:"fee,omitempty"`
		} `json:"tx"`
	} `json:"result"`
	Error interface{} `json:"error"`
	Id    string      `json:"id"`
}