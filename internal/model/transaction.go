package model

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
				Asm       string   `json:"asm"`
				Hex       string   `json:"hex"`
				Address   string   `json:"address"`
				Addresses []string `json:"addresses"`
				Type      string   `json:"type"`
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
type UTXOBlockHash struct {
	Result string      `json:"result"`
	Error  interface{} `json:"error"`
	Id     string      `json:"id"`
}

type MemoryPoolTX struct {
	Result []string    `json:"result"`
	Error  interface{} `json:"error"`
	Id     string      `json:"id"`
}

type BTCBlockInfo struct {
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
type UnSpent struct {
	Result struct {
		Bestblock     string  `json:"bestblock"`
		Confirmations int     `json:"confirmations"`
		Value         float64 `json:"value"`
		ScriptPubKey  struct {
			Asm     string `json:"asm"`
			Hex     string `json:"hex"`
			Address string `json:"address"`
			Type    string `json:"type"`
		} `json:"scriptPubKey"`
		Coinbase bool `json:"coinbase"`
	} `json:"result"`
	Error interface{} `json:"error"`
	Id    string      `json:"id"`
}

type DogeBlockInfo struct {
	Result struct {
		Hash          string   `json:"hash"`
		Confirmations int      `json:"confirmations"`
		Strippedsize  int      `json:"strippedsize"`
		Size          int      `json:"size"`
		Weight        int      `json:"weight"`
		Height        int      `json:"height"`
		Version       int      `json:"version"`
		VersionHex    string   `json:"versionHex"`
		Merkleroot    string   `json:"merkleroot"`
		Tx            []string `json:"tx"`
		Time          int      `json:"time"`
		Mediantime    int      `json:"mediantime"`
		Nonce         int      `json:"nonce"`
		Bits          string   `json:"bits"`
		Difficulty    float64  `json:"difficulty"`
		Chainwork     string   `json:"chainwork"`
		Auxpow        struct {
			Tx struct {
				Hex      string `json:"hex"`
				Txid     string `json:"txid"`
				Hash     string `json:"hash"`
				Size     int    `json:"size"`
				Vsize    int    `json:"vsize"`
				Version  int    `json:"version"`
				Locktime int    `json:"locktime"`
				Vin      []struct {
					Coinbase string `json:"coinbase"`
					Sequence int64  `json:"sequence"`
				} `json:"vin"`
				Vout []struct {
					Value        float64 `json:"value"`
					N            int     `json:"n"`
					ScriptPubKey struct {
						Asm       string   `json:"asm"`
						Hex       string   `json:"hex"`
						ReqSigs   int      `json:"reqSigs,omitempty"`
						Type      string   `json:"type"`
						Addresses []string `json:"addresses,omitempty"`
					} `json:"scriptPubKey"`
				} `json:"vout"`
				Blockhash string `json:"blockhash"`
			} `json:"tx"`
			Index             int      `json:"index"`
			Chainindex        int      `json:"chainindex"`
			Merklebranch      []string `json:"merklebranch"`
			Chainmerklebranch []string `json:"chainmerklebranch"`
			Parentblock       string   `json:"parentblock"`
		} `json:"auxpow"`
		Previousblockhash string `json:"previousblockhash"`
		Nextblockhash     string `json:"nextblockhash"`
	} `json:"result"`
	Error interface{} `json:"error"`
	Id    string      `json:"id"`
}

type UTXOBlockInfo struct {
	Result struct {
		Hash              string   `json:"hash"`
		Confirmations     int      `json:"confirmations"`
		Strippedsize      int      `json:"strippedsize"`
		Size              int      `json:"size"`
		Weight            int      `json:"weight"`
		Height            int      `json:"height"`
		Version           int      `json:"version"`
		VersionHex        string   `json:"versionHex"`
		Merkleroot        string   `json:"merkleroot"`
		Tx                []string `json:"tx"`
		Time              int      `json:"time"`
		Mediantime        int      `json:"mediantime"`
		Nonce             int      `json:"nonce"`
		Bits              string   `json:"bits"`
		Difficulty        float64  `json:"difficulty"`
		Chainwork         string   `json:"chainwork"`
		NTx               int      `json:"nTx"`
		Previousblockhash string   `json:"previousblockhash"`
		Nextblockhash     string   `json:"nextblockhash"`
	} `json:"result"`
	Error interface{} `json:"error"`
	Id    string      `json:"id"`
}
