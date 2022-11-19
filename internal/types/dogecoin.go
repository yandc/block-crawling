package types

type Dogecoin struct {
	Number   int         `json:"number"`
	Id       string      `json:"id"`
	ParentId string      `json:"parent_id"`
	Date     int         `json:"date"`
	NumTxs   int         `json:"num_txs"`
	Meta     interface{} `json:"meta"`
	Txs      []struct {
		Id          string      `json:"id"`
		BlockId     string      `json:"block_id"`
		Date        int         `json:"date"`
		Status      string      `json:"status"`
		NumEvents   int         `json:"num_events"`
		Meta        interface{} `json:"meta"`
		BlockNumber int         `json:"block_number"`
		Events      []Events `json:"events"`
	} `json:"txs"`
	Type   string `json:"type"`
	Code   int    `json:"code"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
}

type Events struct {
	Id            string `json:"id"`
	TransactionId string `json:"transaction_id"`
	Type          string `json:"type"`
	Denomination  string `json:"denomination"`
	Source        string `json:"source,omitempty"`
	Meta          *struct {
		Index int `json:"index"`
	} `json:"meta"`
	Date        int    `json:"date"`
	Decimals    int    `json:"decimals"`
	Destination string `json:"destination,omitempty"`
	Amount      int64  `json:"amount,omitempty"`
}
type Balances struct {
	Currency struct {
		AssetPath string `json:"asset_path"`
		Symbol    string `json:"symbol"`
		Name      string `json:"name"`
		Decimals  int    `json:"decimals"`
		Type      string `json:"type"`
	} `json:"currency"`
	ConfirmedBalance string `json:"confirmed_balance"`
	ConfirmedBlock   int    `json:"confirmed_block"`
}

type TxInfo struct {
	Id          string      `json:"id"`
	BlockId     string      `json:"block_id"`
	Date        int         `json:"date"`
	Status      interface{}      `json:"status"`
	NumEvents   int         `json:"num_events"`
	Meta        interface{} `json:"meta"`
	BlockNumber int         `json:"block_number"`
	Events      []struct {
		Id            string `json:"id"`
		TransactionId string `json:"transaction_id"`
		Type          string `json:"type"`
		Denomination  string `json:"denomination"`
		Source        string `json:"source,omitempty"`
		Meta          *struct {
			Index int `json:"index"`
		} `json:"meta"`
		Date        int    `json:"date"`
		Amount      int64  `json:"amount"`
		Decimals    int    `json:"decimals"`
		Destination string `json:"destination,omitempty"`
	} `json:"events"`

	Type   string `json:"type"`
	Code   int    `json:"code"`
	Title  string `json:"title"`
	Detail string `json:"detail"`

}

