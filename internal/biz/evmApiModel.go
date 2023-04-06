package biz

import "time"

type EvmApiModel struct {
	Status  interface{} `json:"status"`
	Message string      `json:"message"`
	Result  interface{} `json:"result"`
}

type EvmApiRecord struct {
	BlockNumber       string `json:"blockNumber"`
	TimeStamp         string `json:"timeStamp"`
	Hash              string `json:"hash"`
	Nonce             string `json:"nonce"`
	BlockHash         string `json:"blockHash"`
	TransactionIndex  string `json:"transactionIndex"`
	From              string `json:"from"`
	To                string `json:"to"`
	Value             string `json:"value"`
	Gas               string `json:"gas"`
	GasPrice          string `json:"gasPrice"`
	IsError           string `json:"isError"`
	TxreceiptStatus   string `json:"txreceipt_status"`
	Input             string `json:"input"`
	ContractAddress   string `json:"contractAddress"`
	CumulativeGasUsed string `json:"cumulativeGasUsed"`
	GasUsed           string `json:"gasUsed"`
	Confirmations     string `json:"confirmations"`
	MethodId          string `json:"methodId"`
	FunctionName      string `json:"functionName"`
}

type KlaytnApiModel struct {
	Success bool           `json:"success"`
	Code    int            `json:"code"`
	Result  []KlaytnRecord `json:"result"`
	Tokens  struct {
	} `json:"tokens"`
	Page  int `json:"page"`
	Limit int `json:"limit"`
	Total int `json:"total"`
}

type KlaytnRecord struct {
	CreatedAt   int         `json:"createdAt"`
	TxHash      string      `json:"txHash"`
	TxType      string      `json:"txType"`
	TxStatus    int         `json:"txStatus"`
	InputHeader interface{} `json:"inputHeader"`
	MethodName  interface{} `json:"methodName"`
	BlockNumber int         `json:"blockNumber"`
	FromAddress string      `json:"fromAddress"`
	ToAddress   string      `json:"toAddress"`
	Amount      string      `json:"amount"`
	TxFee       string      `json:"txFee"`
	GasLimit    string      `json:"gasLimit"`
	GasUsed     string      `json:"gasUsed"`
	GasPrice    string      `json:"gasPrice"`
	Nonce       string      `json:"nonce"`
}
type ZksyncApiModel struct {
	List []struct {
		TransactionHash string `json:"transactionHash"`
		Data            struct {
			ContractAddress string        `json:"contractAddress"`
			Calldata        string        `json:"calldata"`
			Value           string        `json:"value"`
			FactoryDeps     []interface{} `json:"factoryDeps"`
		} `json:"data"`
		IsL1Originated   bool      `json:"isL1Originated"`
		Status           string    `json:"status"`
		Fee              string    `json:"fee"`
		Nonce            *int      `json:"nonce"`
		BlockNumber      int       `json:"blockNumber"`
		L1BatchNumber    int       `json:"l1BatchNumber"`
		BlockHash        string    `json:"blockHash"`
		IndexInBlock     int       `json:"indexInBlock"`
		InitiatorAddress string    `json:"initiatorAddress"`
		ReceivedAt       time.Time `json:"receivedAt"`
		EthCommitTxHash  string    `json:"ethCommitTxHash"`
		EthProveTxHash   string    `json:"ethProveTxHash"`
		EthExecuteTxHash string    `json:"ethExecuteTxHash"`
		Erc20Transfers   []struct {
			TokenInfo struct {
				L1Address string `json:"l1Address"`
				L2Address string `json:"l2Address"`
				Address   string `json:"address"`
				Symbol    string `json:"symbol"`
				Name      string `json:"name"`
				Decimals  int    `json:"decimals"`
				UsdPrice  string `json:"usdPrice"`
			} `json:"tokenInfo"`
			From   string `json:"from"`
			To     string `json:"to"`
			Amount string `json:"amount"`
		} `json:"erc20Transfers"`
		BalanceChanges []struct {
			TokenInfo struct {
				L1Address string `json:"l1Address"`
				L2Address string `json:"l2Address"`
				Address   string `json:"address"`
				Symbol    string `json:"symbol"`
				Name      string `json:"name"`
				Decimals  int    `json:"decimals"`
				UsdPrice  string `json:"usdPrice"`
			} `json:"tokenInfo"`
			From   string `json:"from"`
			To     string `json:"to"`
			Amount string `json:"amount"`
			Type   string `json:"type"`
		} `json:"balanceChanges"`
		Type     int `json:"type"`
		Transfer struct {
			TokenInfo struct {
				L1Address string `json:"l1Address"`
				L2Address string `json:"l2Address"`
				Address   string `json:"address"`
				Symbol    string `json:"symbol"`
				Name      string `json:"name"`
				Decimals  int    `json:"decimals"`
				UsdPrice  string `json:"usdPrice"`
			} `json:"tokenInfo"`
			From   string `json:"from"`
			To     string `json:"to"`
			Amount string `json:"amount"`
		} `json:"transfer,omitempty"`
	} `json:"list"`
	Total int `json:"total"`
}

type RoninApiModel struct {
	Results []RoninApiRecord `json:"results"`
	Total   int              `json:"total"`
}

type RoninApiRecord struct {
	Hash              string `json:"hash"`
	BlockHash         string `json:"block_hash"`
	BlockNumber       int    `json:"block_number"`
	From              string `json:"from"`
	To                string `json:"to"`
	Gas               string `json:"gas"`
	GasPrice          string `json:"gas_price"`
	Input             string `json:"input"`
	Nonce             int    `json:"nonce"`
	TxIndex           int    `json:"tx_index"`
	Value             string `json:"value"`
	GasUsed           string `json:"gas_used"`
	CumulativeGasUsed string `json:"cumulative_gas_used"`
	ContractAddress   string `json:"contract_address"`
	Status            int    `json:"status"`
	Timestamp         int    `json:"timestamp"`
}

type CasperApiModel struct {
	Data      []CasperApiRecord `json:"data"`
	PageCount int               `json:"pageCount"`
	ItemCount int               `json:"itemCount"`
	Pages     []struct {
		Number int    `json:"number"`
		Url    string `json:"url"`
	} `json:"pages"`
}

type CasperApiRecord struct {
	TransferId            string    `json:"transferId"`
	DeployHash            string    `json:"deployHash"`
	BlockHash             string    `json:"blockHash"`
	SourcePurse           string    `json:"sourcePurse"`
	TargetPurse           string    `json:"targetPurse"`
	Amount                string    `json:"amount"`
	FromAccount           string    `json:"fromAccount"`
	ToAccount             string    `json:"toAccount"`
	Timestamp             time.Time `json:"timestamp"`
	FromAccountPublicKey  string    `json:"fromAccountPublicKey"`
	ToAccountPublicKey    string    `json:"toAccountPublicKey"`
	CurrencyAmount        float64   `json:"currency_amount"`
	Rate                  float64   `json:"rate"`
	CurrentCurrencyAmount float64   `json:"current_currency_amount"`
}

type DogeApiModel struct {
	Transactions []DogeApiRecord `json:"transactions"`
	Success      int             `json:"success"`
	Error        string          `json:"error"`
}
type DogeApiRecord struct {
	Hash  string `json:"hash"`
	Value string `json:"value"`
	Time  int    `json:"time"`
	Block int    `json:"block"`
	Price string `json:"price"`
}
