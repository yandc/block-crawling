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
	Results []*RoninApiRecord `json:"results"`
	Total   int               `json:"total"`
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
	Data      []*CasperApiRecord `json:"data"`
	PageCount int                `json:"pageCount"`
	ItemCount int                `json:"itemCount"`
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
	Transactions []*DogeApiRecord `json:"transactions"`
	Success      int              `json:"success"`
	Error        string           `json:"error"`
}
type DogeApiRecord struct {
	Hash  string `json:"hash"`
	Value string `json:"value"`
	Time  int    `json:"time"`
	Block int    `json:"block"`
	Price string `json:"price"`
}
type PolkadotApiModel struct {
	Code        int    `json:"code"`
	Message     string `json:"message"`
	GeneratedAt int    `json:"generated_at"`
	Data        struct {
		Count     int                  `json:"count"`
		Transfers []*PolkadotApiRecord `json:"transfers"`
	} `json:"data"`
}

type PolkadotApiRecord struct {
	From               string `json:"from"`
	To                 string `json:"to"`
	ExtrinsicIndex     string `json:"extrinsic_index"`
	Success            bool   `json:"success"`
	Hash               string `json:"hash"`
	BlockNum           int    `json:"block_num"`
	BlockTimestamp     int    `json:"block_timestamp"`
	Module             string `json:"module"`
	Amount             string `json:"amount"`
	AmountV2           string `json:"amount_v2"`
	UsdAmount          string `json:"usd_amount"`
	Fee                string `json:"fee"`
	Nonce              int    `json:"nonce"`
	AssetSymbol        string `json:"asset_symbol"`
	AssetType          string `json:"asset_type"`
	FromAccountDisplay struct {
		Address string `json:"address"`
		Merkle  struct {
			TagType    string `json:"tag_type"`
			TagSubtype string `json:"tag_subtype"`
			TagName    string `json:"tag_name"`
		} `json:"merkle,omitempty"`
	} `json:"from_account_display"`
	ToAccountDisplay struct {
		Address string `json:"address"`
	} `json:"to_account_display"`
	EventIdx int `json:"event_idx"`
}
type PolkadotApiReq struct {
	Address   string `json:"address"`
	Direction string `json:"direction"`
	Page      int    `json:"page"`
	Row       int    `json:"row"`
}

type BTCApiModel struct {
	Txid  string `json:"txid"`
	Block struct {
		Height   int `json:"height"`
		Position int `json:"position"`
	} `json:"block"`
	Error   string `json:"error"`
	Message string `json:"message"`
}
type TornApiModel struct {
	Total      int              `json:"total"`
	RangeTotal int              `json:"rangeTotal"`
	Data       []*TornApiRecord `json:"data"`
	Message    string           `json:"message"`
}

type TornApiRecord struct {
	Block         int      `json:"block"`
	Hash          string   `json:"hash"`
	Timestamp     int64    `json:"timestamp"`
	OwnerAddress  string   `json:"ownerAddress"`
	ToAddressList []string `json:"toAddressList"`
	ToAddress     string   `json:"toAddress"`
	ContractType  int      `json:"contractType"`
	Confirmed     bool     `json:"confirmed"`
	Revert        bool     `json:"revert"`
	ContractData  struct {
		Amount          int    `json:"amount,omitempty"`
		OwnerAddress    string `json:"owner_address"`
		ToAddress       string `json:"to_address,omitempty"`
		Data            string `json:"data,omitempty"`
		ContractAddress string `json:"contract_address,omitempty"`
		AssetName       string `json:"asset_name,omitempty"`
		TokenInfo       struct {
			TokenId      string `json:"tokenId"`
			TokenAbbr    string `json:"tokenAbbr"`
			TokenName    string `json:"tokenName"`
			TokenDecimal int    `json:"tokenDecimal"`
			TokenCanShow int    `json:"tokenCanShow"`
			TokenType    string `json:"tokenType"`
			TokenLogo    string `json:"tokenLogo"`
			TokenLevel   string `json:"tokenLevel"`
			Vip          bool   `json:"vip"`
		} `json:"tokenInfo,omitempty"`
	} `json:"contractData"`
	SmartCalls  string `json:"SmartCalls"`
	Events      string `json:"Events"`
	Id          string `json:"id"`
	Data        string `json:"data"`
	Fee         string `json:"fee"`
	ContractRet string `json:"contractRet"`
	Result      string `json:"result"`
	Amount      string `json:"amount"`
	Cost        struct {
		NetFee             int `json:"net_fee"`
		EnergyPenaltyTotal int `json:"energy_penalty_total,omitempty"`
		EnergyUsage        int `json:"energy_usage"`
		Fee                int `json:"fee"`
		EnergyFee          int `json:"energy_fee"`
		EnergyUsageTotal   int `json:"energy_usage_total"`
		OriginEnergyUsage  int `json:"origin_energy_usage"`
		NetUsage           int `json:"net_usage"`
	} `json:"cost"`
	TokenInfo struct {
		TokenId      string `json:"tokenId"`
		TokenAbbr    string `json:"tokenAbbr"`
		TokenName    string `json:"tokenName"`
		TokenDecimal int    `json:"tokenDecimal"`
		TokenCanShow int    `json:"tokenCanShow"`
		TokenType    string `json:"tokenType"`
		TokenLogo    string `json:"tokenLogo"`
		TokenLevel   string `json:"tokenLevel"`
		Vip          bool   `json:"vip"`
	} `json:"tokenInfo"`
	TokenType       string `json:"tokenType"`
	OwnerAddressTag string `json:"ownerAddressTag,omitempty"`
	TriggerInfo     struct {
		Method    string `json:"method"`
		Data      string `json:"data"`
		Parameter struct {
			From  string `json:"_from,omitempty"`
			Value string `json:"_value,omitempty"`
			To    string `json:"_to,omitempty"`
		} `json:"parameter"`
		MethodId        string `json:"methodId"`
		MethodName      string `json:"methodName"`
		ContractAddress string `json:"contract_address"`
		CallValue       int    `json:"call_value"`
	} `json:"trigger_info,omitempty"`
}
