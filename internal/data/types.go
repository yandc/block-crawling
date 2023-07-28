package data

import "time"

type TransactionRequest struct {
	FromUid                  string
	ToUid                    string
	Uid                      string
	FromAddress              string
	FromAddressList          []string
	ToAddress                string
	ToAddressList            []string
	Address                  string
	ContractAddress          string
	ContractAddressList      []string
	StatusList               []string
	StatusNotInList          []string
	TransactionType          string
	TransactionTypeNotEqual  string
	TransactionTypeList      []string
	TransactionTypeNotInList []string
	OperateTypeEmpty         bool
	OperateTypeList          []string
	TransactionHash          string
	TransactionHashList      []string
	TransactionHashNotInList []string
	TransactionHashLike      string
	OriginalHashList         []string
	Nonce                    int64
	DappDataEmpty            bool
	ClientDataNotEmpty       bool
	StartTime                int64
	StopTime                 int64
	TokenAddress             string
	AssetType                string
	OrderBy                  string
	DataDirection            int32
	StartIndex               int64
	PageNum                  int32
	PageSize                 int32
	Total                    bool
	OrParamList              []TransactionRequest
}

type TBTransactionRecord struct {
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
	BlockHash         string    `json:"block_hash"`
	BlockNumber       string    `json:"block_number"`
	TransactionHash   string    `json:"transaction_hash"`
	Status            string    `json:"status"`
	TxTime            int64     `json:"tx_time"`
	Chain             string    `json:"chain"`
	ChainName         string    `json:"chain_name"`
	TransactionType   string    `json:"transaction_type"`
	TransactionSource string    `json:"transaction_source"`
	FromObj           string    `json:"from_obj"`
	ToObj             string    `json:"to_obj"`
	Data              string    `json:"data"`
	Amount            string    `json:"amount"`
	FeeAmount         string    `json:"fee_amount"`
	FeeData           string    `json:"fee_data"`
	ContractAddress   string    `json:"contract_address"`
	ParseData         string    `json:"parse_data"`
	DappData          string    `json:"dapp_data"`
	EventLog          string    `json:"event_log"`
	ClientData        string    `json:"client_data"`
	ApproveLogicData  string    `json:"approve_logic_data"`
}

func (tbTransactionRecord TBTransactionRecord) TableName() string {
	return "tb_transaction_record"
}
