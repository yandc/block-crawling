package types

import (
	"time"
)

type BalanceResponse struct {
	BalanceValue string `json:"balance_value"`
}

type TransferResponse struct {
	DeployHash string `json:"deploy_hash"`
	From       string `json:"from"`
	To         string `json:"to"`
	Source     string `json:"source"`
	Target     string `json:"target"`
	Amount     string `json:"amount"`
	Gas        string `json:"gas"`
}
type TransferResult struct {
	Transfers []TransferResponse `json:"transfers"`
}

type BlockResult struct {
	Block CasperBlockResponse `json:"block"`
}

type CasperBlockResponse struct {
	Hash   string            `json:"hash"`
	Header CasperBlockHeader `json:"header"`
	Body   CasperBlockBody   `json:"body"`
	Proofs []Proof           `json:"proofs"`
}

type CasperBlockHeader struct {
	ParentHash      string    `json:"parent_hash"`
	StateRootHash   string    `json:"state_root_hash"`
	BodyHash        string    `json:"body_hash"`
	RandomBit       bool      `json:"random_bit"`
	AccumulatedSeed string    `json:"accumulated_seed"`
	Timestamp       time.Time `json:"timestamp"`
	EraID           int       `json:"era_id"`
	Height          int       `json:"height"`
	ProtocolVersion string    `json:"protocol_version"`
}

type CasperBlockBody struct {
	Proposer       string   `json:"proposer"`
	DeployHashes   []string `json:"deploy_hashes"`
	TransferHashes []string `json:"transfer_hashes"`
}

type Proof struct {
	PublicKey string `json:"public_key"`
	Signature string `json:"signature"`
}

type BlockParams struct {
	BlockIdentifier BlockIdentifier `json:"block_identifier"`
}

type BlockIdentifier struct {
	Hash   string `json:"Hash,omitempty"`
	Height uint64 `json:"Height,omitempty"`
}

type DeployResult struct {
	Deploy           JsonDeploy            `json:"deploy"`
	ExecutionResults []JsonExecutionResult `json:"execution_results"`
}

type DetailDeployResult struct {
	ClType string `json:"cl_type"`
	Bytes  string `json:"bytes"`
	Parsed string `json:"parsed"`
}

type JsonDeploy struct {
	Hash      string           `json:"hash"`
	Header    JsonDeployHeader `json:"header"`
	Approvals []JsonApproval   `json:"approvals"`
	Payment   struct {
		ModuleBytes struct {
			ModuleBytes string          `json:"module_bytes"`
			Args        [][]interface{} `json:"args"`
		} `json:"ModuleBytes"`
	} `json:"payment"`
	Session struct {
		Transfer struct {
			Args [][]interface{} `json:"args"`
		} `json:"Transfer"`
	} `json:"session"`
}

type JsonPutDeployRes struct {
	Hash string `json:"deploy_hash"`
}

type JsonDeployHeader struct {
	Account      string    `json:"account"`
	Timestamp    time.Time `json:"timestamp"`
	TTL          string    `json:"ttl"`
	GasPrice     int       `json:"gas_price"`
	BodyHash     string    `json:"body_hash"`
	Dependencies []string  `json:"dependencies"`
	ChainName    string    `json:"chain_name"`
}

type JsonApproval struct {
	Signer    string `json:"signer"`
	Signature string `json:"signature"`
}
type JsonExecutionResult struct {
	BlockHash string          `json:"block_hash"`
	Result    ExecutionResult `json:"result"`
}

//包含失败返回值
type ExecutionResult struct {
	Success      SuccessExecutionResult `json:"success"`
	ErrorMessage *string                `json:"error_message,omitempty"`
	Failure      struct {
		Effect struct {
			Operations []interface{} `json:"operations"`
			Transforms []struct {
				Key       string `json:"key"`
				Transform struct {
					WriteCLValue struct {
						ClType string `json:"cl_type"`
						Bytes  string `json:"bytes"`
						Parsed string `json:"parsed"`
					} `json:"WriteCLValue,omitempty"`
					AddUInt512 string `json:"AddUInt512,omitempty"`
				} `json:"transform"`
			} `json:"transforms"`
		} `json:"effect"`
		Transfers    []interface{} `json:"transfers"`
		Cost         string        `json:"cost"`
		ErrorMessage string        `json:"error_message"`
	} `json:"Failure"`
}

//成功返回值
type SuccessExecutionResult struct {
	Transfers []string `json:"transfers"`
	Cost      string   `json:"cost"`
}

type StoredValue struct {
	CLValue         *JsonCLValue          `json:"CLValue,omitempty"`
	Account         *JsonAccount          `json:"Account,omitempty"`
	Contract        *JsonContractMetadata `json:"Contract,omitempty"`
	ContractWASM    *string               `json:"ContractWASM,omitempty"`
	ContractPackage *string               `json:"ContractPackage,omitempty"`
	Transfer        *TransferResponse     `json:"Transfer,omitempty"`
	DeployInfo      *JsonDeployInfo       `json:"DeployInfo,omitempty"`
}

type AccountInfoResult struct {
	Account AccountInfo `json:"account,omitempty"`
}
type AccountInfo struct {
	MainPurse   string `json:"main_purse,omitempty"`
	AccountHash string `json:"account_hash,omitempty"`
}

type JsonCLValue struct {
	Bytes  string      `json:"bytes"`
	CLType string      `json:"cl_type"`
	Parsed interface{} `json:"parsed"`
}

type JsonAccount struct {
	AccountHash      string           `json:"account_hash"`
	NamedKeys        []NamedKey       `json:"named_keys"`
	MainPurse        string           `json:"main_purse"`
	AssociatedKeys   []AssociatedKey  `json:"associated_keys"`
	ActionThresholds ActionThresholds `json:"action_thresholds"`
}
type JsonContractMetadata struct {
	ContractPackageHash string `json:"contract_package_hash"`
	ContractWasmHash    string `json:"contract_wasm_hash"`
	ProtocolVersion     string `json:"protocol_version"`
}
type JsonDeployInfo struct {
	DeployHash string   `json:"deploy_hash"`
	Transfers  []string `json:"transfers"`
	From       string   `json:"from"`
	Source     string   `json:"source"`
	Gas        string   `json:"gas"`
}
type NamedKey struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

type AssociatedKey struct {
	AccountHash string `json:"account_hash"`
	Weight      uint64 `json:"weight"`
}
type ActionThresholds struct {
	Deployment    uint64 `json:"deployment"`
	KeyManagement uint64 `json:"key_management"`
}

type StoredValueResult struct {
	StoredValue StoredValue `json:"stored_value"`
}
