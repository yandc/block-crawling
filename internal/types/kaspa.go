package types

type KaspaBadResp struct {
	Detail string `json:"detail"`
}

type KaspaTransactionInfo struct {
	SubnetworkId            string   `json:"subnetwork_id"`
	TransactionId           string   `json:"transaction_id"`
	Hash                    string   `json:"hash"`
	Mass                    string   `json:"mass"`
	BlockHash               []string `json:"block_hash"`
	BlockTime               int64    `json:"block_time"`
	IsAccepted              bool     `json:"is_accepted"`
	AcceptingBlockHash      string   `json:"accepting_block_hash"`
	AcceptingBlockBlueScore int      `json:"accepting_block_blue_score"`
	Inputs                  []*struct {
		Id                                     int    `json:"id"`
		TransactionId                          string `json:"transaction_id"`
		Index                                  int    `json:"index"`
		PreviousOutpointHash                   string `json:"previous_outpoint_hash"`
		PreviousOutpointIndex                  string `json:"previous_outpoint_index"`
		SignatureScript                        string `json:"signature_script"`
		SigOpCount                             string `json:"sig_op_count"`
		PreviousOutpointAmount                 int64
		PreviousOutpointScriptPublicKeyAddress string
	} `json:"inputs"`
	Outputs []struct {
		Id                     int         `json:"id"`
		TransactionId          string      `json:"transaction_id"`
		Index                  int         `json:"index"`
		Amount                 int64       `json:"amount"`
		ScriptPublicKey        string      `json:"script_public_key"`
		ScriptPublicKeyAddress string      `json:"script_public_key_address"`
		ScriptPublicKeyType    string      `json:"script_public_key_type"`
		AcceptingBlockHash     interface{} `json:"accepting_block_hash"`
	} `json:"outputs"`
	KaspaBadResp
}
