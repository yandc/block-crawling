package tontypes

import (
	"encoding/json"
	"fmt"
)

type Response interface {
	UnwrapErr() error
}

type EmbeddedHeader struct {
	OK     *bool   `json:"ok,omitempty"`
	Result *string `json:"result,omitempty"`
	Code   *int    `json:"code,omitempty"`
}

func (e *EmbeddedHeader) UnwrapErr() error {
	if e.OK != nil && !*e.OK {
		var result string
		var code int
		if e.Result != nil {
			result = *e.Result
		}
		if e.Code != nil {
			code = *e.Code
		}
		return fmt.Errorf("%s(%d)", result, code)
	}
	return nil
}

type MasterchainInfo struct {
	EmbeddedHeader

	Last Shard `json:"last"`
	Init Shard `json:"init"`
}

type Shard struct {
	Workchain int    `json:"workchain"`
	Shard     string `json:"shard"`
	Seqno     int    `json:"seqno"`
	RootHash  string `json:"root_hash"`
	FileHash  string `json:"file_hash"`
	// omit some unnecessary fields
}

type ShardResult struct {
	EmbeddedHeader

	Shards []Shard `json:"blocks"`
}

type ShardState struct {
	EmbeddedHeader

	Blocks []Shard `json:"blocks"`
}

type TXResult struct {
	EmbeddedHeader

	Txs         []TX                 `json:"transactions"`
	AddressBook map[string]TXAddress `json:"address_book"`
	Error       *string              `json:"error"`
}

type TX struct {
	Account       string     `json:"account"`
	Hash          string     `json:"hash"`
	LT            string     `json:"lt"`
	Now           int        `json:"now"`
	OrigStatus    string     `json:"orig_status"`
	EndStatus     string     `json:"end_status"`
	TotalFees     string     `json:"total_fees"`
	PrevTransHash string     `json:"prev_trans_hash"`
	PrevTransLT   string     `json:"prev_trans_lt"`
	Description   TXDesc     `json:"description"`
	BlockRef      TXBlockRef `json:"block_ref"`
	// Incoming message value:
	//   value_{in_msg} - SUM(value_{out_msg}) - fee
	// Fee:
	//   fee + storage_fee + other_fee
	// See also:
	//   https://docs.ton.org/develop/dapps/asset-processing/#incoming-message-value
	// Incoming payments: incoming message has a source address.
	// Outgoing payments: incoming message has no source address.
	InMsg   TXMsg   `json:"in_msg"`
	OutMsgs []TXMsg `json:"out_msgs"`

	AccountStateBefore AccountState `json:"account_state_before"`
	AccountStateAfter  AccountState `json:"account_state_after"`
	MCBlockSeqno       int          `json:"mc_block_seqno"`
}

type TXDesc struct {
	Type     string       `json:"type"`
	Action   TXDescAction `json:"action"`
	Aborted  bool         `json:"aborted"`
	CreditPH struct {
		Credit string `json:"credit"`
	} `json:"credit_ph"`
	Destroyed   bool        `json:"destroyed"`
	ComputePH   TXComputePH `json:"compute_ph"`
	StoragePH   TXStoragePH `json:"storage_ph"`
	CreditFirst bool        `json:"credit_first"`
}

type TXDescAction struct {
	Valid           bool         `json:"valid"`
	Success         bool         `json:"success"`
	NoFunds         bool         `json:"no_funds"`
	ResultCode      int          `json:"result_code"`
	TOTActions      int          `json:"tot_actions"`
	MsgsCreated     int          `json:"msgs_created"`
	SpecActions     int          `json:"spec_actions"`
	TOTMsgSize      TXTOTMsgSize `json:"tot_msg_size"`
	StatusChange    string       `json:"status_change"`
	TotalFWDFees    string       `json:"total_fwd_fees"`
	SkippedActions  int          `json:"skipped_actions"`
	ActionListHash  string       `json:"action_list_hash"`
	TotalActionFees string       `json:"total_action_fees"`
}

type TXTOTMsgSize struct {
	Bits  string `json:"bits"`
	Cells string `json:"cells"`
}

type TXComputePH struct {
	Mode             int    `json:"mode"`
	Type             string `json:"type"`
	Success          bool   `json:"success"`
	GasFees          string `json:"gas_fees"`
	GasUsed          string `json:"gas_used"`
	VMSteps          int    `json:"vm_steps"`
	ExitCode         int    `json:"exit_code"`
	GasLimit         string `json:"gas_limit"`
	MsgStateUsed     bool   `json:"msg_state_used"`
	AccountActivated bool   `json:"account_activated"`
	VMInitStateHash  string `json:"vm_init_state_hash"`
	VMFinalStateHash string `json:"vm_final_state_hash"`
}

type TXStoragePH struct {
	StatusChange         string `json:"status_change"`
	StorageFeesCollected string `json:"storage_fees_collected"`
}

type TXBlockRef struct {
	Workchain int    `json:"workchain"`
	Shard     string `json:"shard"`
	Seqno     int    `json:"seqno"`
}

type TXMsg struct {
	Hash           string        `json:"hash"`
	Source         *string       `json:"source"`
	Destination    string        `json:"destination"`
	Value          *string       `json:"value"`
	FWDFee         *string       `json:"fwd_fee"`
	IHRFee         *string       `json:"ihr_fee"`
	CreatedLT      *string       `json:"created_lt"`
	CreatedAt      *string       `json:"created_at"`
	OPCode         string        `json:"opcode"`
	IHRDisabled    *bool         `json:"ihr_disabled"`
	Bounce         *bool         `json:"bounce"`
	Bounced        bool          `json:"bounced"`
	ImportFee      *string       `json:"import_fee"`
	MessageContent *TXMsgContent `json:"message_content"`
	InitState      *TXMsgState   `json:"init_state"`
}

type TXMsgState struct {
	Hash string `json:"hash"`
	Body string `json:"body"`
}

type TXMsgContent struct {
	Hash string `json:"hash"`
	Body string `json:"body"`
	/*
			   {
		       "type": "text_comment",
		       "comment": "Absurd Check-in #449241, day 22"
		     }
	*/
	Decoded json.RawMessage `json:"decoded"`
}

type AccountState struct {
	Hash          string  `json:"hash"`
	Balance       string  `json:"balance"`
	AccountStatus string  `json:"account_status"`
	FrozenHash    *string `json:"frozen_hash"`
	CodeHash      string  `json:"code_hash"`
	DataHash      string  `json:"data_hash"`
}

type Account struct {
	EmbeddedHeader

	Balance             string          `json:"balance"`
	Code                json.RawMessage `json:"code"`
	Data                string          `json:"data"`
	LastTransactionLT   string          `json:"last_transaction_lt"`
	LastTransactionHash string          `json:"last_transaction_hash"`
	FronzenHash         *string         `json:"frozen_hash"`
	Status              string          `json:"status"`
}

type TXAddress struct {
	UserFriendly string `json:"user_friendly"`
}

type JettonWalletResult struct {
	EmbeddedHeader

	JettonWallets []JettonWallet `json:"jetton_wallets"`
}

type JettonWallet struct {
	Address           string `json:"address"`
	Balance           string `json:"balance"`
	Owner             string `json:"owner"`
	Jetton            string `json:"jetton"`
	LastTransactionLT string `json:"last_transaction_lt"`
	CodeHash          string `json:"code_hash"`
	DataHash          string `json:"data_hash"`
}

type JettonTransferResult struct {
	EmbeddedHeader

	Transfers []JettonTransfer `json:"jetton_transfers"`
}

type JettonTransfer struct {
	QueryID             string  `json:"query_id"`
	Source              *string `json:"source"`
	Destination         *string `json:"destination"`
	Amount              string  `json:"amount"`
	SourceWallet        string  `json:"source_wallet"`
	JettonMaster        string  `json:"jetton_master"`
	TransactionHash     string  `json:"transaction_hash"`
	TransactionLT       string  `json:"transaction_lt"`
	TransactionNow      int     `json:"transaction_now"`
	ResponseDestination *string `json:"response_destination"`
	CustomPayload       *string `json:"custom_payload"`
	ForwardTonAmount    *string `json:"forward_ton_amount"`
	ForwardPayload      *string `json:"forward_payload"`
}

type NFTResult struct {
	EmbeddedHeader

	NFTs []NFT `json:"nft_items"`
}

type NFT struct {
	Address           string          `json:"address"`
	CollectionAddress string          `json:"collection_address"`
	OwnerAddress      string          `json:"owner_address"`
	Init              bool            `json:"init"`
	Index             string          `json:"index"`
	LastTransactionLT string          `json:"last_transaction_lt"`
	CodeHash          string          `json:"code_hash"`
	DataHash          string          `json:"data_hash"`
	Content           json.RawMessage `json:"content"`
	Collection        NFTCollection   `json:"collection"`
}

type NFTCollection struct {
	Address           string          `json:"address"`
	OwnerAddress      string          `json:"owner_address"`
	LastTransactionLT string          `json:"last_transaction_lt"`
	CodeHash          string          `json:"code_hash"`
	DataHash          string          `json:"data_hash"`
	NextItemIndex     string          `json:"next_item_index"`
	Content           json.RawMessage `json:"collection_content"`
}

type NFTTransferResult struct {
	EmbeddedHeader

	Transfers []NFTTransfer `json:"transfers"`
}

type NFTTransfer struct {
	QueryID             string  `json:"query_id"`
	NFTAddress          string  `json:"nft_address"`
	TransactionHash     string  `json:"transaction_hash"`
	TransactionLT       string  `json:"transaction_lt"`
	OldOwner            string  `json:"old_owner"`
	NewOwner            string  `json:"new_owner"`
	ResponseDestination *string `json:"response_destination"`
	CustomPayload       *string `json:"custom_payload"`
	ForwardTonAmount    *string `json:"forward_ton_amount"`
	ForwardPayload      *string `json:"forward_payload"`
}
