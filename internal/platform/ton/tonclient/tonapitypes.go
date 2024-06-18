package tonclient

import (
	"encoding/json"
	"fmt"
	"net/url"
)

type TonAPIAccount struct {
	Address      string `json:"address"`
	Balance      int    `json:"balance"`
	LastActivity int    `json:"last_activity"`
	Status       string `json:"status"`
	Name         string `json:"name"`
	IsScam       bool   `json:"is_scam"`
	Icon         string `json:"icon"`
	MemoRequired bool   `json:"memo_required"`
	IsSuspended  bool   `json:"is_suspended"`
	IsWallet     bool   `json:"is_wallet"`
}

type TonAPIEventResult struct {
	Events   []TonAPIEvent `json:"events"`
	NextFrom int           `json:"next_from"`
}

type TonAPIEvent struct {
	EventID string `json:"event_id"`
	// 该用 /events/:event-id 没有该字段
	// Account    TonAPIAccount  `json:"account"`
	Timestamp  int               `json:"timestamp"`
	Actions    []TonAPIAction    `json:"actions"`
	ValueFlow  []TonAPIValueFlow `json:"value_flow"`
	IsScam     bool              `json:"is_scam"`
	LT         int               `json:"lt"`
	InProgress bool              `json:"in_progress"`
	// Extra      int            `josn:"extra"`
}

type TonAPIAction struct {
	Type             string   `json:"type"`
	Status           string   `json:"status"`
	BaseTransactions []string `json:"base_transactions"`

	TonTransfer     *TonAPIActionTonTransfer     `json:"TonTransfer,omitempty"`
	ContractDeploy  *TonAPIActionContractDeploy  `json:"ContractDeploy,omitempty"`
	JettonTransfer  *TonAPIActionJettonTransfer  `json:"JettonTransfer,omitempty"`
	JettonBurn      *TonAPIActionJettonBurn      `json:"JettonBurn,omitempty"`
	JettonMint      *TonAPIActionJettonMint      `json:"JettonMint,omitempty"`
	NftItemTransfer *TonAPIActionNftItemTransfer `json:"NftItemTransfer,omitempty"`
	Subscribe       *TonAPIActionSubscribe       `json:"Subscribe,omitempty"`
	UnSubscribe     *TonAPIActionSubscribe       `json:"UnSubscribe,omitempty"`
	// Omit:
	// - AuctionBid
	NftPurchase *TonAPIActionNftPurchase `json:"NftPurchase,omitempty"`
	// - DepositStake
	// - WithdrawStake
	// - WithdrawStakeRequest
	// - ElectionsDepositStake
	// - ElectionsRecoverStake
	JettonSwap        *TonAPIActionJettonSwap        `json:"JettonSwap,omitempty"`
	SmartContractExec *TonAPIActionSmartContractExec `json:"SmartContractExec,omitempty"`
	// - DomainRenew
	// - InscriptionTransfer
	// - InscriptionMint
}

type TonAPIValueFlow struct {
	Account TonAPIAccount           `json:"account"`
	Ton     int                     `json:"ton"`
	Fees    int                     `json:"fees"`
	Jettons []TonAPIValueFlowJetton `json:"jettons"`
}

type TonAPIValueFlowJetton struct {
	Account  TonAPIAccount `json:"account"`
	Jetton   TonAPIJetton  `json:"jetton"`
	Quantity int           `json:"quantity"`
}

type TonAPIActionTonTransfer struct {
	Sender           TonAPIAccount          `json:"sender"`
	Recipient        TonAPIAccount          `json:"recipient"`
	Amount           int                    `json:"amount"`
	Comment          string                 `json:"comment"`
	EncryptedComment TonAPIEncryptedComment `json:"encrypted_Comment"`
	Refund           TonAPIRefund           `json:"refund"`
}

type TonAPIEncryptedComment struct {
	EncryptionType string `json:"encryption_type"`
	CipherText     string `json:"cipher_text"`
}

type TonAPIRefund struct {
	Type   string `json:"type"`
	Origin string `json:"origin"`
}

type TonAPIActionContractDeploy struct {
	Address    string   `json:"address"`
	Interfaces []string `json:"interfaces"`
}

type TonAPIActionJettonTransfer struct {
	Sender           TonAPIAccount          `json:"sender"`
	Recipient        TonAPIAccount          `json:"recipient"`
	SendersWallet    string                 `json:"senders_wallet"`
	RecipientsWallet string                 `json:"recipients_wallet"`
	Amount           string                 `json:"amount"`
	Comment          string                 `json:"comment"`
	EncryptedComment TonAPIEncryptedComment `json:"encrypted_comment"`
	Refund           TonAPIRefund           `json:"refund"`
	Jetton           TonAPIJetton           `json:"jetton"`
}

type TonAPIJetton struct {
	Address      string `json:"address"`
	Name         string `json:"name"`
	Symbol       string `json:"symbol"`
	Decimals     int    `json:"decimals"`
	Image        string `json:"image"`
	Verification string `json:"verification"`
}

type TonAPIActionJettonBurn struct {
	Sender        TonAPIAccount `json:"sender"`
	SendersWallet string        `json:"senders_wallet"`
	Amount        string        `json:"amount"`
	Comment       string        `json:"comment"`
	Jetton        TonAPIJetton  `json:"jetton"`
}

type TonAPIActionJettonMint struct {
	Recipient        TonAPIAccount `json:"recipient"`
	RecipientsWallet string        `json:"recipients_wallet"`
	Amount           string        `json:"amount"`
	Comment          string        `json:"comment"`
	Jetton           TonAPIJetton  `json:"jetton"`
}

type TonAPIActionNftItemTransfer struct {
	Sender           TonAPIAccount          `json:"sender"`
	Recipient        TonAPIAccount          `json:"recipient"`
	NFT              string                 `json:"nft"`
	Comment          string                 `json:"comment"`
	EncryptedComment TonAPIEncryptedComment `json:"encrypted_Comment"`
	Refund           TonAPIRefund           `json:"refund"`
	Payload          string                 `json:"payload"`
}

type TonAPIActionSubscribe struct {
	Subscriber   TonAPIAccount `json:"subscriber"`
	Subscription string        `json:"subscription"`
	Beneficiary  TonAPIAccount `json:"beneficiary"`
	Amount       int           `json:"amount"`
	Initial      bool          `json:"initial"`
}

type TonAPIActionJettonSwap struct {
	DEX             string        `json:"dex"`
	AmountIn        string        `json:"amount_in"`
	AmountOut       string        `json:"amount_out"`
	TonIn           int           `json:"ton_in"`
	TonOut          int           `json:"ton_out"`
	UserWallet      TonAPIAccount `json:"user_wallet"`
	Router          TonAPIAccount `json:"router"`
	JettonMasterIn  *TonAPIJetton `json:"jetton_master_in"`
	JettonMasterOut *TonAPIJetton `json:"jetton_master_out"`
}

type TonAPIActionNftPurchase struct {
	AuctionType string        `json:"auction_type"`
	Amount      TonAPIPrice   `json:"amount"`
	NFT         TonAPINFTItem `json:"nft"`
	Seller      TonAPIAccount `json:"seller"`
	Buyer       TonAPIAccount `json:"buyer"`
}

type TonAPIPrice struct {
	Value     string `json:"value"`
	TokenName string `json:"token_name"`
}

type TonAPINFTItem struct {
	Address     string                  `json:"address"`
	Index       int                     `json:"index"`
	Collection  TonAPINFTCollection     `json:"collection"`
	Verified    bool                    `json:"verified"`
	Metadata    json.RawMessage         `json:"metadata"`
	Sale        TonAPINFTSale           `json:"sale"`
	Previews    []TonAPINFTImagePreview `json:"previews"`
	DNS         string                  `json:"dns"`
	ApprovedBy  []string                `json:"approved_by"`
	IncludeCNFT bool                    `json:"include_cnft"`
	Trust       string                  `json:"trust"`
}

type TonAPINFTCollection struct {
	Address     string `json:"address"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type TonAPINFTSale struct {
	Address string        `json:"address"`
	Market  TonAPIAccount `json:"market"`
	Owner   TonAPIAccount `json:"owner"`
	Price   TonAPIPrice   `json:"price"`
}

type TonAPINFTImagePreview struct {
	Resolution string `json:"resolution"`
	URL        string `json:"url"`
}

type TonAPIActionSmartContractExec struct {
	Executor    TonAPIAccount `json:"executor"`
	Contract    TonAPIAccount `json:"contract"`
	TonAttached int           `json:"ton_attached"`
	Operation   string        `json:"operation"`
}

type TonAPIJettonWallet struct {
	Balance string        `json:"balance"`
	Wallet  TonAPIAccount `json:"wallet_address"`
	Jetton  TonAPIAccount `json:"jetton"`
	Lock    TonAPILock    `json:"lock"`
}

type TonAPILock struct {
	Amount string `json:"amount"`
	Till   int    `json:"till"`
}

type TonAPIMasterchainHead struct {
	Seqno int `json:"seqno"`
}

type TonAPIShardResult struct {
	Shards []TonAPIShard `json:"shards"`
}

type TonAPIShard struct {
	WorkchainID int    `json:"workchain_id"`
	Shard       string `json:"shard"`
	Seqno       int    `json:"seqno"`
}

func (s *TonAPIShard) BlockID() string {
	v := fmt.Sprintf("(%d,%s,%d)", s.WorkchainID, s.Shard, s.Seqno)
	return url.PathEscape(v)
}

type TonAPITXResult struct {
	TXs []TonAPITX `json:"transactions"`
}

type TonAPITX struct {
	Hash            string        `json:"hash"`
	LT              int           `json:"lt"`
	Account         TonAPIAccount `json:"account"`
	Success         bool          `json:"success"`
	UTime           int           `json:"utime"`
	OrigStatus      string        `json:"orig_status"`
	EndStatus       string        `json:"end_status"`
	TotalFess       int           `json:"total_fees"`
	EndBalance      int           `json:"end_balance"`
	TransactionType string        `json:"transaction_type"`
	StateUpdateOld  string        `json:"state_update_old"`
	StateUpdateNew  string        `json:"state_update_new"`
	InMsg           TonAPITxMsg   `json:"in_msg"`
	OutMsgs         []TonAPITxMsg `json:"out_msgs"`
	Block           string        `json:"block"`
	PrevTransHash   string        `json:"prev_trans_hash"`
	PrevTransLT     int           `json:"prev_trans_lt"`
	Aborted         bool          `json:"aborted"`
	destroyed       bool          `json:"destroyed"`
}

type TonAPITxMsg struct {
	MsgType       string          `json:"msg_type"`
	CreatedLT     int             `json:"created_lt"`
	IHRDisabled   bool            `json:"ihr_disabled"`
	Bounce        bool            `json:"bounce"`
	Bounced       bool            `json:"bounced"`
	Value         int             `json:"value"`
	FWDFee        int             `json:"fwd_fee"`
	IHRFee        int             `json:"ihr_fee"`
	Destination   TonAPIAccount   `json:"destination"`
	ImportFee     int             `json:"import_fee"`
	CreatedAt     int             `json:"created_at"`
	RawBody       string          `json:"raw_body"`
	DecodedOpName string          `json:"decoded_op_name"`
	DecodedBody   json.RawMessage `json:"decoded_body"`
}
