package tonclient

import (
	"block-crawling/internal/types/tontypes"
	"fmt"
	"strconv"
)

type tonapi struct {
	url       string
	chainName string
}

// GetAccount implements Clienter
func (r *tonapi) GetAccount(address string) (*tontypes.Account, error) {
	var result *TonAPIAccount
	if err := get(r.url, fmt.Sprint("/accounts/", address), nil, &result); err != nil {
		return nil, err
	}
	return &tontypes.Account{
		Balance: strconv.Itoa(result.Balance),
		Status:  result.Status,
	}, nil
}

// GetJettonHistories implements Clienter
func (r *tonapi) GetJettonTransfers(tx *tontypes.TX) ([]tontypes.JettonTransfer, error) {
	var result *TonAPIEventResult
	params := map[string]string{
		"start_date": strconv.Itoa(tx.Now),
		"end_date":   strconv.Itoa(tx.Now),
		"limit":      "100",
	}
	if err := get(r.url, fmt.Sprintf("/accounts/%s/jettons/history", tx.Account), params, &result); err != nil {
		return nil, err
	}
	results := make([]tontypes.JettonTransfer, 0, len(result.Events))
	for _, item := range result.Events {
		for _, action := range item.Actions {
			transfer := action.JettonTransfer
			if transfer == nil {
				continue
			}
			results = append(results, tontypes.JettonTransfer{
				QueryID:         item.EventID,
				Source:          &transfer.Sender.Address,
				Destination:     &transfer.Recipient.Address,
				Amount:          transfer.Amount,
				SourceWallet:    transfer.SendersWallet,
				JettonMaster:    transfer.Jetton.Address,
				TransactionHash: item.EventID,
				TransactionLT:   strconv.Itoa(item.LT),
				TransactionNow:  item.Timestamp,
			})
		}
	}
	return results, nil
}

// GetJettonWallet implements Clienter
func (c *tonapi) GetJettonWallet(address string, tokenAddress string) (*tontypes.JettonWallet, error) {
	var result *TonAPIJettonWallet
	path := fmt.Sprintf("/accounts/%s/jettons/%s", address, tokenAddress)
	if err := get(c.url, path, nil, &result); err != nil {
		return nil, err
	}
	return &tontypes.JettonWallet{
		Address:           address,
		Balance:           result.Balance,
		Owner:             result.Wallet.Address,
		Jetton:            result.Jetton.Address,
		LastTransactionLT: "0",
	}, nil
}

// GetMasterchainSeqno implements Clienter
func (c *tonapi) GetMasterchainSeqno() (uint64, error) {
	var result *TonAPIMasterchainHead
	if err := get(c.url, "/blockchain/masterchain-head", nil, &result); err != nil {
		return 0, err
	}
	return uint64(result.Seqno), nil
}

// GetTxsByMasterchainSeqno implements Clienter
func (c *tonapi) GetTxsByMasterchainSeqno(seqno uint64) ([]tontypes.TX, error) {
	var result *TonAPIShardResult
	path := fmt.Sprintf("/blockchain/masterchain/%d/shards", seqno)
	if err := get(c.url, path, nil, &result); err != nil {
		return nil, err
	}
	results := make([]tontypes.TX, 0, 4)
	for _, shard := range result.Shards {
		path := fmt.Sprintf("/blockchain/blocks/%s/transactions", shard.BlockID())
		var ret *TonAPITXResult
		if err := get(c.url, path, nil, &ret); err != nil {
			return nil, err
		}
		for _, item := range ret.TXs {
			tx := tontypes.TX{
				Account:       item.Account.Address,
				Hash:          item.Hash,
				LT:            strconv.Itoa(item.LT),
				Now:           item.UTime,
				OrigStatus:    item.OrigStatus,
				EndStatus:     item.EndStatus,
				TotalFees:     strconv.Itoa(item.TotalFess),
				PrevTransHash: item.PrevTransHash,
				PrevTransLT:   strconv.Itoa(item.PrevTransLT),
				Description:   tontypes.TXDesc{},
				BlockRef:      tontypes.TXBlockRef{},
				InMsg: tontypes.TXMsg{
					Hash:           "",
					Source:         new(string),
					Destination:    "",
					Value:          new(string),
					FWDFee:         new(string),
					IHRFee:         new(string),
					CreatedLT:      new(string),
					CreatedAt:      new(string),
					OPCode:         "",
					IHRDisabled:    new(bool),
					Bounce:         new(bool),
					Bounced:        false,
					ImportFee:      new(string),
					MessageContent: &tontypes.TXMsgContent{},
					InitState:      &tontypes.TXMsgState{},
				},
				OutMsgs:            []tontypes.TXMsg{},
				AccountStateBefore: tontypes.AccountState{},
				AccountStateAfter:  tontypes.AccountState{},
				MCBlockSeqno:       0,
			}
			results = append(results, tx)
		}
	}
	return results, nil
}

// GetTxsByMessageHash implements Clienter
func (*tonapi) GetTxsByMessageHash(msgHash string) ([]tontypes.TX, error) {
	panic("unimplemented")
}
