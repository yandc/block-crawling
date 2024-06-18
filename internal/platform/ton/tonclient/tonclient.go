package tonclient

import "block-crawling/internal/types/tontypes"

type Clienter interface {
	GetMasterchainSeqno() (uint64, error)
	GetTxsByMasterchainSeqno(seqno uint64) ([]tontypes.TX, error)
	GetTxsByMessageHash(msgHash string) ([]tontypes.TX, error)
	GetAccount(address string) (*tontypes.Account, error)
	GetJettonWallet(address string, tokenAddress string) (*tontypes.JettonWallet, error)
	GetJettonTransfers(tx *tontypes.TX) ([]tontypes.JettonTransfer, error)
}

func NewClient(chainName string, url string) Clienter {
	switch url {
	case "https://toncenter.com/api/v3":
		return &toncenter{
			chainName: chainName,
			url:       url,
		}
	case "https://tonapi.io/v2":
		return &tonapi{
			url:       url,
			chainName: chainName,
		}
	default:
		panic(url)
	}
}
