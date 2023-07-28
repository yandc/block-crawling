package biz

import (
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"fmt"
)

type SignStatusRequest struct {
	TransactionHash string `json:"transaction_hash"`
	Status          string `json:"status"`
	TransactionType string `json:"transactionType" `
	Nonce           int64  `json:"nonce" `
	TxTime          int64  `json:"txTime"`
}

func SyncStatus(request []SignStatusRequest) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("SyncStatus error", e)
			} else {
				log.Errore("SyncStatus panic", e)
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：SyncStatus失败, error：%s",  fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	for _ ,record := range request {
		r, _ := data.UserSendRawHistoryRepoInst.SelectByTxHash(nil,record.TransactionHash)
		if record.Status == SUCCESS || record.Status == FAIL {
			r.SignStatus = "2"
		}
		if record.Status == PENDING || record.Status == NO_STATUS {
			r.SignStatus = "1"
		}
		if record.Status == DROPPED_REPLACED {
			r.Nonce = record.Nonce
			r.SignStatus = "4"
		}
		if record.Status == DROPPED {
			r.SignStatus = "3"
		}

		r.TransactionType = record.TransactionType
		r.TxTime = record.TxTime
		var rr []*data.UserSendRawHistory
		rr = append(rr, r)
		data.UserSendRawHistoryRepoInst.SaveOrUpdate(nil, rr)
	}

}
