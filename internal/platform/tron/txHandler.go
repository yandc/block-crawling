package tron

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type txHandler struct {
	chainName   string
	block       *chain.Block
	txByHash    *chain.Transaction
	chainHeight uint64
	curHeight   uint64
	now         int64
	newTxs      bool

	txRecords []*data.TrxTransactionRecord
}

func (h *txHandler) OnNewTx(c chain.Clienter, block *chain.Block, tx *chain.Transaction) error {
	meta, err := common.AttemptMatchUser(h.chainName, tx)
	if err != nil {
		return err
	}
	if !(meta.User.MatchFrom || meta.User.MatchTo) {
		return nil
	}

	log.Info(
		"GOT NEW TX THAT MATCHED OUR USER",
		meta.WrapFields(
			zap.String("chainName", h.chainName),
			zap.Uint64("height", tx.BlockNumber),
			zap.String("nodeUrl", c.URL()),
			zap.String("txHash", tx.Hash),
			zap.Bool("handlePendingTx", !h.newTxs),
		)...,
	)
	rawTx := tx.Raw.(*rawTxWrapper)
	client := c.(*Client)
	status := "pending"
	if len(rawTx.Ret) > 0 {
		if rawTx.Ret[0].ContractRet == "SUCCESS" {
			status = "success"
		} else {
			status = "fail"
		}
	}
	var tokenInfo types.TokenInfo
	txInfo, err := client.GetTransactionInfoByHash(rawTx.TxID)
	if err != nil {
		return err
	}

	if tx.TxType == "transfer" {
		tokenInfo, _ = biz.GetTokenInfo(nil, h.chainName, rawTx.contractAddress)
		tokenInfo.Amount = rawTx.tokenAmount
	}
	feeData := map[string]interface{}{
		"net_usage": txInfo.Receipt.NetUsage,
	}
	if rawTx.contractAddress != "" {
		feeData["fee_limit"] = rawTx.RawData.FeeLimit
	}
	if txInfo.Receipt.EnergyUsage > 0 {
		feeData["energy_usage"] = txInfo.Receipt.EnergyUsage
	}
	feeAmount := 0
	if txInfo.Fee > 0 {
		feeAmount = txInfo.Fee
	}
	if txInfo.Receipt.NetFee > 0 && feeAmount == 0 {
		feeAmount = txInfo.Receipt.NetFee
	}
	tronMap := map[string]interface{}{
		"tvm":   map[string]string{},
		"token": tokenInfo,
	}
	parseData, _ := json.Marshal(tronMap)
	exTime := rawTx.RawData.Timestamp / 1000
	if rawTx.RawData.Timestamp == 0 {
		exTime = block.Time / 1000
	}
	amount, _ := decimal.NewFromString(tx.Value)
	rawBlock := block.Raw.(*types.BlockResponse)
	trxRecord := &data.TrxTransactionRecord{
		BlockHash:       block.Hash,
		BlockNumber:     rawBlock.BlockHeader.RawData.Number,
		TransactionHash: rawTx.TxID,
		FromAddress:     tx.FromAddress,
		ToAddress:       tx.ToAddress,
		FromUid:         meta.User.FromUid,
		ToUid:           meta.User.ToUid,
		FeeAmount:       decimal.NewFromInt(int64(feeAmount)),
		Amount:          amount,
		Status:          status,
		TxTime:          exTime,
		ContractAddress: rawTx.contractAddress,
		ParseData:       string(parseData),
		NetUsage:        strconv.Itoa(txInfo.Receipt.NetUsage),
		FeeLimit:        strconv.Itoa(rawTx.RawData.FeeLimit),
		EnergyUsage:     strconv.Itoa(txInfo.Receipt.EnergyUsage),
		TransactionType: string(tx.TxType),
		DappData:        "",
		ClientData:      "",
		CreatedAt:       h.now,
		UpdatedAt:       h.now,
	}
	h.txRecords = append(h.txRecords, trxRecord)
	return nil
}

func (h *txHandler) Save(c chain.Clienter) error {
	txRecords := h.txRecords
	curHeight := h.curHeight
	client := c.(*Client)

	if txRecords != nil && len(txRecords) > 0 {
		//保存交易数据
		err := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(h.chainName))
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", h.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(h.chainName+"主网扫块，将数据插入到数据库中失败", zap.Any("current", curHeight), zap.Any("error", err))
			return err
		}
		if h.newTxs {
			go HandleRecord(h.chainName, *client, txRecords)
		} else {
			go HandlePendingRecord(h.chainName, *client, txRecords)
		}

		if h.newTxs {
			records := make([]interface{}, 0, len(txRecords))
			for _, r := range txRecords {
				records = append(records, r)
			}
			common.SetResultOfTxs(h.block, records)
		} else {
			common.SetTxResult(h.txByHash, txRecords[0])
		}
	}
	return nil
}

func (h *txHandler) OnSealedTx(c chain.Clienter, txByHash *chain.Transaction) (err error) {
	rawTx := txByHash.Raw.(*types.TronTxInfoResponse)
	record := txByHash.Record.(*data.TrxTransactionRecord)
	curHeight := rawTx.BlockNumber
	block, err := c.GetBlock(uint64(curHeight))
	if err != nil {
		return err
	}
	for _, chainTx := range block.Transactions {
		tx := chainTx.Raw.(*rawTxWrapper)
		if len(tx.RawData.Contract) > 0 {
			if tx.TxID != record.TransactionHash {
				continue
			}

			if tx.RawData.Contract[0].Type == TRC10TYPE || tx.RawData.Contract[0].Parameter.Value.AssetName != "" {
				continue
			}
			h.OnNewTx(c, block, chainTx)
		}
	}
	return nil
}

func (h *txHandler) OnDroppedTx(c chain.Clienter, tx *chain.Transaction) error {
	record := tx.Record.(*data.TrxTransactionRecord)
	record.Status = biz.FAIL
	h.txRecords = append(h.txRecords, record)
	return nil
}
