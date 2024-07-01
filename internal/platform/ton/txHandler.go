// Package ton handle ton transactions and save into database.
//
// We use the message hash(in_msg or out_msgs) as the unique transaction hash,
// because it can be the in_msg and out_msgs in different transactions at the same time.
// To do so we can avoid duplicate txs.
package ton

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/platform/ton/tonclient"
	"block-crawling/internal/platform/ton/tonutils"
	itypes "block-crawling/internal/types"
	"block-crawling/internal/types/tontypes"
	"encoding/json"
	"errors"
	"strconv"

	"block-crawling/internal/utils"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

var errNotMatchAny = errors.New("no user wallet matched")

type txHandler struct {
	chainName   string
	block       *chain.Block
	txByHash    *chain.Transaction
	chainHeight uint64
	curHeight   uint64
	now         int64
	newTxs      bool

	txRecords []*data.TonTransactionRecord
}

type txHashSuffixer struct {
	txHash  string
	counter int
}

func (s *txHashSuffixer) WithSuffix() string {
	s.counter++
	if s.counter-1 == 0 {
		return s.txHash
	}
	return fmt.Sprintf("%s#result-%d", s.txHash, s.counter-1)
}

func (h *txHandler) OnNewTx(c chain.Clienter, chainBlock *chain.Block, chainTx *chain.Transaction) (err error) {
	tx := chainTx.Raw.(tontypes.TX)
	return h.handleTx(c, &tx)
}

func (h *txHandler) handleTx(c chain.Clienter, tx *tontypes.TX) error {
	// 只处理外部触发的交易记录
	if tx.InMsg.Source != nil {
		return nil
	}

	matchedAny, err := h.matchAnyUsers(tx)
	if err != nil {
		return err
	}

	if !matchedAny {
		return nil
	}

	suffixer := &txHashSuffixer{
		txHash:  tonutils.Base64ToHex(tx.InMsg.Hash),
		counter: 0,
	}
	event, err := c.(*Client).GetEventFromTonAPI(tx)
	if err != nil {
		return fmt.Errorf("[getTokenTransfers] %w", err)
	}
	if event.InProgress {
		return errors.New("tx is still in progress")
	}
	records, err := h.handleEvents(event, tx, suffixer)
	if err != nil {
		return err
	}
	if len(records) > 0 {
		h.txRecords = append(h.txRecords, records...)
		return nil
	}

	if len(tx.OutMsgs) > 0 {
		// 处理交易产生的结果。
		for _, msg := range tx.OutMsgs {
			if err := h.handleMsg(c, tx, msg, suffixer, len(event.Actions)); err != nil {
				return err
			}
		}
	} else {
		if err := h.handleMsg(c, tx, tx.InMsg, suffixer, len(event.Actions)); err != nil {
			return err
		}
	}
	return nil
}

func (h *txHandler) matchAnyUsers(tx *tontypes.TX) (bool, error) {
	acount := unifyAddressToHuman(tx.Account)
	matchedAccount, _, err := biz.UserAddressSwitchRetryAlert(h.chainName, acount)
	if err != nil {
		return false, nil
	}
	if matchedAccount {
		return true, nil
	}

	for _, msg := range tx.OutMsgs {
		fromAddress := unifyAddressToHuman(*msg.Source)
		matchedFrom, _, err := biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
		if err != nil {
			return false, nil
		}
		if matchedFrom {
			return true, nil
		}
		toAddress := unifyAddressToHuman(msg.Destination)
		matchedTo, _, err := biz.UserAddressSwitchRetryAlert(h.chainName, toAddress)
		if err != nil {
			return false, nil
		}
		if matchedTo {
			return true, nil
		}
	}
	return false, nil
}

// For an incoming wallet transaction, the correct data consists of one incoming message and zero outgoing messages.
// Otherwise, either an external message is sent to the wallet, in which case the owner spends Toncoin,
// or the wallet is not deployed and the incoming transaction bounces back.
func (h *txHandler) handleMsg(c chain.Clienter, tx *tontypes.TX, msg tontypes.TXMsg, suffixer *txHashSuffixer, nActions int) (err error) {
	var fromAddress string
	if msg.Source != nil {
		fromAddress = unifyAddressToHuman(*msg.Source)
	} else {
		fromAddress = unifyAddressToHuman(tx.Account)
	}
	toAddress := unifyAddressToHuman(msg.Destination)
	matchedFrom, fromUid, err := biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
	if err != nil {
		return fmt.Errorf("[matchSource] %w", err)
	}
	matchedTo, toUid, err := biz.UserAddressSwitchRetryAlert(h.chainName, toAddress)
	if err != nil {
		return fmt.Errorf("[matchSource] %w", err)
	}
	if !matchedFrom && !matchedTo {
		return nil
	}

	feeAmount, _ := decimal.NewFromString(tx.TotalFees)
	amount := decimal.Zero
	if msg.Value != nil {
		amount, _ = decimal.NewFromString(*msg.Value)
	}
	status := biz.SUCCESS

	// 浏览器显示交易成功，但是没有产生实际的交易作用（余额未变动），此时事件中的 actions 为空，
	// 判定此类交易未失败的交易。
	if msg.Bounced || tx.Description.Aborted || nActions == 0 {
		status = biz.FAIL
	}
	txType := biz.NATIVE
	if len(tx.OutMsgs) > 1 {
		txType = biz.CONTRACT
	}

	var tokenInfo itypes.TokenInfo
	tonMap := map[string]interface{}{
		"token": tokenInfo,
	}
	parseData, _ := utils.JsonEncode(tonMap)
	tokenInfoStr, _ := utils.JsonEncode(tokenInfo)
	h.txRecords = append(h.txRecords, &data.TonTransactionRecord{
		TransactionHash: suffixer.WithSuffix(),
		MessageHash:     msg.Hash,
		BlockNumber:     tx.MCBlockSeqno,
		AccountTxHash:   tx.Hash,
		FromAddress:     fromAddress,
		ToAddress:       toAddress,
		FromUid:         fromUid,
		ToUid:           toUid,
		FeeAmount:       feeAmount,
		Amount:          amount,
		Status:          status,
		TxTime:          int64(tx.Now),
		ContractAddress: "",
		ParseData:       parseData,
		TokenInfo:       tokenInfoStr,
		GasLimit:        tx.Description.ComputePH.GasLimit,
		GasUsed:         tx.Description.ComputePH.GasUsed,
		GasPrice:        tx.Description.ComputePH.GasFees,
		TransactionType: txType,
		CreatedAt:       time.Now().Unix(),
		UpdatedAt:       time.Now().Unix(),
	})
	return nil
}

func (h *txHandler) isIntermediateAct(act tonclient.TonAPIAction) bool {
	// 分发返点
	if act.Type == "JettonTransfer" && act.JettonTransfer.Comment == "Call: StonfiSwapOkRef" {
		return true
	}
	return false
}

func (h *txHandler) handleEvents(event *tonclient.TonAPIEvent, tx *tontypes.TX, suffixer *txHashSuffixer) (records []*data.TonTransactionRecord, err error) {
	var mainRecord *data.TonTransactionRecord
	var eventLogs []*itypes.EventLogUid

	feeAmount, _ := decimal.NewFromString(tx.TotalFees)
	var feeAmountFromValueFlow bool
	account := unifyAddressToHuman(tx.InMsg.Destination)
	for _, vf := range event.ValueFlow {
		if unifyAddressToHuman(vf.Account.Address) == account {
			feeAmount = decimal.NewFromInt(int64(vf.Ton))
			feeAmountFromValueFlow = true
		}
	}

	status := biz.SUCCESS
	for _, act := range event.Actions {
		if h.isIntermediateAct(act) {
			continue
		}
		if act.Status == "failed" {
			status = biz.FAIL
		}
	}

	matchedNum := 0
	for _, act := range event.Actions {
		if h.isIntermediateAct(act) {
			continue
		}

		if act.Type == "SmartContractExec" {
			val := act.SmartContractExec
			if unifyAddressToHuman(val.Executor.Address) == unifyAddressToHuman(tx.Account) {
				mainRecord = &data.TonTransactionRecord{
					BlockNumber:     tx.MCBlockSeqno,
					TransactionHash: suffixer.WithSuffix(),
					AccountTxHash:   tx.Hash,
					FeeAmount:       feeAmount,
					FromAddress:     val.Executor.Address,
					ToAddress:       val.Contract.Address,
					Status:          status,
					TxTime:          int64(tx.Now),
					ContractAddress: unifyAddressToHuman(val.Contract.Address),
					GasLimit:        tx.Description.ComputePH.GasLimit,
					GasUsed:         tx.Description.ComputePH.GasUsed,
					GasPrice:        tx.Description.ComputePH.GasFees,
					TransactionType: biz.CONTRACT,
					CreatedAt:       time.Now().Unix(),
					UpdatedAt:       time.Now().Unix(),
				}
			}
		}

		if _, ok := eventHandlers[act.Type]; ok {
			matchedNum++
		}
	}

	if matchedNum > 1 && mainRecord == nil {
		mainRecord = &data.TonTransactionRecord{
			BlockNumber:     tx.MCBlockSeqno,
			TransactionHash: suffixer.WithSuffix(),
			FromAddress:     tx.InMsg.Destination,
			AccountTxHash:   tx.Hash,
			FeeAmount:       feeAmount,
			Status:          status,
			TxTime:          int64(tx.Now),
			GasLimit:        tx.Description.ComputePH.GasLimit,
			GasUsed:         tx.Description.ComputePH.GasUsed,
			GasPrice:        tx.Description.ComputePH.GasFees,
			TransactionType: biz.CONTRACT,
			CreatedAt:       time.Now().Unix(),
			UpdatedAt:       time.Now().Unix(),
		}
	}

	for _, act := range event.Actions {
		var txRecords []*data.TonTransactionRecord
		var err error

		in := &eventIn{
			h:         h,
			tx:        tx,
			event:     event,
			act:       &act,
			suffixer:  suffixer,
			status:    status,
			feeAmount: feeAmount,
		}

		if _, ok := eventHandlers[act.Type]; !ok {
			continue
		}

		switch act.Type {
		case "TonTransfer":
			if mainRecord != nil {
				txRecords, err = eventHandlers[act.Type](in)
			}
		default:
			if mainRecord == nil {
				if act.Type == "JettonSwap" {
					val := act.JettonSwap
					mainRecord = &data.TonTransactionRecord{
						BlockNumber:     tx.MCBlockSeqno,
						TransactionHash: suffixer.WithSuffix(),
						AccountTxHash:   tx.Hash,
						FeeAmount:       feeAmount,
						FromAddress:     val.UserWallet.Address,
						Status:          status,
						TxTime:          int64(tx.Now),
						ToAddress:       unifyAddressToHuman(val.Router.Address),
						ContractAddress: unifyAddressToHuman(val.Router.Address),
						GasLimit:        tx.Description.ComputePH.GasLimit,
						GasUsed:         tx.Description.ComputePH.GasUsed,
						GasPrice:        tx.Description.ComputePH.GasFees,
						TransactionType: biz.SWAP,
						CreatedAt:       time.Now().Unix(),
						UpdatedAt:       time.Now().Unix(),
					}
				} else if act.Type == "NftPurchase" {
					val := act.NftPurchase
					mainRecord = &data.TonTransactionRecord{
						BlockNumber:     tx.MCBlockSeqno,
						TransactionHash: suffixer.WithSuffix(),
						AccountTxHash:   tx.Hash,
						FeeAmount:       feeAmount,
						FromAddress:     val.Buyer.Address,
						ToAddress:       val.Seller.Address,
						Status:          status,
						TxTime:          int64(tx.Now),
						GasLimit:        tx.Description.ComputePH.GasLimit,
						GasUsed:         tx.Description.ComputePH.GasUsed,
						GasPrice:        tx.Description.ComputePH.GasFees,
						TransactionType: biz.CONTRACT,
						CreatedAt:       time.Now().Unix(),
						UpdatedAt:       time.Now().Unix(),
					}
				}
			} else { // mainRecord != nill
				if act.Type == "JettonSwap" {
					val := act.JettonSwap
					mainRecord.ToAddress = unifyAddressToHuman(val.Router.Address)
				} else if act.Type == "NftPurchase" {
					val := act.NftPurchase
					mainRecord.ToAddress = unifyAddressToHuman(val.Seller.Address)
				}
			}

			// 需要在 mainRecord 创建之后调用，不然交易 hash 会带后缀。
			txRecords, err = eventHandlers[act.Type](in)
		}
		if err != nil {
			return nil, err
		}
		for _, txRecord := range txRecords {
			if txRecord.FromUid == "" && txRecord.ToUid == "" {
				continue
			}
			records = append(records, txRecord)
			eventLogs = append(eventLogs, h.recordToEventLog(txRecord))
		}
	}

	if mainRecord != nil {
		if len(records) == 0 {
			mainRecord = nil
		}
	}

	if mainRecord != nil {
		if err := h.fillUid(mainRecord); err != nil {
			return records, err
		}
		if err := h.fillNativeToken(mainRecord); err != nil {
			return records, err
		}
		for _, x := range records {
			x.TransactionType = biz.EVENTLOG
		}
		if !mainRecord.Amount.IsZero() {
			eventLogs = append(eventLogs, h.recordToEventLog(mainRecord))
		}
		// 如果主记录没有合约地址，则尝试使用非空的子记录合约地址
		for _, r := range records {
			if r.ContractAddress != "" && mainRecord.ContractAddress != "" {
				mainRecord.ContractAddress = r.ContractAddress
			}
			if r.FromAddress == mainRecord.FromAddress && r.ToAddress != "" && mainRecord.ToAddress == "" {
				mainRecord.ToAddress = r.ToAddress
			}
		}
		records = append(records, mainRecord)
		mainRecord.EventLog, _ = utils.JsonEncode(eventLogs)
	}

	// 根据账号净流入计算手续费
	if feeAmountFromValueFlow {
		netReceived := decimal.Zero
		for _, record := range records {
			var token itypes.TokenInfo
			_ = json.Unmarshal([]byte(record.TokenInfo), &token)
			if token.Address == "" {
				if record.FromAddress == account {
					netReceived = netReceived.Sub(record.Amount)
				}
				if record.ToAddress == account {
					netReceived = netReceived.Add(record.Amount)
				}
			}
		}

		// 手续费 = 净接收 - 净流入
		feeAmount = netReceived.Sub(feeAmount)

		// 更新手续费
		for _, record := range records {
			record.FeeAmount = feeAmount
		}
	}
	return records, nil
}

func (h *txHandler) fillUid(txRecord *data.TonTransactionRecord) (err error) {
	if txRecord.FromAddress != "" {
		txRecord.FromAddress = unifyAddressToHuman(txRecord.FromAddress)
		_, txRecord.FromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, txRecord.FromAddress)
		if err != nil {
			return fmt.Errorf("[matchSourceToken] %w", err)
		}
	}
	if txRecord.ToAddress != "" {
		txRecord.ToAddress = unifyAddressToHuman(txRecord.ToAddress)
		_, txRecord.ToUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, txRecord.ToAddress)
		if err != nil {
			return fmt.Errorf("[matchSourceToken] %w", err)
		}
	}
	return
}

func (h *txHandler) fillToken(txRecord *data.TonTransactionRecord, event interface{}, suffixer *txHashSuffixer, tokenAddress string) (err error) {
	tokenAddress = unifyAddressToHuman(tokenAddress)
	tokenInfo, err := biz.GetTokenInfoRetryAlert(nil, h.chainName, tokenAddress)
	if err != nil {
		log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", suffixer.txHash), zap.Any("error", err))
	}
	tokenInfo.Amount = txRecord.Amount.String()
	tokenInfo.Address = tokenAddress
	tonMap := map[string]interface{}{
		"token": tokenInfo,
		"raw":   event,
	}
	txRecord.ParseData, _ = utils.JsonEncode(tonMap)
	txRecord.TokenInfo, _ = utils.JsonEncode(tokenInfo)
	return nil
}

func (h *txHandler) fillNFT(txRecord *data.TonTransactionRecord, event interface{}, suffixer *txHashSuffixer, nftAddress string, nftIndex int) (err error) {
	contractAddress := unifyAddressToHuman(nftAddress)
	tokenId := strconv.Itoa(nftIndex)
	tokenInfo, err := biz.GetNftInfoDirectlyRetryAlert(nil, h.chainName, contractAddress, tokenId)
	if err != nil {
		log.Error("扫块，从nodeProxy中获取NFT信息失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", suffixer.txHash), zap.Any("tokenAddress", contractAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
	}
	tokenInfo.Amount = "1"
	tokenInfo.Address = contractAddress
	if tokenInfo.TokenType == "" {
		tokenInfo.TokenType = biz.TONNFT
	}
	if tokenInfo.TokenId == "" {
		tokenInfo.TokenId = tokenId
	}
	tonMap := map[string]interface{}{
		"token": tokenInfo,
		"raw":   event,
	}
	txRecord.ParseData, _ = utils.JsonEncode(tonMap)
	txRecord.TokenInfo, _ = utils.JsonEncode(tokenInfo)
	return nil
}

func (h *txHandler) fillNativeToken(txRecord *data.TonTransactionRecord) (err error) {
	var tokenInfo itypes.TokenInfo
	tonMap := map[string]interface{}{
		"token": tokenInfo,
	}
	txRecord.ParseData, _ = utils.JsonEncode(tonMap)
	txRecord.TokenInfo, _ = utils.JsonEncode(tokenInfo)
	return nil
}

func (h *txHandler) recordToEventLog(txRecord *data.TonTransactionRecord) *itypes.EventLogUid {
	var token itypes.TokenInfo
	_ = json.Unmarshal([]byte(txRecord.TokenInfo), &token)
	return &itypes.EventLogUid{
		EventLog: itypes.EventLog{
			From:   txRecord.FromAddress,
			To:     txRecord.ToAddress,
			Amount: txRecord.Amount.BigInt(),
			Token:  token,
		},
		FromUid: txRecord.FromUid,
		ToUid:   txRecord.ToUid,
	}
}

func (h *txHandler) OnSealedTx(c chain.Clienter, tx *chain.Transaction) (err error) {
	txs := tx.Raw.([]tontypes.TX)
	for _, tx := range txs {
		if err := h.handleTx(c, &tx); err != nil {
			return err
		}
	}
	return nil
}

func (h *txHandler) OnDroppedTx(c chain.Clienter, tx *chain.Transaction) error {
	record := tx.Record.(*data.TonTransactionRecord)

	nowTime := time.Now().Unix()
	if record.CreatedAt+180 > nowTime {
		if record.Status == biz.PENDING {
			record.Status = biz.NO_STATUS
			record.UpdatedAt = h.now
			h.txRecords = append(h.txRecords, record)
			log.Info(
				"更新 PENDING txhash对象无状态",
				zap.String("chainName", h.chainName),
				zap.Any("txHash", record.AccountTxHash),
				zap.String("nodeUrl", c.URL()),
				zap.Int64("nowTime", nowTime),
				zap.Int64("createTime", record.CreatedAt),
			)
		}
	} else {
		record.Status = biz.DROPPED
		record.UpdatedAt = h.now
		h.txRecords = append(h.txRecords, record)
		log.Info(
			"更新 PENDING txhash对象为终态:交易被抛弃",
			zap.String("chainName", h.chainName),
			zap.Any("txHash", record.AccountTxHash),
			zap.String("nodeUrl", c.URL()),
			zap.Int64("nowTime", nowTime),
			zap.Int64("createTime", record.CreatedAt),
		)
	}

	return nil
}

func (h *txHandler) Save(c chain.Clienter) error {
	client := c.(*Client)

	if h.txRecords != nil && len(h.txRecords) > 0 {
		//保存交易数据
		err := BatchSaveOrUpdate(h.txRecords, biz.GetTableName(h.chainName))
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链扫块，将数据插入到数据库中失败", h.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("扫块，将数据插入到数据库中失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("error", err))
			return err
		}
		if h.newTxs {
			go HandleRecord(h.chainName, client, h.txRecords)
		} else {
			go HandlePendingRecord(h.chainName, client, h.txRecords)
		}

		if h.newTxs {
			records := make([]interface{}, 0, len(h.txRecords))
			for _, r := range h.txRecords {
				records = append(records, r)
			}
			common.SetResultOfTxs(h.block, records)
		} else {
			common.SetTxResult(h.txByHash, h.txRecords[0])
		}
	}
	return nil
}

func unifyAddressToHuman(address string) string {
	return tonutils.UnifyAddressToHuman(address)
}
