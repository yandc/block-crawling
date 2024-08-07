package ton

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/platform/ton/tonclient"
	"block-crawling/internal/types/tontypes"
	"time"

	"github.com/shopspring/decimal"
)

type eventIn struct {
	h         *txHandler
	tx        *tontypes.TX
	event     *tonclient.TonAPIEvent
	act       *tonclient.TonAPIAction
	suffixer  common.TxHashSuffixer
	status    string
	feeAmount decimal.Decimal
}

type tonEventHandlerFn = func(in *eventIn) ([]*data.TonTransactionRecord, error)

var eventHandlers = make(map[string]tonEventHandlerFn)

func handleTonTransfer(in *eventIn) ([]*data.TonTransactionRecord, error) {
	val := in.act.TonTransfer
	tx := in.tx
	txRecord := &data.TonTransactionRecord{
		BlockNumber:     tx.MCBlockSeqno,
		TransactionHash: in.suffixer.WithSuffix(),
		AccountTxHash:   tx.Hash,
		FeeAmount:       in.feeAmount,
		FromAddress:     val.Sender.Address,
		ToAddress:       val.Recipient.Address,
		Status:          in.status,
		TxTime:          int64(tx.Now),
		GasLimit:        tx.Description.ComputePH.GasLimit,
		GasUsed:         tx.Description.ComputePH.GasUsed,
		GasPrice:        tx.Description.ComputePH.GasFees,
		TransactionType: biz.TRANSFER,
		CreatedAt:       time.Now().Unix(),
		UpdatedAt:       time.Now().Unix(),
	}
	h := in.h
	txRecord.Amount = decimal.NewFromInt(int64(val.Amount))
	if err := h.fillUid(txRecord); err != nil {
		return nil, err
	}
	if err := h.fillNativeToken(txRecord); err != nil {
		return nil, err
	}
	return []*data.TonTransactionRecord{txRecord}, nil
}

func handleJettonTransfer(in *eventIn) ([]*data.TonTransactionRecord, error) {
	val := in.act.JettonTransfer
	tx := in.tx
	h := in.h
	feeAmount := in.feeAmount
	txRecord := &data.TonTransactionRecord{
		BlockNumber:     tx.MCBlockSeqno,
		TransactionHash: in.suffixer.WithSuffix(),
		AccountTxHash:   tx.Hash,
		FeeAmount:       feeAmount,
		FromAddress:     val.Sender.Address,
		ToAddress:       val.Recipient.Address,
		Status:          in.status,
		TxTime:          int64(tx.Now),
		ContractAddress: unifyAddressToHuman(val.Jetton.Address),
		GasLimit:        tx.Description.ComputePH.GasLimit,
		GasUsed:         tx.Description.ComputePH.GasUsed,
		GasPrice:        tx.Description.ComputePH.GasFees,
		TransactionType: biz.TRANSFER,
		CreatedAt:       time.Now().Unix(),
		UpdatedAt:       time.Now().Unix(),
	}
	txRecord.Amount, _ = decimal.NewFromString(val.Amount)
	if err := h.fillUid(txRecord); err != nil {
		return nil, err
	}
	if err := h.fillToken(txRecord, in.event, in.suffixer, val.Jetton.Address); err != nil {
		return nil, err
	}
	return []*data.TonTransactionRecord{txRecord}, nil
}

func handleJettonSwap(in *eventIn) ([]*data.TonTransactionRecord, error) {
	val := in.act.JettonSwap
	tx := in.tx
	feeAmount := in.feeAmount
	status := in.status
	h := in.h
	suffixer := in.suffixer
	event := in.event

	records := make([]*data.TonTransactionRecord, 0, 2)

	if val.TonIn > 0 {
		txRecord := &data.TonTransactionRecord{
			BlockNumber:     tx.MCBlockSeqno,
			TransactionHash: suffixer.WithSuffix(),
			AccountTxHash:   tx.Hash,
			FeeAmount:       feeAmount,
			FromAddress:     val.UserWallet.Address,
			Status:          status,
			TxTime:          int64(tx.Now),
			ContractAddress: unifyAddressToHuman(val.Router.Address),
			GasLimit:        tx.Description.ComputePH.GasLimit,
			GasUsed:         tx.Description.ComputePH.GasUsed,
			GasPrice:        tx.Description.ComputePH.GasFees,
			TransactionType: biz.EVENTLOG,
			Amount:          decimal.NewFromInt(int64(val.TonIn)),
			CreatedAt:       time.Now().Unix(),
			UpdatedAt:       time.Now().Unix(),
		}
		if err := h.fillUid(txRecord); err != nil {
			return records, err
		}

		if err := h.fillNativeToken(txRecord); err != nil {
			return records, err
		}
		records = append(records, txRecord)
	}
	if val.TonOut > 0 {
		txRecord := &data.TonTransactionRecord{
			BlockNumber:     tx.MCBlockSeqno,
			TransactionHash: suffixer.WithSuffix(),
			AccountTxHash:   tx.Hash,
			FeeAmount:       feeAmount,
			ToAddress:       val.UserWallet.Address,
			Status:          status,
			TxTime:          int64(tx.Now),
			ContractAddress: unifyAddressToHuman(val.Router.Address),
			GasLimit:        tx.Description.ComputePH.GasLimit,
			GasUsed:         tx.Description.ComputePH.GasUsed,
			GasPrice:        tx.Description.ComputePH.GasFees,
			TransactionType: biz.EVENTLOG,
			Amount:          decimal.NewFromInt(int64(val.TonOut)),
			CreatedAt:       time.Now().Unix(),
			UpdatedAt:       time.Now().Unix(),
		}
		if err := h.fillUid(txRecord); err != nil {
			return records, err
		}
		if err := h.fillNativeToken(txRecord); err != nil {
			return records, err
		}
		records = append(records, txRecord)
	}
	if val.JettonMasterIn != nil {
		amount, _ := decimal.NewFromString(val.AmountIn)
		txRecord := &data.TonTransactionRecord{
			BlockNumber:     tx.MCBlockSeqno,
			TransactionHash: suffixer.WithSuffix(),
			AccountTxHash:   tx.Hash,
			Amount:          amount,
			FeeAmount:       feeAmount,
			FromAddress:     val.UserWallet.Address,
			Status:          status,
			TxTime:          int64(tx.Now),
			ContractAddress: unifyAddressToHuman(val.JettonMasterIn.Address),
			GasLimit:        tx.Description.ComputePH.GasLimit,
			GasUsed:         tx.Description.ComputePH.GasUsed,
			GasPrice:        tx.Description.ComputePH.GasFees,
			TransactionType: biz.EVENTLOG,
			CreatedAt:       time.Now().Unix(),
			UpdatedAt:       time.Now().Unix(),
		}
		if err := h.fillUid(txRecord); err != nil {
			return records, err
		}
		if err := h.fillToken(txRecord, event, suffixer, val.JettonMasterIn.Address); err != nil {
			return records, err
		}
		records = append(records, txRecord)
	}
	if val.JettonMasterOut != nil {
		amount, _ := decimal.NewFromString(val.AmountOut)
		txRecord := &data.TonTransactionRecord{
			BlockNumber:     tx.MCBlockSeqno,
			TransactionHash: suffixer.WithSuffix(),
			AccountTxHash:   tx.Hash,
			FeeAmount:       feeAmount,
			ToAddress:       val.UserWallet.Address,
			Status:          status,
			Amount:          amount,
			TxTime:          int64(tx.Now),
			ContractAddress: unifyAddressToHuman(val.JettonMasterOut.Address),
			GasLimit:        tx.Description.ComputePH.GasLimit,
			GasUsed:         tx.Description.ComputePH.GasUsed,
			GasPrice:        tx.Description.ComputePH.GasFees,
			TransactionType: biz.EVENTLOG,
			CreatedAt:       time.Now().Unix(),
			UpdatedAt:       time.Now().Unix(),
		}
		if err := h.fillUid(txRecord); err != nil {
			return records, err
		}
		if err := h.fillToken(txRecord, event, suffixer, val.JettonMasterOut.Address); err != nil {
			return records, err
		}
		records = append(records, txRecord)
	}
	return records, nil
}

func handleNftPurchase(in *eventIn) ([]*data.TonTransactionRecord, error) {
	tx := in.tx
	feeAmount := in.feeAmount
	status := in.status
	h := in.h
	suffixer := in.suffixer
	event := in.event

	val := in.act.NftPurchase
	txRecord := &data.TonTransactionRecord{
		BlockNumber:     tx.MCBlockSeqno,
		TransactionHash: suffixer.WithSuffix(),
		AccountTxHash:   tx.Hash,
		FeeAmount:       feeAmount,
		FromAddress:     val.Seller.Address,
		ToAddress:       val.Buyer.Address,
		Status:          status,
		TxTime:          int64(tx.Now),
		ContractAddress: "",
		GasLimit:        tx.Description.ComputePH.GasLimit,
		GasUsed:         tx.Description.ComputePH.GasUsed,
		GasPrice:        tx.Description.ComputePH.GasFees,
		TransactionType: biz.TRANSFERNFT,
		CreatedAt:       time.Now().Unix(),
		UpdatedAt:       time.Now().Unix(),
	}
	if err := h.fillUid(txRecord); err != nil {
		return nil, err
	}
	if err := h.fillNFT(txRecord, event, suffixer, val.NFT.Address, val.NFT.Index); err != nil {
		return nil, err
	}

	amount, _ := decimal.NewFromString(val.Amount.Value)
	txRecordBuy := &data.TonTransactionRecord{
		BlockNumber:     tx.MCBlockSeqno,
		TransactionHash: suffixer.WithSuffix(),
		AccountTxHash:   tx.Hash,
		FeeAmount:       feeAmount,
		FromAddress:     val.Buyer.Address,
		ToAddress:       val.Seller.Address,
		Amount:          amount,
		Status:          status,
		TxTime:          int64(tx.Now),
		ContractAddress: "",
		GasLimit:        tx.Description.ComputePH.GasLimit,
		GasUsed:         tx.Description.ComputePH.GasUsed,
		GasPrice:        tx.Description.ComputePH.GasFees,
		TransactionType: biz.TRANSFERNFT,
		CreatedAt:       time.Now().Unix(),
		UpdatedAt:       time.Now().Unix(),
	}
	if err := h.fillUid(txRecordBuy); err != nil {
		return nil, err
	}
	if err := h.fillNativeToken(txRecordBuy); err != nil {
		return nil, err
	}
	return []*data.TonTransactionRecord{txRecord, txRecordBuy}, nil
}

func handleNftItemTransfer(in *eventIn) ([]*data.TonTransactionRecord, error) {
	tx := in.tx
	feeAmount := in.feeAmount
	status := in.status
	h := in.h
	suffixer := in.suffixer
	event := in.event
	val := in.act.NftItemTransfer
	txRecord := &data.TonTransactionRecord{
		BlockNumber:     tx.MCBlockSeqno,
		TransactionHash: suffixer.WithSuffix(),
		AccountTxHash:   tx.Hash,
		FeeAmount:       feeAmount,
		FromAddress:     val.Sender.Address,
		ToAddress:       val.Recipient.Address,
		Status:          status,
		TxTime:          int64(tx.Now),
		ContractAddress: "",
		GasLimit:        tx.Description.ComputePH.GasLimit,
		GasUsed:         tx.Description.ComputePH.GasUsed,
		GasPrice:        tx.Description.ComputePH.GasFees,
		TransactionType: biz.TRANSFERNFT,
		CreatedAt:       time.Now().Unix(),
		UpdatedAt:       time.Now().Unix(),
	}
	if err := h.fillUid(txRecord); err != nil {
		return nil, err
	}
	if err := h.fillNFT(txRecord, event, suffixer, val.NFT, 0); err != nil {
		return nil, err
	}
	return []*data.TonTransactionRecord{txRecord}, nil
}

func handleSmartContractExec(in *eventIn) ([]*data.TonTransactionRecord, error) {
	// 处理合约交互中附上的 TON 手续费（ton_attached）
	tx := in.tx
	feeAmount := in.feeAmount
	status := in.status
	h := in.h
	suffixer := in.suffixer
	val := in.act.SmartContractExec
	txRecord := &data.TonTransactionRecord{
		BlockNumber:     tx.MCBlockSeqno,
		TransactionHash: suffixer.WithSuffix(),
		AccountTxHash:   tx.Hash,
		FeeAmount:       feeAmount,
		FromAddress:     val.Executor.Address,
		ToAddress:       val.Contract.Address,
		Status:          status,
		Amount:          decimal.NewFromInt(int64(val.TonAttached)),
		TxTime:          int64(tx.Now),
		ContractAddress: "",
		GasLimit:        tx.Description.ComputePH.GasLimit,
		GasUsed:         tx.Description.ComputePH.GasUsed,
		GasPrice:        tx.Description.ComputePH.GasFees,
		TransactionType: biz.EVENTLOG,
		CreatedAt:       time.Now().Unix(),
		UpdatedAt:       time.Now().Unix(),
	}
	if err := h.fillUid(txRecord); err != nil {
		return nil, err
	}
	if err := h.fillNativeToken(txRecord); err != nil {
		return nil, err
	}
	return []*data.TonTransactionRecord{txRecord}, nil
}

func init() {
	eventHandlers["TonTransfer"] = handleTonTransfer
	eventHandlers["JettonTransfer"] = handleJettonTransfer
	eventHandlers["JettonSwap"] = handleJettonSwap
	eventHandlers["NftPurchase"] = handleNftPurchase
	eventHandlers["NftItemTransfer"] = handleNftItemTransfer
	eventHandlers["SmartContractExec"] = handleSmartContractExec
}
