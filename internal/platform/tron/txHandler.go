package tron

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

const TRANSFER_TOPIC = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
const WITHDRAWAL_TOPIC = "7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"

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
		tornWhiteMethods := tx.ToAddress
		flag := true
		for _, element := range TronBridgeWhiteAddressList {
			if tornWhiteMethods == element {
				flag = false
				break
			}
		}
		//未命中白名 则丢弃该交易
		if flag {
			return nil
		}
	}

	rawTx := tx.Raw.(*rawTxWrapper)
	transactionHash := rawTx.TxID
	client := c.(*Client)
	status := biz.PENDING
	if len(rawTx.Ret) > 0 {
		if rawTx.Ret[0].ContractRet == "SUCCESS" {
			status = biz.SUCCESS
		} else {
			status = biz.FAIL
		}
	} else {
		status = biz.SUCCESS
	}
	var tokenInfo types.TokenInfo
	txInfo, err := client.GetTransactionInfoByHash(transactionHash)
	if err != nil {
		return err
	}

	if tx.TxType == biz.TRANSFER || tx.TxType == biz.APPROVE {
		tokenInfo, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, rawTx.contractAddress)
		if err != nil {
			log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
		}
		tokenInfo.Amount = rawTx.tokenAmount
		tokenInfo.Address = rawTx.contractAddress
	}

	platformUserEventlog := false
	//获取eventlog
	var eventLogs []types.EventLog
	if tx.TxType != biz.NATIVE {
		for _, log_ := range txInfo.Log {
			if len(log_.Topics) > 1 && (log_.Topics[0] == TRANSFER_TOPIC ||
				log_.Topics[0] == WITHDRAWAL_TOPIC) {
				contranctAddress := log_.Address
				tokenInfo, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, contranctAddress)
				if err != nil {
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
				}
				amount := big.NewInt(0)
				banInt, b := new(big.Int).SetString(log_.Data, 16)
				if b {
					amount = banInt
				}
				tokenInfo.Amount = amount.String()

				if log_.Topics[0] == TRANSFER_TOPIC {
					eventLogInfo := types.EventLog{
						From:   utils.TronHexToBase58(ADDRESS_PREFIX + log_.Topics[1][24:64]),
						To:     utils.TronHexToBase58(ADDRESS_PREFIX + log_.Topics[2][24:64]),
						Amount: amount,
						Token:  tokenInfo,
					}
					eventLogs = append(eventLogs, eventLogInfo)
				}

				if log_.Topics[0] == WITHDRAWAL_TOPIC {
					if strings.HasPrefix(tokenInfo.Symbol, "W") || strings.HasPrefix(tokenInfo.Symbol, "w") {
						tokenInfo.Symbol = tokenInfo.Symbol[1:]
					}
					eventLogInfo := types.EventLog{
						From:   utils.TronHexToBase58(ADDRESS_PREFIX + log_.Topics[1][24:64]),
						To:     tx.FromAddress,
						Amount: amount,
						Token:  tokenInfo,
					}
					eventLogs = append(eventLogs, eventLogInfo)

				}
			}
		}
		if len(eventLogs) > 0 {
			for _, eventLog := range eventLogs {
				var fromAddressExist, toAddressExist bool
				fromAddress := eventLog.From
				toAddress := eventLog.To
				if fromAddress != "" {
					fromAddressExist, _, err = biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
					if err != nil {
						log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("txHash", transactionHash), zap.Any("error", err))
						return err
					}
				}

				if toAddress != "" {
					toAddressExist, _, err = biz.UserAddressSwitchRetryAlert(h.chainName, toAddress)
					if err != nil {
						log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("txHash", transactionHash), zap.Any("error", err))
						return err
					}
				}
				if fromAddressExist || toAddressExist {
					platformUserEventlog = true
					break
				}
			}
		}

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
	contractType := rawTx.RawData.Contract[0].Type
	amount, _ := decimal.NewFromString(tx.Value)
	if contractType == biz.TRON_DAPP {
		txRepecitInfo, _ := client.GetTransactionByHash(transactionHash)
		if txRepecitInfo != nil &&len(txRepecitInfo.RawData.Contract) > 0 {
			if txRepecitInfo.RawData.Contract[0].Parameter.Value.CallValue != nil {
				amount = decimal.NewFromBigInt(txRepecitInfo.RawData.Contract[0].Parameter.Value.CallValue, 0)
			}
		}
	}

	// Transfer TRC10 token
	if contractType == biz.TRON_TRANSFER_TRC10 {
		return nil
	}

	if platformUserEventlog && meta.TransactionType != biz.CONTRACT {
		meta.TransactionType = biz.CONTRACT
	}
	if tx.TxType == biz.CONTRACT {
		tokenInfo = types.TokenInfo{}
	}

	tronMap := map[string]interface{}{
		"tvm":   map[string]string{},
		"token": tokenInfo,
	}
	parseData, _ := utils.JsonEncode(tronMap)
	exTime := rawTx.RawData.Timestamp / 1000
	if rawTx.RawData.Timestamp == 0 {
		exTime = block.Time / 1000
	}

	var eventLog string
	if eventLogs != nil {
		eventLog, _ = utils.JsonEncode(eventLogs)
	}
	rawBlock := block.Raw.(*types.BlockResponse)
	trxRecord := &data.TrxTransactionRecord{
		BlockHash:       block.Hash,
		BlockNumber:     rawBlock.BlockHeader.RawData.Number,
		TransactionHash: transactionHash,
		FromAddress:     tx.FromAddress,
		ToAddress:       tx.ToAddress,
		FromUid:         meta.User.FromUid,
		ToUid:           meta.User.ToUid,
		FeeAmount:       decimal.NewFromInt(int64(feeAmount)),
		Amount:          amount,
		Status:          status,
		TxTime:          exTime,
		EventLog:        eventLog,
		ContractAddress: rawTx.contractAddress,
		ParseData:       parseData,
		NetUsage:        strconv.Itoa(txInfo.Receipt.NetUsage),
		FeeLimit:        strconv.Itoa(rawTx.RawData.FeeLimit),
		EnergyUsage:     strconv.Itoa(txInfo.Receipt.EnergyUsage),
		TransactionType: string(tx.TxType),
		DappData:        "",
		ClientData:      "",
		CreatedAt:       h.now,
		UpdatedAt:       h.now,
	}

	if meta.User.MatchFrom || meta.User.MatchTo || platformUserEventlog {
		h.txRecords = append(h.txRecords, trxRecord)
	}
	if platformUserEventlog && meta.TransactionType == biz.CONTRACT {
		for index, eventLog := range eventLogs {
			eventMap := map[string]interface{}{
				"token": eventLog.Token,
			}
			eventParseData, _ := utils.JsonEncode(eventMap)
			txHash := transactionHash + "#result-" + fmt.Sprintf("%v", index+1)
			txType := biz.EVENTLOG
			contractAddress := eventLog.Token.Address
			amountValue := decimal.NewFromBigInt(eventLog.Amount, 0)
			var eventFromUid, eventToUid string

			userMeta, err := common.MatchUser(eventLog.From, eventLog.To, h.chainName)
			if err == nil {
				eventFromUid = userMeta.FromUid
				eventToUid = userMeta.ToUid
			}
			trxLogRecord := &data.TrxTransactionRecord{
				BlockHash:       block.Hash,
				BlockNumber:     rawBlock.BlockHeader.RawData.Number,
				TransactionHash: txHash,
				FromAddress:     eventLog.From,
				ToAddress:       eventLog.To,
				FromUid:         eventFromUid,
				ToUid:           eventToUid,
				FeeAmount:       decimal.NewFromInt(int64(feeAmount)),
				Amount:          amountValue,
				Status:          status,
				TxTime:          exTime,
				ContractAddress: contractAddress,
				ParseData:       eventParseData,
				NetUsage:        strconv.Itoa(txInfo.Receipt.NetUsage),
				FeeLimit:        strconv.Itoa(rawTx.RawData.FeeLimit),
				EnergyUsage:     strconv.Itoa(txInfo.Receipt.EnergyUsage),
				TransactionType: txType,
				DappData:        "",
				ClientData:      "",
				CreatedAt:       h.now,
				UpdatedAt:       h.now,
			}
			h.txRecords = append(h.txRecords, trxLogRecord)
		}
	}
	return nil
}

func (h *txHandler) Save(c chain.Clienter) error {
	txRecords := h.txRecords
	curHeight := h.curHeight
	client := c.(*Client)

	if txRecords != nil && len(txRecords) > 0 {
		//保存交易数据
		err := BatchSaveOrUpdate(txRecords, biz.GetTableName(h.chainName))
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
