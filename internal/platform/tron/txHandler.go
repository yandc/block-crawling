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

	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
	"gorm.io/datatypes"
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
	rawBlock := block.Raw.(*types.BlockResponse)
	rawTx := tx.Raw.(*RawTxWrapper)
	transactionHash := rawTx.TxID
	txToAddress := tx.ToAddress

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
	energyFee := int64(txInfo.Receipt.EnergyUsage)
	energyTotal := int64(txInfo.Receipt.EnergyUsageTotal)
	netUsage := txInfo.Receipt.NetUsage
	if energyFee != 0 && energyTotal != 0 {
		gasPrice := decimal.NewFromInt(energyFee).Div(decimal.NewFromInt(energyTotal)).String()
		go biz.ChainFeeSwitchRetryAlert(h.chainName, "", "", gasPrice, uint64(rawBlock.BlockHeader.RawData.Number), transactionHash)
	}
	if tx.TxType == biz.TRANSFER || tx.TxType == biz.APPROVE {
		tokenInfo, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, rawTx.contractAddress)
		if err != nil {
			log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
		}
		tokenInfo.Amount = rawTx.tokenAmount
		tokenInfo.Address = rawTx.contractAddress
	}

	//获取eventlog
	var eventLogs []*types.EventLogUid
	if tx.TxType != biz.NATIVE {
		for _, log_ := range txInfo.Log {
			if len(log_.Topics) < 1 {
				continue
			}
			topic0 := log_.Topics[0]

			var token types.TokenInfo
			var tokenAddress string
			amount := big.NewInt(0)
			var fromAddress, toAddress, fromUid, toUid string
			var fromAddressExist, toAddressExist bool

			if topic0 == TRANSFER_TOPIC || topic0 == WITHDRAWAL_TOPIC {
				banInt, b := new(big.Int).SetString(log_.Data, 16)
				if b {
					amount = banInt
				}

				tokenAddress = log_.Address

				if topic0 == TRANSFER_TOPIC {
					fromAddress = utils.TronHexToBase58(ADDRESS_PREFIX + log_.Topics[1][24:64])
					toAddress = utils.TronHexToBase58(ADDRESS_PREFIX + log_.Topics[2][24:64])

					token, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, tokenAddress)
					if err != nil {
						log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
					}
					token.Amount = amount.String()
				} else if topic0 == WITHDRAWAL_TOPIC {
					//https://tronscan.org/#/transaction/59eb44f3c4a51f65a6555a57f73c7d68d192b6d32b896e3ac88a1376d81259e4
					//https://tronscan.org/#/transaction/ead0b8be0f242dbb891a274003e53f875825e3964b004c5dfe05b77bc8f7e9b7
					/*if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
						token.Symbol = token.Symbol[1:]
					}*/
					fromAddress = utils.TronHexToBase58(ADDRESS_PREFIX + log_.Topics[1][24:64])
					toAddress = tx.FromAddress
					tokenAddress = ""
				}

				if tokenAddress != "" {
					token.Address = tokenAddress
				} else {
					token = types.TokenInfo{}
				}

				if fromAddress != "" {
					fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
					if err != nil {
						log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
						return err
					}
				}

				if toAddress != "" {
					toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, toAddress)
					if err != nil {
						log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.chainName), zap.Any("current", h.curHeight), zap.Any("new", h.chainHeight), zap.Any("txHash", transactionHash), zap.Any("error", err))
						return err
					}
				}
				if !fromAddressExist && !toAddressExist {
					continue
				}

				eventLogInfo := &types.EventLogUid{
					EventLog: types.EventLog{
						From:   fromAddress,
						To:     toAddress,
						Amount: amount,
						Token:  token,
					},
					FromUid: fromUid,
					ToUid:   toUid,
				}

				var isContinue bool
				for i, eventLog := range eventLogs {
					if eventLog == nil {
						continue
					}
					if eventLog.From == eventLogInfo.To && eventLog.To == eventLogInfo.From && eventLog.Token.Address == eventLogInfo.Token.Address &&
						eventLog.Token.TokenId == eventLogInfo.Token.TokenId {
						cmp := eventLog.Amount.Cmp(eventLogInfo.Amount)
						if cmp == 1 {
							isContinue = true
							subAmount := new(big.Int).Sub(eventLog.Amount, eventLogInfo.Amount)
							eventLogs[i].Amount = subAmount
						} else if cmp == 0 {
							isContinue = true
							eventLogs[i] = nil
						} else if cmp == -1 {
							eventLogs[i] = nil
						}
						break
					} else if eventLog.From == eventLogInfo.From && eventLog.To == eventLogInfo.To && eventLog.Token.Address == eventLogInfo.Token.Address &&
						eventLog.Token.TokenId == eventLogInfo.Token.TokenId {
						isContinue = true
						addAmount := new(big.Int).Add(eventLog.Amount, eventLogInfo.Amount)
						eventLogs[i].Amount = addAmount
						break
					}
				}
				if isContinue {
					continue
				}
				eventLogs = append(eventLogs, eventLogInfo)
			} else if topic0 == FEE_TOPIC { //gas 代付，处理 to 地址
				logData, err := hexutil.Decode(log_.Data)
				if err != nil {
					continue
				}
				//https://tronscan.org/#/transaction/380891a3c51cfbfded646b737cf47fa537c21fb7bd1aaa48d43e2d4dbeef8017
				transferType := new(big.Int).SetBytes(logData[32:])
				if transferType.Int64() != 2 {
					continue
				}
				txToAddress = utils.TronHexToBase58(ADDRESS_PREFIX + log_.Topics[2])
			}
		}
	}

	feeData := map[string]interface{}{
		"net_usage": netUsage,
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
		if txRepecitInfo != nil && len(txRepecitInfo.RawData.Contract) > 0 {
			if txRepecitInfo.RawData.Contract[0].Parameter.Value.CallValue != nil {
				amount = decimal.NewFromBigInt(txRepecitInfo.RawData.Contract[0].Parameter.Value.CallValue, 0)
			}
		}
	}

	// Transfer TRC10 token
	if contractType == biz.TRON_TRANSFER_TRC10 {
		return nil
	}

	// 代理能量
	if contractType == DELEGATERESOURCES || contractType == RECLAIMRESOURCES {
		return nil
	}

	isPlatformUser := false
	if len(eventLogs) > 0 {
		isPlatformUser = true
	}

	if isPlatformUser && meta.TransactionType != biz.CONTRACT {
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
	tokenInfoStr, _ := utils.JsonEncode(tokenInfo)
	exTime := rawTx.RawData.Timestamp / 1000
	if rawTx.RawData.Timestamp == 0 {
		exTime = block.Time / 1000
	}

	var eventLog string
	if eventLogs != nil {
		eventLog, _ = utils.JsonEncode(eventLogs)
	}

	var logAddress datatypes.JSON
	if isPlatformUser && meta.TransactionType == biz.CONTRACT {
		logAddress = biz.GetLogAddressFromEventLogUid(eventLogs)
		// database btree index maximum is 2704
		logAddressLen := len(logAddress)
		if logAddressLen > 2704 {
			log.Error("扫块，logAddress长度超过最大限制", zap.Any("chainName", h.chainName), zap.Any("txHash", transactionHash), zap.Any("logAddressLen", logAddressLen))
			logAddress = nil
		}
	}
	trxContractRecord := &data.TrxTransactionRecord{
		BlockHash:       block.Hash,
		BlockNumber:     rawBlock.BlockHeader.RawData.Number,
		TransactionHash: transactionHash,
		FromAddress:     tx.FromAddress,
		ToAddress:       txToAddress,
		FromUid:         meta.User.FromUid,
		ToUid:           meta.User.ToUid,
		FeeAmount:       decimal.NewFromInt(int64(feeAmount)),
		Amount:          amount,
		Status:          status,
		TxTime:          exTime,
		EventLog:        eventLog,
		LogAddress:      logAddress,
		ContractAddress: rawTx.contractAddress,
		ParseData:       parseData,
		NetUsage:        strconv.Itoa(netUsage),
		FeeLimit:        strconv.Itoa(rawTx.RawData.FeeLimit),
		EnergyUsage:     strconv.Itoa(txInfo.Receipt.EnergyUsage),
		TransactionType: string(tx.TxType),
		TokenInfo:       tokenInfoStr,
		CreatedAt:       h.now,
		UpdatedAt:       h.now,
	}

	if meta.User.MatchFrom || meta.User.MatchTo || isPlatformUser {
		h.txRecords = append(h.txRecords, trxContractRecord)
	}

	if isPlatformUser && meta.TransactionType == biz.CONTRACT {
		for index, eventLog := range eventLogs {
			eventMap := map[string]interface{}{
				"token": eventLog.Token,
			}
			eventParseData, _ := utils.JsonEncode(eventMap)
			eventTokenInfoStr, _ := utils.JsonEncode(eventLog.Token)
			txHash := transactionHash + "#result-" + fmt.Sprintf("%v", index+1)
			txType := biz.EVENTLOG
			contractAddress := eventLog.Token.Address
			amountValue := decimal.NewFromBigInt(eventLog.Amount, 0)

			trxLogRecord := &data.TrxTransactionRecord{
				BlockHash:       block.Hash,
				BlockNumber:     rawBlock.BlockHeader.RawData.Number,
				TransactionHash: txHash,
				FromAddress:     eventLog.From,
				ToAddress:       eventLog.To,
				FromUid:         eventLog.FromUid,
				ToUid:           eventLog.ToUid,
				FeeAmount:       decimal.NewFromInt(int64(feeAmount)),
				Amount:          amountValue,
				Status:          status,
				TxTime:          exTime,
				ContractAddress: contractAddress,
				ParseData:       eventParseData,
				NetUsage:        strconv.Itoa(netUsage),
				FeeLimit:        strconv.Itoa(rawTx.RawData.FeeLimit),
				EnergyUsage:     strconv.Itoa(txInfo.Receipt.EnergyUsage),
				TransactionType: txType,
				TokenInfo:       eventTokenInfoStr,
				CreatedAt:       h.now,
				UpdatedAt:       h.now,
			}
			h.txRecords = append(h.txRecords, trxLogRecord)
		}
	}

	eventLogLen := len(eventLogs)
	if eventLogLen == 1 && trxContractRecord.FromAddress == eventLogs[0].To && trxContractRecord.Amount.String() != "0" && eventLogs[0].Token.Address != "" {
		trxContractRecord.TransactionType = biz.SWAP
	} else if eventLogLen == 2 && ((trxContractRecord.FromAddress == eventLogs[0].From && trxContractRecord.FromAddress == eventLogs[1].To) ||
		(trxContractRecord.FromAddress == eventLogs[0].To && trxContractRecord.FromAddress == eventLogs[1].From)) {
		if trxContractRecord.Amount.String() == "0" {
			trxContractRecord.TransactionType = biz.SWAP
		} else {
			var hasMain bool
			var mainTotal int
			for _, eventLog := range eventLogs {
				if trxContractRecord.FromAddress == eventLog.From {
					if eventLog.Token.Address == "" {
						mainTotal++
						if trxContractRecord.ToAddress == eventLog.To || trxContractRecord.Amount.String() == eventLog.Amount.String() {
							hasMain = true
							break
						}
					} else {
						var mainSymbol string
						if platInfo, ok := biz.GetChainPlatInfo(h.chainName); ok {
							mainSymbol = platInfo.NativeCurrency
						}
						if trxContractRecord.ToAddress == eventLog.To && trxContractRecord.Amount.String() == eventLog.Amount.String() && eventLog.Token.Symbol == mainSymbol {
							hasMain = true
							break
						}
					}
				}
			}
			if !hasMain && mainTotal == 1 {
				hasMain = true
			}
			if hasMain {
				trxContractRecord.TransactionType = biz.SWAP
			}
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
			alarmMsg := fmt.Sprintf("请注意：%s链扫块，将数据插入到数据库中失败", h.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("扫块，将数据插入到数据库中失败", zap.Any("chainName", h.chainName), zap.Any("current", curHeight), zap.Any("error", err))
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
		tx := chainTx.Raw.(*RawTxWrapper)
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
