package ethereum

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	types2 "github.com/ethereum/go-ethereum/core/types"
	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type txDecoder struct {
	chainName string
	block     *chain.Block
	blockHash string
	now       time.Time
	newTxs    bool

	blockHashRetrieved bool

	txRecords      []*data.EvmTransactionRecord
	txNonceRecords []*data.EvmTransactionRecord
}

func (h *txDecoder) OnNewTx(c chain.Clienter, block *chain.Block, tx *chain.Transaction) error {
	h.newTxs = true
	transaction := tx.Raw.(*types2.Transaction)
	client := c.(*Client)

	start := time.Now()
	if err := h.doOnNewTx(client, block, tx, transaction); err != nil {
		return err
	}

	handleTxElapsed := time.Now().Sub(start).String()

	var updateHashElapsed string
	if h.blockHash == "" {
		start := time.Now()
		hash, err := h.getBlockHashFromReceipt(client, transaction)
		updateHashElapsed = time.Now().Sub(start).String()
		if err != nil {
			log.Debug(
				"UPDATE BLOCK HASH FROM RECEIPT FAILED WITH ERROR",
				zap.String("chainName", h.chainName),
				zap.Uint64("height", block.Number),
				zap.String("prevHash", block.Hash),
				zap.String("postHash", h.blockHash),
				zap.String("updateHashElapsed", updateHashElapsed),
				zap.String("handleTxElapsed", handleTxElapsed),
				zap.Error(err),
			)
			return err
		}
		h.blockHash = hash
	}

	// Use block hash in receipt to fix block hash mismatch
	if h.blockHashRetrieved && h.blockHash != block.Hash {
		log.Debug(
			"UPDATE BLOCK HASH FROM RECEIPT",
			zap.String("chainName", h.chainName),
			zap.Uint64("height", block.Number),
			zap.String("prevHash", block.Hash),
			zap.String("postHash", h.blockHash),
			zap.String("updateHashElapsed", updateHashElapsed),
			zap.String("handleTxElapsed", handleTxElapsed),
		)
		block.Hash = h.blockHash
	}
	return nil
}

func (h *txDecoder) doOnNewTx(client *Client, block *chain.Block, tx *chain.Transaction, transaction *types2.Transaction) error {
	meta, err := h.parseTxMeta(tx)
	if err != nil {
		return err
	}

	// Ignore this transaction.
	if !(meta.user.matchFrom || meta.user.matchTo) {
		return nil
	}

	log.Info(
		"GOT NEW TX THAT MATCHED OUR USER",
		meta.wrapFields(
			zap.String("chainName", h.chainName),
			zap.Uint64("height", tx.BlockNumber),
			zap.String("nodeUrl", client.URL()),
			zap.String("txHash", tx.Hash),
			zap.Bool("handlePendingTx", !h.newTxs),
		)...,
	)

	receipt, err := client.GetTransactionReceipt(context.Background(), transaction.Hash())

	if err != nil {
		if err == ethereum.NotFound {
			log.Warn(
				"THE RECEIPT OF TX IS NOT FOUND, THIS BLOCK WILL BE HANDLED LATER",
				meta.wrapFields(
					zap.String("chainName", h.chainName),
					zap.String("txHash", tx.Hash),
					zap.Uint64("curHeight", block.Number),
					zap.Bool("handlePendingTx", !h.newTxs),
				)...,
			)
			// Returens err to avoid increase block height.
			return err
		}
		log.Error(
			h.chainName+"扫块，从链上获取交易receipt失败",
			meta.wrapFields(
				zap.String("chainName", h.chainName),
				zap.Any("curHeight", block.Number),
				zap.String("txHash", tx.Hash),
				zap.Any("error", err),
				zap.Bool("handlePendingTx", !h.newTxs),
			)...,
		)
		return err
	}

	job := &txHandleJob{
		block:       block,
		tx:          tx,
		transaction: transaction,
		meta:        meta,
		receipt:     receipt,
	}

	return h.handleEachTransaction(client, job)
}

type txHandleJob struct {
	block       *chain.Block
	tx          *chain.Transaction
	transaction *types2.Transaction
	meta        *txMeta
	receipt     *Receipt
}

func (h *txDecoder) handleEachTransaction(client *Client, job *txHandleJob) error {
	block := job.block
	transaction := job.transaction
	meta := job.meta
	receipt := job.receipt

	var feeAmount string
	var tokenInfo types.TokenInfo

	if transaction.To() != nil {
		codeAt, err := client.CodeAt(context.Background(), common.HexToAddress(transaction.To().String()), nil)
		if err != nil {
			return err
		}
		if len(codeAt) > 0 {
			if meta.transactionType == "native" {
				meta.transactionType = "contract"
			} else {
				ctx := context.Background()
				getTokenInfo, err := biz.GetTokenInfo(ctx, h.chainName, transaction.To().String())
				for i := 0; i < 3 && err != nil; i++ {
					time.Sleep(time.Duration(i*1) * time.Second)
					getTokenInfo, err = biz.GetTokenInfo(ctx, h.chainName, transaction.To().String())
				}
				if err != nil {
					// nodeProxy出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("error", err))
				}
				if err != nil || getTokenInfo.Decimals == 0 || getTokenInfo.Symbol == "" {
					meta.transactionType = "contract"
				} else {
					tokenInfo = types.TokenInfo{Decimals: getTokenInfo.Decimals, Amount: meta.value, Symbol: getTokenInfo.Symbol}
				}

				tokenInfo.Address = transaction.To().String()
			}
		}
	}

	if h.blockHash == "" {
		// OKEX Chain 交易中取出来和链上的不一样，所以需要从交易中取一次。
		h.blockHash = receipt.BlockHash
	}

	var eventLogs []types.EventLog
	if meta.transactionType != "native" {
		eventLogs = h.extractEventLogs(client, meta, receipt)
	}
	evmMap := map[string]interface{}{
		"evm": map[string]string{
			"nonce": fmt.Sprintf("%v", transaction.Nonce()),
			"type":  fmt.Sprintf("%v", transaction.Type()),
		},
		"token": tokenInfo,
	}
	parseData, _ := json.Marshal(evmMap)
	gasUsedInt, _ := utils.HexStringToInt(receipt.GasUsed)
	gasUsed := gasUsedInt.String()
	gasPriceInt := transaction.GasPrice()
	if receipt.EffectiveGasPrice != "" {
		gasPriceInt, _ = utils.HexStringToInt(receipt.EffectiveGasPrice)
	}
	gasPrice := gasPriceInt.String()
	var maxFeePerGas string
	var maxPriorityFeePerGas string
	if transaction.Type() == types2.DynamicFeeTxType {
		if transaction.GasFeeCap() != nil {
			maxFeePerGas = transaction.GasFeeCap().String()
		}
		if transaction.GasTipCap() != nil {
			maxPriorityFeePerGas = transaction.GasTipCap().String()
		}
	}
	feeAmount = new(big.Int).Mul(gasUsedInt, gasPriceInt).String()
	status := biz.PENDING
	if receipt.Status == "0x0" {
		status = biz.FAIL
	} else if receipt.Status == "0x1" {
		status = biz.SUCCESS
	}
	intBlockNumber, _ := utils.HexStringToInt(receipt.BlockNumber)
	bn := int(intBlockNumber.Int64())
	fa, _ := decimal.NewFromString(feeAmount)
	at, _ := decimal.NewFromString(meta.value)
	var eventLog string
	if eventLogs != nil {
		eventLogJson, _ := json.Marshal(eventLogs)
		eventLog = string(eventLogJson)
	}
	evmTransactionRecord := &data.EvmTransactionRecord{
		BlockHash:            h.blockHash,
		BlockNumber:          bn,
		Nonce:                int64(transaction.Nonce()),
		TransactionHash:      transaction.Hash().String(),
		FromAddress:          meta.fromAddress,
		ToAddress:            meta.toAddress,
		FromUid:              meta.user.fromUid,
		ToUid:                meta.user.toUid,
		FeeAmount:            fa,
		Amount:               at,
		Status:               status,
		TxTime:               h.block.Time,
		ContractAddress:      tokenInfo.Address,
		ParseData:            string(parseData),
		Type:                 fmt.Sprintf("%v", transaction.Type()),
		GasLimit:             fmt.Sprintf("%v", transaction.Gas()),
		GasUsed:              gasUsed,
		GasPrice:             gasPrice,
		BaseFee:              block.BaseFee,
		MaxFeePerGas:         maxFeePerGas,
		MaxPriorityFeePerGas: maxPriorityFeePerGas,
		Data:                 hex.EncodeToString(transaction.Data()),
		EventLog:             eventLog,
		TransactionType:      meta.transactionType,
		DappData:             "",
		ClientData:           "",
		CreatedAt:            h.now.Unix(),
		UpdatedAt:            h.now.Unix(),
	}
	h.txRecords = append(h.txRecords, evmTransactionRecord)
	if !h.newTxs {
		h.txNonceRecords = append(h.txNonceRecords, evmTransactionRecord)
	}

	if len(eventLogs) > 0 && meta.transactionType == "contract" {
		for index, eventLog := range eventLogs {
			eventMap := map[string]interface{}{
				"evm": map[string]string{
					"nonce": fmt.Sprintf("%v", transaction.Nonce()),
					"type":  fmt.Sprintf("%v", transaction.Type()),
				},
				"token": eventLog.Token,
			}
			eventParseData, _ := json.Marshal(eventMap)
			//b, _ := json.Marshal(eventLog)
			txHash := transaction.Hash().String() + "#result-" + fmt.Sprintf("%v", index+1)
			txType := "eventLog"
			contractAddress := eventLog.Token.Address
			amountValue := decimal.NewFromBigInt(eventLog.Amount, 0)
			var eventFromUid, eventToUid string

			userMeta, err := h.matchUser(eventLog.From, eventLog.To)
			if err == nil {
				eventFromUid = userMeta.fromUid
				eventToUid = userMeta.toUid
			}
			evmlogTransactionRecord := &data.EvmTransactionRecord{
				BlockHash:            h.blockHash,
				BlockNumber:          bn,
				Nonce:                int64(transaction.Nonce()),
				TransactionHash:      txHash,
				FromAddress:          eventLog.From,
				ToAddress:            eventLog.To,
				FromUid:              eventFromUid,
				ToUid:                eventToUid,
				FeeAmount:            fa,
				Amount:               amountValue,
				Status:               status,
				TxTime:               h.block.Time,
				ContractAddress:      contractAddress,
				ParseData:            string(eventParseData),
				Type:                 fmt.Sprintf("%v", transaction.Type()),
				GasLimit:             fmt.Sprintf("%v", transaction.Gas()),
				GasUsed:              gasUsed,
				GasPrice:             gasPrice,
				BaseFee:              block.BaseFee,
				MaxFeePerGas:         maxFeePerGas,
				MaxPriorityFeePerGas: maxPriorityFeePerGas,
				Data:                 hex.EncodeToString(transaction.Data()),
				TransactionType:      txType,
				DappData:             "",
				ClientData:           "",
				CreatedAt:            h.now.Unix(),
				UpdatedAt:            h.now.Unix(),
			}
			h.txRecords = append(h.txRecords, evmlogTransactionRecord)
			if !h.newTxs {
				h.txNonceRecords = append(h.txNonceRecords, evmTransactionRecord)
			}
		}
	}
	return nil
}

type txMeta struct {
	fromAddress string
	value       string
	toAddress   string

	transactionType string

	user *userMeta
}

func (meta *txMeta) wrapFields(results ...zap.Field) []zap.Field {
	results = append(
		results,
		zap.String("fromAddr", meta.fromAddress),
		zap.String("toAddr", meta.toAddress),
		zap.String("txType", meta.transactionType),
	)
	if meta.user != nil {
		results = append(
			results,
			zap.Bool("matchFrom", meta.user.matchFrom),
			zap.Bool("matchTo", meta.user.matchTo),
			zap.String("fromUid", meta.user.fromUid),
			zap.String("toUid", meta.user.toUid),
		)
	}
	return results
}

type userMeta struct {
	matchFrom bool
	fromUid   string

	matchTo bool
	toUid   string
}

func (h *txDecoder) parseTxMeta(tx *chain.Transaction) (meta *txMeta, err error) {
	meta = &txMeta{
		fromAddress:     tx.FromAddress,
		value:           tx.Value,
		toAddress:       tx.ToAddress,
		transactionType: string(tx.TxType),
	}
	user, err := h.matchUser(meta.fromAddress, meta.toAddress)
	if err != nil {
		return nil, err
	}
	meta.user = user
	return meta, nil
}

func (h *txDecoder) matchUser(fromAddress, toAddress string) (*userMeta, error) {
	userFromAddress, fromUid, err := biz.UserAddressSwitch(fromAddress)
	if err != nil {
		// redis出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户地址失败", h.chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Info("查询redis缓存报错：用户中心获取", zap.Any(h.chainName, fromAddress), zap.Any("error", err))
		return nil, err
	}
	userToAddress, toUid, err := biz.UserAddressSwitch(toAddress)
	if err != nil {
		// redis出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户地址失败", h.chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Info("查询redis缓存报错：用户中心获取", zap.Any(h.chainName, toAddress), zap.Any("error", err))
		return nil, err
	}
	return &userMeta{
		matchFrom: userFromAddress,
		fromUid:   fromUid,

		matchTo: userToAddress,
		toUid:   toUid,
	}, nil
}

func (h *txDecoder) Save(client chain.Clienter) error {
	txRecords := h.txRecords
	txNonceRecords := h.txNonceRecords
	if txRecords != nil && len(txRecords) > 0 {
		err := BatchSaveOrUpdate(txRecords, biz.GetTalbeName(h.chainName))
		if err != nil {
			// postgres出错 接入lark报警
			log.Error("插入数据到数据库库中失败", zap.Any("current", h.block.Number), zap.Any("chain", h.chainName))
			alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库库中失败", h.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Info("插入数据库报错：", zap.Any(h.chainName, err))
			return err
		}
		if h.newTxs {
			go HandleRecord(h.chainName, *(client.(*Client)), txRecords)
		} else {
			go handleUserNonce(h.chainName, txNonceRecords)
		}
	}
	return nil
}

func (h *txDecoder) extractEventLogs(client *Client, meta *txMeta, receipt *Receipt) (eventLogs []types.EventLog) {
	for _, log_ := range receipt.Logs {
		if len(log_.Topics) > 1 && (log_.Topics[0].String() == TRANSFER_TOPIC ||
			log_.Topics[0].String() == WITHDRAWAL_TOPIC || log_.Topics[0].String() == DEPOST_TOPIC) {
			var token types.TokenInfo
			var err error
			amount := big.NewInt(0)
			if len(log_.Data) >= 32 {
				amount = new(big.Int).SetBytes(log_.Data[:32])
			}
			if log_.Address.String() != "" {
				ctx := context.Background()
				token, err = biz.GetTokenInfo(ctx, h.chainName, log_.Address.String())
				for i := 0; i < 3 && err != nil; i++ {
					time.Sleep(time.Duration(i*1) * time.Second)
					token, err = biz.GetTokenInfo(ctx, h.chainName, log_.Address.String())
				}
				if err != nil {
					// nodeProxy出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("error", err))
				}
				token.Amount = amount.String()

			}
			eventFrom := common.HexToAddress(log_.Topics[1].String()).String()
			var to string
			//判断合约 转账， 提现， 兑换。
			if len(log_.Topics) > 2 && log_.Topics[0].String() == TRANSFER_TOPIC {
				to = common.HexToAddress(log_.Topics[2].String()).String()
			} else if log_.Topics[0].String() == WITHDRAWAL_TOPIC {
				//提现，判断 用户无需话费value 判断value是否为0
				if meta.value == "0" {
					to = meta.fromAddress
					if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
						token.Symbol = token.Symbol[1:]
					}
				} else {
					to = meta.toAddress
				}
			} else if log_.Topics[0].String() == DEPOST_TOPIC {
				//兑换时判断 交易金额不能为 0
				//判断 value是否为0 不为 0 则增加记录
				to = common.HexToAddress(log_.Topics[1].String()).String()
				if meta.value != "0" {
					eventFrom = meta.fromAddress
					if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
						token.Symbol = token.Symbol[1:]
					}
				} else {
					eventFrom = meta.toAddress
				}
			}

			eventLog := types.EventLog{
				From:   eventFrom,
				To:     to,
				Amount: amount,
				Token:  token,
			}
			eventLogs = append(eventLogs, eventLog)
		}
	}
	return
}

func (h *txDecoder) getBlockHashFromReceipt(client *Client, transaction *types2.Transaction) (string, error) {
	if isNonstandardEVM(h.chainName) {
		if h.blockHashRetrieved {
			return "", nil
		}

		receipt, err := client.GetTransactionReceipt(context.Background(), transaction.Hash())

		h.blockHashRetrieved = true

		if err != nil {
			if err == ethereum.NotFound {
				return "", nil
			}
			return "", err
		}
		return receipt.BlockHash, nil
	}
	return "", nil
}

func (h *txDecoder) OnSealedTx(c chain.Clienter, block *chain.Block, tx *chain.Transaction, rawReceipt interface{}) error {
	h.newTxs = false
	transaction := tx.Raw.(*types2.Transaction)
	client := c.(*Client)

	meta, err := h.parseTxMeta(tx)
	if err != nil {
		return err
	}

	// Ignore this transaction.
	if !(meta.user.matchFrom || meta.user.matchTo) {
		log.Warn(
			"PENDING TX COULD NOT MATCH USER",
			meta.wrapFields(
				zap.String("chainName", h.chainName),
				zap.Uint64("height", tx.BlockNumber),
				zap.String("nodeUrl", client.URL()),
				zap.String("txHash", tx.Hash),
			)...,
		)
		return nil
	}

	log.Info(
		"PENDING TX HAS SEALED",
		meta.wrapFields(
			zap.String("chainName", h.chainName),
			zap.Uint64("height", tx.BlockNumber),
			zap.String("nodeUrl", client.URL()),
			zap.String("txHash", tx.Hash),
		)...,
	)

	job := &txHandleJob{
		block:       block,
		tx:          tx,
		transaction: transaction,
		meta:        meta,
		receipt:     rawReceipt.(*Receipt),
	}

	if err := h.handleEachTransaction(c.(*Client), job); err != nil {
		return err
	}
	return nil
}

func (h *txDecoder) OnDroppedTx(c chain.Clienter, block *chain.Block, tx *chain.Transaction) error {
	client := c.(*Client)
	ctx := context.Background()
	record := tx.Record.(*data.EvmTransactionRecord)

	log.Info(
		"PENDING TX COULD NOT FOUND ON THE CHAIN",
		zap.String("chainName", h.chainName),
		zap.Uint64("height", tx.BlockNumber),
		zap.String("nodeUrl", c.URL()),
		zap.String("txHash", tx.Hash),
		zap.String("fromUid", record.FromUid),
		zap.String("toUid", record.ToUid),
		zap.String("fromAddress", record.FromAddress),
		zap.String("toAddress", record.ToAddress),
		zap.Int64("recordNonce", record.Nonce),
	)

	//判断nonce 是否小于 当前链上的nonce
	nonce, nonceErr := client.NonceAt(ctx, common.HexToAddress(record.FromAddress), nil)
	if nonceErr != nil {
		return nil
	}
	if int(record.Nonce) < int(nonce) {
		record.Status = biz.DROPPED_REPLACED
		h.txRecords = append(h.txRecords, record)
		log.Info(
			"更新 PENDING txhash对象为丢弃置换状态",
			zap.Any("txId", record.TransactionHash),
			zap.Int64("recordNonce", record.Nonce),
			zap.Uint64("chainNonce", nonce),
		)
		return nil
	} else {
		now := time.Now().Unix()
		ctime := record.CreatedAt + 21600
		if ctime < now {
			record.Status = biz.FAIL
			h.txRecords = append(h.txRecords, record)
			log.Info(
				"更新 PENDING txhash对象为终态:交易被抛弃",
				zap.Any("txId", record.TransactionHash),
				zap.Int64("recordNonce", record.Nonce),
				zap.Uint64("chainNonce", nonce),
				zap.Int64("nowTime", now),
				zap.Int64("createTime", record.CreatedAt),
			)
			return nil
		} else {
			record.Status = biz.NO_STATUS
			h.txRecords = append(h.txRecords, record)
			log.Info(
				"更新 PENDING txhash对象无状态",
				zap.Any("txId", record.TransactionHash),
				zap.Int64("recordNonce", record.Nonce),
				zap.Uint64("chainNonce", nonce),
				zap.Int64("nowTime", now),
				zap.Int64("createTime", record.CreatedAt),
			)
			return nil
		}
	}
}
