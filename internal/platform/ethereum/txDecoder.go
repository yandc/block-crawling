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
	"errors"
	"fmt"
	"gorm.io/datatypes"
	"math/big"
	"strings"
	"time"

	pCommon "block-crawling/internal/platform/common"

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
	blocksStore        map[uint64]*chain.Block

	txRecords      []*data.EvmTransactionRecord
	txNonceRecords []*data.EvmTransactionRecord
}

func (h *txDecoder) OnNewTx(c chain.Clienter, block *chain.Block, tx *chain.Transaction) error {
	transaction := tx.Raw.(*Transaction)
	client := c.(*Client)

	/*start := time.Now()
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

	func (h *txDecoder) doOnNewTx(client *Client, block *chain.Block, tx *chain.Transaction, transaction *Transaction) error {*/
	meta, err := pCommon.AttemptMatchUser(h.chainName, tx)
	if err != nil {
		return err
	}

	// Ignore this transaction.
	if !(meta.User.MatchFrom || meta.User.MatchTo) {
		return nil
	}

	log.Info(
		"GOT NEW TX THAT MATCHED OUR USER",
		meta.WrapFields(
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
			/*log.Warn(
				"THE RECEIPT OF TX IS NOT FOUND, THIS BLOCK WILL BE HANDLED LATER",
				meta.WrapFields(
					zap.String("chainName", h.chainName),
					zap.String("txHash", tx.Hash),
					zap.Uint64("curHeight", block.Number),
					zap.Bool("handlePendingTx", !h.newTxs),
				)...,
			)
			// Returens err to avoid increase block height.
			return err*/
			return errors.New("transaction not found") // retry on next node
		}
		log.Warn(
			h.chainName+"扫块，从链上获取交易receipt失败",
			meta.WrapFields(
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
	transaction *Transaction
	meta        *pCommon.TxMeta
	receipt     *Receipt
}

func (h *txDecoder) handleEachTransaction(client *Client, job *txHandleJob) error {
	block := job.block
	transaction := job.transaction
	meta := job.meta
	receipt := job.receipt

	var feeAmount decimal.Decimal
	amount := meta.Value
	var tokenInfo types.TokenInfo
	var eventLogs []types.EventLog
	var contractAddress, tokenId string
	if meta.TransactionType != biz.NATIVE {
		eventLogs, tokenId = h.extractEventLogs(client, meta, receipt)
	}

	if transaction.To() != nil {
		toAddress := transaction.To().String()
		codeAt, err := client.CodeAt(context.Background(), common.HexToAddress(toAddress), nil)
		if err != nil {
			return err
		}
		if len(codeAt) > 0 {
			contractAddress = toAddress
			if meta.TransactionType == biz.NATIVE {
				meta.TransactionType = biz.CONTRACT
				eventLogs, tokenId = h.extractEventLogs(client, meta, receipt)
			} else if meta.TransactionType == biz.APPROVE || meta.TransactionType == biz.TRANSFER || meta.TransactionType == biz.TRANSFERFROM {
				ctx := context.Background()
				if tokenId != "" {
					if meta.TransactionType == biz.APPROVE {
						meta.TransactionType = biz.APPROVENFT
					} else if meta.TransactionType == biz.TRANSFERFROM {
						meta.TransactionType = biz.TRANSFERNFT
					}
					tokenInfo, err = biz.GetNftInfoDirectly(ctx, h.chainName, contractAddress, tokenId)
					for i := 0; i < 3 && err != nil; i++ {
						time.Sleep(time.Duration(i*1) * time.Second)
						tokenInfo, err = biz.GetNftInfoDirectly(ctx, h.chainName, contractAddress, tokenId)
					}
					if err != nil {
						// nodeProxy出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("error", err))
					}
					tokenInfo.TokenType = biz.ERC721
					amount = "1"
					tokenInfo.Amount = amount
				} else {
					if meta.TransactionType == biz.TRANSFERFROM {
						meta.TransactionType = biz.TRANSFER
					}
					//Polygon链的主币地址为空或0x0000000000000000000000000000000000001010
					if strings.HasPrefix(h.chainName, "Polygon") && meta.TransactionType == biz.TRANSFER &&
						(contractAddress == POLYGON_CODE || len(eventLogs) == 0) {
						meta.TransactionType = biz.NATIVE
						contractAddress = ""
					} else {
						tokenInfo, err = biz.GetTokenInfo(ctx, h.chainName, contractAddress)
						for i := 0; i < 3 && err != nil; i++ {
							time.Sleep(time.Duration(i*1) * time.Second)
							tokenInfo, err = biz.GetTokenInfo(ctx, h.chainName, contractAddress)
						}
						if err != nil {
							// nodeProxy出错 接入lark报警
							alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
							alarmOpts := biz.WithMsgLevel("FATAL")
							biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
							log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("error", err))
						}
						tokenInfo.Amount = amount
						if err == nil && tokenInfo.Decimals == 0 && tokenInfo.Symbol == "" {
							meta.TransactionType = biz.CONTRACT
						}
					}
				}
			} else if meta.TransactionType == biz.SETAPPROVALFORALL {
				ctx := context.Background()
				meta.TransactionType = biz.APPROVENFT
				tokenInfo, err = biz.GetCollectionInfoDirectly(ctx, h.chainName, contractAddress)
				for i := 0; i < 3 && err != nil; i++ {
					time.Sleep(time.Duration(i*1) * time.Second)
					tokenInfo, err = biz.GetCollectionInfoDirectly(ctx, h.chainName, contractAddress)
				}
				if err != nil {
					// nodeProxy出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("error", err))
				}
				tokenInfo.Amount = amount
			} else if meta.TransactionType == biz.SAFETRANSFERFROM {
				ctx := context.Background()
				meta.TransactionType = biz.TRANSFERNFT
				var tokenType string
				if !strings.Contains(meta.Value, ",") {
					tokenType = biz.ERC721
					tokenId = meta.Value
					amount = "1"
				} else {
					tokenType = biz.ERC1155
					values := strings.Split(meta.Value, ",")
					tokenId = values[0]
					amount = values[1]
				}

				tokenInfo, err = biz.GetNftInfoDirectly(ctx, h.chainName, contractAddress, tokenId)
				for i := 0; i < 3 && err != nil; i++ {
					time.Sleep(time.Duration(i*1) * time.Second)
					tokenInfo, err = biz.GetNftInfoDirectly(ctx, h.chainName, contractAddress, tokenId)
				}
				if err != nil {
					// nodeProxy出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("error", err))
				}
				tokenInfo.TokenType = tokenType
				tokenInfo.Amount = amount
			} else if meta.TransactionType == biz.SAFEBATCHTRANSFERFROM {
				meta.TransactionType = biz.TRANSFERNFT
				tokenInfo.TokenType = biz.ERC1155
				// TODO
			}
		} else {
			//Polygon链的主币地址为空或0x0000000000000000000000000000000000001010
			if strings.HasPrefix(h.chainName, "Polygon") && meta.TransactionType == biz.TRANSFER &&
				(contractAddress == POLYGON_CODE || len(eventLogs) == 0) {
				meta.TransactionType = biz.NATIVE
				contractAddress = ""
			}
		}
	}

	if h.blockHash == "" {
		// OKEX Chain 交易中取出来和链上的不一样，所以需要从交易中取一次。
		h.blockHash = receipt.BlockHash
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
	status := biz.PENDING
	if receipt.Status == "0x0" {
		status = biz.FAIL
	} else if receipt.Status == "0x1" {
		status = biz.SUCCESS
	}
	intBlockNumber, _ := utils.HexStringToInt(receipt.BlockNumber)
	bn := int(intBlockNumber.Int64())
	feeAmount = decimal.NewFromBigInt(new(big.Int).Mul(gasUsedInt, gasPriceInt), 0)
	amountValue, _ := decimal.NewFromString(amount)
	var eventLog string
	if eventLogs != nil {
		eventLogJson, _ := json.Marshal(eventLogs)
		eventLog = string(eventLogJson)
	}
	var logAddress datatypes.JSON
	if len(eventLogs) > 0 && meta.TransactionType == biz.CONTRACT {
		logFromAddressMap := make(map[string]string)
		logAddressMap := make(map[string]string)
		var oneLogFromAddress []string
		var logFromAddress []string
		var logToAddress []string
		for _, log := range eventLogs {
			_, fromOk := logFromAddressMap[log.From]
			if !fromOk {
				logFromAddressMap[log.From] = ""
				oneLogFromAddress = append(oneLogFromAddress, log.From)
			}

			key := log.From + log.To
			_, ok := logAddressMap[key]
			if !ok {
				logAddressMap[key] = ""
				logFromAddress = append(logFromAddress, log.From)
				logToAddress = append(logToAddress, log.To)
			}
		}
		if len(oneLogFromAddress) == 1 {
			logFromAddress = oneLogFromAddress
		}
		logAddressList := [][]string{logFromAddress, logToAddress}
		logAddress, _ = json.Marshal(logAddressList)

		// database btree index maximum is 2704
		logAddressLen := len(logAddress)
		if logAddressLen > 2704 {
			log.Error(h.chainName+"扫块，logAddress长度超过最大限制", zap.Any("logAddressLen", logAddressLen))
			logAddress = nil
		}
	}
	evmTransactionRecord := &data.EvmTransactionRecord{
		BlockHash:            h.blockHash,
		BlockNumber:          bn,
		Nonce:                int64(transaction.Nonce()),
		TransactionHash:      transaction.Hash().String(),
		FromAddress:          meta.FromAddress,
		ToAddress:            meta.ToAddress,
		FromUid:              meta.User.FromUid,
		ToUid:                meta.User.ToUid,
		FeeAmount:            feeAmount,
		Amount:               amountValue,
		Status:               status,
		TxTime:               h.block.Time,
		ContractAddress:      contractAddress,
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
		LogAddress:           logAddress,
		TransactionType:      meta.TransactionType,
		DappData:             "",
		ClientData:           "",
		CreatedAt:            h.now.Unix(),
		UpdatedAt:            h.now.Unix(),
	}
	h.txRecords = append(h.txRecords, evmTransactionRecord)
	if !h.newTxs {
		h.txNonceRecords = append(h.txNonceRecords, evmTransactionRecord)
	}

	if len(eventLogs) > 0 && meta.TransactionType == biz.CONTRACT {
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
			txType := biz.EVENTLOG
			contractAddress := eventLog.Token.Address
			amountValue := decimal.NewFromBigInt(eventLog.Amount, 0)
			var eventFromUid, eventToUid string

			userMeta, err := pCommon.MatchUser(eventLog.From, eventLog.To, h.chainName)
			if err == nil {
				eventFromUid = userMeta.FromUid
				eventToUid = userMeta.ToUid
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
				FeeAmount:            feeAmount,
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
			go HandlePendingRecord(h.chainName, *(client.(*Client)), txRecords)
		}
	}
	return nil
}

func (h *txDecoder) extractEventLogs(client *Client, meta *pCommon.TxMeta, receipt *Receipt) (eventLogList []types.EventLog, tokenId string) {
	var eventLogs []*types.EventLog
	for _, log_ := range receipt.Logs {
		if len(log_.Topics) <= 1 {
			continue
		}

		topic0 := log_.Topics[0].String()
		if topic0 != APPROVAL_TOPIC && topic0 != APPROVALFORALL_TOPIC && topic0 != TRANSFER_TOPIC &&
			topic0 != TRANSFERSINGLE_TOPIC && topic0 != WITHDRAWAL_TOPIC && topic0 != DEPOSIT_TOPIC {
			continue
		}

		var token types.TokenInfo
		var err error
		tokenAddress := log_.Address.String()
		amount := big.NewInt(0)
		fromAddress := common.HexToAddress(log_.Topics[1].String()).String()
		var toAddress string

		//判断合约 转账， 提现， 兑换。
		if topic0 == TRANSFER_TOPIC || topic0 == WITHDRAWAL_TOPIC || topic0 == DEPOSIT_TOPIC {
			if tokenAddress != "" {
				ctx := context.Background()
				if len(log_.Topics) == 4 {
					tokenId = log_.Topics[3].Big().String()
					token, err = biz.GetNftInfoDirectly(ctx, h.chainName, tokenAddress, tokenId)
					for i := 0; i < 3 && err != nil; i++ {
						time.Sleep(time.Duration(i*1) * time.Second)
						token, err = biz.GetNftInfoDirectly(ctx, h.chainName, tokenAddress, tokenId)
					}
					if err != nil {
						// nodeProxy出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("error", err))
					}
					token.TokenType = biz.ERC721
					amount, _ = new(big.Int).SetString("1", 0)
				} else {
					token, err = biz.GetTokenInfo(ctx, h.chainName, tokenAddress)
					for i := 0; i < 3 && err != nil; i++ {
						time.Sleep(time.Duration(i*1) * time.Second)
						token, err = biz.GetTokenInfo(ctx, h.chainName, tokenAddress)
					}
					if err != nil {
						// nodeProxy出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("error", err))
					}
					if len(log_.Data) >= 32 {
						amount = new(big.Int).SetBytes(log_.Data[:32])
					}
					if topic0 == TRANSFER_TOPIC && amount.String() == "0" {
						continue
					}
				}
				token.Amount = amount.String()
			}
		}

		if topic0 == APPROVAL_TOPIC {
			toAddress = common.HexToAddress(log_.Topics[2].String()).String()
			if toAddress == "0x0000000000000000000000000000000000000000" {
				continue
			}
			if tokenAddress != "" {
				ctx := context.Background()
				if len(log_.Topics) == 4 {
					tokenId = log_.Topics[3].Big().String()
					token, err = biz.GetNftInfoDirectly(ctx, h.chainName, tokenAddress, tokenId)
					for i := 0; i < 3 && err != nil; i++ {
						time.Sleep(time.Duration(i*1) * time.Second)
						token, err = biz.GetNftInfoDirectly(ctx, h.chainName, tokenAddress, tokenId)
					}
					if err != nil {
						// nodeProxy出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("error", err))
					}
					token.TokenType = biz.ERC721
					amount, _ = new(big.Int).SetString("1", 0)
				} else {
					token, err = biz.GetTokenInfo(ctx, h.chainName, tokenAddress)
					for i := 0; i < 3 && err != nil; i++ {
						time.Sleep(time.Duration(i*1) * time.Second)
						token, err = biz.GetTokenInfo(ctx, h.chainName, tokenAddress)
					}
					if err != nil {
						// nodeProxy出错 接入lark报警
						alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
						alarmOpts := biz.WithMsgLevel("FATAL")
						biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
						log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("error", err))
					}
					if len(log_.Data) >= 32 {
						amount = new(big.Int).SetBytes(log_.Data[:32])
					}
				}
				token.Amount = amount.String()
			}
		} else if topic0 == APPROVALFORALL_TOPIC {
			toAddress = common.HexToAddress(log_.Topics[2].String()).String()
			if toAddress == "0x0000000000000000000000000000000000000000" {
				continue
			}
			if tokenAddress != "" {
				ctx := context.Background()
				tokenId = log_.Topics[2].Big().String()
				token, err = biz.GetCollectionInfoDirectly(ctx, h.chainName, tokenAddress)
				for i := 0; i < 3 && err != nil; i++ {
					time.Sleep(time.Duration(i*1) * time.Second)
					token, err = biz.GetCollectionInfoDirectly(ctx, h.chainName, tokenAddress)
				}
				if err != nil {
					// nodeProxy出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("error", err))
				}
				amount = new(big.Int).SetBytes(log_.Data)
				token.Amount = amount.String()
			}
		} else if topic0 == TRANSFER_TOPIC {
			toAddress = common.HexToAddress(log_.Topics[2].String()).String()
		} else if topic0 == TRANSFERSINGLE_TOPIC {
			ctx := context.Background()
			tokenId = new(big.Int).SetBytes(log_.Data[:32]).String()
			amount = new(big.Int).SetBytes(log_.Data[32:64])
			token, err = biz.GetNftInfoDirectly(ctx, h.chainName, tokenAddress, tokenId)
			for i := 0; i < 3 && err != nil; i++ {
				time.Sleep(time.Duration(i*1) * time.Second)
				token, err = biz.GetNftInfoDirectly(ctx, h.chainName, tokenAddress, tokenId)
			}
			if err != nil {
				// nodeProxy出错 接入lark报警
				alarmMsg := fmt.Sprintf("请注意：%s链查询nodeProxy中代币精度失败", h.chainName)
				alarmOpts := biz.WithMsgLevel("FATAL")
				biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
				log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("error", err))
			}
			token.TokenType = biz.ERC1155
			token.Amount = amount.String()
			fromAddress = common.HexToAddress(log_.Topics[2].String()).String()
			toAddress = common.HexToAddress(log_.Topics[3].String()).String()
		} else if topic0 == WITHDRAWAL_TOPIC {
			//提现，判断 用户无需话费value 判断value是否为0
			if meta.Value == "0" {
				toAddress = meta.FromAddress
				if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
					token.Symbol = token.Symbol[1:]
				}
			} else {
				toAddress = meta.ToAddress
			}
		} else if topic0 == DEPOSIT_TOPIC {
			//兑换时判断 交易金额不能为 0
			//判断 value是否为0 不为 0 则增加记录
			toAddress = common.HexToAddress(log_.Topics[1].String()).String()
			if meta.Value != "0" {
				fromAddress = meta.FromAddress
				if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
					token.Symbol = token.Symbol[1:]
				}
			} else {
				fromAddress = meta.ToAddress
			}
		}
		//不展示event log中的授权记录
		if topic0 == APPROVAL_TOPIC || topic0 == APPROVALFORALL_TOPIC {
			continue
		}

		eventLogInfo := &types.EventLog{
			From:   fromAddress,
			To:     toAddress,
			Amount: amount,
			Token:  token,
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
	}

	for _, eventLog := range eventLogs {
		if eventLog != nil {
			eventLogList = append(eventLogList, *eventLog)
		}
	}
	return
}

func (h *txDecoder) getBlockHashFromReceipt(client *Client, transaction *Transaction) (string, error) {
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

func (h *txDecoder) OnSealedTx(c chain.Clienter, txByHash *chain.Transaction) error {
	client := c.(*Client)

	rawReceipt := txByHash.Raw

	meta, err := pCommon.AttemptMatchUser(h.chainName, txByHash)
	if err != nil {
		return err
	}

	// Ignore this transaction.
	if !(meta.User.MatchFrom || meta.User.MatchTo) {
		log.Warn(
			"PENDING TX COULD NOT MATCH USER",
			meta.WrapFields(
				zap.String("chainName", h.chainName),
				zap.Uint64("height", txByHash.BlockNumber),
				zap.String("nodeUrl", client.URL()),
				zap.String("txHash", txByHash.Hash),
			)...,
		)
		return nil
	}

	log.Info(
		"PENDING TX HAS SEALED",
		meta.WrapFields(
			zap.String("chainName", h.chainName),
			zap.Uint64("height", txByHash.BlockNumber),
			zap.String("nodeUrl", client.URL()),
			zap.String("txHash", txByHash.Hash),
		)...,
	)

	var block *chain.Block
	curHeight := txByHash.BlockNumber
	if blk, ok := h.blocksStore[curHeight]; ok {
		block = blk
	} else {
		var err error
		block, err = client.GetBlock(curHeight)
		if err != nil {
			return err
		}
		h.blocksStore[curHeight] = block
	}

	for _, blkTx := range block.Transactions {
		if txByHash.Hash == blkTx.Hash {
			job := &txHandleJob{
				block:       block,
				tx:          txByHash,
				transaction: blkTx.Raw.(*Transaction),
				meta:        meta,
				receipt:     rawReceipt.(*Receipt),
			}
			h.block = block // to let below invocation work.
			if err := h.handleEachTransaction(c.(*Client), job); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *txDecoder) OnDroppedTx(c chain.Clienter, tx *chain.Transaction) error {
	client := c.(*Client)
	ctx := context.Background()
	record := tx.Record.(*data.EvmTransactionRecord)
	//a.扫块时候 给扫到 一样nonce的值 成功后  更新 当nonce一样，并且状态不是 biz.DROPPED_REPLACED 更新状态为 biz.DROPPED_REPLACED
	//新成功后的 speed_up

	//兜底时，成功后 执行 a 一样的步骤

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
			//更新抛弃状态
			record.Status = biz.DROPPED
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
