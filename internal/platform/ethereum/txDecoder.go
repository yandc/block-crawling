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

	"gorm.io/datatypes"

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
	txByHash  *chain.Transaction
	blockHash string
	now       int64
	newTxs    bool

	blockHashRetrieved bool
	blocksStore        map[uint64]*chain.Block

	txRecords      []*data.EvmTransactionRecord
	txNonceRecords []*data.EvmTransactionRecord
	kanbanRecords  []*data.EvmTransactionRecord

	kanbanEnabled bool
	matchedUser   bool
}

func (h *txDecoder) OnNewTx(c chain.Clienter, block *chain.Block, tx *chain.Transaction) error {
	transaction := tx.Raw.(*Transaction)
	client := c.(*Client)

	meta, err := pCommon.AttemptMatchUser(h.chainName, tx)
	if err != nil {
		return err
	}
	h.matchedUser = meta.User.MatchFrom || meta.User.MatchTo

	// Ignore this transaction.
	if !h.matchedUser && !h.kanbanEnabled {
		if len(transaction.Data()) < 4 || transaction.To() == nil {
			return nil
		}
		// dapp 白名单 判断 ，非白名单 直接 扔掉交易
		whiteMethods := BridgeWhiteMethodIdList[h.chainName+"_MethodId"]
		flag := true
		s := h.chainName + "_" + transaction.To().String() + "_" + hex.EncodeToString(transaction.Data()[:4])
		for _, whiteMethod := range whiteMethods {
			if whiteMethod == s {
				flag = false
				break
			}
		}
		//未命中白名 则丢弃该交易
		if flag {
			return nil
		}
	}

	/*log.Info(
		"GOT NEW TX THAT MATCHED OUR USER",
		meta.WrapFields(
			zap.String("chainName", h.chainName),
			zap.Uint64("height", tx.BlockNumber),
			zap.String("nodeUrl", client.URL()),
			zap.String("txHash", tx.Hash),
			zap.Bool("handlePendingTx", !h.newTxs),
		)...,
	)*/

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
			return pCommon.TransactionNotFound // retry on next node
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
	var eventLogs []types.EventLogUid
	var contractAddress, tokenId string
	if meta.TransactionType != biz.NATIVE {
		eventLogs, tokenId = h.extractEventLogs(client, meta, receipt, transaction)
	}
	status := biz.PENDING
	if receipt.Status == "0x0" || receipt.Status == "0x00" {
		status = biz.FAIL
	} else if receipt.Status == "0x1" || receipt.Status == "0x01" {
		status = biz.SUCCESS
	}
	transactionHash := transaction.Hash().String()

	if transaction.To() != nil {
		toAddress := transaction.To().String()
		cli,_ := getETHClient(client.url)
		defer cli.Close()
		codeAt, err := cli.CodeAt(context.Background(), common.HexToAddress(toAddress), nil)
		if err != nil {
			return err
		}
		if len(codeAt) > 0 {
			ctx := context.Background()
			contractAddress = toAddress
			var getTokenType bool
			var tokenType string
			var tokenTypeErr error
			if meta.TransactionType == biz.NATIVE {
				meta.TransactionType = biz.CONTRACT
				eventLogs, tokenId = h.extractEventLogs(client, meta, receipt, transaction)
			} else if meta.TransactionType == biz.APPROVE || meta.TransactionType == biz.TRANSFER || meta.TransactionType == biz.TRANSFERFROM {
				if status == biz.FAIL && tokenId == "" {
					tokenType, tokenTypeErr = GetTokenType(client, h.chainName, contractAddress, codeAt)
					getTokenType = true
					if tokenType == biz.ERC721 || tokenType == biz.ERC1155 {
						tokenId = meta.Value
					}
				}
				if tokenId != "" {
					if meta.TransactionType == biz.APPROVE {
						meta.TransactionType = biz.APPROVENFT
					} else if meta.TransactionType == biz.TRANSFERFROM {
						meta.TransactionType = biz.TRANSFERNFT
					}
					tokenInfo, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, contractAddress, tokenId)
					if err != nil {
						log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", contractAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
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
						tokenInfo, err = biz.GetTokenInfoRetryAlert(ctx, h.chainName, contractAddress)
						if err != nil {
							log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
						}
						tokenInfo.Amount = amount
					}
				}
			} else if meta.TransactionType == biz.SETAPPROVALFORALL {
				meta.TransactionType = biz.APPROVENFT
				tokenInfo, err = biz.GetCollectionInfoDirectlyRetryAlert(ctx, h.chainName, contractAddress)
				if err != nil {
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
				}
				tokenInfo.Amount = amount
			} else if meta.TransactionType == biz.SAFETRANSFERFROM {
				meta.TransactionType = biz.TRANSFERNFT
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

				tokenInfo, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, contractAddress, tokenId)
				if err != nil {
					log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", contractAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
				}
				tokenInfo.TokenType = tokenType
				tokenInfo.Amount = amount
			} else if meta.TransactionType == biz.SAFEBATCHTRANSFERFROM {
				meta.TransactionType = biz.TRANSFERNFT
				tokenInfo.TokenType = biz.ERC1155
				// TODO
			}

			if err != nil || (meta.TransactionType != biz.NATIVE && meta.TransactionType != biz.CONTRACT &&
				(tokenInfo.TokenType == "" && tokenInfo.Decimals == 0 && (tokenInfo.Symbol == "" || tokenInfo.Symbol == "Unknown Token"))) {
				if !getTokenType {
					tokenType, tokenTypeErr = GetTokenType(client, h.chainName, contractAddress, codeAt)
					getTokenType = true
				}
				if tokenTypeErr != nil {
					// code出错 接入lark报警
					alarmMsg := fmt.Sprintf("请注意：%s链解析contract的code失败", h.chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error(h.chainName+"扫块，解析contract的code失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
				}
				if tokenType != "" {
					if tokenInfo.TokenType == "" && tokenType != biz.ERC20 {
						tokenInfo.TokenType = tokenType
					}
					tokenInfo.Address = contractAddress
					if tokenType != biz.ERC20 {
						tokenInfo.TokenId = tokenId
					}
				} else if tokenTypeErr == nil {
					meta.TransactionType = biz.CONTRACT
					tokenInfo.Address = ""
					tokenInfo.TokenId = ""
					tokenInfo.Symbol = ""
				}
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

	platformUserCount := len(eventLogs)
	/*var platformUserCount int
	if len(eventLogs) > 0 {
		for _, eventLog := range eventLogs {
			var err error
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
				platformUserCount++
			}
		}
	}*/

	isPlatformUser := false
	if platformUserCount > 0 {
		isPlatformUser = true
	}

	if platformUserCount > 1 && meta.TransactionType != biz.CONTRACT {
		meta.TransactionType = biz.CONTRACT
		amount = transaction.Value().String()
		tokenInfo = types.TokenInfo{}
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
	parseData, _ := utils.JsonEncode(evmMap)
	gasUsedInt, _ := utils.HexStringToBigInt(receipt.GasUsed)
	gasUsed := gasUsedInt.String()
	gasPriceInt := transaction.GasPrice()
	if receipt.EffectiveGasPrice != "" {
		gasPriceInt, _ = utils.HexStringToBigInt(receipt.EffectiveGasPrice)
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
	blockNumber, _ := utils.HexStringToInt64(receipt.BlockNumber)
	feeAmount = decimal.NewFromBigInt(new(big.Int).Mul(gasUsedInt, gasPriceInt), 0)
	if h.chainName == "Optimism" {
		l1Fee, err := utils.HexStringToBigInt(receipt.L1Fee)
		if err == nil {
			feeAmount = feeAmount.Add(decimal.NewFromBigInt(l1Fee, 0))
		}
	}
	amountValue, _ := decimal.NewFromString(amount)
	var eventLog string
	if eventLogs != nil {
		eventLog, _ = utils.JsonEncode(eventLogs)
	}

	var logAddress datatypes.JSON
	if isPlatformUser && meta.TransactionType == biz.CONTRACT {
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
			log.Error(h.chainName+"扫块，logAddress长度超过最大限制", zap.Any("txHash", transactionHash), zap.Any("logAddressLen", logAddressLen))
			logAddress = nil
		}
	}
	evmTransactionRecord := &data.EvmTransactionRecord{
		BlockHash:            h.blockHash,
		BlockNumber:          int(blockNumber),
		Nonce:                int64(transaction.Nonce()),
		TransactionHash:      transactionHash,
		FromAddress:          meta.FromAddress,
		ToAddress:            meta.ToAddress,
		FromUid:              meta.User.FromUid,
		ToUid:                meta.User.ToUid,
		FeeAmount:            feeAmount,
		Amount:               amountValue,
		Status:               status,
		TxTime:               h.block.Time,
		ContractAddress:      contractAddress,
		ParseData:            parseData,
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
		OperateType:          "",
		DappData:             "",
		ClientData:           "",
		CreatedAt:            h.now,
		UpdatedAt:            h.now,
	}

	if meta.User.MatchFrom || meta.User.MatchTo || isPlatformUser {
		h.txRecords = append(h.txRecords, evmTransactionRecord)
		if !h.newTxs {
			h.txNonceRecords = append(h.txNonceRecords, evmTransactionRecord)
		}
	}

	if h.kanbanEnabled {
		h.kanbanRecords = append(h.kanbanRecords, evmTransactionRecord)
	}

	for index, eventLog := range eventLogs {
		eventMap := map[string]interface{}{
			"evm": map[string]string{
				"nonce": fmt.Sprintf("%v", transaction.Nonce()),
				"type":  fmt.Sprintf("%v", transaction.Type()),
			},
			"token": eventLog.Token,
		}
		eventParseData, _ := utils.JsonEncode(eventMap)
		txHash := transactionHash + "#result-" + fmt.Sprintf("%v", index+1)
		txType := biz.EVENTLOG
		contractAddress := eventLog.Token.Address
		amountValue := decimal.NewFromBigInt(eventLog.Amount, 0)

		evmlogTransactionRecord := &data.EvmTransactionRecord{
			BlockHash:            h.blockHash,
			BlockNumber:          int(blockNumber),
			Nonce:                int64(transaction.Nonce()),
			TransactionHash:      txHash,
			FromAddress:          eventLog.From,
			ToAddress:            eventLog.To,
			FromUid:              eventLog.FromUid,
			ToUid:                eventLog.ToUid,
			FeeAmount:            feeAmount,
			Amount:               amountValue,
			Status:               status,
			TxTime:               h.block.Time,
			ContractAddress:      contractAddress,
			ParseData:            eventParseData,
			Type:                 fmt.Sprintf("%v", transaction.Type()),
			GasLimit:             fmt.Sprintf("%v", transaction.Gas()),
			GasUsed:              gasUsed,
			GasPrice:             gasPrice,
			BaseFee:              block.BaseFee,
			MaxFeePerGas:         maxFeePerGas,
			MaxPriorityFeePerGas: maxPriorityFeePerGas,
			Data:                 hex.EncodeToString(transaction.Data()),
			TransactionType:      txType,
			OperateType:          "",
			DappData:             "",
			ClientData:           "",
			CreatedAt:            h.now,
			UpdatedAt:            h.now,
		}
		if isPlatformUser && meta.TransactionType == biz.CONTRACT {
			h.txRecords = append(h.txRecords, evmlogTransactionRecord)
			if !h.newTxs {
				h.txNonceRecords = append(h.txNonceRecords, evmTransactionRecord)
			}
		}

		if h.kanbanEnabled {
			h.kanbanRecords = append(h.kanbanRecords, evmlogTransactionRecord)
		}
	}
	return nil
}

func (h *txDecoder) Save(client chain.Clienter) error {
	txRecords := h.txRecords
	txNonceRecords := h.txNonceRecords

	if h.kanbanRecords != nil && len(h.kanbanRecords) > 0 {
		err := BatchSaveOrUpdate(h.kanbanRecords, biz.GetTableName(h.chainName), true)
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", h.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(h.chainName+"扫块，将数据插入到数据库中失败", zap.Any("current", h.block.Number), zap.Any("error", err))
			return err
		}
	}

	// Id may be setted when save to kanban, we need to reset it to zero to avoid conflict.
	for _, item := range txRecords {
		item.Id = 0
	}

	if txRecords != nil && len(txRecords) > 0 {
		err := BatchSaveOrUpdate(txRecords, biz.GetTableName(h.chainName), false)
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链插入数据到数据库中失败", h.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(h.chainName+"扫块，将数据插入到数据库中失败", zap.Any("current", h.block.Number), zap.Any("error", err))
			return err
		}
		if h.newTxs {
			go HandleRecord(h.chainName, *(client.(*Client)), txRecords)
		} else {
			go HandleUserNonce(h.chainName, *(client.(*Client)), txNonceRecords)
			go HandlePendingRecord(h.chainName, *(client.(*Client)), txRecords)
		}

		if h.newTxs {
			records := make([]interface{}, 0, len(txRecords))
			for _, r := range txRecords {
				records = append(records, r)
			}
			pCommon.SetResultOfTxs(h.block, records)
		} else {
			pCommon.SetTxResult(h.txByHash, txRecords[0])
		}
	}
	return nil
}

func (h *txDecoder) extractEventLogs(client *Client, meta *pCommon.TxMeta, receipt *Receipt, transaction *Transaction) (eventLogList []types.EventLogUid, eventLogTokenId string) {
	var eventLogs []*types.EventLogUid
	arbitrumAmount := big.NewInt(0)
	transactionHash := transaction.Hash().String()
	gmxSwapFlag := false
	gmxFromAddress := ""
	gmxAmount := big.NewInt(0)
	xDaiDapp := false
	// token 地址 一样  toaddress 一样 amount 一样 则 不添加transfer  判断 logswap 有咩有 ，有 则判断这三个
	for _, log_ := range receipt.Logs {
		if len(log_.Topics) < 1 {
			continue
		}
		topic0 := log_.Topics[0].String()
		if topic0 != APPROVAL_TOPIC && topic0 != APPROVALFORALL_TOPIC && topic0 != TRANSFER_TOPIC && topic0 != TRANSFERSINGLE_TOPIC &&
			topic0 != TRANSFERBATCH_TOPIC && topic0 != WITHDRAWAL_TOPIC && topic0 != DEPOSIT_TOPIC {
			whiteTopics := BridgeWhiteTopicList[h.chainName+"_Topic"]
			flag := true
			s := h.chainName + "_" + receipt.To + "_" + topic0
			for _, whiteTopic := range whiteTopics {
				if whiteTopic == s {
					flag = false
					break
				}
			}
			if flag {
				continue
			}
		}

		var token types.TokenInfo
		var tokens []*types.TokenInfo
		var err error
		ctx := context.Background()
		tokenAddress := log_.Address.String()
		var tokenId string
		amount := big.NewInt(0)
		var fromAddress, toAddress, fromUid, toUid string
		var fromAddressExist, toAddressExist bool
		if len(log_.Topics) >= 2 {
			fromAddress = common.HexToAddress(log_.Topics[1].String()).String()
		}

		//判断合约 转账， 提现， 兑换。
		if topic0 == TRANSFER_TOPIC || topic0 == WITHDRAWAL_TOPIC || topic0 == DEPOSIT_TOPIC {
			if tokenAddress != "" {
				if len(log_.Topics) == 4 {
					tokenId = log_.Topics[3].Big().String()
					/*token, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, tokenAddress, tokenId)
					if err != nil {
						log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
					}*/
					token.TokenType = biz.ERC721
					amount, _ = new(big.Int).SetString("1", 0)
				} else if len(log_.Topics) == 1 {
					//https://cn.etherscan.com/tx/0x2c355d0b5419ca267344ed6e19ceb8fc20d102f6e67c312b38e047f1031998ee
					/*token, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, tokenAddress, tokenId)
					if err != nil {
						log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
					}*/
					token.TokenType = biz.ERC721
					amount, _ = new(big.Int).SetString("1", 0)
					if len(log_.Data) >= 96 {
						tokenId = new(big.Int).SetBytes(log_.Data[64:96]).String()
						toAddress = common.HexToAddress(hex.EncodeToString(log_.Data[32:64])).String()
						fromAddress = common.HexToAddress(hex.EncodeToString(log_.Data[:32])).String()
					}
				} else {
					token, err = biz.GetTokenInfoRetryAlert(ctx, h.chainName, tokenAddress)
					if err != nil {
						log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
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
			if len(log_.Topics) < 3 {
				log.Warn(
					"EXPECT AT LEAST THREE TOPICS",
					zap.Any("topics", log_.Topics),
					zap.String("chainName", h.chainName),
					zap.String("txhash", transactionHash),
				)
				continue
			}

			toAddress = common.HexToAddress(log_.Topics[2].String()).String()
			if toAddress == "0x0000000000000000000000000000000000000000" {
				continue
			}
			if tokenAddress != "" {
				if len(log_.Topics) == 4 {
					tokenId = log_.Topics[3].Big().String()
					/*token, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, tokenAddress, tokenId)
					if err != nil {
						log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
					}*/
					token.TokenType = biz.ERC721
					amount, _ = new(big.Int).SetString("1", 0)
				} else {
					token, err = biz.GetTokenInfoRetryAlert(ctx, h.chainName, tokenAddress)
					if err != nil {
						log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
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
				token, err = biz.GetCollectionInfoDirectlyRetryAlert(ctx, h.chainName, tokenAddress)
				if err != nil {
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
				}
				amount = new(big.Int).SetBytes(log_.Data)
				token.Amount = amount.String()
			}
		} else if topic0 == TRANSFER_TOPIC {
			if len(log_.Topics) >= 2 {
				toAddress = common.HexToAddress(log_.Topics[2].String()).String()
			}
			// token 地址 一样  toaddress 一样 amount 一样 则 不添加transfer  判断 logswap 有咩有 ，有 则判断这三个
			//uniswap v3 代币换主币 function 销毁 主币 再发送主币。
			if toAddress == "0x0000000000000000000000000000000000000000" && common.HexToAddress(log_.Topics[1].String()).String() == "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45" {
				fromAddress = common.HexToAddress(log_.Topics[1].String()).String()
				toAddress = common.HexToAddress(receipt.From).String()
				amount = new(big.Int).SetBytes(log_.Data)
				tokenAddress = ""
			}
		} else if topic0 == TRANSFERSINGLE_TOPIC {
			tokenId = new(big.Int).SetBytes(log_.Data[:32]).String()
			amount = new(big.Int).SetBytes(log_.Data[32:64])
			/*token, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, tokenAddress, tokenId)
			if err != nil {
				log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
			}*/
			token.TokenType = biz.ERC1155
			token.Amount = amount.String()
			fromAddress = common.HexToAddress(log_.Topics[2].String()).String()
			toAddress = common.HexToAddress(log_.Topics[3].String()).String()
		} else if topic0 == TRANSFERBATCH_TOPIC {
			//https://etherscan.io/tx/0x247e793635ff121dc2500564c7f9c81fbeb8063859428a77da46cc44f5cf515c
			//https://ftmscan.com/tx/0xf280f166ed86b7b02229d012dfdccd406407b11f67d74915920e632d8692be58
			//https://www.bscscan.com/tx/0x852ae27d53936378f08c0b6e97d57dc11c214d9eecac049df821aa5a6109cc59
			//https://arbiscan.io/tx/0xd204a3aa6f3c505ac0a4fcf6de4c7da9fa81097bfae8cdc31c756268a0f082ab
			if len(log_.Data) < 192 {
				//https://mumbai.polygonscan.com/tx/0xc126afea00adcd311900c73a556794504128141d8b69a8fa7f0de980cb16336b
				continue
			}
			tokenNumIndex := new(big.Int).SetBytes(log_.Data[:32]).Int64()
			tokenNum := int(new(big.Int).SetBytes(log_.Data[tokenNumIndex : tokenNumIndex+32]).Int64())
			amountNumIndex := new(big.Int).SetBytes(log_.Data[32:64]).Int64()
			amountNum := int(new(big.Int).SetBytes(log_.Data[amountNumIndex : amountNumIndex+32]).Int64())
			if tokenNum != amountNum {
				continue
			}
			tokenIdIndex := tokenNumIndex + 32
			amountIndex := amountNumIndex + 32
			for i := 0; i < tokenNum; i++ {
				tokenId = new(big.Int).SetBytes(log_.Data[tokenIdIndex : tokenIdIndex+32]).String()
				amount = new(big.Int).SetBytes(log_.Data[amountIndex : amountIndex+32])
				amountStr := amount.String()
				if amountStr == "0" {
					//https://snowtrace.io/tx/0x2a9fa926d53c73fdb8c7c0ce9f28aa0b80e5927cb730d86ea1f8b30a24693edc
					continue
				}
				tokens = append(tokens, &types.TokenInfo{
					TokenType: biz.ERC1155,
					TokenId:   tokenId,
					Amount:    amountStr,
				})
				tokenIdIndex += 32
				amountIndex += 32
			}
			if len(tokens) == 0 {
				continue
			}
			/*token, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, tokenAddress, tokenId)
			if err != nil {
				log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
			}*/
			/*token.TokenType = biz.ERC1155
			token.Amount = amount.String()*/
			fromAddress = common.HexToAddress(log_.Topics[2].String()).String()
			toAddress = common.HexToAddress(log_.Topics[3].String()).String()
		} else if topic0 == WITHDRAWAL_TOPIC {
			//https://etherscan.io/tx/0xe510a2d99d95a6974e5f95a3a745b2ffe873bf6645b764658d978856ac180cd2
			//https://polygonscan.com/tx/0x72ce3718c81bae2c888d0403d33d2f9e5c533c601c90aaaa4158a8439c6f7630
			//提现，判断 用户无需话费value 判断value是否为0
			if meta.Value == "0" {
				toAddress = meta.FromAddress
				/*if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
					token.Symbol = token.Symbol[1:]
				}*/
				tokenAddress = ""
			} else {
				toAddress = meta.ToAddress
			}

			if strings.HasPrefix(h.chainName, "BSC") {
				//https://bscscan.com/tx/0x538168688bcdb857bb0fd00d586b79be0ef17e6188f68d50dfbc829e2b40e890
				if len(eventLogs) > 0 {
					haveTransfer := false
					for _, eventLog := range eventLogs {
						if eventLog != nil && eventLog.From == toAddress && eventLog.To == fromAddress && eventLog.Amount.Cmp(amount) == 0 {
							haveTransfer = true
							break
						}
					}
					if haveTransfer {
						continue
					}
				}
			}

			if strings.HasPrefix(h.chainName, "Polygon") {
				//https://polygonscan.com/tx/0xbf82a6ee9eb2cdd4e63822f247912024760693c60cc521c8118539faef745d18
				if transaction.To().String() == "0xc1DCb196BA862B337Aa23eDA1Cb9503C0801b955" && hex.EncodeToString(transaction.Data()[:4]) == "439dff06" {
					if len(transaction.Data()) >= 100 {
						toAddress = common.HexToAddress(hex.EncodeToString(transaction.Data()[68:100])).String()
					}
				}
			}
		} else if topic0 == DEPOSIT_TOPIC {
			//https://etherscan.io/tx/0x763f368cd98ebca2bda591ab610aa5b6dc6049fadae9ce04394fc7a8b7304976
			if h.chainName == "Ronin" && len(log_.Topics) == 1 {
				//https://explorer.roninchain.com/tx/0x0b93df20612bdd000e23f9e3158325fcec6c0459ea90ce30420a6380e6b706a7
				//兑换时判断 交易金额不能为 0
				//判断 value是否为0 不为 0 则增加记录
				if len(log_.Data) >= 32 {
					toAddress = common.HexToAddress(hex.EncodeToString(log_.Data[:32])).String()
				}
				token, err = biz.GetTokenInfoRetryAlert(context.Background(), h.chainName, tokenAddress)
				if err != nil {
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
				}
				if len(log_.Data) >= 64 {
					amount = new(big.Int).SetBytes(log_.Data[32:64])
				}
				token.Amount = amount.String()
				token.TokenType = biz.ERC20
				if meta.Value != "0" {
					fromAddress = meta.FromAddress
					/*if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
						token.Symbol = token.Symbol[1:]
					}*/
					tokenAddress = ""
				} else {
					fromAddress = meta.ToAddress
				}
			}
			if strings.HasPrefix(h.chainName, "zkSync") {
				continue
			}

			if len(log_.Topics) >= 2 {
				//兑换时判断 交易金额不能为 0
				//判断 value是否为0 不为 0 则增加记录
				toAddress = common.HexToAddress(log_.Topics[1].String()).String()
				if meta.Value != "0" {
					fromAddress = meta.FromAddress
					/*if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
						token.Symbol = token.Symbol[1:]
					}*/
					tokenAddress = ""
				} else {
					fromAddress = meta.ToAddress
				}
			}

			if strings.HasPrefix(h.chainName, "BSC") {
				//https://bscscan.com/tx/0x7e011215ceb9c3318c75a3d0604b9a936141935e801c5e2080659349fe67c1a0
				if len(eventLogs) > 0 {
					haveTransfer := false
					for _, eventLog := range eventLogs {
						if eventLog != nil && eventLog.From == toAddress && eventLog.To == fromAddress && eventLog.Amount.Cmp(amount) == 0 {
							haveTransfer = true
							break
						}
					}
					if haveTransfer {
						continue
					}
				}
			}
		} else if topic0 == BRIDGE_TRANSFERNATIVE {
			//https://optimistic.etherscan.io/tx/0xc94501aeaf350dc5a5e4ecddc5f2d5dba090255a7057d60f16d9f115655f46cf
			fromAddress = common.HexToAddress(log_.Topics[1].String()).String()
			amount = new(big.Int).SetBytes(log_.Data)
			toAddress = common.HexToAddress(log_.Topics[2].String()).String()
			tokenAddress = ""
		} else if topic0 == FANTOM_SWAPED {
			fromAddress = common.HexToAddress(receipt.To).String()
			if len(log_.Data) > 32 {
				toAddress = common.HexToAddress(hex.EncodeToString(log_.Data[:32])).String()
			}
			if len(log_.Data) > 128 {
				amount = new(big.Int).SetBytes(log_.Data[96:128])
			}
			tokenAddress = ""
		} else if topic0 == FANTOM_SWAPED_V1 {
			if h.chainName == "ArbitrumNova" && log_.Topics[1].String() == log_.Topics[2].String() {
				fromAddress = common.HexToAddress(receipt.To).String()
				toAddress = common.HexToAddress(receipt.From).String()
				if len(log_.Data) >= 128 {
					amount = new(big.Int).SetBytes(log_.Data[96:128])
					if amount.String() == "0" {
						amount = new(big.Int).SetBytes(log_.Data[64:96])
					}
				}
			} else if h.chainName == "Fantom" {
				fromAddress = common.HexToAddress(receipt.To).String()
				toAddress = common.HexToAddress(receipt.From).String()
				if len(log_.Data) > 96 {
					amount = new(big.Int).SetBytes(log_.Data[64:96])
				}
			}
			tokenAddress = ""
		} else if topic0 == ARBITRUM_TRANSFERNATIVE {
			//https://arbiscan.io/tx/0xc03bc0de5428c81bddb027358154fc2355225bf8492125bc95cf6699cef87c3f 主币
			//https://arbiscan.io/tx/0xa459004e8f9ea67cb1174d3f8d0e2b42450bae69b6feb9644b1c654eac66e598 代币
			fromAddress = common.HexToAddress(receipt.To).String()
			if len(log_.Data) > 96 {
				toAddress = common.HexToAddress(hex.EncodeToString(log_.Data[64:96])).String()
			}
			if len(log_.Data) > 160 {
				amount = new(big.Int).SetBytes(log_.Data[128:160])
			}
			tokenAddress = common.HexToAddress(hex.EncodeToString(log_.Data[96:128])).String()

			if len(eventLogs) > 0 {
				nativeFlag := false
				for _, eventLog := range eventLogs {
					if eventLog != nil && eventLog.From == fromAddress && eventLog.To == "0x0000000000000000000000000000000000000000" && eventLog.Token.Address == tokenAddress && eventLog.Amount.Cmp(amount) == 0 {
						nativeFlag = true
						break
					}
				}
				if !nativeFlag {
					continue
				}
			}
			/*if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
				token.Symbol = token.Symbol[1:]
			}*/
			tokenAddress = ""
		} else if topic0 == OPTIMISM_WITHDRAWETH {
			//无 转出地址
			fromAddress = common.HexToAddress(receipt.To).String()
			amount = new(big.Int).SetBytes(log_.Data)
			toAddress = common.BytesToAddress(transaction.Data()[4:36]).String()
			tokenAddress = ""
		} else if topic0 == OPTIMISM_FANTOM_LOGANYSWAPIN {
			fromAddress = tokenAddress
			if len(log_.Data) >= 32 {
				amount = new(big.Int).SetBytes(log_.Data[:32])
			}
			toAddress = common.HexToAddress(log_.Topics[3].String()).String()
			tokenAddress = common.HexToAddress(log_.Topics[2].String()).String()

			if h.chainName == "Optimism" {
				token, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, tokenAddress)
				if err != nil {
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
				}
				sy := strings.ToUpper(token.Symbol)
				if strings.Contains(sy, "ETH") {
					tokenAddress = ""
				}
			} else {
				tokenAddress = ""
			}
		} else if topic0 == OPTIMISM_NONE {
			fromAddress = tokenAddress
			if len(log_.Data) >= 64 {
				amount = new(big.Int).SetBytes(log_.Data[32:64])
			}
			toAddress = common.HexToAddress(log_.Topics[1].String()).String()
			tokenAddress = ""
		} else if topic0 == ARBITRUM_INTXAMOUNT {
			//获取amount
			if len(log_.Data) >= 64 {
				arbitrumAmount = new(big.Int).SetBytes(log_.Data[32:64])
			}
			continue
		} else if topic0 == ARBITRUM_UNLOCKEVENT {
			fromAddress = tokenAddress
			toAddress = common.BytesToAddress(log_.Data[32:64]).String()
			amount = arbitrumAmount
			tokenAddress = ""
		} else if topic0 == KLAYTN_EXCHANGEPOS && tokenAddress == "0xC6a2Ad8cC6e4A7E08FC37cC5954be07d499E7654" {
			fromAddress = tokenAddress
			toAddress = common.HexToAddress(receipt.From).String()
			if len(log_.Data) >= 128 {
				amount = new(big.Int).SetBytes(log_.Data[96:128])
			}
			tokenAddress = ""
		} else if topic0 == FANTOM_NEWLIQUIDITYORDER {
			fromAddress = tokenAddress
			toAddress = common.HexToAddress(log_.Topics[1].String()).String()
			if len(log_.Data) >= 64 {
				amount = new(big.Int).SetBytes(log_.Data[32:64])
			}
			tokenAddress = ""
		} else if topic0 == ARBITRUM_GMX_SWAP {
			gmxSwapFlag = true
			if len(log_.Data) >= 224 {
				gmxFromAddress = common.BytesToAddress(log_.Data[:32]).String()
				xx := log_.Data[160:192]
				gmxAmount = new(big.Int).SetBytes(xx)
				continue
			}
		} else if topic0 == ARBITRUM_GMX_EXECUTEDECREASEPOSITION && gmxSwapFlag {
			fromAddress = gmxFromAddress
			amount = gmxAmount
			if len(log_.Data) >= 192 {
				toAddress = common.BytesToAddress(log_.Data[160:192]).String()
			}
			tokenAddress = ""
		} else if topic0 == ETH_BRIDGECALLTRIGGERED {
			fromAddress = common.HexToAddress(log_.Topics[1].String()).String()
			toAddress = common.HexToAddress(log_.Topics[2].String()).String()
			if len(log_.Data) >= 32 {
				amount = new(big.Int).SetBytes(log_.Data[:32])
			}
			tokenAddress = ""
		} else if topic0 == ARBITRUM_GMX_SWAP_V2 {
			fromAddress = tokenAddress
			if len(log_.Data) >= 160 {
				toAddress = common.BytesToAddress(log_.Data[:32]).String()
				amount = new(big.Int).SetBytes(log_.Data[128:160])
				tokenAddress = common.BytesToAddress(log_.Data[64:96]).String()
				token, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, tokenAddress)
				if err != nil {
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
				}
				/*if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
					token.Symbol = token.Symbol[1:]
				}*/
				token.Amount = amount.String()
				tokenAddress = ""
			}
		} else if topic0 == MATIC_BRIDGE {
			fromAddress = tokenAddress
			if len(log_.Topics) >= 4 {
				toAddress = common.HexToAddress(log_.Topics[3].String()).String()
			}
			if len(log_.Data) >= 160 {
				amount = new(big.Int).SetBytes(log_.Data[:32])
			}
			token, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, tokenAddress)
			if err != nil {
				log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
			}
			/*if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
				token.Symbol = token.Symbol[1:]
			}*/
			token.Amount = amount.String()
			tokenAddress = ""
		}

		if xDaiDapp {
			break
		}
		//https://blockscout.com/xdai/mainnet/tx/0xb8a9f18ec9cfa01eb1822724983629e28d5b09010a32efeb1563de49f935d007 无法通过  log获取
		if transaction.To().String() == "0x0460352b91D7CF42B0E1C1c30f06B602D9ef2238" && hex.EncodeToString(transaction.Data()[:4]) == "3d12a85a" {
			fromAddress = transaction.To().String()
			toAddress = common.HexToAddress(hex.EncodeToString(transaction.Data()[4:36])).String()
			amountTotal := new(big.Int).SetBytes(transaction.Data()[36:68])
			bonderFeeAmount := new(big.Int).SetBytes(transaction.Data()[100:132])

			amount = new(big.Int).Sub(amountTotal, bonderFeeAmount)

			tokenAddress = "0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d"
			token, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, tokenAddress)
			if err != nil {
				log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
			}
			/*if strings.HasPrefix(token.Symbol, "WX") || strings.HasPrefix(token.Symbol, "wx") {
				token.Symbol = token.Symbol[2:]
			}*/
			token.Amount = amount.String()
			xDaiDapp = true
			tokenAddress = ""
		}

		//https://optimistic.etherscan.io/tx/0x637856c0d87d452bf68376fdc91ffc53cb44cdad30c61030d2c7a438e58a8587
		if transaction.To().String() == "0x83f6244Bd87662118d96D9a6D44f09dffF14b30E" && hex.EncodeToString(transaction.Data()[:4]) == "3d12a85a" {
			fromAddress = transaction.To().String()
			toAddress = common.HexToAddress(hex.EncodeToString(transaction.Data()[4:36])).String()
			amountTotal := new(big.Int).SetBytes(transaction.Data()[36:68])
			bonderFeeAmount := new(big.Int).SetBytes(transaction.Data()[100:132])
			amount = new(big.Int).Sub(amountTotal, bonderFeeAmount)
			xDaiDapp = true
			tokenAddress = ""
		}

		if strings.HasPrefix(h.chainName, "zkSync") && (fromAddress == ZKSYNC_ADDRESS || toAddress == ZKSYNC_ADDRESS) {
			continue
		}

		if fromAddress != "" {
			fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
			if err != nil {
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("txHash", transactionHash), zap.Any("error", err))
				continue
			}
		}

		if toAddress != "" {
			toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, toAddress)
			if err != nil {
				log.Error(h.chainName+"扫块，从redis中获取用户地址失败", zap.Any("txHash", transactionHash), zap.Any("error", err))
				continue
			}
		}
		if !fromAddressExist && !toAddressExist {
			continue
		}

		if tokenId != "" {
			eventLogTokenId = tokenId
		}

		if topic0 == APPROVALFORALL_TOPIC {
			continue
		}

		//不展示event log中的授权记录
		//https://polygonscan.com/tx/0xdd8635bfce70c989487eea4403826e691efbf230887e92cc958d53e79281b7b9#eventlog
		if topic0 == APPROVAL_TOPIC {
			if "0xf0511f123164602042ab2bCF02111fA5D3Fe97CD" == receipt.To && strings.HasPrefix(h.chainName, "Polygon") {
				//更新 敞口
				data.DappApproveRecordRepoClient.UpdateAddressBalanceByTokenAndContract(nil, fromAddress, tokenAddress, toAddress, amount.String(), h.chainName)
				continue
			} else {
				continue
			}
		}

		if len(tokens) > 0 {
			for _, tokenInfo := range tokens {
				tokenType := tokenInfo.TokenType
				tokenId := tokenInfo.TokenId
				tokenAmount := tokenInfo.Amount
				token, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, tokenAddress, tokenId)
				if err != nil {
					log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
				}
				token.TokenType = tokenType
				token.Address = tokenAddress
				token.TokenId = tokenId
				token.Amount = tokenAmount

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

				eventLogs = append(eventLogs, eventLogInfo)
			}
			continue
		}

		if token.TokenType == biz.ERC721 || token.TokenType == biz.ERC1155 {
			tokenType := token.TokenType
			tokenAmount := token.Amount
			token, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, tokenAddress, tokenId)
			if err != nil {
				log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
			}
			token.TokenType = tokenType
			token.Amount = tokenAmount
		}

		if tokenAddress != "" {
			token.Address = tokenAddress
		} else {
			token = types.TokenInfo{}
		}
		if tokenId != "" {
			token.TokenId = tokenId
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
	}

	for _, eventLog := range eventLogs {
		if eventLog != nil {
			eventLogList = append(eventLogList, *eventLog)
		}
	}
	return
}

func (h *txDecoder) OnSealedTx(c chain.Clienter, txByHash *chain.Transaction) error {
	client := c.(*Client)
	raw := txByHash.Raw.([]interface{})
	rawTransaction := raw[0].(*Transaction)
	rawReceipt := raw[1].(*Receipt)

	meta, err := pCommon.AttemptMatchUser(h.chainName, txByHash)
	if err != nil {
		return err
	}

	// Ignore this transaction.
	//if !(meta.User.MatchFrom || meta.User.MatchTo) {
	//	log.Warn(
	//		"PENDING TX COULD NOT MATCH USER",
	//		meta.WrapFields(
	//			zap.String("chainName", h.chainName),
	//			zap.Uint64("height", txByHash.BlockNumber),
	//			zap.String("nodeUrl", client.URL()),
	//			zap.String("txHash", txByHash.Hash),
	//		)...,
	//	)
	//	return nil
	//}

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

	job := &txHandleJob{
		block:       block,
		tx:          txByHash,
		transaction: rawTransaction,
		meta:        meta,
		receipt:     rawReceipt,
	}
	h.block = block // to let below invocation work.
	if err := h.handleEachTransaction(c.(*Client), job); err != nil {
		return err
	}
	return nil
}

func (h *txDecoder) OnDroppedTx(c chain.Clienter, tx *chain.Transaction) error {
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
	result, err := data.EvmTransactionRecordRepoClient.FindLastNonce(nil, biz.GetTableName(h.chainName), record.FromAddress)
	if err != nil {
		return nil
	}
	if result != nil {
		if record.TransactionHash == result.TransactionHash {
			return nil
		}
		nonce := uint64(result.Nonce)
		if uint64(record.Nonce) <= nonce {
			record.Status = biz.DROPPED_REPLACED
			record.UpdatedAt = h.now
			h.txRecords = append(h.txRecords, record)
			log.Info(
				"更新 PENDING txhash对象为丢弃置换状态",
				zap.Any("txId", record.TransactionHash),
				zap.Int64("recordNonce", record.Nonce),
				zap.Uint64("chainNonce", nonce),
			)
			return nil
		}
	}

	result1, err := ExecuteRetry(h.chainName, func(client Client) (interface{}, error) {
		cli,_ := getETHClient(client.url)
		defer cli.Close()
		return cli.NonceAt(ctx, common.HexToAddress(record.FromAddress), nil)
	})
	if err != nil {
		return nil
	}
	nonce := result1.(uint64)
	if uint64(record.Nonce) < nonce {
		record.Status = biz.DROPPED_REPLACED
		record.UpdatedAt = h.now
		h.txRecords = append(h.txRecords, record)
		log.Info(
			"更新 PENDING txhash对象为丢弃置换状态",
			zap.Any("txId", record.TransactionHash),
			zap.Int64("recordNonce", record.Nonce),
			zap.Uint64("chainNonce", nonce),
		)
		return nil
	}

	nowTime := time.Now().Unix()
	if record.CreatedAt+21600 >= nowTime {
		if record.Status == biz.PENDING {
			record.Status = biz.NO_STATUS
			record.UpdatedAt = h.now
			h.txRecords = append(h.txRecords, record)
			log.Info(
				"更新 PENDING txhash对象无状态",
				zap.Any("txId", record.TransactionHash),
				zap.Int64("recordNonce", record.Nonce),
				zap.Uint64("chainNonce", nonce),
				zap.Int64("nowTime", nowTime),
				zap.Int64("createTime", record.CreatedAt),
			)
		}
	} else {
		//更新抛弃状态
		record.Status = biz.DROPPED
		record.UpdatedAt = h.now
		h.txRecords = append(h.txRecords, record)
		log.Info(
			"更新 PENDING txhash对象为终态:交易被抛弃",
			zap.Any("txId", record.TransactionHash),
			zap.Int64("recordNonce", record.Nonce),
			zap.Uint64("chainNonce", nonce),
			zap.Int64("nowTime", nowTime),
			zap.Int64("createTime", record.CreatedAt),
		)
	}
	return nil
}
