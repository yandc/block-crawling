package ethereum

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
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
	client := c.(*Client)
	transaction := tx.Raw.(*Transaction)
	txHash := transaction.Hash().String()
	maxFeePerGasNode := ""
	maxPriorityFeePerGasNode := ""
	gpi := transaction.GasPrice()
	gasPriceNode := gpi.String()
	//小费 + basefee

	if transaction.GasFeeCap() != nil {
		maxFeePerGasNode = transaction.GasFeeCap().String()
	}
	if transaction.GasTipCap() != nil {
		maxPriorityFeePerGasNode = transaction.GasTipCap().String()
	}

	if transaction.Type() == types2.DynamicFeeTxType {
		bf, _ := strconv.Atoi(block.BaseFee)
		mpfpg, _ := strconv.Atoi(maxPriorityFeePerGasNode)
		maxFee, _ := strconv.Atoi(maxFeePerGasNode)
		totalFee := bf + mpfpg
		if totalFee >= maxFee {
			gasPriceNode = strconv.Itoa(maxFee)
		} else {
			gasPriceNode = strconv.Itoa(totalFee)
		}
	}

	if h.chainName == "ETH" || h.chainName == "Polygon" || h.chainName == "ScrollL2TEST" || h.chainName == "BSC" || h.chainName == "Optimism" {
		go biz.ChainFeeSwitchRetryAlert(h.chainName, maxFeePerGasNode, maxPriorityFeePerGasNode, gasPriceNode, block.Number, txHash)
	}
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

	err = h.handleEachTransaction(client, block, tx, receipt, meta)
	return err
}

func (h *txDecoder) handleEachTransaction(client *Client, block *chain.Block, tx *chain.Transaction, receipt *Receipt, meta *pCommon.TxMeta) error {
	transaction := tx.Raw.(*Transaction)

	if receipt.ContractAddress != "" && receipt.To == "" {
		meta.TransactionType = biz.CREATECONTRACT
	}

	var feeAmount decimal.Decimal
	amount := meta.Value
	var tokenInfo types.TokenInfo
	var eventLogs []*types.EventLogUid
	var contractAddress, tokenId string
	status := biz.PENDING
	if receipt.Status == "0x0" || receipt.Status == "0x00" {
		status = biz.FAIL
	} else if receipt.Status == "0x1" || receipt.Status == "0x01" {
		status = biz.SUCCESS
	}
	transactionHash := transaction.Hash().String()
	hexData := hex.EncodeToString(transaction.Data())

	if meta.TransactionType != biz.NATIVE && meta.TransactionType != biz.CREATECONTRACT {
		eventLogs, tokenId = h.extractEventLogs(client, meta, receipt, transaction)
	}

	if transaction.To() != nil {
		toAddress := transaction.To().String()
		cli, _ := getETHClient(client.url)
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
				log.Info(h.chainName+"扫块，将native交易类型修正为contract", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", contractAddress), zap.Any("tokenId", tokenId))
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
					amount = "1"
					tokenInfo.Amount = amount
					tokenInfo.Address = contractAddress
					if tokenInfo.TokenType == "" {
						tokenInfo.TokenType = biz.ERC721
					}
					if tokenInfo.TokenId == "" {
						tokenInfo.TokenId = tokenId
					}
				} else {
					if meta.TransactionType == biz.TRANSFERFROM {
						meta.TransactionType = biz.TRANSFER
					}
					//Polygon链的主币地址为空或0x0000000000000000000000000000000000001010
					//zkSync链的主币地址为空或0x000000000000000000000000000000000000800A
					if meta.TransactionType == biz.TRANSFER &&
						((strings.HasPrefix(h.chainName, "Polygon") && (contractAddress == POLYGON_CODE || len(eventLogs) == 0)) ||
							(strings.HasPrefix(h.chainName, "zkSync") && (contractAddress == ZKSYNC_CODE || len(eventLogs) == 0))) {
						//https://polygonscan.com/tx/0x2eae53e26d24435213c25910f7a2498b08bcd002a33ec7f02c31d8b2dae72052
						meta.TransactionType = biz.NATIVE
						contractAddress = ""
					} else {
						tokenInfo, err = biz.GetTokenInfoRetryAlert(ctx, h.chainName, contractAddress)
						if err != nil {
							log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
						}
						tokenInfo.Amount = amount
						tokenInfo.Address = contractAddress
					}
				}
			} else if meta.TransactionType == biz.SETAPPROVALFORALL {
				meta.TransactionType = biz.APPROVENFT
				tokenInfo, err = biz.GetCollectionInfoDirectlyRetryAlert(ctx, h.chainName, contractAddress)
				if err != nil {
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
				}
				tokenInfo.Amount = amount
				tokenInfo.Address = contractAddress
			} else if meta.TransactionType == biz.SAFETRANSFERFROM {
				meta.TransactionType = biz.TRANSFERNFT
				if !strings.Contains(meta.Value, ":") {
					tokenType = biz.ERC721
					tokenId = meta.Value
					amount = "1"
				} else {
					tokenType = biz.ERC1155
					values := strings.Split(meta.Value, ":")
					tokenId = values[0]
					amount = values[1]
				}

				tokenInfo, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, contractAddress, tokenId)
				if err != nil {
					log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", contractAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
				}
				tokenInfo.Amount = amount
				tokenInfo.Address = contractAddress
				if tokenInfo.TokenType == "" {
					tokenInfo.TokenType = tokenType
				}
				if tokenInfo.TokenId == "" {
					tokenInfo.TokenId = tokenId
				}
			} else if meta.TransactionType == biz.SAFEBATCHTRANSFERFROM {
				meta.TransactionType = biz.TRANSFERNFT
				valueList := strings.Split(meta.Value, ",")
				if len(valueList) <= 1 {
					if len(valueList) == 0 {
						amount = "0"
						tokenId = ""
					} else {
						values := strings.Split(valueList[0], ":")
						tokenId = values[0]
						amount = values[1]
						tokenInfo, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, contractAddress, tokenId)
						if err != nil {
							log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", contractAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
						}
					}
					tokenInfo.Amount = amount
					tokenInfo.Address = contractAddress
					if tokenInfo.TokenType == "" {
						tokenInfo.TokenType = biz.ERC1155
					}
					if tokenInfo.TokenId == "" {
						tokenInfo.TokenId = tokenId
					}
				} else {
					meta.TransactionType = biz.CONTRACT
					if len(eventLogs) == 0 {
						for _, valueArr := range valueList {
							values := strings.Split(valueArr, ":")
							tokenId = values[0]
							amount = values[1]
							tokenInfo, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, contractAddress, tokenId)
							if err != nil {
								log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", contractAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
							}
							tokenInfo.Amount = amount
							tokenInfo.Address = contractAddress
							if tokenInfo.TokenType == "" {
								tokenInfo.TokenType = biz.ERC1155
							}
							if tokenInfo.TokenId == "" {
								tokenInfo.TokenId = tokenId
							}

							amountInt, _ := new(big.Int).SetString(amount, 0)
							eventLogInfo := &types.EventLogUid{
								EventLog: types.EventLog{
									From:   meta.FromAddress,
									To:     meta.ToAddress,
									Amount: amountInt,
									Token:  tokenInfo,
								},
								FromUid: meta.User.FromUid,
								ToUid:   meta.User.ToUid,
							}

							eventLogs = append(eventLogs, eventLogInfo)
						}
					}
					amount = "0"
				}
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
					if meta.TransactionType != biz.CONTRACT {
						amount = "0"
					}
					meta.TransactionType = biz.CONTRACT
					tokenInfo.Address = ""
					tokenInfo.TokenId = ""
					tokenInfo.Symbol = ""
				}
			}
		}
	}

	platformUserCount := len(eventLogs)

	isPlatformUser := false
	if platformUserCount > 0 {
		isPlatformUser = true
	}

	if platformUserCount > 1 && meta.TransactionType != biz.CONTRACT {
		meta.TransactionType = biz.CONTRACT
		amount = "0"
	}
	if meta.TransactionType == biz.CONTRACT {
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
		l1FeeStr := utils.GetHexString(receipt.L1Fee)
		l1Fee, err := utils.HexStringToBigInt(l1FeeStr)
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
		logAddress = biz.GetLogAddressFromEventLogUid(eventLogs)
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
		Data:                 hexData,
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

	var isMint bool
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
			Data:                 hexData,
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

		if (eventLog.From == "" || eventLog.From == "0x0000000000000000000000000000000000000000") && evmTransactionRecord.FromAddress == eventLog.To && (eventLog.Token.TokenType == biz.ERC721 || eventLog.Token.TokenType == biz.ERC1155) {
			isMint = true
		}
	}

	var contractEventLogs []*types.EventLogUid
	if evmTransactionRecord.TransactionType == biz.CONTRACT {
		if evmTransactionRecord.Amount.String() != "" && evmTransactionRecord.Amount.String() != "0" {
			contractEventLogs = biz.HandleEventLogUid(h.chainName, evmTransactionRecord.FromAddress, evmTransactionRecord.ToAddress, evmTransactionRecord.Amount.String(), eventLogs)
		} else {
			contractEventLogs = eventLogs
		}
		eventLogLen := len(contractEventLogs)

		if eventLogLen == 1 {
			if evmTransactionRecord.FromAddress == contractEventLogs[0].To &&
				(contractEventLogs[0].From == "" || contractEventLogs[0].From == "0x0000000000000000000000000000000000000000") &&
				(contractEventLogs[0].Token.TokenType == biz.ERC721 || contractEventLogs[0].Token.TokenType == biz.ERC1155) {
				evmTransactionRecord.TransactionType = biz.MINT
			}
		} else if eventLogLen == 2 {
			if (evmTransactionRecord.FromAddress == contractEventLogs[0].To &&
				(contractEventLogs[0].From == "" || contractEventLogs[0].From == "0x0000000000000000000000000000000000000000") &&
				(contractEventLogs[0].Token.TokenType == biz.ERC721 || contractEventLogs[0].Token.TokenType == biz.ERC1155)) ||
				(evmTransactionRecord.FromAddress == contractEventLogs[1].To &&
					(contractEventLogs[1].From == "" || contractEventLogs[1].From == "0x0000000000000000000000000000000000000000") &&
					(contractEventLogs[1].Token.TokenType == biz.ERC721 || contractEventLogs[1].Token.TokenType == biz.ERC1155)) {
				evmTransactionRecord.TransactionType = biz.MINT
			} else if (evmTransactionRecord.FromAddress == contractEventLogs[0].From && evmTransactionRecord.FromAddress == contractEventLogs[1].To) ||
				(evmTransactionRecord.FromAddress == contractEventLogs[0].To && evmTransactionRecord.FromAddress == contractEventLogs[1].From) {
				evmTransactionRecord.TransactionType = biz.SWAP
			}
		}
	}
	if evmTransactionRecord.TransactionType == biz.CONTRACT && isMint {
		evmTransactionRecord.TransactionType = biz.MINT
	}
	return nil
}

func (h *txDecoder) extractEventLogs(client *Client, meta *pCommon.TxMeta, receipt *Receipt, transaction *Transaction) (eventLogList []*types.EventLogUid, eventLogTokenId string) {
	var eventLogs []*types.EventLogUid
	arbitrumAmount := big.NewInt(0)
	transactionHash := transaction.Hash().String()
	txData := transaction.Data()
	gmxSwapFlag := false
	gmxFromAddress := ""
	gmxAmount := big.NewInt(0)
	xDaiDapp := false
	contractAddress := receipt.To
	var methodId string
	if len(txData) >= 4 {
		methodId = hex.EncodeToString(txData[:4])
	} else {
		log.Warn("transaction data is illegal", zap.String("chainName", h.chainName), zap.String("txHash", transactionHash))
	}
	// token 地址 一样  toaddress 一样 amount 一样 则 不添加transfer  判断 logswap 有咩有 ，有 则判断这三个
	for index, log_ := range receipt.Logs {
		if len(log_.Topics) < 1 {
			continue
		}
		topic0 := log_.Topics[0].String()
		if topic0 != APPROVAL_TOPIC && topic0 != APPROVALFORALL_TOPIC && topic0 != TRANSFER_TOPIC && topic0 != TRANSFERSINGLE_TOPIC &&
			topic0 != TRANSFERBATCH_TOPIC && topic0 != WITHDRAWAL_TOPIC && topic0 != DEPOSIT_TOPIC {
			inWhiteList := false
			if whiteTopics, ok := BridgeWhiteTopicList[h.chainName+"_Topic"]; ok {
				topicKey := h.chainName + "_" + contractAddress + "_" + topic0
				for _, whiteTopic := range whiteTopics {
					if topicKey == whiteTopic {
						inWhiteList = true
						break
					}
				}
			}

			if !inWhiteList {
				if whiteTopics, ok := WhiteListTopicMap[h.chainName+"_Contract_Method_Topic"]; ok {
					topicKey := contractAddress + "_" + methodId + "_" + topic0
					for _, whiteTopic := range whiteTopics {
						if topicKey == whiteTopic {
							inWhiteList = true
							break
						}
					}
				}
			}
			if !inWhiteList {
				if whiteTopics, ok := WhiteListTopicMap["Contract_Method_Topic"]; ok {
					topicKey := contractAddress + "_" + methodId + "_" + topic0
					for _, whiteTopic := range whiteTopics {
						if topicKey == whiteTopic {
							inWhiteList = true
							break
						}
					}
				}
			}
			if !inWhiteList {
				if whiteTopics, ok := WhiteListTopicMap["Method_Topic"]; ok {
					topicKey := methodId + "_" + topic0
					for _, whiteTopic := range whiteTopics {
						if topicKey == whiteTopic {
							inWhiteList = true
							break
						}
					}
				}
			}
			if !inWhiteList {
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
		if len(log_.Topics) >= 3 {
			toAddress = common.HexToAddress(log_.Topics[2].String()).String()
		}

		if topic0 == APPROVAL_TOPIC {
			if len(log_.Topics) < 3 {
				log.Warn(
					"EXPECT AT LEAST THREE TOPICS",
					zap.Any("topics", log_.Topics),
					zap.String("chainName", h.chainName),
					zap.String("txHash", transactionHash),
				)
				continue
			}

			//toAddress = common.HexToAddress(log_.Topics[2].String()).String()
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
			//toAddress = common.HexToAddress(log_.Topics[2].String()).String()
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
						fromAddress = common.HexToAddress(hex.EncodeToString(log_.Data[:32])).String()
						toAddress = common.HexToAddress(hex.EncodeToString(log_.Data[32:64])).String()
						tokenId = new(big.Int).SetBytes(log_.Data[64:96]).String()
					}
				} else {
					token, err = biz.GetTokenInfoRetryAlert(ctx, h.chainName, tokenAddress)
					if err != nil {
						log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
					}
					if len(log_.Data) >= 32 {
						amount = new(big.Int).SetBytes(log_.Data[:32])
					}
					if amount.String() == "0" {
						continue
					}
					//https://ftmscan.com/tx/0x560fd26e7c66098468a533c8905b28abd3c7214692f454b1f2e29082afad681d
					if toAddress == "0x0000000000000000000000000000000000000000" && "0xb7fdda5330daea72514db2b84211afebd19277ca" == contractAddress && strings.HasPrefix(h.chainName, "Fantom") {
						log.Info("9999999", zap.Any(contractAddress, "0xB7FDda5330DaEA72514Db2b84211afEBD19277Ca" == contractAddress), zap.Any(toAddress, toAddress == "0x0000000000000000000000000000000000000000"), zap.Any("", strings.HasPrefix(h.chainName, "Fantom")))
						toAddress = common.HexToAddress(receipt.From).String()
						log.Info("777777", zap.Any("li", toAddress))
						//token.Address = ""
					}
				}
				token.Amount = amount.String()
			}

			//代币换主币function销毁主币再发送主币
			if toAddress == "0x0000000000000000000000000000000000000000" && (common.HexToAddress(log_.Topics[1].String()).String() == "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45" ||
				//https://arbiscan.io/tx/0x63c5cdddecd584f25eae98be154fa588380f2ebe3a42d0f6f704c080c00b31c0
				(contractAddress == "0xe05dd51e4eb5636f4f0e8e7fbe82ea31a2ecef16" && methodId == "a8676443") ||
				//https://nova.arbiscan.io/tx/0x9db5e750af7dd1cfcd9b74f2ae72cb8fec180ae3b660dbde5a9a6ffb3c57e2e3
				(contractAddress == "0x67844f0f0dd3d770ff29b0ace50e35a853e4655e" && methodId == "a6cbf417")) {
				//fromAddress = common.HexToAddress(log_.Topics[1].String()).String()
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
			if strings.HasPrefix(h.chainName, "ETH") && contractAddress == "0x7a250d5630b4cf539739df2c5dacb4c659f2488d" && methodId == "791ac947" && len(receipt.Logs) != index+1 {
				//https://etherscan.io/tx/0x8cbd92071ccf7faeea8258e4fb350dec5554532fa85edcc96e4fdbe01aafb3f6
				continue
			}
			if strings.HasPrefix(h.chainName, "BSC") && contractAddress == "0x3a23f943181408eac424116af7b7790c94cb97a5" && methodId == "00000003" {
				//https://bscscan.com/tx/0xbf03de9d2c1c5869e854510d3d88e2b86df33b12a3f83eeac831e02190d11a23
				continue
			}
			if strings.HasPrefix(h.chainName, "Polygon") && contractAddress == "0x3a23f943181408eac424116af7b7790c94cb97a5" && methodId == "00000007" {
				//https://polygonscan.com/tx/0xf05bb66381f26d8b1003a5672923a614ece772ae875de90ae95a557b48be4bd4
				continue
			}

			//https://etherscan.io/tx/0xe510a2d99d95a6974e5f95a3a745b2ffe873bf6645b764658d978856ac180cd2
			//https://app.roninchain.com/tx/0x408b4fe71ec6ce7987721188879e80b437e84e9a38dd16049b8aba7df2358793
			//https://polygonscan.com/tx/0xcfe4c0f8208ef8ff7c12ffa99bf9dfe3236f867176c6730a04cdaf11bd640fd8
			//提现，判断 用户无需话费value 判断value是否为0
			if meta.Value == "0" {
				fromAddress = meta.ToAddress
				toAddress = meta.FromAddress
				//https://polygonscan.com/tx/0x42b89465a8c6a10321dc9b0eb7f483d912d1a0397631be1c76e8846bba359f26
				if !(strings.HasPrefix(h.chainName, "Polygon") && contractAddress == "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270" && methodId == "2e1a7d4d") {
					tokenAddress = ""
				}
			} else {
				toAddress = meta.ToAddress
				log.Warn(h.chainName+"扫块，Withdrawal事件value不为0", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash))
			}

			if len(log_.Topics) == 1 {
				if len(log_.Data) >= 64 {
					amount = new(big.Int).SetBytes(log_.Data[32:64])
				}
			} else if len(log_.Topics) >= 2 {
				if len(log_.Data) >= 32 {
					amount = new(big.Int).SetBytes(log_.Data[:32])
				}
			}

			if strings.HasPrefix(h.chainName, "Polygon") {
				if contractAddress == "0xc1dcb196ba862b337aa23eda1cb9503c0801b955" && methodId == "439dff06" {
					//https://polygonscan.com/tx/0xbf82a6ee9eb2cdd4e63822f247912024760693c60cc521c8118539faef745d18
					fromAddress = common.HexToAddress(log_.Topics[1].String()).String()
					if len(txData) >= 100 {
						toAddress = common.HexToAddress(hex.EncodeToString(txData[68:100])).String()
					}
				}
			}

			if tokenAddress != "" {
				token, err = biz.GetTokenInfoRetryAlert(context.Background(), h.chainName, tokenAddress)
				if err != nil {
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
				}
				token.Amount = amount.String()
			}
		} else if topic0 == DEPOSIT_TOPIC {
			if strings.HasPrefix(h.chainName, "zkSync") {
				continue
			}
			if strings.HasPrefix(h.chainName, "Polygon") && contractAddress == "0x2967e7bb9daa5711ac332caf874bd47ef99b3820" && methodId == "eedfa7cd" {
				//https://polygonscan.com/tx/0x25c01773d91cccf45c3dfc7d25e6598204ac9dc6355ed7c7dce6046f5513432c
				continue
			}

			//https://etherscan.io/tx/0x763f368cd98ebca2bda591ab610aa5b6dc6049fadae9ce04394fc7a8b7304976
			//https://etherscan.io/tx/0xc71fabfc3f65a180eb30d2f47c88837dda08d9535718ab3e12e1eee3ca287bd8
			//https://explorer.roninchain.com/tx/0x0b93df20612bdd000e23f9e3158325fcec6c0459ea90ce30420a6380e6b706a7
			//https://polygonscan.com/tx/0xf42c21bd7df31b2fc75139c2d20b42b38462b9f0269e198a21b59ec273013eb2
			//判断 value是否为0 不为 0 则增加记录
			if meta.Value != "0" {
				fromAddress = meta.FromAddress
				toAddress = meta.ToAddress
				if strings.HasPrefix(h.chainName, "OEC") && methodId == "d0e30db0" && len(receipt.Logs) == 1 {
					//https://www.oklink.com/cn/oktc/tx/0xc98b6d13535bbad27978b1c09185c32641604d6c580dfc1df894f6449f075c81
					fromAddress = meta.ToAddress
				} else {
					tokenAddress = ""
				}
			} else {
				if len(log_.Topics) == 1 {
					if len(log_.Data) >= 32 {
						fromAddress = common.HexToAddress(hex.EncodeToString(log_.Data[:32])).String()
					}
				} else if len(log_.Topics) == 2 {
					fromAddress = common.HexToAddress(log_.Topics[1].String()).String()
				}
				toAddress = meta.ToAddress
			}

			if len(log_.Topics) == 1 {
				if len(log_.Data) >= 64 {
					amount = new(big.Int).SetBytes(log_.Data[32:64])
				}
			} else if len(log_.Topics) >= 2 {
				if len(log_.Data) >= 32 {
					amount = new(big.Int).SetBytes(log_.Data[:32])
				}
			}

			//https://etherscan.io/tx/0x45c6a74bcbfb54c22e86900a946d7838f620cdccd513a86f27df4d31b09a6ab4
			//https://etherscan.io/tx/0xbf636bee525f33c492be4175617a1054c22ca01f69b2ed567c52967d09d26b3b
			//https://bscscan.com/tx/0x7e011215ceb9c3318c75a3d0604b9a936141935e801c5e2080659349fe67c1a0
			//https://arbiscan.io/tx/0x4e56cc436b8ef723574bb707e261e3d7ec8285dc3227bb74ac202b6904f5479a
			//https://arbiscan.io/tx/0x053c7d07c1409ca8c0905fbb6e7f5570394f8ae56571f27a8420e764c17b03f5
			//https://arbiscan.io/tx/0xe099fa3c525f236a2406f6bbffbdce0f699073bfae78cd510febc088a5a7ef8a
			//https://arbiscan.io/tx/0xd642ab8eb4d67473b958f3bfbfb848d1a1553a62c7f1eb99785a4db963f9afdc
			//https://arbiscan.io/tx/0x9606d01664471f434c7cb3c66cee4feb29ce968a1db4e758aa763b0924a2a79d
			if len(eventLogs) > 0 {
				haveTransfer := false
				for _, eventLog := range eventLogs {
					if eventLog != nil && eventLog.From == fromAddress && (eventLog.To == toAddress || len(receipt.Logs) == 2 || contractAddress == "0x28114a7cfac7d617f4de0325997a1f2726af95ea") && eventLog.Amount.Cmp(amount) == 0 {
						haveTransfer = true
						break
					}
				}
				if haveTransfer {
					continue
				}
			}

			if tokenAddress != "" {
				token, err = biz.GetTokenInfoRetryAlert(context.Background(), h.chainName, tokenAddress)
				if err != nil {
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
				}
				token.Amount = amount.String()
			}
		} else if topic0 == BRIDGE_TRANSFERNATIVE {
			//https://optimistic.etherscan.io/tx/0xc94501aeaf350dc5a5e4ecddc5f2d5dba090255a7057d60f16d9f115655f46cf
			//fromAddress = common.HexToAddress(log_.Topics[1].String()).String()
			//toAddress = common.HexToAddress(log_.Topics[2].String()).String()
			amount = new(big.Int).SetBytes(log_.Data)
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
			//https://optimistic.etherscan.io/tx/0xcbfaeb2d83f0235577343d7f35c0ec305a8f188465fc6a2ad78382ccfae3836d op主币
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
		} else if topic0 == WITHDRAWETH_TOPIC {
			//https://arbiscan.io/tx/0xf65c3b8a2a31754059a90fcf65ed3ff7a672c46abf84d30d80dd7d09c8a9d3bb
			//https://optimistic.etherscan.io/tx/0x1de553537b19e29619da0112c688ce4ecc5e185c2e289d757084148f6d4c6d6c
			//https://ftmscan.com/tx/0xce25179db51f9ee48fbdc518b96d2cf584af655a34b95bc535544c1a653be9a8
			//https://bscscan.com/tx/0x076501069df7ab50acb5244bcefcfe8940d970095a93a5287b75ae8fb3d9269b
			if len(receipt.Logs) > 1 {
				continue
			}
			if methodId != "4782f779" {
				continue
			}
			//无转出地址
			fromAddress = common.HexToAddress(receipt.To).String()
			amount = new(big.Int).SetBytes(log_.Data)
			toAddress = common.BytesToAddress(txData[4:36]).String()
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
		} else if topic0 == TOKENSWAP_TOPIC {
			if contractAddress == "0x3749c4f034022c39ecaffaba182555d4508caccc" {
				//https://arbiscan.io/tx/0xed0b45e9dc70fde48288f21fdcef0d6677e84d7387ac10d5cc5130fcc22f317d
				if methodId != "cc29a306" {
					continue
				}

				fromAddress = transaction.To().String()
				toAddress = common.HexToAddress(hex.EncodeToString(txData[4:36])).String()
				amount = new(big.Int).SetBytes(log_.Data[32:64])
				tokenAddress = ""
			} else {
				//获取amount
				if len(log_.Data) >= 64 {
					arbitrumAmount = new(big.Int).SetBytes(log_.Data[32:64])
				}
				continue
			}
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
		} else if topic0 == ARBITRUM_GMX_EXECUTEDECREASEPOSITION {
			if !gmxSwapFlag {
				//https://arbiscan.io/tx/0x9b3c991d268c5f19d95ec586993cb50e51371ee607910f18612f5c7708235b2c
				continue
			}
			fromAddress = gmxFromAddress
			amount = gmxAmount
			if len(log_.Data) >= 192 {
				toAddress = common.BytesToAddress(log_.Data[160:192]).String()
			}
			tokenAddress = ""
		} else if topic0 == ETH_BRIDGECALLTRIGGERED {
			//fromAddress = common.HexToAddress(log_.Topics[1].String()).String()
			//toAddress = common.HexToAddress(log_.Topics[2].String()).String()
			if len(log_.Data) >= 32 {
				amount = new(big.Int).SetBytes(log_.Data[:32])
			}
			tokenAddress = ""
		} else if topic0 == ARBITRUM_GMX_SWAP_V2 {
			fromAddress = tokenAddress
			if len(log_.Data) >= 160 {
				toAddress = common.BytesToAddress(log_.Data[:32]).String()
				amount = new(big.Int).SetBytes(log_.Data[128:160])
				/*tokenAddress = common.BytesToAddress(log_.Data[64:96]).String()
				token, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, tokenAddress)
				if err != nil {
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
				}
				token.Amount = amount.String()*/
				/*if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
					token.Symbol = token.Symbol[1:]
				}*/
				tokenAddress = ""
			}
		} else if topic0 == MATIC_BRIDGE {
			if strings.HasPrefix(h.chainName, "Polygon") && contractAddress == "0xb7fdda5330daea72514db2b84211afebd19277ca" && methodId == "4630a0d8" {
				//https://polygonscan.com/tx/0xcfe4c0f8208ef8ff7c12ffa99bf9dfe3236f867176c6730a04cdaf11bd640fd8
				//https://polygonscan.com/tx/0xf42c21bd7df31b2fc75139c2d20b42b38462b9f0269e198a21b59ec273013eb2
				continue
			}
			fromAddress = tokenAddress
			if len(log_.Topics) >= 4 {
				toAddress = common.HexToAddress(log_.Topics[3].String()).String()
			}
			if len(log_.Data) >= 160 {
				amount = new(big.Int).SetBytes(log_.Data[:32])
			}
			/*token, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, tokenAddress)
			if err != nil {
				log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
			}
			token.Amount = amount.String()*/
			/*if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
				token.Symbol = token.Symbol[1:]
			}*/
			tokenAddress = ""
		} else if topic0 == WITHDRAWALBONDED_TOPIC {
			//https://etherscan.io/tx/0xf929807379db7a1b2c9827a00e51d512b992b4b85130ec4b0be4f53faf292742
			//https://etherscan.io/tx/0xc231255bec37807b2a124ed86a89b6c74bf5a8cfc8f1b77a7d880ae22de3e3e7
			if len(receipt.Logs) > 1 {
				continue
			}
			if methodId != "23c452cd" {
				continue
			}

			fromAddress = transaction.To().String()
			toAddress = common.HexToAddress(hex.EncodeToString(txData[4:36])).String()
			amountTotal := new(big.Int).SetBytes(txData[36:68])
			bonderFeeAmount := new(big.Int).SetBytes(txData[100:132])
			amount = new(big.Int).Sub(amountTotal, bonderFeeAmount)
			tokenAddress = ""
		} else if topic0 == REDEEM_TOPIC {
			//https://bscscan.com/tx/0xd0b6d155be809d384dffe89e8d50e2284bb7dbfa79a5934beeed8efe7680550c
			if methodId != "db006a75" {
				continue
			}
			if len(log_.Data) != 160 {
				continue
			}

			fromAddress = transaction.To().String()
			toAddress = common.BytesToAddress(log_.Data[:32]).String()
			amount = new(big.Int).SetBytes(log_.Data[32:64])
			tokenAddress = ""
		} else if topic0 == NEWPOSITIONORDER_TOPIC {
			//https://bscscan.com/tx/0x0ed1855e0ec98218eff586c0ea772976ee23a0455aae30853d63472aeb042e68
			if len(receipt.Logs) > 1 {
				continue
			}
			if methodId != "df70baa3" {
				continue
			}
			if len(log_.Data) != 192 {
				continue
			}

			fromAddress = transaction.To().String()
			toAddress = transaction.From.String()
			amount = new(big.Int).SetBytes(log_.Data[32:64])
			tokenAddress = ""
		} else if topic0 == CLAIMED_TOPIC {
			//https://goerli.etherscan.io/tx/0xbdd255a08a6e568983002b6505d57de28f27a7a28696dcadeb869272dad2a4a1
			if len(receipt.Logs) > 2 {
				continue
			}
			if methodId != "5b4363bf" {
				continue
			}
			fromAddress = tokenAddress
			amount = new(big.Int).SetBytes(log_.Data[:32])
			toAddress = common.HexToAddress(log_.Topics[1].String()).String()
			tokenAddress = ""
		} else if topic0 == RUN_METHOD_TOPIC {
			//https://ftmscan.com/tx/0x9bcb66793ae2030eb3686d988134ecce640e6162f6c492d2a39914b7ce44841c
			if methodId != "ba847759" {
				continue
			}
			if len(log_.Data) < 128 {
				continue
			}
			fromAddress = transaction.From.String()
			toAddress = common.BytesToAddress(log_.Data[:32]).String()
			amount = new(big.Int).SetBytes(log_.Data[96:128])
			tokenAddress = ""
		} else if topic0 == SOLDLISTING_TOPIC {
			//https://cronoscan.com/tx/0xe30b613e03e7ee2fb136a5f93f4af25ce3cb2305212ea7d024a0725b168e25a5
			if len(receipt.Logs) > 2 {
				continue
			}
			if methodId != "0d7bb214" {
				continue
			}
			if len(log_.Data) < 128 {
				continue
			}
			fromAddress = transaction.From.String()
			toAddress = common.BytesToAddress(log_.Data[:32]).String()
			amount = new(big.Int).SetBytes(log_.Data[96:128])
			tokenAddress = ""
		} else if topic0 == FILL_TOPIC {
			//https://bscscan.com/tx/0xd882bdcde047e7429653090e2f531e4130c46babba6e4b5952a7c80fe5cd9411
			if len(receipt.Logs) > 2 {
				continue
			}
			if methodId != "e98b3b7e" {
				continue
			}
			if len(log_.Data) < 192 {
				continue
			}
			fromAddress = transaction.From.String()
			toAddress = common.HexToAddress(log_.Topics[1].String()).String()
			amount = new(big.Int).SetBytes(log_.Data[160:192])
			tokenAddress = ""
		} else if topic0 == SEND_TOPIC {
			//https://zkevm.polygonscan.com/tx/0xd2b8469b94f2795cb52e486c440f1215f02b0dd5e5720e37880085a1795e9699
			if len(log_.Data) < 64 {
				continue
			}
			fromAddress = transaction.From.String()
			toAddress = common.BytesToAddress(log_.Data[:32]).String()
			amount = new(big.Int).SetBytes(log_.Data[32:64])
			tokenAddress = ""
		} else if topic0 == BASE_TOPIC {
			//https://base.blockscout.com/tx/0x22868b9eba4332dd06a0bbe7c26a44bd9c5b7d60128a8873d66e7ffbc509bf77
			//https://base.blockscout.com/tx/0x318e8b25d1860a7e94a63cf54e577a1a48fb980b35f55f71757358ec7357e052
			if len(receipt.Logs) > 3 {
				continue
			}
			if methodId != "00000000" {
				continue
			}
			fromAddress = transaction.From.String()
			toAddress = common.HexToAddress(log_.Topics[1].String()).String()
			amount, _ = new(big.Int).SetString(meta.Value, 0)
			tokenAddress = ""
		}

		if xDaiDapp {
			break
		}
		//https://blockscout.com/xdai/mainnet/tx/0xb8a9f18ec9cfa01eb1822724983629e28d5b09010a32efeb1563de49f935d007 无法通过log获取
		if contractAddress == "0x0460352b91d7cf42b0e1c1c30f06b602d9ef2238" && methodId == "3d12a85a" {
			fromAddress = transaction.To().String()
			toAddress = common.HexToAddress(hex.EncodeToString(txData[4:36])).String()
			amountTotal := new(big.Int).SetBytes(txData[36:68])
			bonderFeeAmount := new(big.Int).SetBytes(txData[100:132])
			amount = new(big.Int).Sub(amountTotal, bonderFeeAmount)
			/*tokenAddress = "0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d"
			token, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, tokenAddress)
			if err != nil {
				log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
			}
			token.Amount = amount.String()*/
			/*if strings.HasPrefix(token.Symbol, "WX") || strings.HasPrefix(token.Symbol, "wx") {
				token.Symbol = token.Symbol[2:]
			}*/
			xDaiDapp = true
			//tokenAddress = ""
		}

		//https://optimistic.etherscan.io/tx/0x637856c0d87d452bf68376fdc91ffc53cb44cdad30c61030d2c7a438e58a8587
		if contractAddress == "0x83f6244bd87662118d96d9a6d44f09dfff14b30e" && methodId == "3d12a85a" {
			fromAddress = transaction.To().String()
			toAddress = common.HexToAddress(hex.EncodeToString(txData[4:36])).String()
			amountTotal := new(big.Int).SetBytes(txData[36:68])
			bonderFeeAmount := new(big.Int).SetBytes(txData[100:132])
			amount = new(big.Int).Sub(amountTotal, bonderFeeAmount)
			xDaiDapp = true
			tokenAddress = ""
		}

		if strings.HasPrefix(h.chainName, "Polygon") {
			//https://polygonscan.com/tx/0x2eae53e26d24435213c25910f7a2498b08bcd002a33ec7f02c31d8b2dae72052
			if tokenAddress == POLYGON_CODE {
				tokenAddress = ""
			}
		}
		if strings.HasPrefix(h.chainName, "zkSync") {
			if fromAddress == ZKSYNC_ADDRESS || toAddress == ZKSYNC_ADDRESS {
				continue
			}
			//https://explorer.zksync.io/tx/0x7cd3f94cf26c8d7fa18509dae6a8021916945a0f117a1d17af73171338095024
			if tokenAddress == ZKSYNC_CODE {
				tokenAddress = ""
			}
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

		if topic0 == APPROVAL_TOPIC {
			continue
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
			eventLogList = append(eventLogList, eventLog)
		}
	}
	return
}

func (h *txDecoder) OnSealedTx(c chain.Clienter, txByHash *chain.Transaction) error {
	client := c.(*Client)
	rawReceipt := txByHash.Raw.(*Receipt)
	txHash := txByHash.Hash

	curHeight := txByHash.BlockNumber
	block, err := client.GetBlockTransaction(curHeight, txHash)
	if err != nil {
		return err
	}
	var tx *chain.Transaction
	for _, tx = range block.Transactions {
		if tx.Hash == txHash {
			break
		}
	}

	meta, err := pCommon.AttemptMatchUser(h.chainName, tx)
	if err != nil {
		return err
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

	h.block = block // to let below invocation work.

	err = h.handleEachTransaction(client, block, tx, rawReceipt, meta)
	return err
}

func (h *txDecoder) OnDroppedTx(c chain.Clienter, tx *chain.Transaction) error {
	ctx := context.Background()
	record := tx.Record.(*data.EvmTransactionRecord)
	//a.扫块时候 给扫到 一样nonce的值 成功后  更新 当nonce一样，并且状态不是 biz.DROPPED_REPLACED 更新状态为 biz.DROPPED_REPLACED
	//新成功后的 speed_up

	//兜底时，成功后 执行 a 一样的步骤

	log.Info(
		"pending tx could not found on the chain",
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

	recordNonce := record.Nonce
	var dbTxHash string
	var dbNonce int64
	//判断nonce 是否小于当前数据库中保存的nonce，需要排除掉transfer和eventLog类型的，transfer类型的有可能不是自己发送的，例如：https://etherscan.io/tx/0xfd16650aae9125b98c9dc9f78b2b04deed5279eeedabf72d254af3e9a881ffbf
	result, err := data.EvmTransactionRecordRepoClient.FindLastNonce(nil, biz.GetTableName(h.chainName), record.FromAddress)
	if err == nil && result != nil {
		dbTxHash = result.TransactionHash
		if record.TransactionHash == dbTxHash {
			//updateMap := map[string]interface{}{}
			//updateMap["sign_status"] = "2"
			//updateMap["tx_time"] = record.TxTime
			//data.UserSendRawHistoryRepoInst.UpdateSignStatusByTxHash(nil, record.TransactionHash, updateMap, -1, "")
			return nil
		}
		dbNonce = result.Nonce
		if dbNonce > 0 && recordNonce <= dbNonce {
			record.Status = biz.DROPPED_REPLACED

			var ssr []biz.SignStatusRequest
			ssr = append(ssr, biz.SignStatusRequest{
				TransactionHash: record.TransactionHash,
				Status:          record.Status,
				TransactionType: record.TransactionType,
				Nonce:           record.Nonce,
				TxTime:          record.TxTime,
			})
			go biz.SyncStatus(ssr)

			record.UpdatedAt = h.now
			h.txRecords = append(h.txRecords, record)
			log.Info(
				"更新pending状态交易为dropped_replaced状态",
				zap.String("chainName", h.chainName),
				zap.Any("txHash", record.TransactionHash),
				zap.Any("dbTxHash", dbTxHash),
				zap.Int64("recordNonce", recordNonce),
				zap.Int64("dbNonce", dbNonce),
				zap.Int64("now", h.now),
			)
			return nil
		}
	}

	//判断nonce 是否小于当前链上的nonce
	result1, err := ExecuteRetry(h.chainName, func(client Client) (interface{}, error) {
		cli, _ := getETHClient(client.url)
		defer cli.Close()
		return cli.NonceAt(ctx, common.HexToAddress(record.FromAddress), nil)
	})
	if err != nil {
		return nil
	}
	chainNonce := result1.(uint64)
	if uint64(recordNonce) < chainNonce {
		record.Status = biz.DROPPED_REPLACED
		var ssr []biz.SignStatusRequest
		ssr = append(ssr, biz.SignStatusRequest{
			TransactionHash: record.TransactionHash,
			Status:          record.Status,
			TransactionType: record.TransactionType,
			Nonce:           record.Nonce,
			TxTime:          record.TxTime,
		})
		go biz.SyncStatus(ssr)
		record.UpdatedAt = h.now
		h.txRecords = append(h.txRecords, record)
		log.Info(
			"更新pending状态交易为dropped_replaced状态",
			zap.String("chainName", h.chainName),
			zap.Any("txHash", record.TransactionHash),
			zap.Any("dbTxHash", dbTxHash),
			zap.Int64("recordNonce", recordNonce),
			zap.Int64("dbNonce", dbNonce),
			zap.Uint64("chainNonce", chainNonce),
			zap.Int64("now", h.now),
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
				"更新pending状态交易为no_status状态",
				zap.String("chainName", h.chainName),
				zap.Any("txHash", record.TransactionHash),
				zap.Any("dbTxHash", dbTxHash),
				zap.Int64("recordNonce", record.Nonce),
				zap.Int64("dbNonce", dbNonce),
				zap.Uint64("chainNonce", chainNonce),
				zap.Int64("now", h.now),
				zap.Int64("nowTime", nowTime),
				zap.Int64("createTime", record.CreatedAt),
			)
		}
	} else {
		//更新抛弃状态
		record.Status = biz.DROPPED
		var ssr []biz.SignStatusRequest
		ssr = append(ssr, biz.SignStatusRequest{
			TransactionHash: record.TransactionHash,
			Status:          record.Status,
			TransactionType: record.TransactionType,
			Nonce:           record.Nonce,
			TxTime:          record.TxTime,
		})
		go biz.SyncStatus(ssr)
		record.UpdatedAt = h.now
		h.txRecords = append(h.txRecords, record)
		log.Info(
			"更新pending状态交易为dropped状态",
			zap.String("chainName", h.chainName),
			zap.Any("txHash", record.TransactionHash),
			zap.Any("dbTxHash", dbTxHash),
			zap.Int64("recordNonce", record.Nonce),
			zap.Int64("dbNonce", dbNonce),
			zap.Uint64("chainNonce", chainNonce),
			zap.Int64("now", h.now),
			zap.Int64("nowTime", nowTime),
			zap.Int64("createTime", record.CreatedAt),
		)
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
