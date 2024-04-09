package ethereum

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"gorm.io/datatypes"

	pCommon "block-crawling/internal/platform/common"
	"block-crawling/internal/platform/ethereum/rtypes"
	_ "block-crawling/internal/platform/ethereum/swap" // register swap contracts
	"block-crawling/internal/platform/swap"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	types2 "github.com/ethereum/go-ethereum/core/types"
	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

type txDecoder struct {
	chainName     string
	block         *chain.Block
	txEventLogMap map[string][]*rtypes.Log
	txByHash      *chain.Transaction
	blockHash     string
	now           int64
	newTxs        bool

	blockHashRetrieved bool
	blocksStore        map[uint64]*chain.Block

	txRecords      []*data.EvmTransactionRecord
	txNonceRecords []*data.EvmTransactionRecord

	matchedUser bool

	receipts *sync.Map
}

func (h *txDecoder) OnNewTx(c chain.Clienter, block *chain.Block, tx *chain.Transaction) error {
	var receipt *rtypes.Receipt

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

	var methodId string
	if len(transaction.Data()) >= 4 {
		methodId = hex.EncodeToString(transaction.Data()[:4])
	}

	defer func() {
		if !swap.Is(h.chainName, tx) {
			return
		}
		if receipt == nil {
			receipt, _ = client.GetTransactionReceipt(context.Background(), transaction.Hash())
		}
		if receipt != nil {
			_, err := swap.AttemptToPushSwapPairs(h.chainName, tx.ToAddress, block, tx, receipt)
			if err != nil {
				log.Info("EXTRACT SWAP FAILED", zap.String("chainName", h.chainName), zap.Error(err))
				return
			}
		}
	}()

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

	if h.chainName == "ETH" || h.chainName == "Polygon" || h.chainName == "ScrollL2TEST" || h.chainName == "BSC" || h.chainName == "Optimism" || h.chainName == "ETHGoerliTEST" || h.chainName == "BSCTEST" || h.chainName == "PolygonTEST" {
		go biz.ChainFeeSwitchRetryAlert(h.chainName, maxFeePerGasNode, maxPriorityFeePerGasNode, gasPriceNode, block.Number, txHash)
	}
	meta, err := pCommon.AttemptMatchUser(h.chainName, tx)
	if err != nil {
		return err
	}
	h.matchedUser = meta.User.MatchFrom || meta.User.MatchTo

	// Ignore this transaction.
	if !h.matchedUser {
		if methodId == "" || transaction.To() == nil {
			if !(strings.HasPrefix(h.chainName, "Polygon") && tx.FromAddress == "0x0000000000000000000000000000000000000000" && tx.ToAddress == "0x0000000000000000000000000000000000000000") {
				return nil
			}
		}
		var contractAddress string
		if transaction.To() != nil {
			contractAddress = transaction.To().String()
		}
		inWhiteList := isMethodInWhiteList(h.chainName, contractAddress, methodId)

		if !inWhiteList {
			if hasUserAddress(h.txEventLogMap[txHash]) {
				inWhiteList = true
			}
		}
		if !inWhiteList {
			return nil
		}
		//校验合约地址是否是高风险代币
		if contractAddress != "" {
			tokenInfo, err := biz.GetTokenInfoRetryAlert(context.Background(), h.chainName, contractAddress)
			if err == nil && tokenInfo.Symbol != "" && tokenInfo.Symbol != "Unknown Token" && tokenInfo.Address != "" {
				isFake, FakeErr := biz.GetTokenIsFakeRetryAlert(context.Background(), h.chainName, contractAddress, tokenInfo.Symbol)
				if isFake && FakeErr == nil {
					return nil
				}
			}
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

	receipt, err = client.GetTransactionReceipt(context.Background(), transaction.Hash())
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
	h.receipts.Store(tx.Hash, receipt)

	err = h.handleEachTransaction(client, block, tx, transaction, meta, receipt)
	return err
}

// 交易中是否有平台用户的 address，至少一个时返回 true
func hasUserAddress(logs []*rtypes.Log) bool {
	if len(logs) == 0 {
		return false
	}

	for _, log := range logs {
		if len(log.Topics) == 0 {
			continue
		}
		if log.Topics[0].String() == TRANSFER_TOPIC {
			if len(log.Topics) < 3 {
				continue
			}
			fromAddress := common.HexToAddress(log.Topics[1].Hex()).Hex()
			if isUser(fromAddress) {
				return true
			}

			toAddress := common.HexToAddress(log.Topics[2].Hex()).Hex()
			if isUser(toAddress) {
				return true
			}
		} else if log.Topics[0].String() == APPROVAL_TOPIC {
			if len(log.Topics) < 3 {
				continue
			}
			fromAddress := common.HexToAddress(log.Topics[1].Hex()).Hex()
			if isUser(fromAddress) {
				return true
			}

			toAddress := common.HexToAddress(log.Topics[2].Hex()).Hex()
			if isUser(toAddress) {
				return true
			}

		} else if log.Topics[0].String() == TRANSFERSINGLE_TOPIC {
			if len(log.Topics) < 4 {
				continue
			}
			fromAddress := common.HexToAddress(log.Topics[2].Hex()).Hex()
			if isUser(fromAddress) {
				return true
			}

			toAddress := common.HexToAddress(log.Topics[3].Hex()).Hex()
			if isUser(toAddress) {
				return true
			}

		} else if log.Topics[0].String() == TRANSFERBATCH_TOPIC {
			if len(log.Topics) < 4 {
				continue
			}
			fromAddress := common.HexToAddress(log.Topics[2].Hex()).Hex()
			if isUser(fromAddress) {
				return true
			}

			toAddress := common.HexToAddress(log.Topics[3].Hex()).Hex()
			if isUser(toAddress) {
				return true
			}
		} else if log.Topics[0].String() == APPROVALFORALL_TOPIC {
			if len(log.Topics) < 3 {
				continue
			}
			fromAddress := common.HexToAddress(log.Topics[1].Hex()).Hex()
			if isUser(fromAddress) {
				return true
			}

			toAddress := common.HexToAddress(log.Topics[2].Hex()).Hex()
			if isUser(toAddress) {
				return true
			}
		}
	}

	return false
}

func isUser(address string) bool {
	info, err := biz.GetUserInfo(address)
	if err != nil {
		return false
	}
	return info.Uid != ""
}

func (h *txDecoder) handleEachTransaction(
	client *Client,
	block *chain.Block,
	tx *chain.Transaction,
	transaction *Transaction,
	meta *pCommon.TxMeta,
	receipt *rtypes.Receipt,
) error {
	if receipt.ContractAddress != "" && receipt.To == "" {
		meta.TransactionType = biz.CREATECONTRACT
	}

	var feeAmount decimal.Decimal
	amount := meta.Value
	var tokenInfo types.TokenInfo
	var eventLogs []*types.EventLogUid
	feeTokenInfo := ""
	tokenGasless := ""
	var feeLog *types.EventLogUid
	var realContractAddress string
	var contractAddress, tokenId string
	status := biz.PENDING
	if receipt.Status == "0x0" || receipt.Status == "0x00" {
		status = biz.FAIL
	} else if receipt.Status == "0x1" || receipt.Status == "0x01" {
		status = biz.SUCCESS
	}
	blockNumber, _ := utils.HexStringToInt64(receipt.BlockNumber)
	transactionHash := transaction.Hash().String()
	hexData := hex.EncodeToString(transaction.Data())

	if meta.TransactionType != biz.NATIVE && meta.TransactionType != biz.CREATECONTRACT {
		eventLogs, tokenId, feeLog, realContractAddress = h.extractEventLogs(client, meta, receipt, transaction)
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
				eventLogs, tokenId, feeLog, realContractAddress = h.extractEventLogs(client, meta, receipt, transaction)
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
						log.Error("扫块，从nodeProxy中获取NFT信息失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", contractAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
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
							log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
						}
						tokenInfo.Amount = amount
						tokenInfo.Address = contractAddress
					}
				}
			} else if meta.TransactionType == biz.SETAPPROVALFORALL {
				meta.TransactionType = biz.APPROVENFT
				tokenInfo, err = biz.GetCollectionInfoDirectlyRetryAlert(ctx, h.chainName, contractAddress)
				if err != nil {
					log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
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
					log.Error("扫块，从nodeProxy中获取NFT信息失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", contractAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
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
							log.Error("扫块，从nodeProxy中获取NFT信息失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", contractAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
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
								log.Error("扫块，从nodeProxy中获取NFT信息失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", contractAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
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
					alarmMsg := fmt.Sprintf("请注意：%s链扫块，解析contract的code失败", h.chainName)
					alarmOpts := biz.WithMsgLevel("FATAL")
					biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
					log.Error("扫块，解析contract的code失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
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

	//手续费代付
	if feeLog != nil {
		feeTokenInfo, _ = utils.JsonEncode(feeLog.Token)
		tokenGasless, _ = utils.JsonEncode(map[string]interface{}{
			"fee_token_info": feeLog.Token,
		})

		if len(eventLogs) == 0 {
			eventLogs = append(eventLogs, feeLog)
		} else {
			feeEventMap := map[string]interface{}{
				"evm": map[string]string{
					"nonce": fmt.Sprintf("%v", transaction.Nonce()),
					"type":  fmt.Sprintf("%v", transaction.Type()),
				},
				"token": feeLog.Token,
			}
			feeEventParseData, _ := utils.JsonEncode(feeEventMap)
			feeEventTokenInfoStr, _ := utils.JsonEncode(feeLog.Token)
			txHash := transactionHash + "#result-" + fmt.Sprintf("%v", -1)
			feeTxRecords := []*data.EvmTransactionRecord{{
				BlockNumber:     int(blockNumber),
				TransactionHash: txHash,
				FromAddress:     feeLog.From,
				ToAddress:       feeLog.To,
				FromUid:         feeLog.FromUid,
				ToUid:           feeLog.ToUid,
				Status:          status,
				ContractAddress: feeLog.Token.Address,
				ParseData:       feeEventParseData,
				TransactionType: biz.EVENTLOG,
				TokenInfo:       feeEventTokenInfoStr,
			}}
			go HandleUserAsset(h.chainName, *(client), feeTxRecords)
		}
	}

	platformUserCount := len(eventLogs)

	isPlatformUser := false
	if platformUserCount > 0 {
		isPlatformUser = true
	}

	if platformUserCount > 1 && meta.TransactionType != biz.CONTRACT {
		//https://polygonscan.com/tx/0xec7d73f2204d09fe978e0d8afb1ffea9937b2d0659ee2c5dc271bf78673a0b18
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
	tokenInfoStr, _ := utils.JsonEncode(tokenInfo)
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
			log.Error("扫块，logAddress长度超过最大限制", zap.Any("chainName", h.chainName), zap.Any("txHash", transactionHash), zap.Any("logAddressLen", logAddressLen))
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
		FeeTokenInfo:         feeTokenInfo,
		TokenGasless:         tokenGasless,
		MaxFeePerGas:         maxFeePerGas,
		MaxPriorityFeePerGas: maxPriorityFeePerGas,
		Data:                 hexData,
		EventLog:             eventLog,
		LogAddress:           logAddress,
		TransactionType:      meta.TransactionType,
		OperateType:          "",
		TokenInfo:            tokenInfoStr,
		CreatedAt:            h.now,
		UpdatedAt:            h.now,
	}

	if meta.User.MatchFrom || meta.User.MatchTo || isPlatformUser {
		h.txRecords = append(h.txRecords, evmTransactionRecord)
		if !h.newTxs {
			h.txNonceRecords = append(h.txNonceRecords, evmTransactionRecord)
		}
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
		eventTokenInfoStr, _ := utils.JsonEncode(eventLog.Token)
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
			FeeTokenInfo:         feeTokenInfo,
			TokenGasless:         tokenGasless,
			MaxFeePerGas:         maxFeePerGas,
			MaxPriorityFeePerGas: maxPriorityFeePerGas,
			Data:                 hexData,
			TransactionType:      txType,
			OperateType:          "",
			TokenInfo:            eventTokenInfoStr,
			CreatedAt:            h.now,
			UpdatedAt:            h.now,
		}
		if isPlatformUser && meta.TransactionType == biz.CONTRACT {
			h.txRecords = append(h.txRecords, evmlogTransactionRecord)
			if !h.newTxs {
				h.txNonceRecords = append(h.txNonceRecords, evmTransactionRecord)
			}
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
				//https://etherscan.io/tx/0xbbb8644c8f1feb5ea22433889a07b37aac4e8efee1d406dacea7742c0422ade2
				evmTransactionRecord.TransactionType = biz.MINT
			}
		} else if eventLogLen == 2 {
			if (evmTransactionRecord.FromAddress == contractEventLogs[0].To &&
				(contractEventLogs[0].From == "" || contractEventLogs[0].From == "0x0000000000000000000000000000000000000000") &&
				(contractEventLogs[0].Token.TokenType == biz.ERC721 || contractEventLogs[0].Token.TokenType == biz.ERC1155)) ||
				(evmTransactionRecord.FromAddress == contractEventLogs[1].To &&
					(contractEventLogs[1].From == "" || contractEventLogs[1].From == "0x0000000000000000000000000000000000000000") &&
					(contractEventLogs[1].Token.TokenType == biz.ERC721 || contractEventLogs[1].Token.TokenType == biz.ERC1155)) {
				//https://etherscan.io/tx/0xa8dc1971cbcef530d354e257c645e61bd7edd287d9c5db70bf6890ffea79b5d7
				evmTransactionRecord.TransactionType = biz.MINT
			} else if (evmTransactionRecord.FromAddress == contractEventLogs[0].From && evmTransactionRecord.FromAddress == contractEventLogs[1].To) ||
				(evmTransactionRecord.FromAddress == contractEventLogs[0].To && evmTransactionRecord.FromAddress == contractEventLogs[1].From) ||
				(contractEventLogs[0].From == contractEventLogs[1].To && contractEventLogs[0].To == contractEventLogs[1].From) {
				//https://etherscan.io/tx/0xf324e2c9bb6b2c5335223545fadc601a53afb23c988e4967339a66d8a59a129d
				//https://etherscan.io/tx/0xdf3bb87c57ee023f68dfcb745bd953a1ddb7ac2e9b852e339c771fa09a39cee6
				//https://blockscout.scroll.io/tx/0xd8b357d1968f15f18f9579afa238cb0e94bd55b1e4173cd44744d2bd61f67c0d
				//https://explorer.zksync.io/tx/0x82c353e3764142e2623b5a5b1f2c4122565defb0b919bcd4e74263cbe3e5d238
				//https://explorer.zksync.io/tx/0x4a452e2dd92d04ee42cad5df59444779251cf971865e12164df3f68d85fb2b54
				//https://etherscan.io/tx/0xae8a5918c0ccc7a8658dcd3fcb92a9c28fe58e48b92731e788a0884994ab3818
				//https://etherscan.io/tx/0x305f8afd5b9374d1fc51f7ff2a6ac976c561d8cde8ed42bef464102b3cde2516
				//https://bscscan.com/tx/0xd8f79cea2dde996d96e3d296a2ad0debbeacc683fcadb4f076d54dc99d0cfe6c
				evmTransactionRecord.TransactionType = biz.SWAP
			}
		} else if eventLogLen == 3 {
			if (evmTransactionRecord.FromAddress == contractEventLogs[0].To && contractEventLogs[0].Token.Address != "" &&
				evmTransactionRecord.FromAddress == contractEventLogs[1].From && evmTransactionRecord.FromAddress == contractEventLogs[2].From) ||
				(evmTransactionRecord.FromAddress == contractEventLogs[1].To && contractEventLogs[1].Token.Address != "" &&
					evmTransactionRecord.FromAddress == contractEventLogs[0].From && evmTransactionRecord.FromAddress == contractEventLogs[2].From) ||
				(evmTransactionRecord.FromAddress == contractEventLogs[2].To && contractEventLogs[2].Token.Address != "" &&
					evmTransactionRecord.FromAddress == contractEventLogs[0].From && evmTransactionRecord.FromAddress == contractEventLogs[1].From) {
				//https://polygonscan.com/tx/0xa4de5f9f8a51bfef48cb4995a2b0a1a796e6b283387b6a96faae12955b4eeede
				//https://www.oklink.com/cn/oktc/tx/0xb78cc4625592ec49f7f562c801ac0657743694e9c0d6ec0884023a51bd5c4ebf
				//https://bscscan.com/tx/0xa3d25941921ede8398b9a5bdb30cf8bec148280ebf2bcf9f55e45751dcf9ad67
				//https://explorer.zksync.io/tx/0x226c2be9e972995d72788faae48aafb561dc7b14bcf983e6a16d9f83e44f62b3
				//https://etherscan.io/tx/0x42b099f8362a30c45e8487b7b1508988d298036251ba6f3ad1de38491e03c2c0
				evmTransactionRecord.TransactionType = biz.ADDLIQUIDITY
			}
		} else if eventLogLen == 4 {
			if (evmTransactionRecord.FromAddress == contractEventLogs[0].To && contractEventLogs[0].Token.Address != "" &&
				evmTransactionRecord.FromAddress == contractEventLogs[1].From && evmTransactionRecord.FromAddress == contractEventLogs[2].From && evmTransactionRecord.FromAddress == contractEventLogs[3].From &&
				(contractEventLogs[1].Token.Address == "" || contractEventLogs[2].Token.Address == "" || contractEventLogs[3].Token.Address == "")) ||
				(evmTransactionRecord.FromAddress == contractEventLogs[1].To && contractEventLogs[1].Token.Address != "" &&
					evmTransactionRecord.FromAddress == contractEventLogs[0].From && evmTransactionRecord.FromAddress == contractEventLogs[2].From && evmTransactionRecord.FromAddress == contractEventLogs[3].From &&
					(contractEventLogs[0].Token.Address == "" || contractEventLogs[2].Token.Address == "" || contractEventLogs[3].Token.Address == "")) ||
				(evmTransactionRecord.FromAddress == contractEventLogs[2].To && contractEventLogs[2].Token.Address != "" &&
					evmTransactionRecord.FromAddress == contractEventLogs[0].From && evmTransactionRecord.FromAddress == contractEventLogs[1].From && evmTransactionRecord.FromAddress == contractEventLogs[3].From &&
					(contractEventLogs[0].Token.Address == "" || contractEventLogs[1].Token.Address == "" || contractEventLogs[3].Token.Address == "")) ||
				(evmTransactionRecord.FromAddress == contractEventLogs[3].To && contractEventLogs[3].Token.Address != "" &&
					evmTransactionRecord.FromAddress == contractEventLogs[0].From && evmTransactionRecord.FromAddress == contractEventLogs[1].From && evmTransactionRecord.FromAddress == contractEventLogs[2].From &&
					(contractEventLogs[0].Token.Address == "" || contractEventLogs[1].Token.Address == "" || contractEventLogs[2].Token.Address == "")) {
				//https://etherscan.io/tx/0x8d09740a110a3a3cb8bab241acb873719e7828d3704c8166e9e6c85cfe67c635
				evmTransactionRecord.TransactionType = biz.ADDLIQUIDITY
			}
		}
	}
	if evmTransactionRecord.TransactionType == biz.CONTRACT && isMint {
		//https://etherscan.io/tx/0x563e7660b9b393653f922626fd4a12e960ce629c4200d7d04dbbbac1e4576c0e
		//https://etherscan.io/tx/0x375aad9406dd7cc41d00ebf9df7611c5134984c986a4730bf45fb12c3c23a55c
		evmTransactionRecord.TransactionType = biz.MINT
	}

	//补全走手续费代付时真正的调用关系
	if feeLog != nil {
		var feeTxRecords []*data.EvmTransactionRecord
		var fromAddress, fromUid string

		if platformUserCount == 1 {
			feeEventLog := eventLogs[0]
			if feeEventLog.FromUid != "" {
				fromAddress = feeEventLog.From
				fromUid = feeEventLog.FromUid
			} else {
				fromAddress = feeEventLog.To
				fromUid = feeEventLog.ToUid
			}

			var transactionType string
			if feeEventLog.Token.Address == "" {
				transactionType = biz.NATIVE
			} else if feeEventLog.Token.TokenType == "" {
				transactionType = biz.TRANSFER
			} else {
				transactionType = biz.TRANSFERNFT
			}
			//真正调用
			feeTxRecords = append(feeTxRecords, &data.EvmTransactionRecord{
				BlockNumber:     int(blockNumber),
				TransactionHash: transactionHash,
				FromAddress:     feeEventLog.From,
				ToAddress:       feeEventLog.To,
				FromUid:         feeEventLog.FromUid,
				ToUid:           feeEventLog.ToUid,
				Status:          status,
				TransactionType: transactionType,
			})
		} else {
			oneSender := true
			for _, eventLogUid := range eventLogs {
				if eventLogUid.FromUid != "" {
					if fromAddress == "" {
						fromAddress = eventLogUid.From
						fromUid = eventLogUid.FromUid
					} else if fromAddress != eventLogUid.From {
						oneSender = false
						break
					}
				}
				if eventLogUid.ToUid != "" {
					if fromAddress == "" {
						fromAddress = eventLogUid.To
						fromUid = eventLogUid.ToUid
					} else if fromAddress != eventLogUid.To {
						oneSender = false
						break
					}
				}
			}
			if !oneSender || realContractAddress == "" {
				feeTx, err := data.EvmTransactionRecordRepoClient.SelectColumnByTxHash(nil, biz.GetTableName(h.chainName), transactionHash, []string{"from_address", "from_uid", "to_address"})
				if err == nil && feeTx != nil {
					if !oneSender {
						fromAddress = feeTx.FromAddress
						fromUid = feeTx.FromUid
					} else if realContractAddress == "" {
						realContractAddress = feeTx.ToAddress
					}
				}
			}
			//真正调用
			feeTxRecords = append(feeTxRecords, &data.EvmTransactionRecord{
				BlockNumber:     int(blockNumber),
				TransactionHash: transactionHash,
				FromAddress:     fromAddress,
				ToAddress:       realContractAddress,
				FromUid:         fromUid,
				ToUid:           "",
				Status:          status,
				TransactionType: evmTransactionRecord.TransactionType,
			})
		}

		//手续费代付合约调用
		feeTxRecords = append(feeTxRecords, &data.EvmTransactionRecord{
			BlockNumber:     int(blockNumber),
			TransactionHash: transactionHash,
			FromAddress:     fromAddress,
			ToAddress:       evmTransactionRecord.ToAddress,
			FromUid:         fromUid,
			ToUid:           "",
			Status:          status,
			TransactionType: evmTransactionRecord.TransactionType,
		})

		go HandleTransactionCount(h.chainName, *(client), feeTxRecords)
	}

	return nil
}

func (h *txDecoder) extractEventLogs(client *Client, meta *pCommon.TxMeta, receipt *rtypes.Receipt, transaction *Transaction) (eventLogList []*types.EventLogUid, eventLogTokenId string, feeEvent *types.EventLogUid, realContractAddress string) {
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
			if !isTopic0InWhiteList(h.chainName, contractAddress, methodId, topic0) {
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
						log.Error("扫块，从nodeProxy中获取NFT信息失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
					}*/
					token.TokenType = biz.ERC721
					amount, _ = new(big.Int).SetString("1", 0)
				} else {
					token, err = biz.GetTokenInfoRetryAlert(ctx, h.chainName, tokenAddress)
					if err != nil {
						log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
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
					log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
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
						log.Error("扫块，从nodeProxy中获取NFT信息失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
					}*/
					token.TokenType = biz.ERC721
					amount, _ = new(big.Int).SetString("1", 0)
				} else if len(log_.Topics) == 1 {
					//https://cn.etherscan.com/tx/0x2c355d0b5419ca267344ed6e19ceb8fc20d102f6e67c312b38e047f1031998ee
					/*token, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, tokenAddress, tokenId)
					if err != nil {
						log.Error("扫块，从nodeProxy中获取NFT信息失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
					}*/
					token.TokenType = biz.ERC721
					amount, _ = new(big.Int).SetString("1", 0)
					if len(log_.Data) >= 96 {
						fromAddress = common.BytesToAddress(log_.Data[:32]).String()
						toAddress = common.BytesToAddress(log_.Data[32:64]).String()
						tokenId = new(big.Int).SetBytes(log_.Data[64:96]).String()
					}
				} else {
					token, err = biz.GetTokenInfoRetryAlert(ctx, h.chainName, tokenAddress)
					if err != nil {
						log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
					}
					if len(log_.Data) >= 32 {
						amount = new(big.Int).SetBytes(log_.Data[:32])
					}
					if amount.String() == "0" {
						continue
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
				log.Error("扫块，从nodeProxy中获取NFT信息失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
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
				log.Error("扫块，从nodeProxy中获取NFT信息失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
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
			if strings.HasPrefix(h.chainName, "ETH") && contractAddress == "0x10fc9e6ed49e648bde6b6214f07d8e63e12349e8" && methodId == "11290d59" {
				//https://etherscan.io/tx/0x2e251f2e4f2c55fff87ea22b62c4ed0afac263e696c6f4fbd3a2c0556a72b9a0
				continue
			}
			if strings.HasPrefix(h.chainName, "BSC") && contractAddress == "0xa2d57fa083ad42fe7d042fe0794cfef13bd2603c" && methodId == "11290d59" {
				//https://bscscan.com/tx/0xdde0fb537b3ff7b15b089e977d733bdec0db202c402999bf45884f3677bc40d8
				continue
			}
			if strings.HasPrefix(h.chainName, "Polygon") && contractAddress == "0xd0db2f29056e0226168c6b32363a339fe8fd46b5" && methodId == "11290d59" {
				//https://polygonscan.com/tx/0x850bd8c530edc73d2f28e63f4092a99942c1a325c430fade0b80ea218173d928
				continue
			}
			if contractAddress == "0xb7fdda5330daea72514db2b84211afebd19277ca" && methodId == "4630a0d8" {
				//https://etherscan.io/tx/0xb26e642b81bae1ae4c832794fc54fb2c671de76d22e15202c68d52a65f5b9dad
				//https://bscscan.com/tx/0xe5fb2301ac9a3245b8daf665ca67247f890fbe9a0f1aa7562881177483cb37a2
				//https://polygonscan.com/tx/0x5a8bd32b246357ef05694bf4f0694f57f24587ef7ec3dd99b48366c96a75cf6f
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
						toAddress = common.BytesToAddress(txData[68:100]).String()
					}
				}
			}

			if tokenAddress != "" {
				token, err = biz.GetTokenInfoRetryAlert(context.Background(), h.chainName, tokenAddress)
				if err != nil {
					log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
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
						fromAddress = common.BytesToAddress(log_.Data[:32]).String()
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
					log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
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
				toAddress = common.BytesToAddress(log_.Data[:32]).String()
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
				toAddress = common.BytesToAddress(log_.Data[64:96]).String()
			}
			if len(log_.Data) > 160 {
				amount = new(big.Int).SetBytes(log_.Data[128:160])
			}
			tokenAddress = common.BytesToAddress(log_.Data[96:128]).String()

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
					log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
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
				toAddress = common.BytesToAddress(txData[4:36]).String()
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
					log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
				}
				token.Amount = amount.String()*/
				/*if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
					token.Symbol = token.Symbol[1:]
				}*/
				tokenAddress = ""
			}
		} else if topic0 == MATIC_BRIDGE {
			if strings.HasPrefix(h.chainName, "Polygon") && ((contractAddress == "0xb7fdda5330daea72514db2b84211afebd19277ca" && methodId == "4630a0d8") ||
				(contractAddress == "0xd0db2f29056e0226168c6b32363a339fe8fd46b5" && methodId == "11290d59")) {
				//https://polygonscan.com/tx/0xcfe4c0f8208ef8ff7c12ffa99bf9dfe3236f867176c6730a04cdaf11bd640fd8
				//https://polygonscan.com/tx/0xf42c21bd7df31b2fc75139c2d20b42b38462b9f0269e198a21b59ec273013eb2
				//https://polygonscan.com/tx/0x850bd8c530edc73d2f28e63f4092a99942c1a325c430fade0b80ea218173d928
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
				log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
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
			toAddress = common.BytesToAddress(txData[4:36]).String()
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
		} else if topic0 == OPENBLOCK_SWAP_TOPIC {
			//swap
			//https://etherscan.io/tx/0xb26e642b81bae1ae4c832794fc54fb2c671de76d22e15202c68d52a65f5b9dad 老的swap合约 接收主币
			//https://etherscan.io/tx/0x0f55b2139c5e6bee8658836b104eeb6848935befc71f482fed475c3c4a991377 老的swap合约 接收代币
			//https://etherscan.io/tx/0x5560844905d4f09eb5555310e9b4dcaa31fe22d238588ec9b30fb7f2f70ca977 新的swap合约 接收代币
			//https://polygonscan.com/tx/0xf8c87e264fb54d02a625d8c1f1af4ec0109126127d11daebea046a8210ea71f1 老的swap合约 接收代币
			//https://polygonscan.com/tx/0x5a8bd32b246357ef05694bf4f0694f57f24587ef7ec3dd99b48366c96a75cf6f 新的swap合约 接收主币
			//https://polygonscan.com/tx/0x74c66217bb55960831956d1503492fe4a493b9798684bc48f715a787dd5638aa 新的swap合约 接收代币
			//https://ftmscan.com/tx/0x560fd26e7c66098468a533c8905b28abd3c7214692f454b1f2e29082afad681d 老的swap合约 接收主币
			//手续费代付
			//https://etherscan.io/tx/0x2e251f2e4f2c55fff87ea22b62c4ed0afac263e696c6f4fbd3a2c0556a72b9a0 老的swap合约 接收主币 address=0xAE04C60489d65f29db0bA1b1F54553f31A8b9358
			//https://bscscan.com/tx/0x75dad8c8a0dee2f17b2613378b1ffb2f5b2a3678dc26d34273d64dead95b3f00 新的swap合约 接收主币
			//https://bscscan.com/tx/0x6b3dc3f5bd41f9248c9c11dc3b2679014e8a5edddd24f8df6f7854ba2f545d83 新的swap合约 接收代币
			//https://polygonscan.com/tx/0x850bd8c530edc73d2f28e63f4092a99942c1a325c430fade0b80ea218173d928 新的swap合约 接收主币
			//https://polygonscan.com/tx/0x7e443a0e1378ef57a739c7364c9fec2dac432c5bbf0e6abecca3bac422d7ae45 新的swap合约 接收代币
			if methodId != "4630a0d8" && methodId != "11290d59" {
				continue
			}
			if len(log_.Data) < 32 {
				continue
			}
			realContractAddress = tokenAddress
			tokenAddress = common.BytesToAddress(log_.Data[96:128]).String()
			//只处理主币转账，代币转账可以通过标准Transfer事件解析到
			if tokenAddress != "0x0000000000000000000000000000000000000000" {
				continue
			}
			amount = new(big.Int).SetBytes(log_.Data[160:192])
			var hasOpenBlockSwapAddressTopic bool
			if len(receipt.Logs) != index+1 {
				nextLog := receipt.Logs[index+1]
				nextTopic0 := nextLog.Topics[0].String()
				if nextTopic0 == OPENBLOCK_SWAP_ADDRESS_TOPIC {
					fromAddress = common.BytesToAddress(nextLog.Data[:32]).String()
					toAddress = common.BytesToAddress(nextLog.Data[32:64]).String()
					if fromAddress == toAddress {
						fromAddress = contractAddress
					}
					hasOpenBlockSwapAddressTopic = true
				}
			}
			if !hasOpenBlockSwapAddressTopic && methodId == "4630a0d8" {
				fromAddress = contractAddress
				toAddress = common.BytesToAddress(txData[100:132]).String()
			}
			tokenAddress = ""
		} else if topic0 == GAS_PAY_TOPIC {
			//https://polygonscan.com/tx/0xa62c745a5879f88f952a513bf07976085cf93c8d4ea0f561baca7ac65832f2e9
			if len(log_.Data) < 32 {
				continue
			}
			fromAddress = contractAddress
			toAddress = common.HexToAddress(log_.Topics[1].String()).String()
			amount = new(big.Int).SetBytes(log_.Data[:32])
			tokenAddress = ""
		} else if topic0 == FEE_TOPIC {
			//https://polygonscan.com/tx/0x9872188870383ddcd1a0db806e6667b858bd30145af0e273cda4a0f7eaf7418b
			tokenAddress = common.HexToAddress(log_.Topics[1].String()).String()
			fromAddress = common.HexToAddress(log_.Topics[2].String()).String()
			toAddress = common.HexToAddress(log_.Topics[3].String()).String()
			fromAddressExist, fromUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, fromAddress)
			toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, toAddress)
			amount = new(big.Int).SetBytes(log_.Data[0:32])

			token, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, tokenAddress)
			if err != nil {
				log.Error("扫块，从nodeProxy中获取NFT信息失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
			}
			token.Address = tokenAddress
			token.Amount = amount.String()
			//获取 手续费 eventlog
			feeEvent = &types.EventLogUid{
				EventLog: types.EventLog{
					From:   fromAddress,
					To:     toAddress,
					Amount: amount,
					Token:  token,
				},
				FromUid: fromUid,
				ToUid:   toUid,
			}
			log.Info("gaspayment", zap.Any("feeEvent", feeEvent))
			continue
		} else if topic0 == TRANSFER_FROM_L1_COMPLETED_TOPIC {
			//https://polygonscan.com/tx/0xc23acd6f333d9e5b832d24268662f2d4c524a7b1b807beef664933d5088d3adb
			toAddress = common.HexToAddress(log_.Topics[1].String()).String()
			fromAddress = common.HexToAddress(log_.Topics[2].String()).String()
			amount = new(big.Int).SetBytes(log_.Data[:32])
			tokenAddress = ""
		} else if topic0 == FINALIZEDEPOSITETH_TOPIC {
			//https://blockscout.scroll.io/tx/0xf1191f68d880e190bc0948dcea397ad99392a3bec33fff216b573ff764a4ec9f
			if methodId != "8ef1332e" {
				continue
			}
			if len(log_.Data) < 32 {
				continue
			}
			fromAddress = tokenAddress
			toAddress = common.HexToAddress(log_.Topics[2].String()).String()
			amount = new(big.Int).SetBytes(log_.Data[:32])
			tokenAddress = ""
		} else if topic0 == BORROW_TOPIC {
			fromAddress = tokenAddress
			toAddress = common.BytesToAddress(log_.Data[:32]).String()
			amount = new(big.Int).SetBytes(log_.Data[32:64])
			tokenAddress = ""
		}

		if xDaiDapp {
			break
		}
		//https://blockscout.com/xdai/mainnet/tx/0xb8a9f18ec9cfa01eb1822724983629e28d5b09010a32efeb1563de49f935d007 无法通过log获取
		if contractAddress == "0x0460352b91d7cf42b0e1c1c30f06b602d9ef2238" && methodId == "3d12a85a" {
			fromAddress = transaction.To().String()
			toAddress = common.BytesToAddress(txData[4:36]).String()
			amountTotal := new(big.Int).SetBytes(txData[36:68])
			bonderFeeAmount := new(big.Int).SetBytes(txData[100:132])
			amount = new(big.Int).Sub(amountTotal, bonderFeeAmount)
			/*tokenAddress = "0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d"
			token, err = biz.GetTokenInfoRetryAlert(nil, h.chainName, tokenAddress)
			if err != nil {
				log.Error("扫块，从nodeProxy中获取代币精度失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
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
			toAddress = common.BytesToAddress(txData[4:36]).String()
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
				log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.chainName), zap.Any("txHash", transactionHash), zap.Any("error", err))
				continue
			}
		}

		if toAddress != "" {
			toAddressExist, toUid, err = biz.UserAddressSwitchRetryAlert(h.chainName, toAddress)
			if err != nil {
				log.Error("扫块，从redis中获取用户地址失败", zap.Any("chainName", h.chainName), zap.Any("txHash", transactionHash), zap.Any("error", err))
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

		//https://polygonscan.com/tx/0xdd8635bfce70c989487eea4403826e691efbf230887e92cc958d53e79281b7b9#eventlog 合约地址：0xf0511f123164602042ab2bcf02111fa5d3fe97cd
		//https://polygonscan.com/tx/0x1444da34ed2dff311adb0a79382e94af6987b861e525ea5c1ecc9e6a49988119 ob闪对 代币 动态approve处理 合约地址：0xb7fdda5330daea72514db2b84211afebd19277ca
		//https://polygonscan.com/tx/0x948468d279eb77a23b43c6122d885ecae02a7cae930647ac831972ffb8dc70b2 ob闪对 代币 动态approve处理 合约地址：0xd0db2f29056e0226168c6b32363a339fe8fd46b5
		//if topic0 == APPROVAL_TOPIC {
		//	if (("0xf0511f123164602042ab2bcf02111fa5d3fe97cd" != contractAddress || methodId != "41706c4e") &&
		//		("0xb7fdda5330daea72514db2b84211afebd19277ca" != contractAddress || methodId != "4630a0d8") &&
		//		("0xa2d57fa083ad42fe7d042fe0794cfef13bd2603c" != contractAddress || methodId != "11290d59") &&
		//		("0xd0db2f29056e0226168c6b32363a339fe8fd46b5" != contractAddress || methodId != "11290d59")) ||
		//		!(strings.HasPrefix(h.chainName, "Polygon") || strings.HasPrefix(h.chainName, "BSC")) {
		//		continue
		//	}
		//}

		if len(tokens) > 0 {
			for _, tokenInfo := range tokens {
				tokenType := tokenInfo.TokenType
				tokenId := tokenInfo.TokenId
				tokenAmount := tokenInfo.Amount
				token, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, tokenAddress, tokenId)
				if err != nil {
					log.Error("扫块，从nodeProxy中获取NFT信息失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
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
				log.Error("扫块，从nodeProxy中获取NFT信息失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("tokenId", tokenId), zap.Any("error", err))
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

		//不展示event log中的授权记录
		if topic0 == APPROVAL_TOPIC {
			//更新 敞口
			//data.DappApproveRecordRepoClient.SaveOrUpdateSelective(nil, fromAddress, tokenAddress, toAddress, amount.String(), h.chainName)
			var ercType string
			if token.TokenType == biz.ERC20 || token.TokenType == "" {
				ercType = biz.APPROVE
			} else {
				ercType = biz.APPROVENFT
			}

			dappApproveRecord := data.DappApproveRecord{
				Id:         0,
				Uid:        fromUid,
				LastTxhash: transactionHash,
				Decimals:   token.Decimals,
				ChainName:  h.chainName,
				Address:    fromAddress,
				Token:      tokenAddress,
				ToAddress:  toAddress,
				Amount:     amount.String(),
				Original:   "",
				Symbol:     token.Symbol,
				TxTime:     transaction.time.Unix(),
				ErcType:    ercType,
			}

			data.DappApproveRecordRepoClient.SaveOrUpdateSelective(nil, &dappApproveRecord)
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
	}

	for _, eventLog := range eventLogs {
		if eventLog != nil && (feeEvent == nil || !(feeEvent.EventLog.From == eventLog.EventLog.From && feeEvent.EventLog.To == eventLog.EventLog.To && feeEvent.EventLog.Amount.String() == eventLog.EventLog.Amount.String() && feeEvent.EventLog.Token.Address == eventLog.EventLog.Token.Address)) {
			eventLogList = append(eventLogList, eventLog)
		}
	}
	return
}

func (h *txDecoder) OnSealedTx(c chain.Clienter, txByHash *chain.Transaction) error {
	client := c.(*Client)
	txHash := txByHash.Hash
	rawReceipt := txByHash.Raw.(*rtypes.Receipt)

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

	if tx == nil {
		//部分节点返回数据不准确，发现数据不匹配时，暂不处理，下次更换节点再处理
		return errors.New("transaction's block number not match")
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

	if err := h.handleEachTransaction(client, block, tx, tx.Raw.(*Transaction), meta, rawReceipt); err != nil {
		return err
	}
	matchedUser := meta.User.MatchFrom || meta.User.MatchTo
	if len(h.txRecords) == 0 && txByHash.Record != nil && !matchedUser {
		if record, ok := txByHash.Record.(*data.EvmTransactionRecord); ok {
			record.Status = biz.DROPPED
			h.txRecords = append(h.txRecords, record)
			log.Info("PENDING TX DROPPED AS NOT MATCH ANY", zap.String("chainName", h.chainName), zap.Any("record", record))
		}
	}
	return nil
}

func (h *txDecoder) OnDroppedTx(c chain.Clienter, tx *chain.Transaction) error {
	ctx := context.Background()
	record := tx.Record.(*data.EvmTransactionRecord)
	//a.扫块时候 给扫到 一样nonce的值 成功后  更新 当nonce一样，并且状态不是 biz.DROPPED_REPLACED 更新状态为 biz.DROPPED_REPLACED
	//新成功后的 speed_up

	//兜底时，成功后 执行 a 一样的步骤
	elapsed := time.Now().Unix() - record.TxTime

	// Polygon 链 Pending 时会出现从链上获取不到的情况，尝试设置少于 10 分钟不设置为失败。
	if h.chainName == "Polygon" && elapsed < 600 {
		return nil
	}

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
		zap.Int64("pendingElapsed", elapsed),
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

	// Id may be setted when save to kanban, we need to reset it to zero to avoid conflict.
	for _, item := range txRecords {
		item.Id = 0
	}

	if txRecords != nil && len(txRecords) > 0 {
		err := BatchSaveOrUpdate(txRecords, biz.GetTableName(h.chainName))
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链扫块，将数据插入到数据库中失败", h.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("扫块，将数据插入到数据库中失败", zap.Any("chainName", h.chainName), zap.Any("current", h.block.Number), zap.Any("error", err))
			return err
		}
		if h.newTxs {
			go HandleRecord(h.chainName, *(client.(*Client)), txRecords)
		} else {
			go HandleUserNonce(h.chainName, *(client.(*Client)), txNonceRecords)
			go HandlePendingRecord(h.chainName, *(client.(*Client)), txRecords)
		}
		receipts := make(map[string]*rtypes.Receipt)
		h.receipts.Range(func(key, value interface{}) bool {
			receipts[key.(string)] = value.(*rtypes.Receipt)
			return true
		})
		go HandleDeFiAsset(h.chainName, txRecords, receipts)

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

func isBridgeMethod(chainName, contractAddress, methodId string) bool {
	// dapp 白名单 判断 ，非白名单 直接 扔掉交易
	if whiteMethods, ok := BridgeWhiteMethodIdList[chainName+"_MethodId"]; ok {
		methodKey := chainName + "_" + contractAddress + "_" + methodId
		for _, whiteMethod := range whiteMethods {
			if methodKey == whiteMethod {
				return true
			}
		}
	}
	return false
}

func isMethodInWhiteList(chainName, contractAddress, methodId string) bool {
	inWhiteList := isBridgeMethod(chainName, contractAddress, methodId)

	if !inWhiteList {
		if whiteMethods, ok := WhiteListMethodMap[chainName+"_Contract_Method"]; ok {
			methodKey := contractAddress + "_" + methodId
			for _, whiteMethod := range whiteMethods {
				if methodKey == whiteMethod {
					inWhiteList = true
					break
				}
			}
		}
	}
	if !inWhiteList {
		if whiteMethods, ok := WhiteListMethodMap["Contract_Method"]; ok {
			methodKey := contractAddress + "_" + methodId
			for _, whiteMethod := range whiteMethods {
				if methodKey == whiteMethod {
					inWhiteList = true
					break
				}
			}
		}
	}
	if !inWhiteList {
		if whiteMethods, ok := WhiteListMethodMap["Method"]; ok {
			methodKey := methodId
			for _, whiteMethod := range whiteMethods {
				if methodKey == whiteMethod {
					inWhiteList = true
					break
				}
			}
		}
	}
	return inWhiteList
}

func isTopic0Bridge(chainName, contractAddress, methodId, topic0 string) bool {
	if whiteTopics, ok := BridgeWhiteTopicList[chainName+"_Topic"]; ok {
		topicKey := chainName + "_" + contractAddress + "_" + topic0
		for _, whiteTopic := range whiteTopics {
			if topicKey == whiteTopic {
				return true
			}
		}
	}
	return false
}

func isTopic0InWhiteList(chainName, contractAddress, methodId, topic0 string) bool {
	inWhiteList := isTopic0Bridge(chainName, contractAddress, methodId, topic0)
	if !inWhiteList {
		if whiteTopics, ok := WhiteListTopicMap[chainName+"_Contract_Method_Topic"]; ok {
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
	return inWhiteList
}
