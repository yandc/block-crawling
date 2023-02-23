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

	meta, err := pCommon.AttemptMatchUser(h.chainName, tx)
	if err != nil {
		return err
	}

	// Ignore this transaction.
	if !(meta.User.MatchFrom || meta.User.MatchTo) {
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
		eventLogs, tokenId = h.extractEventLogs(client, meta, receipt, transaction)
	}
	status := biz.PENDING
	if receipt.Status == "0x0" {
		status = biz.FAIL
	} else if receipt.Status == "0x1" {
		status = biz.SUCCESS
	}
	transactionHash := transaction.Hash().String()

	if transaction.To() != nil {
		toAddress := transaction.To().String()
		codeAt, err := client.CodeAt(context.Background(), common.HexToAddress(toAddress), nil)
		if err != nil {
			return err
		}
		if len(codeAt) > 0 {
			contractAddress = toAddress
			var getTokenType bool
			var tokenType string
			var tokenTypeErr error
			if meta.TransactionType == biz.NATIVE {
				meta.TransactionType = biz.CONTRACT
				eventLogs, tokenId = h.extractEventLogs(client, meta, receipt, transaction)
			} else if meta.TransactionType == biz.APPROVE || meta.TransactionType == biz.TRANSFER || meta.TransactionType == biz.TRANSFERFROM {
				ctx := context.Background()
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
						log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
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
				ctx := context.Background()
				meta.TransactionType = biz.APPROVENFT
				tokenInfo, err = biz.GetCollectionInfoDirectlyRetryAlert(ctx, h.chainName, contractAddress)
				if err != nil {
					log.Error(h.chainName+"扫块，从nodeProxy中获取代币精度失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
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

				tokenInfo, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, contractAddress, tokenId)
				if err != nil {
					log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
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

	var platformUserCount int
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
	}

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
	intBlockNumber, _ := utils.HexStringToInt(receipt.BlockNumber)
	bn := int(intBlockNumber.Int64())
	feeAmount = decimal.NewFromBigInt(new(big.Int).Mul(gasUsedInt, gasPriceInt), 0)
	amountValue, _ := decimal.NewFromString(amount)
	var eventLog string
	if eventLogs != nil {
		eventLog, _ = utils.JsonEncode(eventLogs)
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
			log.Error(h.chainName+"扫块，logAddress长度超过最大限制", zap.Any("txHash", transactionHash), zap.Any("logAddressLen", logAddressLen))
			logAddress = nil
		}
	}
	evmTransactionRecord := &data.EvmTransactionRecord{
		BlockHash:            h.blockHash,
		BlockNumber:          bn,
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
		CreatedAt:            h.now.Unix(),
		UpdatedAt:            h.now.Unix(),
	}

	if meta.User.MatchFrom || meta.User.MatchTo || isPlatformUser {
		h.txRecords = append(h.txRecords, evmTransactionRecord)
		if !h.newTxs {
			h.txNonceRecords = append(h.txNonceRecords, evmTransactionRecord)
		}
	}

	if isPlatformUser && meta.TransactionType == biz.CONTRACT {
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
		err := BatchSaveOrUpdate(txRecords, biz.GetTableName(h.chainName))
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

func (h *txDecoder) extractEventLogs(client *Client, meta *pCommon.TxMeta, receipt *Receipt, transaction *Transaction) (eventLogList []types.EventLog, tokenId string) {
	var eventLogs []*types.EventLog
	arbitrumAmount := big.NewInt(0)
	transactionHash := transaction.Hash().String()
	gmxSwapFlag := false
	gmxFromAddress := ""
	gmxAmount := big.NewInt(0)
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
		var err error
		tokenAddress := log_.Address.String()
		amount := big.NewInt(0)
		var fromAddress string
		if len(log_.Topics) >= 2 {
			fromAddress = common.HexToAddress(log_.Topics[1].String()).String()
		}
		var toAddress string

		//判断合约 转账， 提现， 兑换。
		if topic0 == TRANSFER_TOPIC || topic0 == WITHDRAWAL_TOPIC || topic0 == DEPOSIT_TOPIC {
			if tokenAddress != "" {
				ctx := context.Background()
				if len(log_.Topics) == 4 {
					tokenId = log_.Topics[3].Big().String()
					token, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, tokenAddress, tokenId)
					if err != nil {
						log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
					}
					token.TokenType = biz.ERC721
					amount, _ = new(big.Int).SetString("1", 0)
				} else if len(log_.Topics) == 1 {
					//https://cn.etherscan.com/tx/0x2c355d0b5419ca267344ed6e19ceb8fc20d102f6e67c312b38e047f1031998ee
					token, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, tokenAddress, tokenId)
					if err != nil {
						log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
					}
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
			toAddress = common.HexToAddress(log_.Topics[2].String()).String()
			if toAddress == "0x0000000000000000000000000000000000000000" {
				continue
			}
			if tokenAddress != "" {
				ctx := context.Background()
				if len(log_.Topics) == 4 {
					tokenId = log_.Topics[3].Big().String()
					token, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, tokenAddress, tokenId)
					if err != nil {
						log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
					}
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
				ctx := context.Background()
				tokenId = log_.Topics[2].Big().String()
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
			ctx := context.Background()
			tokenId = new(big.Int).SetBytes(log_.Data[:32]).String()
			amount = new(big.Int).SetBytes(log_.Data[32:64])
			token, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, tokenAddress, tokenId)
			if err != nil {
				log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
			}
			token.TokenType = biz.ERC1155
			token.Amount = amount.String()
			fromAddress = common.HexToAddress(log_.Topics[2].String()).String()
			toAddress = common.HexToAddress(log_.Topics[3].String()).String()
		} else if topic0 == TRANSFERBATCH_TOPIC {
			ctx := context.Background()
			tokenId = new(big.Int).SetBytes(log_.Data[96:128]).String()
			amount = new(big.Int).SetBytes(log_.Data[128:160])
			token, err = biz.GetNftInfoDirectlyRetryAlert(ctx, h.chainName, tokenAddress, tokenId)
			if err != nil {
				log.Error(h.chainName+"扫块，从nodeProxy中获取NFT信息失败", zap.Any("current", h.block.Number), zap.Any("txHash", transactionHash), zap.Any("error", err))
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
			if len(log_.Topics) >= 2 {
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
		} else if topic0 == BRIDGE_TRANSFERNATIVE {
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
			fromAddress = common.HexToAddress(receipt.To).String()
			if len(log_.Data) > 96 {
				toAddress = common.HexToAddress(hex.EncodeToString(log_.Data[64:96])).String()
			}
			if len(log_.Data) > 160 {
				amount = new(big.Int).SetBytes(log_.Data[128:160])
			}
			tokenAddress = ""
		} else if topic0 == OPTIMISM_WITHDRAWETH {
			//无 转出地址
			fromAddress = common.HexToAddress(receipt.To).String()
			amount = new(big.Int).SetBytes(log_.Data)
			tokenAddress = ""
			toAddress = common.BytesToAddress(transaction.Data()[4:36]).String()
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
				} else {
					continue
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
			tokenAddress = ""
			amount = arbitrumAmount
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
				if strings.HasPrefix(token.Symbol, "W") || strings.HasPrefix(token.Symbol, "w") {
					token.Symbol = token.Symbol[1:]
				}
				token.Amount = amount.String()
			}
		}
		//不展示event log中的授权记录
		if topic0 == APPROVAL_TOPIC || topic0 == APPROVALFORALL_TOPIC {
			continue
		}
		if tokenAddress != "" {
			token.Address = tokenAddress
		}
		if tokenId != "" {
			token.TokenId = tokenId
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

func (h *txDecoder) OnSealedTx(c chain.Clienter, txByHash *chain.Transaction) error {
	client := c.(*Client)

	rawReceipt := txByHash.Raw

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
