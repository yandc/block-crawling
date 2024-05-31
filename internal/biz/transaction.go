package biz

import (
	pb "block-crawling/api/transaction/v1"
	v1 "block-crawling/internal/client"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/signhash"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"

	"gorm.io/datatypes"

	types2 "github.com/ethereum/go-ethereum/common"
	"github.com/go-redis/redis"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type TransactionUsecase struct {
	gormDB          *gorm.DB
	lark            Larker
	chainListClient v1.ChainListClient
}

var sendLock sync.RWMutex

func NewTransactionUsecase(grom *gorm.DB, lark Larker, bundle *data.Bundle, txcRepo TransactionRecordRepo, chainListClient v1.ChainListClient) *TransactionUsecase {
	data.NewOklinkRepo("https://83c5939d-9051-46d9-9e72-ed69d5855209@www.oklink.com")
	data.NewKaspaRepoClient("https://api.kaspa.org")
	return &TransactionUsecase{
		gormDB:          grom,
		lark:            lark,
		chainListClient: chainListClient,
	}
}

func (s *TransactionUsecase) GetAllOpenAmount(ctx context.Context, req *pb.OpenAmountReq) (*pb.OpenAmoutResp, error) {
	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		if req.Address != "" {
			req.Address = types2.HexToAddress(req.Address).Hex()
		}
		if req.ContractAddress != "" {
			req.ContractAddress = types2.HexToAddress(req.ContractAddress).Hex()
		}
	}
	var oai = &pb.OpenAmountInfo{}
	amountTotal := decimal.Zero
	//根据条件去数据库查询list，
	dapps, err := data.DappApproveRecordRepoClient.GetAmountList(ctx, req)
	if err != nil {
		log.Error("查询授权dapp列表报错！", zap.Any("param", req), zap.Any("error", err))
		return nil, err
	}
	//返回空列表
	if dapps == nil {
		oai.RiskExposureAmount = amountTotal.String()
		oai.DappCount = 0
		return &pb.OpenAmoutResp{
			Ok:   true,
			Data: oai,
		}, nil
	} else {
		var chainNameTokenAddressMap = make(map[string][]string)
		var tokenAddressMapMap = make(map[string]map[string]string)
		for _, record := range dapps {
			if record == nil || len(record.Amount) >= 40 {
				continue
			}

			tokenAddressMap, ok := tokenAddressMapMap[record.ChainName]
			if !ok {
				tokenAddressMap = make(map[string]string)
				tokenAddressMapMap[record.ChainName] = tokenAddressMap
			}
			tokenAddressMap[record.Token] = ""
		}

		for chainName, tokenAddressMap := range tokenAddressMapMap {
			tokenAddressList := make([]string, 0, len(tokenAddressMap))
			for key, _ := range tokenAddressMap {
				tokenAddressList = append(tokenAddressList, key)
			}
			chainNameTokenAddressMap[chainName] = tokenAddressList
		}

		resultMap, err := GetTokensPrice(nil, req.Currency, chainNameTokenAddressMap)
		if err != nil {
			return nil, err
		}

		for _, record := range dapps {
			if record == nil || len(record.Amount) >= 40 {
				continue
			}

			chainName := record.ChainName
			tokenAddress := record.Token
			var price string
			tokenAddressPriceMap := resultMap[chainName]
			if tokenAddress == "" {
				price = tokenAddressPriceMap[chainName]
			} else {
				price = tokenAddressPriceMap[tokenAddress]
			}
			prices, _ := decimal.NewFromString(price)
			balance := utils.StringDecimals(record.Amount, int(record.Decimals))
			balances, _ := decimal.NewFromString(balance)
			cnyAmount := prices.Mul(balances)
			amountTotal = amountTotal.Add(cnyAmount)
		}

		return &pb.OpenAmoutResp{
			Ok: true,
			Data: &pb.OpenAmountInfo{
				RiskExposureAmount: amountTotal.String(),
				DappCount:          int64(len(dapps)),
			},
		}, nil
	}
}

func (s *TransactionUsecase) GetDappList(ctx context.Context, req *pb.DappListReq) (*pb.DappListResp, error) {
	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		req.Addresses = utils.HexToAddress(req.Addresses)
		if req.ContractAddress != "" {
			req.ContractAddress = types2.HexToAddress(req.ContractAddress).Hex()
		}
	}
	var dappAll []*pb.DappInfo
	dapps, err := data.DappApproveRecordRepoClient.ListByCondition(ctx, req)
	if err != nil {
		log.Errore("返回授权dapp列表报错！", err)
		return &pb.DappListResp{
			Ok:   false,
			Data: dappAll,
		}, err
	}
	if dapps == nil {
		return &pb.DappListResp{
			Ok:   true,
			Data: dappAll,
		}, err
	}

	for _, da := range dapps {
		//查询
		dappInfo := ""
		parseData := ""
		tokenInfoStr := ""
		transcationType := ""
		chainType, _ := GetChainNameType(da.ChainName)
		switch chainType {
		case EVM:
			evm, err := data.EvmTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(da.ChainName), da.LastTxhash)
			if err == nil && evm != nil {
				dappInfo = evm.DappData
				parseData = evm.ParseData
				tokenInfoStr = evm.TokenInfo
				transcationType = evm.TransactionType
			}
		case STC:
			stc, err := data.StcTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(da.ChainName), da.LastTxhash)
			if err == nil && stc != nil {
				dappInfo = stc.DappData
				parseData = stc.ParseData
				tokenInfoStr = stc.TokenInfo
				transcationType = stc.TransactionType
			}
		case TVM:
			tvm, err := data.TrxTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(da.ChainName), da.LastTxhash)
			if err == nil && tvm != nil {
				dappInfo = tvm.DappData
				parseData = tvm.ParseData
				tokenInfoStr = tvm.TokenInfo
				transcationType = tvm.TransactionType
			}
		case APTOS:
			apt, err := data.AptTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(da.ChainName), da.LastTxhash)
			if err == nil && apt != nil {
				dappInfo = apt.DappData
				parseData = apt.ParseData
				tokenInfoStr = apt.TokenInfo
				transcationType = apt.TransactionType
			}
		case SUI:
			sui, err := data.SuiTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(da.ChainName), da.LastTxhash)
			if err == nil && sui != nil {
				dappInfo = sui.DappData
				parseData = sui.ParseData
				tokenInfoStr = sui.TokenInfo
				transcationType = sui.TransactionType
			}
		case SOLANA:
			sol, err := data.SolTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(da.ChainName), da.LastTxhash)
			if err == nil && sol != nil {
				dappInfo = sol.DappData
				parseData = sol.ParseData
				tokenInfoStr = sol.TokenInfo
				transcationType = sol.TransactionType
			}
		}
		if req.DappType == "approveNFT" && transcationType != req.DappType {
			continue
		}
		if req.DappType == "approve" && transcationType == "approveNFT" {
			continue
		}

		ds := strconv.FormatInt(da.Decimals, 10)
		status := ""
		if da.Amount == "0" {
			status = "2"
		}
		if da.Amount != "0" && da.Amount != "" {
			status = "1"
		}
		dif := &pb.DappInfo{
			ContractAddress: da.ToAddress,
			Chain:           da.ChainName,
			Uid:             da.Uid,
			LastTxhash:      da.LastTxhash,
			Decimals:        ds,
			Address:         da.Address,
			Token:           da.Token,
			ToAddress:       da.ToAddress,
			Amount:          da.Amount,
			Original:        da.Original,
			Symbol:          da.Symbol,
			Status:          status,
			DappInfo:        dappInfo,
			TxTime:          da.TxTime,
		}
		var tokenInfo *types.TokenInfo
		if tokenInfoStr != "" {
			if jsonErr := json.Unmarshal([]byte(tokenInfoStr), &tokenInfo); jsonErr == nil {
				dif.DappType = tokenInfo.TokenType
				dif.CollectionName = tokenInfo.CollectionName
				dif.Logo = tokenInfo.TokenUri
			}
		} else if parseData != "" {
			tokenInfo, _ = ParseTokenInfo(parseData)
			dif.DappType = tokenInfo.TokenType
			dif.CollectionName = tokenInfo.CollectionName
			dif.Logo = tokenInfo.TokenUri
		}
		dappAll = append(dappAll, dif)
	}
	return &pb.DappListResp{
		Ok:   true,
		Data: dappAll,
	}, err
}

func (s *TransactionUsecase) CreateRecordFromWallet(ctx context.Context, pbb *pb.TransactionReq) (*pb.CreateResponse, error) {
	var result int64
	var err error
	var a, fa decimal.Decimal
	chainType, _ := GetChainNameType(pbb.ChainName)

	p1 := decimal.NewFromInt(100000000)

	if pbb.FeeAmount == "" {
		fa = decimal.Zero
	} else {
		fa, _ = decimal.NewFromString(pbb.FeeAmount)
		if chainType == BTC {
			fa = fa.Mul(p1).Round(0)
		}
	}
	if pbb.Amount == "" {
		a = decimal.Zero
	} else {
		a, _ = decimal.NewFromString(pbb.Amount)
		//兼容 btc 0901
		if chainType == BTC {
			a = a.Mul(p1).Round(0)
		}
	}
	pbb.TxTime = time.Now().Unix()

	// 处理eventlog数据，gas代付会传入此参数
	var logAddress datatypes.JSON
	if pbb.EventLog != "" {
		var eventLogs []*types.EventLogUid
		if jsonErr := json.Unmarshal([]byte(pbb.EventLog), &eventLogs); jsonErr == nil {
			if pbb.TransactionType == CONTRACT {
				logAddress = GetLogAddressFromEventLogUid(eventLogs)
			}
		} else {
			log.Warn("插入pending记录，解析eventLog数据失败", zap.Any("chainName", pbb.ChainName), zap.Any("eventLog", pbb.EventLog), zap.Any("error", jsonErr))
		}
	}

	//整理feeDate
	//var maxFeePerGas,maxPriorityFeePerGas string
	feeDataMap := make(map[string]string)
	if pbb.FeeData != "" {
		if jsonErr := json.Unmarshal([]byte(pbb.FeeData), &feeDataMap); jsonErr != nil {
			log.Warn("插入pending记录，解析feeData数据失败", zap.Any("chainName", pbb.ChainName), zap.Any("feeData", pbb.FeeData), zap.Any("error", jsonErr))
		}
	}
	pendingNonceKey := ADDRESS_PENDING_NONCE + pbb.ChainName + ":" + pbb.FromAddress + ":"
	switch chainType {
	case EVM:
		if pbb.ContractAddress != "" {
			pbb.ContractAddress = types2.HexToAddress(pbb.ContractAddress).Hex()
		}
		if pbb.FromAddress != "" {
			pbb.FromAddress = types2.HexToAddress(pbb.FromAddress).Hex()
			pendingNonceKey = ADDRESS_PENDING_NONCE + pbb.ChainName + ":" + pbb.FromAddress + ":"
		}
		if strings.HasPrefix(pbb.ToAddress, "0x") {
			pbb.ToAddress = types2.HexToAddress(pbb.ToAddress).Hex()
		}
	case COSMOS:
		if pbb.ContractAddress != "" {
			pbb.ContractAddress = utils.AddressIbcToLower(pbb.ContractAddress)
		}
	}

	var tokenInfoStr string
	sendTime := pbb.SendTime
	sessionId := pbb.SessionId
	shortHost := pbb.ShortHost
	tokenGasless := pbb.TokenGasless
	if pbb.TokenInfo != "" {
		tokenInfoStr = addTokenUri(pbb.TokenInfo, pbb.ChainName, pbb.TransactionHash)
	}
	feeTokenInfo := feeDataMap["fee_token_info"]
	if pbb.ClientData != "" {
		sendTime, sessionId, shortHost, tokenGasless = parseClientData(pbb.ClientData)
	}

	switch chainType {
	case CASPER:
		if pbb.ParseData != "" {
			parseDataMap := make(map[string]interface{})
			if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &parseDataMap); jsonErr == nil {
				tokenMap, ok := parseDataMap["token"].(map[string]interface{})
				if ok {
					tokenAddress, _ := tokenMap["address"].(string)
					if tokenAddress != "" {
						tokenType, _ := tokenMap["token_type"].(string)
						if tokenType == "" {
							tokenInfo, err := GetTokenInfoRetryAlert(context.Background(), pbb.ChainName, tokenAddress)
							if err != nil {
								log.Error("插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("chainName", pbb.ChainName), zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
							} else {
								tokenMap["token_uri"] = tokenInfo.TokenUri
								parseData, _ := utils.JsonEncode(parseDataMap)
								pbb.ParseData = parseData
								tokenInfoStr, _ = utils.JsonEncode(tokenMap)
							}
						}
					}
				}
			}
		}

		casperRecord := &data.CsprTransactionRecord{
			BlockHash:       pbb.BlockHash,
			BlockNumber:     int(pbb.BlockNumber),
			TransactionHash: pbb.TransactionHash,
			FromAddress:     pbb.FromAddress,
			ToAddress:       pbb.ToAddress,
			FromUid:         pbb.Uid,
			FeeAmount:       fa,
			Amount:          a,
			Status:          pbb.Status,
			TxTime:          pbb.TxTime,
			ParseData:       pbb.ParseData,
			Data:            pbb.Data,
			TransactionType: pbb.TransactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			TokenInfo:       tokenInfoStr,
			SendTime:        sendTime,
			SessionId:       sessionId,
			ShortHost:       shortHost,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.CsprTransactionRecordRepoClient.SaveOrUpdateClient(ctx, GetTableName(pbb.ChainName), casperRecord)
	case STC:
		if pbb.ParseData != "" {
			parseDataMap := make(map[string]interface{})
			if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &parseDataMap); jsonErr == nil {
				evmMap := parseDataMap["stc"]
				ret := evmMap.(map[string]interface{})
				nonceInt, _ := utils.GetInt(ret["sequence_number"])
				pbb.Nonce = int64(nonceInt)

				tokenMap, ok := parseDataMap["token"].(map[string]interface{})
				if ok {
					tokenAddress, _ := tokenMap["address"].(string)
					if tokenAddress != "" {
						tokenType, _ := tokenMap["token_type"].(string)
						if tokenType == "" {
							tokenInfo, err := GetTokenInfoRetryAlert(context.Background(), pbb.ChainName, tokenAddress)
							if err != nil {
								log.Error("插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("chainName", pbb.ChainName), zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
							} else {
								tokenMap["token_uri"] = tokenInfo.TokenUri
								parseData, _ := utils.JsonEncode(parseDataMap)
								pbb.ParseData = parseData
								tokenInfoStr, _ = utils.JsonEncode(tokenMap)
							}
						}
					}
				}
			}
		}

		transactionType := pbb.TransactionType
		if transactionType == CONTRACT {
			data := make(map[string]interface{})
			if jsonErr := json.Unmarshal([]byte(pbb.Data), &data); jsonErr == nil {
				var function string
				var ok bool
				payloadMap, pok := data["ScriptFunction"].(map[string]interface{})
				if pok {
					function, ok = payloadMap["function"].(string)
				}
				if !ok {
					clientData := make(map[string]interface{})
					if jsonErr = json.Unmarshal([]byte(pbb.ClientData), &clientData); jsonErr == nil {
						dappTxinfoMap, dok := clientData["dappTxinfo"].(map[string]interface{})
						if dok {
							payloadMap, pok := dappTxinfoMap["txPayload"].(map[string]interface{})
							if pok {
								valueMap, vok := payloadMap["value"].(map[string]interface{})
								if vok {
									funcMap, fok := valueMap["func"].(map[string]interface{})
									if fok {
										function, ok = funcMap["value"].(string)
									}
								}
							}
						}
					}
				}
				if ok {
					methodName := function
					if strings.Contains(methodName, "Mint") || strings.Contains(methodName, "_mint") || strings.HasPrefix(methodName, "mint") {
						transactionType = MINT
					}

					if strings.Contains(methodName, "Swap") || strings.Contains(methodName, "_swap") || strings.HasPrefix(methodName, "swap") {
						transactionType = SWAP
					}

					if strings.Contains(methodName, "AddLiquidity") || strings.Contains(methodName, "_add_liquidity") ||
						strings.HasPrefix(methodName, "addLiquidity") || strings.HasPrefix(methodName, "add_liquidity") ||
						(strings.HasPrefix(methodName, "add") && strings.Contains(methodName, "Liquidity")) ||
						(strings.HasPrefix(methodName, "add") && strings.Contains(methodName, "_liquidity")) {
						transactionType = ADDLIQUIDITY
					}
				}
			}
		}

		stcRecord := &data.StcTransactionRecord{
			BlockHash:       pbb.BlockHash,
			BlockNumber:     int(pbb.BlockNumber),
			TransactionHash: pbb.TransactionHash,
			OriginalHash:    pbb.OriginalHash,
			Nonce:           pbb.Nonce,
			FromAddress:     pbb.FromAddress,
			ToAddress:       pbb.ToAddress,
			FromUid:         pbb.Uid,
			FeeAmount:       fa,
			Amount:          a,
			Status:          pbb.Status,
			TxTime:          pbb.TxTime,
			ContractAddress: pbb.ContractAddress,
			ParseData:       pbb.ParseData,
			GasLimit:        feeDataMap["gas_limit"],
			GasUsed:         feeDataMap["gas_used"],
			GasPrice:        feeDataMap["gas_price"],
			Data:            pbb.Data,
			EventLog:        pbb.EventLog,
			LogAddress:      logAddress,
			TransactionType: transactionType,
			OperateType:     pbb.OperateType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			TokenInfo:       tokenInfoStr,
			SendTime:        sendTime,
			SessionId:       sessionId,
			ShortHost:       shortHost,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.StcTransactionRecordRepoClient.SaveOrUpdateClient(ctx, GetTableName(pbb.ChainName), stcRecord)
		if result == 1 {
			key := pendingNonceKey + strconv.Itoa(int(pbb.Nonce))
			data.RedisClient.Set(key, pbb.Uid, 6*time.Hour)
		}
	case POLKADOT:
		if pbb.ParseData != "" {
			parseDataMap := make(map[string]interface{})
			if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &parseDataMap); jsonErr == nil {
				polkadotMap := parseDataMap["polkadot"]
				ret := polkadotMap.(map[string]interface{})
				nonceInt, _ := utils.GetInt(ret["nonce"])
				pbb.Nonce = int64(nonceInt)

				tokenMap, ok := parseDataMap["token"].(map[string]interface{})
				if ok {
					tokenAddress, _ := tokenMap["address"].(string)
					if tokenAddress != "" {
						tokenType, _ := tokenMap["token_type"].(string)
						if tokenType == "" {
							tokenInfo, err := GetTokenInfoRetryAlert(context.Background(), pbb.ChainName, tokenAddress)
							if err != nil {
								log.Error("插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("chainName", pbb.ChainName), zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
							} else {
								tokenMap["token_uri"] = tokenInfo.TokenUri
								parseData, _ := utils.JsonEncode(parseDataMap)
								pbb.ParseData = parseData
								tokenInfoStr, _ = utils.JsonEncode(tokenMap)
							}
						}
					}
				}
			}
		}

		dotTransactionRecord := &data.DotTransactionRecord{
			BlockHash:       pbb.BlockHash,
			BlockNumber:     int(pbb.BlockNumber),
			Nonce:           pbb.Nonce,
			TransactionHash: pbb.TransactionHash,
			FromAddress:     pbb.FromAddress,
			ToAddress:       pbb.ToAddress,
			FromUid:         pbb.Uid,
			FeeAmount:       fa,
			Amount:          a,
			Status:          pbb.Status,
			TxTime:          pbb.TxTime,
			ContractAddress: pbb.ContractAddress,
			ParseData:       pbb.ParseData,
			Data:            pbb.Data,
			EventLog:        pbb.EventLog,
			LogAddress:      logAddress,
			TransactionType: pbb.TransactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			TokenInfo:       tokenInfoStr,
			SendTime:        sendTime,
			SessionId:       sessionId,
			ShortHost:       shortHost,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.DotTransactionRecordRepoClient.SaveOrUpdateClient(ctx, GetTableName(pbb.ChainName), dotTransactionRecord)
	case EVM:
		if pbb.ParseData != "" {
			parseDataMap := make(map[string]interface{})
			if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &parseDataMap); jsonErr == nil {
				evmMap := parseDataMap["evm"]
				ret := evmMap.(map[string]interface{})
				nonceInt, _ := utils.GetInt(ret["nonce"])
				pbb.Nonce = int64(nonceInt)

				tokenMap, ok := parseDataMap["token"].(map[string]interface{})
				if ok {
					tokenAddress, _ := tokenMap["address"].(string)
					if tokenAddress != "" {
						tokenType, _ := tokenMap["token_type"].(string)
						if tokenType == "" || tokenType == "ERC20" {
							tokenInfo, err := GetTokenInfoRetryAlert(context.Background(), pbb.ChainName, tokenAddress)
							if err != nil {
								log.Error("插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("chainName", pbb.ChainName), zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
							} else {
								tokenMap["token_uri"] = tokenInfo.TokenUri
								parseData, _ := utils.JsonEncode(parseDataMap)
								pbb.ParseData = parseData
								tokenInfoStr, _ = utils.JsonEncode(tokenMap)
							}
						}
					}
				}
			}
		}

		evmTransactionRecord := &data.EvmTransactionRecord{
			BlockHash:            pbb.BlockHash,
			BlockNumber:          int(pbb.BlockNumber),
			Nonce:                pbb.Nonce,
			TransactionHash:      pbb.TransactionHash,
			OriginalHash:         pbb.OriginalHash,
			FromAddress:          pbb.FromAddress,
			ToAddress:            pbb.ToAddress,
			FromUid:              pbb.Uid,
			FeeAmount:            fa,
			Amount:               a,
			Status:               pbb.Status,
			TxTime:               pbb.TxTime,
			ContractAddress:      pbb.ContractAddress,
			ParseData:            pbb.ParseData,
			GasLimit:             feeDataMap["gas_limit"],
			GasUsed:              feeDataMap["gas_used"],
			GasPrice:             feeDataMap["gas_price"],
			BaseFee:              feeDataMap["base_fee"],
			MaxFeePerGas:         feeDataMap["max_fee_per_gas"],
			MaxPriorityFeePerGas: feeDataMap["max_priority_fee_per_gas"],
			Data:                 pbb.Data,
			EventLog:             pbb.EventLog,
			LogAddress:           logAddress,
			TransactionType:      pbb.TransactionType,
			OperateType:          pbb.OperateType,
			DappData:             pbb.DappData,
			ClientData:           pbb.ClientData,
			FeeTokenInfo:         feeTokenInfo,
			TokenInfo:            tokenInfoStr,
			TokenGasless:         tokenGasless,
			SendTime:             sendTime,
			SessionId:            sessionId,
			ShortHost:            shortHost,
			CreatedAt:            pbb.CreatedAt,
			UpdatedAt:            pbb.UpdatedAt,
		}

		result, err = data.EvmTransactionRecordRepoClient.SaveOrUpdateClient(ctx, GetTableName(pbb.ChainName), evmTransactionRecord)
		if result == 1 {
			isGasless, _ := isGasLess(tokenGasless)
			if !isGasless && pbb.Nonce >= 0 {
				key := pendingNonceKey + strconv.Itoa(int(pbb.Nonce))
				data.RedisClient.Set(key, pbb.Uid, 6*time.Hour)
			}

			if pbb.TransactionType == CONTRACT {
				go UpdateTransactionType(pbb)
			}
		}
	case BTC:
		btcTransactionRecord := &data.BtcTransactionRecord{
			BlockHash:       pbb.BlockHash,
			BlockNumber:     int(pbb.BlockNumber),
			TransactionHash: pbb.TransactionHash,
			FromAddress:     pbb.FromAddress,
			ToAddress:       pbb.ToAddress,
			FromUid:         pbb.Uid,
			FeeAmount:       fa,
			Amount:          a,
			Status:          pbb.Status,
			TxTime:          pbb.TxTime,
			ConfirmCount:    0,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			SendTime:        sendTime,
			SessionId:       sessionId,
			ShortHost:       shortHost,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.BtcTransactionRecordRepoClient.SaveOrUpdateClient(ctx, GetTableName(pbb.ChainName), btcTransactionRecord)
		if result > 0 {
			go UpdateUtxo(pbb)
		}
	case TVM:
		if pbb.ParseData != "" {
			parseDataMap := make(map[string]interface{})
			if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &parseDataMap); jsonErr == nil {
				tokenMap, ok := parseDataMap["token"].(map[string]interface{})
				if ok {
					tokenAddress, _ := tokenMap["address"].(string)
					if tokenAddress != "" {
						tokenType, _ := tokenMap["token_type"].(string)
						if tokenType == "" {
							tokenInfo, err := GetTokenInfoRetryAlert(context.Background(), pbb.ChainName, tokenAddress)
							if err != nil {
								log.Error("插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("chainName", pbb.ChainName), zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
							} else {
								tokenMap["token_uri"] = tokenInfo.TokenUri
								parseData, _ := utils.JsonEncode(parseDataMap)
								pbb.ParseData = parseData
								tokenInfoStr, _ = utils.JsonEncode(tokenMap)
							}
						}
					}
				}
			}
		}

		trxRecord := &data.TrxTransactionRecord{
			BlockHash:       pbb.BlockHash,
			BlockNumber:     int(pbb.BlockNumber),
			TransactionHash: pbb.TransactionHash,
			FromAddress:     pbb.FromAddress,
			ToAddress:       pbb.ToAddress,
			FromUid:         pbb.Uid,
			FeeAmount:       fa,
			Amount:          a,
			Status:          pbb.Status,
			TxTime:          pbb.TxTime,
			ContractAddress: pbb.ContractAddress,
			ParseData:       pbb.ParseData,
			NetUsage:        feeDataMap["net_usage"],
			FeeLimit:        feeDataMap["fee_limit"],
			EnergyUsage:     feeDataMap["energy_usage"],
			FeeTokenInfo:    feeDataMap["fee_token_info"],
			Data:            pbb.Data,
			EventLog:        pbb.EventLog,
			LogAddress:      logAddress,
			TransactionType: pbb.TransactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			TokenInfo:       tokenInfoStr,
			SendTime:        sendTime,
			SessionId:       sessionId,
			ShortHost:       shortHost,
			TokenGasless:    tokenGasless,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.TrxTransactionRecordRepoClient.SaveOrUpdateClient(ctx, GetTableName(pbb.ChainName), trxRecord)
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			result, err = data.TrxTransactionRecordRepoClient.SaveOrUpdateClient(nil, GetTableName(pbb.ChainName), trxRecord)
		}
		if result == 1 && pbb.TransactionType == CONTRACT {
			go UpdateTransactionType(pbb)
		}
	case APTOS:
		if pbb.ParseData != "" {
			parseDataMap := make(map[string]interface{})
			if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &parseDataMap); jsonErr == nil {
				evmMap := parseDataMap["aptos"]
				ret := evmMap.(map[string]interface{})
				nonceInt, _ := utils.GetInt(ret["sequence_number"])
				pbb.Nonce = int64(nonceInt)

				tokenMap, ok := parseDataMap["token"].(map[string]interface{})
				if ok {
					tokenAddress, _ := tokenMap["address"].(string)
					if tokenAddress != "" {
						tokenType, _ := tokenMap["token_type"].(string)
						if tokenType == "" {
							tokenInfo, err := GetTokenInfoRetryAlert(context.Background(), pbb.ChainName, tokenAddress)
							if err != nil {
								log.Error("插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("chainName", pbb.ChainName), zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
							} else {
								tokenMap["token_uri"] = tokenInfo.TokenUri
								parseData, _ := utils.JsonEncode(parseDataMap)
								pbb.ParseData = parseData
								tokenInfoStr, _ = utils.JsonEncode(tokenMap)
							}
						}
					}
				}
			}
		}

		transactionType := pbb.TransactionType
		if transactionType == CONTRACT {
			data := make(map[string]interface{})
			if jsonErr := json.Unmarshal([]byte(pbb.Data), &data); jsonErr == nil {
				var function string
				var ok bool
				payloadMap, pok := data["payload"].(map[string]interface{})
				if pok {
					function, ok = payloadMap["function"].(string)
				}
				if !ok {
					clientData := make(map[string]interface{})
					if jsonErr = json.Unmarshal([]byte(pbb.ClientData), &clientData); jsonErr == nil {
						dappTxinfoMap, dok := clientData["dappTxinfo"].(map[string]interface{})
						if dok {
							payloadMap, pok := dappTxinfoMap["payload"].(map[string]interface{})
							if pok {
								function, ok = payloadMap["function"].(string)
							}
						}
					}
				}
				if ok {
					mode := strings.Split(function, "::")
					methodName := mode[len(mode)-1]
					if strings.Contains(methodName, "Mint") || strings.Contains(methodName, "_mint") || strings.HasPrefix(methodName, "mint") {
						transactionType = MINT
					}

					if strings.Contains(methodName, "Swap") || strings.Contains(methodName, "_swap") || strings.HasPrefix(methodName, "swap") {
						transactionType = SWAP
					}

					if strings.Contains(methodName, "AddLiquidity") || strings.Contains(methodName, "_add_liquidity") ||
						strings.HasPrefix(methodName, "addLiquidity") || strings.HasPrefix(methodName, "add_liquidity") ||
						(strings.HasPrefix(methodName, "add") && strings.Contains(methodName, "Liquidity")) ||
						(strings.HasPrefix(methodName, "add") && strings.Contains(methodName, "_liquidity")) {
						transactionType = ADDLIQUIDITY
					}
				}
			}
		}

		aptRecord := &data.AptTransactionRecord{
			BlockHash:       pbb.BlockHash,
			BlockNumber:     int(pbb.BlockNumber),
			Nonce:           pbb.Nonce,
			TransactionHash: pbb.TransactionHash,
			FromAddress:     pbb.FromAddress,
			ToAddress:       pbb.ToAddress,
			FromUid:         pbb.Uid,
			FeeAmount:       fa,
			Amount:          a,
			Status:          pbb.Status,
			TxTime:          pbb.TxTime,
			ContractAddress: pbb.ContractAddress,
			ParseData:       pbb.ParseData,
			GasLimit:        feeDataMap["gas_limit"],
			GasUsed:         feeDataMap["gas_used"],
			GasPrice:        feeDataMap["gas_price"],
			Data:            pbb.Data,
			EventLog:        pbb.EventLog,
			LogAddress:      logAddress,
			TransactionType: transactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			TokenInfo:       tokenInfoStr,
			SendTime:        sendTime,
			SessionId:       sessionId,
			ShortHost:       shortHost,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}

		result, err = data.AptTransactionRecordRepoClient.SaveOrUpdateClient(ctx, GetTableName(pbb.ChainName), aptRecord)
		if result == 1 {
			key := pendingNonceKey + strconv.Itoa(int(pbb.Nonce))
			data.RedisClient.Set(key, pbb.Uid, 6*time.Hour)
		}
	case SUI:
		if pbb.ParseData != "" {
			parseDataMap := make(map[string]interface{})
			if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &parseDataMap); jsonErr == nil {
				tokenMap, ok := parseDataMap["token"].(map[string]interface{})
				if ok {
					tokenAddress, _ := tokenMap["address"].(string)
					if tokenAddress != "" {
						tokenType, _ := tokenMap["token_type"].(string)
						if tokenType == "" {
							tokenInfo, err := GetTokenInfoRetryAlert(context.Background(), pbb.ChainName, tokenAddress)
							if err != nil {
								log.Error("插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("chainName", pbb.ChainName), zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
							} else {
								tokenMap["token_uri"] = tokenInfo.TokenUri
								parseData, _ := utils.JsonEncode(parseDataMap)
								pbb.ParseData = parseData
								tokenInfoStr, _ = utils.JsonEncode(tokenMap)
							}
						}
					}
				}
			}
		}

		suiRecord := &data.SuiTransactionRecord{
			TransactionHash: pbb.TransactionHash,
			FromAddress:     pbb.FromAddress,
			ToAddress:       pbb.ToAddress,
			FromUid:         pbb.Uid,
			FeeAmount:       fa,
			Amount:          a,
			Status:          pbb.Status,
			TxTime:          pbb.TxTime,
			ContractAddress: pbb.ContractAddress,
			ParseData:       pbb.ParseData,
			GasLimit:        feeDataMap["gas_limit"],
			GasUsed:         feeDataMap["gas_used"],
			Data:            pbb.Data,
			EventLog:        pbb.EventLog,
			LogAddress:      logAddress,
			TransactionType: pbb.TransactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			TokenInfo:       tokenInfoStr,
			SendTime:        sendTime,
			SessionId:       sessionId,
			ShortHost:       shortHost,
			TokenGasless:    tokenGasless,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.SuiTransactionRecordRepoClient.SaveOrUpdateClient(ctx, GetTableName(pbb.ChainName), suiRecord)
	case SOLANA:
		if pbb.ParseData != "" {
			parseDataMap := make(map[string]interface{})
			if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &parseDataMap); jsonErr == nil {
				tokenMap, ok := parseDataMap["token"].(map[string]interface{})
				if ok {
					tokenAddress, _ := tokenMap["address"].(string)
					if tokenAddress != "" {
						tokenType, _ := tokenMap["token_type"].(string)
						if tokenType == "" {
							tokenInfo, err := GetTokenInfoRetryAlert(context.Background(), pbb.ChainName, tokenAddress)
							if err != nil {
								log.Error("插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("chainName", pbb.ChainName), zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
							} else {
								tokenMap["token_uri"] = tokenInfo.TokenUri
								parseData, _ := utils.JsonEncode(parseDataMap)
								pbb.ParseData = parseData
								tokenInfoStr, _ = utils.JsonEncode(tokenMap)
							}
						}
					}
				}
			}
		}

		solRecord := &data.SolTransactionRecord{
			BlockHash:       pbb.BlockHash,
			BlockNumber:     int(pbb.BlockNumber),
			TransactionHash: pbb.TransactionHash,
			FromAddress:     pbb.FromAddress,
			ToAddress:       pbb.ToAddress,
			FromUid:         pbb.Uid,
			FeeAmount:       fa,
			Amount:          a,
			Status:          pbb.Status,
			TxTime:          pbb.TxTime,
			ContractAddress: pbb.ContractAddress,
			ParseData:       pbb.ParseData,
			Data:            pbb.Data,
			EventLog:        pbb.EventLog,
			LogAddress:      logAddress,
			TransactionType: pbb.TransactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			TokenInfo:       tokenInfoStr,
			SendTime:        sendTime,
			SessionId:       sessionId,
			ShortHost:       shortHost,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}

		result, err = data.SolTransactionRecordRepoClient.SaveOrUpdateClient(ctx, GetTableName(pbb.ChainName), solRecord)
	case NERVOS:
		if pbb.ParseData != "" {
			parseDataMap := make(map[string]interface{})
			if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &parseDataMap); jsonErr == nil {
				tokenMap, ok := parseDataMap["token"].(map[string]interface{})
				if ok {
					tokenAddress, _ := tokenMap["address"].(string)
					if tokenAddress != "" {
						tokenType, _ := tokenMap["token_type"].(string)
						if tokenType == "" {
							tokenInfo, err := GetTokenInfoRetryAlert(context.Background(), pbb.ChainName, tokenAddress)
							if err != nil {
								log.Error("插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("chainName", pbb.ChainName), zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
							} else {
								tokenMap["token_uri"] = tokenInfo.TokenUri
								parseData, _ := utils.JsonEncode(parseDataMap)
								pbb.ParseData = parseData
								tokenInfoStr, _ = utils.JsonEncode(tokenMap)
							}
						}
					}
				}
			}
		}

		ckbTransactionRecord := &data.CkbTransactionRecord{
			BlockHash:       pbb.BlockHash,
			BlockNumber:     int(pbb.BlockNumber),
			TransactionHash: pbb.TransactionHash,
			FromAddress:     pbb.FromAddress,
			ToAddress:       pbb.ToAddress,
			FromUid:         pbb.Uid,
			FeeAmount:       fa,
			Amount:          a,
			Status:          pbb.Status,
			TxTime:          pbb.TxTime,
			ContractAddress: pbb.ContractAddress,
			ParseData:       pbb.ParseData,
			TransactionType: pbb.TransactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			TokenInfo:       tokenInfoStr,
			SendTime:        sendTime,
			SessionId:       sessionId,
			ShortHost:       shortHost,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.CkbTransactionRecordRepoClient.SaveOrUpdateClient(ctx, GetTableName(pbb.ChainName), ckbTransactionRecord)
		if result > 0 {
			//修改 未花费
			tx, err := GetNervosUTXOTransaction(pbb.TransactionHash)
			if err != nil {
				log.Error("插入pending记录，从节点中查询UTXO失败", zap.Any("chainName", pbb.ChainName), zap.Any("txHash", pbb.TransactionHash), zap.Any("error", err))
			}
			if tx != nil {
				cellInputs := tx.Transaction.Inputs
				for _, ci := range cellInputs {
					th := ci.PreviousOutput.TxHash.String()
					index := ci.PreviousOutput.Index
					//更新 状态为pending
					_, err := data.NervosCellRecordRepoClient.SaveOrUpdate(ctx, &data.NervosCellRecord{
						Uid:                pbb.Uid,
						TransactionHash:    th,
						UseTransactionHash: pbb.TransactionHash,
						Index:              int(index),
						Status:             "4",
						UpdatedAt:          time.Now().Unix(),
					})
					if err != nil {
						log.Error("插入pending记录，将UTXO更新到数据库中失败", zap.Any("chainName", pbb.ChainName), zap.Any("txHash", pbb.TransactionHash), zap.Any("error", err))
					}
				}
			}
		}
	case COSMOS:
		if pbb.ParseData != "" {
			parseDataMap := make(map[string]interface{})
			if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &parseDataMap); jsonErr == nil {
				evmMap := parseDataMap["cosmos"]
				ret := evmMap.(map[string]interface{})
				nonceInt, _ := utils.GetInt(ret["sequence_number"])
				pbb.Nonce = int64(nonceInt)

				tokenMap, ok := parseDataMap["token"].(map[string]interface{})
				if ok {
					tokenAddress, _ := tokenMap["address"].(string)
					if tokenAddress != "" {
						tokenType, _ := tokenMap["token_type"].(string)
						if tokenType == "" {
							tokenInfo, err := GetTokenInfoRetryAlert(context.Background(), pbb.ChainName, tokenAddress)
							if err != nil {
								log.Error("插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("chainName", pbb.ChainName), zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
							} else {
								tokenMap["token_uri"] = tokenInfo.TokenUri
								parseData, _ := utils.JsonEncode(parseDataMap)
								pbb.ParseData = parseData
								tokenInfoStr, _ = utils.JsonEncode(tokenMap)
							}
						}
					}
				}
			}
		}
		memo := pbb.Memo
		if pbb.Data != "" {
			dataMap := make(map[string]interface{})
			if jsonErr := json.Unmarshal([]byte(pbb.Data), &dataMap); jsonErr == nil {
				memo, _ = dataMap["memo"].(string)
			}
		}

		atomRecord := &data.AtomTransactionRecord{
			BlockHash:       pbb.BlockHash,
			BlockNumber:     int(pbb.BlockNumber),
			Nonce:           pbb.Nonce,
			TransactionHash: pbb.TransactionHash,
			FromAddress:     pbb.FromAddress,
			ToAddress:       pbb.ToAddress,
			FromUid:         pbb.Uid,
			FeeAmount:       fa,
			Amount:          a,
			Status:          pbb.Status,
			TxTime:          pbb.TxTime,
			ContractAddress: pbb.ContractAddress,
			ParseData:       pbb.ParseData,
			GasLimit:        feeDataMap["gas_limit"],
			GasUsed:         feeDataMap["gas_used"],
			GasPrice:        feeDataMap["gas_price"],
			Data:            pbb.Data,
			EventLog:        pbb.EventLog,
			LogAddress:      logAddress,
			TransactionType: pbb.TransactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			TokenInfo:       tokenInfoStr,
			SendTime:        sendTime,
			SessionId:       sessionId,
			ShortHost:       shortHost,
			Memo:            memo,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.AtomTransactionRecordRepoClient.SaveOrUpdateClient(ctx, GetTableName(pbb.ChainName), atomRecord)
		if result == 1 {
			key := pendingNonceKey + strconv.Itoa(int(pbb.Nonce))
			data.RedisClient.Set(key, pbb.Uid, 6*time.Hour)
		}
	case KASPA:
		kasTransactionRecord := &data.KasTransactionRecord{
			BlockHash:       pbb.BlockHash,
			BlockNumber:     int(pbb.BlockNumber),
			TransactionHash: pbb.TransactionHash,
			FromAddress:     pbb.FromAddress,
			ToAddress:       pbb.ToAddress,
			FromUid:         pbb.Uid,
			FeeAmount:       fa,
			Amount:          a,
			Status:          pbb.Status,
			TxTime:          pbb.TxTime,
			ConfirmCount:    0,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			SendTime:        sendTime,
			SessionId:       sessionId,
			ShortHost:       shortHost,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.KasTransactionRecordRepoClient.SaveOrUpdateClient(ctx, GetTableName(pbb.ChainName), kasTransactionRecord)
		if result > 0 {
			//修改 未花费
			go KaspaUpdateUtxo(pbb)
		}
	}

	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链插入pending记录，将数据插入到数据库中失败，txHash:%s", pbb.ChainName, pbb.TransactionHash)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("插入pending记录，将数据插入到数据库中失败", zap.Any("chainName", pbb.ChainName), zap.Any("txHash", pbb.TransactionHash), zap.Any("error", err))
	}

	flag := result == 1
	return &pb.CreateResponse{
		Status: flag,
		Code:   uint64(200),
		Mes:    "",
	}, err
}

func addTokenUri(tokenInfoStr, chainName, transactionHash string) string {
	var tokenInfo *types.TokenInfo
	if err := json.Unmarshal([]byte(tokenInfoStr), &tokenInfo); err == nil {
		if tokenInfo != nil && tokenInfo.Address != "" {
			tokenType := tokenInfo.TokenType
			if tokenType == "" || tokenType == "ERC20" {
				ti, err := GetTokenInfoRetryAlert(context.Background(), chainName, tokenInfo.Address)
				if err != nil {
					log.Error("插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("chainName", chainName), zap.Any("txHash", transactionHash), zap.Any("tokenAddress", tokenInfo.Address), zap.Any("error", err))
				} else {
					tokenInfo.TokenUri = ti.TokenUri
					tir, _ := utils.JsonEncode(tokenInfo)
					return tir
				}
			}
		}
	}
	return tokenInfoStr
}

func parseClientData(clientData string) (int64, string, string, string) {
	var sendTime int64
	var sessionId, shortHost, tokenGasless string
	clientDataMap := make(map[string]interface{})
	if jsonErr := json.Unmarshal([]byte(clientData), &clientDataMap); jsonErr == nil {
		if sendTimei, ok := clientDataMap["sendTime"]; ok {
			sendTimeInt, _ := utils.GetInt(sendTimei)
			sendTime = int64(sendTimeInt)
		}
		sessionId, _ = clientDataMap["sessionId"].(string)
		if s, ok := clientDataMap["shortHost"]; ok {
			shortHost, _ = s.(string)
		}
		if dappTxInfo, ok := clientDataMap["dappTxinfo"]; ok {
			if gasless, ok := dappTxInfo.(map[string]interface{})["tokenGasless"]; ok {
				tokenGasless, _ = utils.JsonEncode(gasless)
			}
		}
	}
	return sendTime, sessionId, shortHost, tokenGasless
}

func isGasLess(tokenGasless string) (bool, string) {
	mmm := map[string]interface{}{}
	err := json.Unmarshal([]byte(tokenGasless), &mmm)
	if err != nil {
		return false, ""
	}

	// TODO(wanghui): use gasFeeInfo and transactionType instead
	txType, ok := mmm["tx_type"].(string)
	if !ok {
		return false, ""
	}

	if txType == SWAP || txType == TRANSFER {
		return true, txType
	}

	return false, ""
}

func UpdateTransactionType(pbb *pb.TransactionReq) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("UpdateTransactionType error, chainName:"+pbb.ChainName, e)
			} else {
				log.Errore("UpdateTransactionType panic, chainName:"+pbb.ChainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链插入pending交易记录时更新用户TransactionType，txHash:%s, error：%s", pbb.ChainName, pbb.TransactionHash, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()
	chainType, _ := GetChainNameType(pbb.ChainName)

	transactionType := pbb.TransactionType
	if (len(pbb.Data) >= 10 && strings.HasPrefix(pbb.Data, "0x")) || (len(pbb.Data) >= 8 && !strings.HasPrefix(pbb.Data, "0x")) {
		contractAddress := pbb.ContractAddress
		if contractAddress == "" {
			contractAddress = pbb.ToAddress
		}
		var methodId string
		if strings.HasPrefix(pbb.Data, "0x") {
			methodId = pbb.Data[2:10]
		} else {
			methodId = pbb.Data[:8]
		}
		methodName, err := GetMethodNameRetryAlert(nil, pbb.ChainName, contractAddress, methodId)
		if err != nil {
			log.Warn("查询nodeProxy中合约ABI失败", zap.Any("chainName", pbb.ChainName), zap.Any("txHash", pbb.TransactionHash), zap.Any("contractAddress", contractAddress), zap.Any("methodId", methodId), zap.Any("error", err))
		}

		if strings.Contains(methodName, "Mint") || strings.Contains(methodName, "_mint") || strings.HasPrefix(methodName, "mint") {
			transactionType = MINT
		}

		if strings.Contains(methodName, "Swap") || strings.Contains(methodName, "_swap") || strings.HasPrefix(methodName, "swap") {
			transactionType = SWAP
		}

		if strings.Contains(methodName, "AddLiquidity") || strings.Contains(methodName, "_add_liquidity") ||
			strings.HasPrefix(methodName, "addLiquidity") || strings.HasPrefix(methodName, "add_liquidity") ||
			(strings.HasPrefix(methodName, "add") && strings.Contains(methodName, "Liquidity")) ||
			(strings.HasPrefix(methodName, "add") && strings.Contains(methodName, "_liquidity")) {
			transactionType = ADDLIQUIDITY
		}
	}
	if transactionType != CONTRACT {
		if chainType == EVM {
			data.EvmTransactionRecordRepoClient.UpdateTransactionTypeByTxHash(nil, GetTableName(pbb.ChainName), pbb.TransactionHash, transactionType)
		}
		if chainType == TVM {
			data.TrxTransactionRecordRepoClient.UpdateTransactionTypeByTxHash(nil, GetTableName(pbb.ChainName), pbb.TransactionHash, transactionType)
		}
	}
}

func UpdateUtxo(pbb *pb.TransactionReq) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("UpdateUtxo error, chainName:"+pbb.ChainName, e)
			} else {
				log.Errore("UpdateUtxo panic, chainName:"+pbb.ChainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链插入pending交易记录时更新用户UTXO状态，txHash:%s, error：%s", pbb.ChainName, pbb.TransactionHash, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	time.Sleep(time.Duration(1) * time.Minute)
	tx, err := GetTxByHashFuncMap[pbb.ChainName](pbb.TransactionHash)
	for i := 0; i < 10 && err != nil; i++ {
		time.Sleep(time.Duration(i*5) * time.Second)
		tx, err = GetTxByHashFuncMap[pbb.ChainName](pbb.TransactionHash)
	}
	if err != nil {
		// 更新用户资产出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链插入pending记录，更新用户UTXO状态，请求节点查询交易记录占用的UTXO失败，txHash:%s", pbb.ChainName, pbb.TransactionHash)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("插入pending记录，更新用户UTXO状态，请求节点查询交易记录占用的UTXO失败", zap.Any("chainName", pbb.ChainName), zap.Any("txHash", pbb.TransactionHash),
			zap.Any("fromAddress", pbb.FromAddress), zap.Any("error", err))
		return
	}
	log.Info(pbb.ChainName, zap.Any("更新UTXO状态为pending 4", tx))
	cellInputs := tx.Inputs
	for _, ci := range cellInputs {
		th := ci.PrevHash
		index := ci.OutputIndex
		//更新 状态为pending
		_, err := data.UtxoUnspentRecordRepoClient.UpdateUnspentToPending(nil, pbb.ChainName, pbb.FromAddress, index, th, pbb.TransactionHash)
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链插入pending记录，更新用户UTXO状态，将UTXO插入到数据库中失败，txHash:%s", pbb.ChainName, pbb.TransactionHash)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error("插入pending记录，更新用户UTXO状态，将UTXO插入到数据库中失败", zap.Any("chainName", pbb.ChainName), zap.Any("txHash", pbb.TransactionHash),
				zap.Any("fromAddress", pbb.FromAddress), zap.Any("error", err))
		}
	}
}

func KaspaUpdateUtxo(pbb *pb.TransactionReq) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("KaspaUpdateUtxo error, chainName:"+pbb.ChainName, e)
			} else {
				log.Errore("KaspaUpdateUtxo panic, chainName:"+pbb.ChainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链插入pending交易记录时更新用户UTXO状态，txHash:%s, error：%s", pbb.ChainName, pbb.TransactionHash, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	time.Sleep(time.Duration(1) * time.Minute)
	tx, err := GetKaspaUTXOTransaction(pbb.TransactionHash)
	for i := 0; i < 10 && err != nil; i++ {
		time.Sleep(time.Duration(i*5) * time.Second)
		tx, err = GetKaspaUTXOTransaction(pbb.TransactionHash)
	}
	if err != nil {
		// 更新用户资产出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链插入pending记录，更新用户UTXO状态，请求节点查询交易记录占用的UTXO失败，txHash:%s", pbb.ChainName, pbb.TransactionHash)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("插入pending记录，更新用户UTXO状态，请求节点查询交易记录占用的UTXO失败", zap.Any("chainName", pbb.ChainName), zap.Any("txHash", pbb.TransactionHash),
			zap.Any("fromAddress", pbb.FromAddress), zap.Any("error", err))
		return
	}

	var utxoRecords []*data.UtxoUnspentRecord
	inputs := tx.Inputs
	for _, ci := range inputs {
		index := ci.PreviousOutpointIndex
		preIndex, _ := strconv.Atoi(index)
		var utxoUnspentRecord = &data.UtxoUnspentRecord{
			ChainName: pbb.ChainName,
			Uid:       pbb.Uid,
			Address:   pbb.FromAddress,
			Hash:      ci.PreviousOutpointHash,
			N:         preIndex,
			//Script:    ci.SignatureScript,
			Unspent: data.UtxoStatusPending, //1 未花费 2 已花费 联合索引
			//Amount:    ci.UtxoEntry.Amount,
			//TxTime:    txTime,
			UpdatedAt: time.Now().Unix(),
		}
		utxoRecords = append(utxoRecords, utxoUnspentRecord)
	}

	_, err = data.UtxoUnspentRecordRepoClient.BatchSaveOrUpdate(nil, utxoRecords)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		_, err = data.UtxoUnspentRecordRepoClient.BatchSaveOrUpdate(nil, utxoRecords)
	}
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链插入pending记录，更新用户UTXO状态，将UTXO插入到数据库中失败，txHash:%s", pbb.ChainName, pbb.TransactionHash)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("插入pending记录，更新用户UTXO状态，将UTXO插入到数据库中失败", zap.Any("chainName", pbb.ChainName), zap.Any("txHash", pbb.TransactionHash),
			zap.Any("fromAddress", pbb.FromAddress), zap.Any("error", err))
	}
}

func (s *TransactionUsecase) GetTransactionByHash(ctx context.Context, chainName, hash string) (*pb.TransactionRecord, error) {
	chainType, _ := GetChainNameType(chainName)

	var record *pb.TransactionRecord
	var err error
	switch chainType {
	case POLKADOT:
		var oldRecord *data.DotTransactionRecord
		oldRecord, err = data.DotTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(chainName), hash)
		if err == nil {
			err = utils.CopyProperties(oldRecord, &record)
		}
	case CASPER:
		var oldRecord *data.CsprTransactionRecord
		oldRecord, err = data.CsprTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(chainName), hash)
		if err == nil {
			err = utils.CopyProperties(oldRecord, &record)
		}
	case NERVOS:
		var oldRecord *data.CkbTransactionRecord
		oldRecord, err = data.CkbTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(chainName), hash)
		if err == nil {
			err = utils.CopyProperties(oldRecord, &record)
		}
	case BTC:
		var oldRecord *data.BtcTransactionRecord
		oldRecord, err = data.BtcTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(chainName), hash)
		if err == nil {
			err = utils.CopyProperties(oldRecord, &record)
		}

		amount := utils.StringDecimals(record.Amount, 8)
		feeAmount := utils.StringDecimals(record.FeeAmount, 8)
		record.Amount = amount
		record.FeeAmount = feeAmount
		record.TransactionType = NATIVE
	case EVM:
		var oldRecord *data.EvmTransactionRecord
		oldRecord, err = data.EvmTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(chainName), hash)
		if err == nil {
			err = utils.CopyProperties(oldRecord, &record)
			handleNativeTokenEvent(chainName, record)
		}
	case STC:
		var oldRecord *data.StcTransactionRecord
		oldRecord, err = data.StcTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(chainName), hash)
		if err == nil {
			err = utils.CopyProperties(oldRecord, &record)
		}
	case TVM:
		var oldRecord *data.TrxTransactionRecord
		oldRecord, err = data.TrxTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(chainName), hash)
		if err == nil {
			err = utils.CopyProperties(oldRecord, &record)
		}
	case APTOS:
		var oldRecord *data.AptTransactionRecord
		oldRecord, err = data.AptTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(chainName), hash)
		if err == nil {
			err = utils.CopyProperties(oldRecord, &record)
		}
	case SUI:
		var oldRecord *data.SuiTransactionRecord
		oldRecord, err = data.SuiTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(chainName), hash)
		if err == nil {
			err = utils.CopyProperties(oldRecord, &record)
		}
	case SOLANA:
		var oldRecord *data.SolTransactionRecord
		oldRecord, err = data.SolTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(chainName), hash)
		if err == nil {
			err = utils.CopyProperties(oldRecord, &record)
		}
	case COSMOS:
		var oldRecord *data.AtomTransactionRecord
		oldRecord, err = data.AtomTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(chainName), hash)
		if err == nil {
			err = utils.CopyProperties(oldRecord, &record)
		}
	case KASPA:
		var oldRecord *data.KasTransactionRecord
		oldRecord, err = data.KasTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(chainName), hash)
		if err == nil {
			err = utils.CopyProperties(oldRecord, &record)
		}
	}

	if record == nil {
		return nil, errors.New("transaction not found , hash : " + hash)
	}

	if err == nil {
		convertFeeData(chainName, chainType, "", record)
	}
	return record, err
}

func (s *TransactionUsecase) PageList(ctx context.Context, req *pb.PageListRequest) (*pb.PageListResponse, error) {
	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		if req.ContractAddress != "" {
			req.ContractAddress = types2.HexToAddress(req.ContractAddress).Hex()
		}
		if req.TokenAddress != "" && req.TokenAddress != data.MAIN_ADDRESS_PARAM {
			req.TokenAddress = types2.HexToAddress(req.TokenAddress).Hex()
		}
		req.FromAddressList = utils.HexToAddress(req.FromAddressList)
		req.ToAddressList = utils.HexToAddress(req.ToAddressList)
		if req.Address != "" {
			req.Address = types2.HexToAddress(req.Address).Hex()
		}
	case COSMOS:
		if req.ContractAddress != "" {
			req.ContractAddress = utils.AddressIbcToLower(req.ContractAddress)
		}
		if req.TokenAddress != "" && req.TokenAddress != data.MAIN_ADDRESS_PARAM {
			req.TokenAddress = utils.AddressIbcToLower(req.TokenAddress)
		}
	}

	orderBys := strings.Split(req.OrderBy, " ")
	orderByColumn := orderBys[0]

	var request *data.TransactionRequest
	utils.CopyProperties(req, &request)
	if req.Nonce == nil {
		request.Nonce = -1
	}

	var result = &pb.PageListResponse{}
	var total int64
	var list []*pb.TransactionRecord
	var err error

	switch chainType {
	case POLKADOT:
		var recordList []*data.DotTransactionRecord
		recordList, total, err = data.DotTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case CASPER:
		var recordList []*data.CsprTransactionRecord
		recordList, total, err = data.CsprTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case NERVOS:
		var recordList []*data.CkbTransactionRecord
		recordList, total, err = data.CkbTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case BTC:
		var recordList []*data.BtcTransactionRecord
		recordList, total, err = data.BtcTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
			if len(list) > 0 {
				for _, record := range list {
					amount := utils.StringDecimals(record.Amount, 8)
					feeAmount := utils.StringDecimals(record.FeeAmount, 8)
					record.Amount = amount
					record.FeeAmount = feeAmount
					record.TransactionType = NATIVE
				}
			}
		}
	case EVM:
		var recordList []*data.EvmTransactionRecord
		recordList, total, err = data.EvmTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
		if len(list) > 0 {
			for _, record := range list {
				//将发送给合约的主币转成一条eventLog
				handleNativeTokenEvent(req.ChainName, record)
			}
		}
	case STC:
		var recordList []*data.StcTransactionRecord
		recordList, total, err = data.StcTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case TVM:
		var recordList []*data.TrxTransactionRecord
		recordList, total, err = data.TrxTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		for _, rl := range recordList {
			var ptr *pb.TransactionRecord
			err = utils.CopyProperties(rl, &ptr)
			list = append(list, ptr)
		}
		if len(list) > 0 {
			for _, record := range list {
				//将发送给合约的主币转成一条eventLog
				handleNativeTokenEvent(req.ChainName, record)
			}
		}
	case APTOS:
		var recordList []*data.AptTransactionRecord
		recordList, total, err = data.AptTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case SUI:
		var recordList []*data.SuiTransactionRecord
		recordList, total, err = data.SuiTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case SOLANA:
		var recordList []*data.SolTransactionRecord
		recordList, total, err = data.SolTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case COSMOS:
		var recordList []*data.AtomTransactionRecord
		recordList, total, err = data.AtomTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case KASPA:
		var recordList []*data.KasTransactionRecord
		recordList, total, err = data.KasTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
			if len(list) > 0 {
				for _, record := range list {
					record.TransactionType = NATIVE
				}
			}
		}
	}
	if err == nil {
		result.Total = total
		result.List = list
		if len(list) > 0 {
			for _, record := range list {
				if record == nil {
					continue
				}

				if orderByColumn == "id" {
					record.Cursor = record.Id
				} else if orderByColumn == "block_number" {
					record.Cursor = record.BlockNumber
				} else if orderByColumn == "nonce" {
					record.Cursor = record.Nonce
				} else if orderByColumn == "tx_time" {
					record.Cursor = record.TxTime
				} else if orderByColumn == "created_at" {
					record.Cursor = record.CreatedAt
				} else if orderByColumn == "updated_at" {
					record.Cursor = record.UpdatedAt
				}

				convertFeeData(req.ChainName, chainType, req.Address, record)
			}
		}
	}
	return result, err
}

func (s *TransactionUsecase) ClientPageList(ctx context.Context, req *pb.PageListRequest) (*pb.PageListResponse, error) {
	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		if req.ContractAddress != "" {
			req.ContractAddress = types2.HexToAddress(req.ContractAddress).Hex()
		}
		if req.TokenAddress != "" && req.TokenAddress != data.MAIN_ADDRESS_PARAM {
			req.TokenAddress = types2.HexToAddress(req.TokenAddress).Hex()
		}
		req.FromAddressList = utils.HexToAddress(req.FromAddressList)
		req.ToAddressList = utils.HexToAddress(req.ToAddressList)
		if req.Address != "" {
			req.Address = types2.HexToAddress(req.Address).Hex()
		}
	case COSMOS:
		if req.ContractAddress != "" {
			req.ContractAddress = utils.AddressIbcToLower(req.ContractAddress)
		}
		if req.TokenAddress != "" && req.TokenAddress != data.MAIN_ADDRESS_PARAM {
			req.TokenAddress = utils.AddressIbcToLower(req.TokenAddress)
		}
	}

	orderBys := strings.Split(req.OrderBy, " ")
	orderByColumn := orderBys[0]
	orderByDirection := orderBys[1]

	var request *data.TransactionRequest
	utils.CopyProperties(req, &request)
	if req.Nonce == nil {
		request.Nonce = -1
	}

	var result = &pb.PageListResponse{}
	var total int64
	var list []*pb.TransactionRecord
	var err error

	switch chainType {
	case POLKADOT:
		var recordList []*data.DotTransactionRecord
		recordList, total, err = data.DotTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case CASPER:
		var recordList []*data.CsprTransactionRecord
		recordList, total, err = data.CsprTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case NERVOS:
		var recordList []*data.CkbTransactionRecord
		recordList, total, err = data.CkbTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case BTC:
		var recordList []*data.BtcTransactionRecord
		recordList, total, err = data.BtcTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
			if len(list) > 0 {
				for _, record := range list {
					amount := utils.StringDecimals(record.Amount, 8)
					feeAmount := utils.StringDecimals(record.FeeAmount, 8)
					record.Amount = amount
					record.FeeAmount = feeAmount
					record.TransactionType = NATIVE
				}
			}
		}
	case EVM:
		var recordList []*data.EvmTransactionRecordWrapper
		recordList, total, err = data.EvmTransactionRecordRepoClient.PageListRecord(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}

		if len(list) > 0 {
			var records []*data.EvmTransactionRecord
			var originalHashList, transactionHashList, transactionHashNotInList []string
			for _, record := range list {
				if record.OriginalHash == "" {
					originalHashList = append(originalHashList, record.TransactionHash)
				} else {
					originalHashList = append(originalHashList, record.OriginalHash)
					transactionHashList = append(transactionHashList, record.OriginalHash)
					transactionHashNotInList = append(transactionHashNotInList, record.TransactionHash)
				}

				handleNativeTokenEvent(req.ChainName, record)
			}
			operateRequest := &data.TransactionRequest{
				Nonce:                    -1,
				TransactionHashNotInList: transactionHashNotInList,
				OrParamList: []data.TransactionRequest{
					{
						TransactionHashList: transactionHashList,
						OriginalHashList:    originalHashList,
					},
				},
				OrderBy: req.OrderBy,
			}

			records, err = data.EvmTransactionRecordRepoClient.List(ctx, GetTableName(req.ChainName), operateRequest)
			if err == nil && len(records) > 0 {
				recordMap := make(map[string][]*pb.TransactionRecord)
				for _, record := range records {
					var hash string
					if record.OriginalHash == "" {
						hash = record.TransactionHash
					} else {
						hash = record.OriginalHash
					}
					operateRecordList, ok := recordMap[hash]
					if !ok {
						operateRecordList = make([]*pb.TransactionRecord, 0)
					}

					//将发送给合约的主币转成一条eventLog
					if (record.TransactionType == CONTRACT || record.TransactionType == MINT || record.TransactionType == SWAP) && record.Amount.String() != "" && record.Amount.String() != "0" {
						eventLogStr := handleEventLog(req.ChainName, record.FromAddress, record.ToAddress, record.Amount.String(), record.EventLog)
						record.EventLog = eventLogStr
					}

					var pbRecord *pb.TransactionRecord
					err = utils.CopyProperties(record, &pbRecord)
					operateRecordList = append(operateRecordList, pbRecord)
					recordMap[hash] = operateRecordList
				}

				for i, record := range list {
					var hash string
					if record.OriginalHash == "" {
						hash = record.TransactionHash
					} else {
						hash = record.OriginalHash
					}
					operateRecordList, ok := recordMap[hash]
					if ok {
						operateRecordList = append(operateRecordList, record)
						sort.SliceStable(operateRecordList, func(i, j int) bool {
							iTxTime := operateRecordList[i].TxTime
							jTxTime := operateRecordList[j].TxTime
							return iTxTime > jTxTime
						})
						record = operateRecordList[0]

						operateRecordList = operateRecordList[1:]
						sort.SliceStable(operateRecordList, func(i, j int) bool {
							var iValue, jValue int64

							if orderByColumn == "id" {
								iValue = operateRecordList[i].Id
								jValue = operateRecordList[j].Id
							} else if orderByColumn == "block_number" {
								iValue = operateRecordList[i].BlockNumber
								jValue = operateRecordList[j].BlockNumber
							} else if orderByColumn == "nonce" {
								iValue = operateRecordList[i].Nonce
								jValue = operateRecordList[j].Nonce
							} else if orderByColumn == "tx_time" {
								iValue = operateRecordList[i].TxTime
								jValue = operateRecordList[j].TxTime
							} else if orderByColumn == "created_at" {
								iValue = operateRecordList[i].CreatedAt
								jValue = operateRecordList[j].CreatedAt
							} else if orderByColumn == "updated_at" {
								iValue = operateRecordList[i].UpdatedAt
								jValue = operateRecordList[j].UpdatedAt
							}

							if strings.EqualFold(orderByDirection, "asc") {
								return iValue < jValue
							} else {
								return iValue > jValue
							}
						})
						record.OperateRecordList = operateRecordList
						list[i] = record
					}
				}
			}
		}
	case STC:
		var recordList []*data.StcTransactionRecordWrapper
		recordList, total, err = data.StcTransactionRecordRepoClient.PageListRecord(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}

		if len(list) > 0 {
			var records []*data.StcTransactionRecord
			var originalHashList, transactionHashList, transactionHashNotInList []string
			for _, record := range list {
				if record.OriginalHash == "" {
					originalHashList = append(originalHashList, record.TransactionHash)
				} else {
					originalHashList = append(originalHashList, record.OriginalHash)
					transactionHashList = append(transactionHashList, record.OriginalHash)
					transactionHashNotInList = append(transactionHashNotInList, record.TransactionHash)
				}
			}
			operateRequest := &data.TransactionRequest{
				Nonce:                    -1,
				TransactionHashNotInList: transactionHashNotInList,
				OrParamList: []data.TransactionRequest{
					{
						TransactionHashList: transactionHashList,
						OriginalHashList:    originalHashList,
					},
				},
				OrderBy: req.OrderBy,
			}

			records, err = data.StcTransactionRecordRepoClient.List(ctx, GetTableName(req.ChainName), operateRequest)
			if err == nil && len(records) > 0 {
				recordMap := make(map[string][]*pb.TransactionRecord)
				for _, record := range records {
					var hash string
					if record.OriginalHash == "" {
						hash = record.TransactionHash
					} else {
						hash = record.OriginalHash
					}
					operateRecordList, ok := recordMap[hash]
					if !ok {
						operateRecordList = make([]*pb.TransactionRecord, 0)
					}

					var pbRecord *pb.TransactionRecord
					err = utils.CopyProperties(record, &pbRecord)
					operateRecordList = append(operateRecordList, pbRecord)
					recordMap[hash] = operateRecordList
				}

				for i, record := range list {
					var hash string
					if record.OriginalHash == "" {
						hash = record.TransactionHash
					} else {
						hash = record.OriginalHash
					}
					operateRecordList, ok := recordMap[hash]
					if ok {
						operateRecordList = append(operateRecordList, record)
						sort.SliceStable(operateRecordList, func(i, j int) bool {
							iTxTime := operateRecordList[i].TxTime
							jTxTime := operateRecordList[j].TxTime
							return iTxTime > jTxTime
						})
						record = operateRecordList[0]

						operateRecordList = operateRecordList[1:]
						sort.SliceStable(operateRecordList, func(i, j int) bool {
							var iValue, jValue int64

							if orderByColumn == "id" {
								iValue = operateRecordList[i].Id
								jValue = operateRecordList[j].Id
							} else if orderByColumn == "block_number" {
								iValue = operateRecordList[i].BlockNumber
								jValue = operateRecordList[j].BlockNumber
							} else if orderByColumn == "nonce" {
								iValue = operateRecordList[i].Nonce
								jValue = operateRecordList[j].Nonce
							} else if orderByColumn == "tx_time" {
								iValue = operateRecordList[i].TxTime
								jValue = operateRecordList[j].TxTime
							} else if orderByColumn == "created_at" {
								iValue = operateRecordList[i].CreatedAt
								jValue = operateRecordList[j].CreatedAt
							} else if orderByColumn == "updated_at" {
								iValue = operateRecordList[i].UpdatedAt
								jValue = operateRecordList[j].UpdatedAt
							}

							if strings.EqualFold(orderByDirection, "asc") {
								return iValue < jValue
							} else {
								return iValue > jValue
							}
						})
						record.OperateRecordList = operateRecordList
						list[i] = record
					}
				}
			}
		}
	case TVM:
		var recordList []*data.TrxTransactionRecord
		recordList, total, err = data.TrxTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			for _, rl := range recordList {
				var ptr *pb.TransactionRecord
				err = utils.CopyProperties(rl, &ptr)
				list = append(list, ptr)
			}
		}
		if len(list) > 0 {
			for _, record := range list {
				//将发送给合约的主币转成一条eventLog
				handleNativeTokenEvent(req.ChainName, record)
			}
		}
	case APTOS:
		var recordList []*data.AptTransactionRecord
		recordList, total, err = data.AptTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case SUI:
		var recordList []*data.SuiTransactionRecord
		recordList, total, err = data.SuiTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case SOLANA:
		var recordList []*data.SolTransactionRecord
		recordList, total, err = data.SolTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case COSMOS:
		var recordList []*data.AtomTransactionRecord
		recordList, total, err = data.AtomTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case KASPA:
		var recordList []*data.KasTransactionRecord
		recordList, total, err = data.KasTransactionRecordRepoClient.PageList(ctx, GetTableName(req.ChainName), request)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
			if len(list) > 0 {
				for _, record := range list {
					record.TransactionType = NATIVE
				}
			}
		}
	}
	if err == nil {
		result.Total = total
		result.List = list
		if len(list) > 0 {
			for _, record := range list {
				if record == nil {
					continue
				}

				if orderByColumn == "id" {
					record.Cursor = record.Id
				} else if orderByColumn == "block_number" {
					record.Cursor = record.BlockNumber
				} else if orderByColumn == "nonce" {
					record.Cursor = record.Nonce
				} else if orderByColumn == "tx_time" {
					record.Cursor = record.TxTime
				} else if orderByColumn == "created_at" {
					record.Cursor = record.CreatedAt
				} else if orderByColumn == "updated_at" {
					record.Cursor = record.UpdatedAt
				}

				convertFeeData(req.ChainName, chainType, req.Address, record)
			}
		}
	}
	return result, err
}

func handleEventLog(chainName, recordFromAddress, recordToAddress, recordAmount, recordEventLog string) string {
	if recordAmount == "" || recordAmount == "0" {
		return recordEventLog
	}

	eventLogStr := recordEventLog
	var eventLogs []*types.EventLog
	if eventLogStr != "" {
		err := json.Unmarshal([]byte(eventLogStr), &eventLogs)
		if err != nil {
			log.Error("parse EventLog failed", zap.Any("chainName", chainName), zap.Any("eventLog", eventLogStr), zap.Any("error", err))
			return eventLogStr
		}

		var hasMain bool
		var mainTotal int
		//https://polygonscan.com/tx/0x8b455005112a9e744ec143ccfa81d4185fbc936162367eb33d8e9f6f704a6ec2
		mainAmount := new(big.Int)
		for _, eventLog := range eventLogs {
			if recordFromAddress == eventLog.From {
				if eventLog.Token.Address == "" {
					mainTotal++
					mainAmount = mainAmount.Add(mainAmount, eventLog.Amount)
					if recordToAddress == eventLog.To || recordAmount == eventLog.Amount.String() {
						hasMain = true
						break
					}
				} else {
					var mainSymbol string
					if platInfo, ok := GetChainPlatInfo(chainName); ok {
						mainSymbol = platInfo.NativeCurrency
					}
					if recordToAddress == eventLog.To && recordAmount == eventLog.Amount.String() && eventLog.Token.Symbol == mainSymbol {
						hasMain = true
						break
					}
				}
			}
		}
		if !hasMain && (mainTotal == 1 || recordAmount == mainAmount.String()) {
			hasMain = true
		}
		if !hasMain {
			amount, _ := new(big.Int).SetString(recordAmount, 0)
			eventLog := &types.EventLog{
				From:   recordFromAddress,
				To:     recordToAddress,
				Amount: amount,
			}
			eventLogs = append(eventLogs, eventLog)
			eventLogJson, _ := utils.JsonEncode(eventLogs)
			eventLogStr = eventLogJson
		}
	} else {
		amount, _ := new(big.Int).SetString(recordAmount, 0)
		eventLog := &types.EventLog{
			From:   recordFromAddress,
			To:     recordToAddress,
			Amount: amount,
		}
		eventLogs = append(eventLogs, eventLog)
		eventLogJson, _ := utils.JsonEncode(eventLogs)
		eventLogStr = eventLogJson
	}
	return eventLogStr
}

func (s *TransactionUsecase) GetAmount(ctx context.Context, req *pb.AmountRequest) (*pb.AmountResponse, error) {
	var result = &pb.AmountResponse{}
	var amount string
	var err error

	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		req.FromAddressList = utils.HexToAddress(req.FromAddressList)
		req.ToAddressList = utils.HexToAddress(req.ToAddressList)
	}

	switch chainType {
	case BTC:
		amount, err = data.BtcTransactionRecordRepoClient.GetAmount(ctx, GetTableName(req.ChainName), req, PENDING)
	case EVM:
		amount, err = data.EvmTransactionRecordRepoClient.GetAmount(ctx, GetTableName(req.ChainName), req, PENDING)
	case STC:
		amount, err = data.StcTransactionRecordRepoClient.GetAmount(ctx, GetTableName(req.ChainName), req, PENDING)
	case TVM:
		amount, err = data.TrxTransactionRecordRepoClient.GetAmount(ctx, GetTableName(req.ChainName), req, PENDING)
	case APTOS:
		amount, err = data.AptTransactionRecordRepoClient.GetAmount(ctx, GetTableName(req.ChainName), req, PENDING)
	case SUI:
		amount, err = data.SuiTransactionRecordRepoClient.GetAmount(ctx, GetTableName(req.ChainName), req, PENDING)
	case COSMOS:
		amount, err = data.AtomTransactionRecordRepoClient.GetAmount(ctx, GetTableName(req.ChainName), req, PENDING)
	}

	if err == nil {
		result.Amount = amount
	}
	return result, err
}

func (s *TransactionUsecase) GetDappListPageList(ctx context.Context, req *pb.DappPageListReq) (*pb.DappPageListResp, error) {
	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		if req.ContractAddress != "" {
			req.ContractAddress = types2.HexToAddress(req.ContractAddress).Hex()
		}
		if req.FromAddress != "" {
			req.FromAddress = types2.HexToAddress(req.FromAddress).Hex()
		}
	}

	dapps, err := data.DappApproveRecordRepoClient.GetDappListPageList(ctx, req)
	if err != nil {
		log.Errore("返回授权dapp列表报错！", err)
		return &pb.DappPageListResp{
			Ok: false,
		}, err
	}
	//分组 根据 token 然后sum出结果 过滤出amount len > 40的
	total := data.DappApproveRecordRepoClient.GetDappListPageCount(ctx, req)

	if len(dapps) == 0 {
		return &pb.DappPageListResp{
			Ok: true,
			Data: &pb.DappPageData{
				Page:  req.Page,
				Limit: req.Limit,
				Total: total,
			},
		}, err
	} else {
		var trs []*pb.TransactionRecord
		for _, value := range dapps {
			tokenAddress := value.Token
			dappChainType, _ := GetChainNameType(value.ChainName)
			feeData := make(map[string]string)
			switch dappChainType {
			case BTC:
				btc, err := data.BtcTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(value.ChainName), value.LastTxhash)
				if err == nil && btc != nil {
					var r *pb.TransactionRecord
					utils.CopyProperties(btc, &r)
					r.ChainName = value.ChainName
					r.Cursor = value.TxTime
					r.Amount = value.Amount
					trs = append(trs, r)
				}
			case EVM:
				evm, err := data.EvmTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(value.ChainName), value.LastTxhash)
				if err == nil && evm != nil {
					var r *pb.TransactionRecord
					utils.CopyProperties(evm, &r)

					if evm.TransactionType == APPROVENFT || evm.TransactionType == APPROVE {
						r.ParseData = evm.ParseData
						r.TokenInfo = evm.TokenInfo
					} else if value.ErcType == APPROVE {
						tokenInfo, err := GetTokenInfoRetryAlert(ctx, value.ChainName, tokenAddress)
						if err == nil {
							tokenInfo.Amount = value.Amount
							feeEventMap := map[string]interface{}{
								"evm": map[string]string{
									"nonce": fmt.Sprintf("%v", evm.Nonce),
									"type":  fmt.Sprintf("%v", evm.Type),
								},
								"token": tokenInfo,
							}
							feeEventParseData, _ := utils.JsonEncode(feeEventMap)
							feeEventTokenInfo, _ := utils.JsonEncode(tokenInfo)
							r.ParseData = feeEventParseData
							r.TokenInfo = feeEventTokenInfo
						}
					}

					//eventLogInfo, err1 := data.EvmTransactionRecordRepoClient.FindParseDataByTxHashAndToken(ctx, GetTableName(value.ChainName), value.LastTxhash, tokenAddress)

					feeData["gas_limit"] = r.GasLimit
					feeData["gas_used"] = r.GasUsed
					feeData["gas_price"] = r.GasPrice
					feeData["base_fee"] = r.BaseFee
					feeData["max_fee_per_gas"] = r.MaxFeePerGas
					feeData["max_priority_fee_per_gas"] = r.MaxPriorityFeePerGas
					r.ChainName = value.ChainName
					feeDataStr, _ := utils.JsonEncode(feeData)
					r.FromAddress = value.Address
					r.ToAddress = value.ToAddress
					r.FeeData = feeDataStr
					r.Cursor = value.TxTime
					r.Amount = value.Amount
					trs = append(trs, r)
				}
			case STC:
				stc, err := data.StcTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(value.ChainName), value.LastTxhash)
				if err == nil && stc != nil {
					var r *pb.TransactionRecord
					utils.CopyProperties(stc, &r)
					r.ChainName = value.ChainName
					feeData["gas_limit"] = r.GasLimit
					feeData["gas_used"] = r.GasUsed
					feeData["gas_price"] = r.GasPrice
					feeDataStr, _ := utils.JsonEncode(feeData)
					r.FeeData = feeDataStr
					r.Cursor = value.TxTime
					r.Amount = value.Amount
					trs = append(trs, r)
				}
			case TVM:
				tvm, err := data.TrxTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(value.ChainName), value.LastTxhash)
				if err == nil && tvm != nil {
					var r *pb.TransactionRecord
					utils.CopyProperties(tvm, &r)
					r.ChainName = value.ChainName
					feeData["fee_limit"] = r.FeeLimit
					feeData["net_usage"] = r.NetUsage
					feeData["energy_usage"] = r.EnergyUsage
					feeDataStr, _ := utils.JsonEncode(feeData)
					r.FeeData = feeDataStr
					r.Cursor = value.TxTime
					r.Amount = value.Amount
					if tvm.TransactionType != APPROVE {
						var tokenInfoMap = make(map[string]types.TokenInfo)
						tokenInfo := types.TokenInfo{
							Address:  value.Token,
							Amount:   value.Amount,
							Decimals: value.Decimals,
							Symbol:   value.Symbol,
						}
						tokenInfoMap["token"] = tokenInfo
						parseDate, _ := utils.JsonEncode(tokenInfoMap)
						r.ParseData = parseDate
						tokenInfoStr, _ := utils.JsonEncode(tokenInfo)
						r.TokenInfo = tokenInfoStr
					}

					trs = append(trs, r)
				}
			case APTOS:
				apt, err := data.AptTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(value.ChainName), value.LastTxhash)
				if err == nil && apt != nil {
					var r *pb.TransactionRecord
					utils.CopyProperties(apt, &r)
					r.ChainName = value.ChainName
					feeData["gas_limit"] = r.GasLimit
					feeData["gas_used"] = r.GasUsed
					feeData["gas_price"] = r.GasPrice
					feeDataStr, _ := utils.JsonEncode(feeData)
					r.FeeData = feeDataStr
					r.Cursor = value.TxTime
					r.Amount = value.Amount
					trs = append(trs, r)
				}
			case SUI:
				sui, err := data.SuiTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(value.ChainName), value.LastTxhash)
				if err == nil && sui != nil {
					var r *pb.TransactionRecord
					utils.CopyProperties(sui, &r)
					r.ChainName = value.ChainName
					feeData["gas_limit"] = r.GasLimit
					feeData["gas_used"] = r.GasUsed
					feeDataStr, _ := utils.JsonEncode(feeData)
					r.FeeData = feeDataStr
					r.Cursor = value.TxTime
					r.Amount = value.Amount
					trs = append(trs, r)
				}
			case SOLANA:
				sol, err := data.SolTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(value.ChainName), value.LastTxhash)
				if err == nil && sol != nil {
					if req.DappType == "approveNFT" && sol.TransactionType != req.DappType {
						continue
					}
					if req.DappType == "approve" && sol.TransactionType == "approveNFT" {
						continue
					}
					var r *pb.TransactionRecord
					utils.CopyProperties(sol, &r)
					r.ChainName = value.ChainName
					r.Cursor = value.TxTime
					r.Amount = value.Amount
					trs = append(trs, r)
				}
			}
		}
		return &pb.DappPageListResp{
			Ok: true,
			Data: &pb.DappPageData{
				Page:  req.Page,
				Limit: req.Limit,
				Total: total,
				Data:  trs,
			},
		}, err
	}
}

// GetNonce 获取nonce，供客户端打包交易上链使用
func (s *TransactionUsecase) GetNonce(ctx context.Context, req *pb.NonceReq) (*pb.NonceResp, error) {
	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		if req.Address != "" {
			req.Address = types2.HexToAddress(req.Address).Hex()
		}
	}

	doneNonceKey := ADDRESS_DONE_NONCE + req.ChainName + ":" + req.Address
	nonce, err := data.RedisClient.Get(doneNonceKey).Result()
	if err == redis.Nil {
		return findNonce(0, req)
	} else if err != nil {
		alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户链上nonce失败，address:%s", req.ChainName, req.Address)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Info("查询redis缓存，从redis获取用户链上nonce失败", zap.Any("chainName", req.ChainName), zap.Any("address", req.Address), zap.Any("error", err))
		return &pb.NonceResp{
			Ok: false,
		}, err
	} else {
		//取出 nonce值
		n, _ := strconv.Atoi(nonce)
		nn := n + 1
		return findNonce(nn, req)
	}
}

func findNonce(start int, req *pb.NonceReq) (*pb.NonceResp, error) {
	pendingNonceKey := ADDRESS_PENDING_NONCE + req.ChainName + ":" + req.Address + ":"
	for i := start; true; i++ {
		_, err1 := data.RedisClient.Get(pendingNonceKey + strconv.Itoa(i)).Result()
		if err1 != nil && err1 != redis.Nil {
			alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户pending状态nonce失败，address:%s", req.ChainName, req.Address)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Info("查询redis缓存，从redis获取用户pending状态nonce失败", zap.Any("chainName", req.ChainName), zap.Any("address", req.Address), zap.Any("error", err1))
			return &pb.NonceResp{
				Ok: false,
			}, err1
		}
		if err1 == redis.Nil {
			return &pb.NonceResp{
				Ok:    true,
				Nonce: int64(i),
			}, nil
		}
	}
	return nil, nil
}

func (s *TransactionUsecase) PageListAsset(ctx context.Context, req *pb.PageListAssetRequest) (*pb.PageListAssetResponse, error) {
	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		req.AddressList = utils.HexToAddress(req.AddressList)
		req.TokenAddressList = utils.HexToAddress(req.TokenAddressList)
	case COSMOS:
		req.TokenAddressList = utils.AddressListIbcToLower(req.TokenAddressList)
	}

	var request = &data.AssetRequest{
		ChainName:        req.ChainName,
		Uid:              req.Uid,
		AddressList:      req.AddressList,
		TokenAddressList: req.TokenAddressList,
		UidType:          req.AddressType,
		AmountType:       req.AmountType,
		TokenType:        req.TokenType,
		OrderBy:          req.OrderBy,
		DataDirection:    req.DataDirection,
		StartIndex:       req.StartIndex,
		PageNum:          req.PageNum,
		PageSize:         req.PageSize,
		Total:            req.Total,
	}
	var result = &pb.PageListAssetResponse{}
	var total int64
	var list []*pb.AssetResponse
	var err error

	var recordList []*data.UserAsset
	recordList, total, err = data.UserAssetRepoClient.PageList(ctx, request)
	if err == nil {
		err = utils.CopyProperties(recordList, &list)
	}

	if err == nil {
		result.Total = total
		result.List = list
	}
	return result, err
}

func (s *TransactionUsecase) PageListAssetCurrency(ctx context.Context, req *pb.PageListAssetRequest) (*pb.PageListAssetResponse, error) {
	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		req.AddressList = utils.HexToAddress(req.AddressList)
		req.TokenAddressList = utils.HexToAddress(req.TokenAddressList)
	case COSMOS:
		req.TokenAddressList = utils.AddressListIbcToLower(req.TokenAddressList)
	}

	var request = &data.AssetRequest{
		ChainName:        req.ChainName,
		Uid:              req.Uid,
		AddressList:      req.AddressList,
		TokenAddressList: req.TokenAddressList,
		UidType:          req.AddressType,
		AmountType:       req.AmountType,
		TokenType:        req.TokenType,
		OrderBy:          req.OrderBy,
		DataDirection:    req.DataDirection,
		StartIndex:       req.StartIndex,
		PageNum:          req.PageNum,
		PageSize:         req.PageSize,
		Total:            req.Total,
	}
	var result = &pb.PageListAssetResponse{}
	var total int64
	var list []*pb.AssetResponse
	var err error

	var recordList []*data.UserAsset
	recordList, total, err = data.UserAssetRepoClient.PageList(ctx, request)
	if err == nil {
		err = utils.CopyProperties(recordList, &list)
	}

	if err == nil {
		var chainNameTokenAddressMap = make(map[string][]string)
		var tokenAddressMapMap = make(map[string]map[string]string)
		for _, asset := range recordList {
			tokenAddressMap, ok := tokenAddressMapMap[asset.ChainName]
			if !ok {
				tokenAddressMap = make(map[string]string)
				tokenAddressMapMap[asset.ChainName] = tokenAddressMap
			}
			tokenAddressMap[asset.TokenAddress] = ""
		}

		for chainName, tokenAddressMap := range tokenAddressMapMap {
			tokenAddressList := make([]string, 0, len(tokenAddressMap))
			for key := range tokenAddressMap {
				tokenAddressList = append(tokenAddressList, key)
			}
			chainNameTokenAddressMap[chainName] = tokenAddressList
		}

		resultMap, err := GetTokensPrice(nil, req.Currency, chainNameTokenAddressMap)
		if err != nil {
			return result, err
		}

		if len(list) > 0 {
			for _, record := range list {
				if record == nil {
					continue
				}

				if strings.Contains(req.OrderBy, "id ") {
					record.Cursor = record.Id
				} else if strings.Contains(req.OrderBy, "created_at ") {
					record.Cursor = record.CreatedAt
				} else if strings.Contains(req.OrderBy, "updated_at ") {
					record.Cursor = record.UpdatedAt
				}

				chainName := record.ChainName
				tokenAddress := record.TokenAddress
				var price string
				tokenAddressPriceMap := resultMap[chainName]
				if tokenAddress == "" {
					price = tokenAddressPriceMap[chainName]
				} else {
					price = tokenAddressPriceMap[tokenAddress]
				}
				prices, _ := decimal.NewFromString(price)
				balances, _ := decimal.NewFromString(record.Balance)
				cnyAmount := prices.Mul(balances)
				record.CurrencyAmount = cnyAmount.String()
				record.Price = price
			}
		}
		result.Total = total
		result.List = list
	}
	return result, err
}

func (s *TransactionUsecase) BackendPageListAsset(ctx context.Context, req *pb.PageListAssetRequest) (*pb.PageListAssetResponse, error) {
	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		req.AddressList = utils.HexToAddress(req.AddressList)
		req.TokenAddressList = utils.HexToAddress(req.TokenAddressList)
	case COSMOS:
		req.TokenAddressList = utils.AddressListIbcToLower(req.TokenAddressList)
	}

	var request = &data.AssetRequest{
		ChainName:        req.ChainName,
		Uid:              req.Uid,
		AddressList:      req.AddressList,
		TokenAddressList: req.TokenAddressList,
		UidType:          req.AddressType,
		AmountType:       req.AmountType,
		TokenType:        req.TokenType,
		OrderBy:          req.OrderBy,
		GroupBy:          "chain_name, token_address, symbol",
		DataDirection:    req.DataDirection,
		StartIndex:       req.StartIndex,
		PageNum:          req.PageNum,
		PageSize:         req.PageSize,
		Total:            req.Total,
	}
	var result = &pb.PageListAssetResponse{}
	var total int64
	var totalCurrencyAmount decimal.Decimal
	var list []*pb.AssetResponse
	var err error

	var recordList []*data.UserAsset
	recordList, total, err = data.UserAssetRepoClient.PageList(ctx, request)
	if err == nil {
		err = utils.CopyProperties(recordList, &list)
	}

	var recordGroupList []*data.UserAsset
	if err == nil {
		recordGroupList, err = data.UserAssetRepoClient.ListBalanceGroup(ctx, request)
	}

	if err == nil && len(recordGroupList) > 0 {
		var chainNameTokenAddressMap = make(map[string][]string)
		var tokenAddressMapMap = make(map[string]map[string]string)
		for _, asset := range recordList {
			tokenAddressMap, ok := tokenAddressMapMap[asset.ChainName]
			if !ok {
				tokenAddressMap = make(map[string]string)
				tokenAddressMapMap[asset.ChainName] = tokenAddressMap
			}
			tokenAddressMap[asset.TokenAddress] = ""
		}
		for _, asset := range recordGroupList {
			tokenAddressMap, ok := tokenAddressMapMap[asset.ChainName]
			if !ok {
				tokenAddressMap = make(map[string]string)
				tokenAddressMapMap[asset.ChainName] = tokenAddressMap
			}
			tokenAddressMap[asset.TokenAddress] = ""
		}

		for chainName, tokenAddressMap := range tokenAddressMapMap {
			tokenAddressList := make([]string, 0, len(tokenAddressMap))
			for key := range tokenAddressMap {
				tokenAddressList = append(tokenAddressList, key)
			}
			chainNameTokenAddressMap[chainName] = tokenAddressList
		}

		resultMap, err := GetTokensPrice(nil, req.Currency, chainNameTokenAddressMap)
		if err != nil {
			return result, err
		}

		if len(list) > 0 {
			for _, record := range list {
				if record == nil {
					continue
				}

				if strings.Contains(req.OrderBy, "id ") {
					record.Cursor = record.Id
				} else if strings.Contains(req.OrderBy, "created_at ") {
					record.Cursor = record.CreatedAt
				} else if strings.Contains(req.OrderBy, "updated_at ") {
					record.Cursor = record.UpdatedAt
				}

				chainName := record.ChainName
				tokenAddress := record.TokenAddress
				var price string
				tokenAddressPriceMap := resultMap[chainName]
				if tokenAddress == "" {
					price = tokenAddressPriceMap[chainName]
				} else {
					price = tokenAddressPriceMap[tokenAddress]
				}
				prices, _ := decimal.NewFromString(price)
				balances, _ := decimal.NewFromString(record.Balance)
				cnyAmount := prices.Mul(balances)
				record.CurrencyAmount = cnyAmount.String()
				record.Price = price
			}
		}

		if len(recordGroupList) > 0 {
			for _, record := range recordGroupList {
				if record == nil {
					continue
				}

				chainName := record.ChainName
				tokenAddress := record.TokenAddress
				var price string
				tokenAddressPriceMap := resultMap[chainName]
				if tokenAddress == "" {
					price = tokenAddressPriceMap[chainName]
				} else {
					price = tokenAddressPriceMap[tokenAddress]
				}
				prices, _ := decimal.NewFromString(price)
				balances, _ := decimal.NewFromString(record.Balance)
				cnyAmount := prices.Mul(balances)
				totalCurrencyAmount = totalCurrencyAmount.Add(cnyAmount)
			}
		}
		result.Total = total
		result.List = list
		result.TotalCurrencyAmount = totalCurrencyAmount.String()
	}
	return result, err
}

func (s *TransactionUsecase) PageListAssetGroup(ctx context.Context, req *pb.PageListAssetRequest) (*pb.PageListAssetResponse, error) {
	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		req.AddressList = utils.HexToAddress(req.AddressList)
		req.TokenAddressList = utils.HexToAddress(req.TokenAddressList)
	case COSMOS:
		req.TokenAddressList = utils.AddressListIbcToLower(req.TokenAddressList)
	}

	var request = &data.AssetRequest{
		ChainName:        req.ChainName,
		Uid:              req.Uid,
		AddressList:      req.AddressList,
		TokenAddressList: req.TokenAddressList,
		UidType:          req.AddressType,
		AmountType:       req.AmountType,
		GroupBy:          "chain_name, token_address, symbol",
	}
	var result = &pb.PageListAssetResponse{}
	var total int64
	var list []*pb.AssetResponse
	var err error

	var recordList []*data.UserAsset
	recordList, err = data.UserAssetRepoClient.ListBalanceGroup(ctx, request)
	if err == nil {
		if req.Total {
			total = int64(len(recordList))
		}
		err = utils.CopyProperties(recordList, &list)
	}

	if err == nil {
		var chainNameTokenAddressMap = make(map[string][]string)
		var tokenAddressMapMap = make(map[string]map[string]string)
		for _, asset := range recordList {
			tokenAddressMap, ok := tokenAddressMapMap[asset.ChainName]
			if !ok {
				tokenAddressMap = make(map[string]string)
				tokenAddressMapMap[asset.ChainName] = tokenAddressMap
			}
			tokenAddressMap[asset.TokenAddress] = ""
		}

		for chainName, tokenAddressMap := range tokenAddressMapMap {
			tokenAddressList := make([]string, 0, len(tokenAddressMap))
			for key := range tokenAddressMap {
				tokenAddressList = append(tokenAddressList, key)
			}
			chainNameTokenAddressMap[chainName] = tokenAddressList
		}

		resultMap, err := GetTokensPrice(nil, req.Currency, chainNameTokenAddressMap)
		if err != nil {
			return result, err
		}

		if len(list) > 0 {
			for _, record := range list {
				if record == nil {
					continue
				}

				chainName := record.ChainName
				tokenAddress := record.TokenAddress
				var price string
				tokenAddressPriceMap := resultMap[chainName]
				if tokenAddress == "" {
					price = tokenAddressPriceMap[chainName]
				} else {
					price = tokenAddressPriceMap[tokenAddress]
				}
				prices, _ := decimal.NewFromString(price)
				balances, _ := decimal.NewFromString(record.Balance)
				cnyAmount := prices.Mul(balances)
				record.CurrencyAmount = cnyAmount.String()
				//record.Price = price

				/*if strings.Contains(req.OrderBy, "currencyAmount ") {
					record.Cursor = cnyAmount.IntPart()
				} else if strings.Contains(req.OrderBy, "price ") {
					record.Cursor = prices.IntPart()
				}*/
			}

			//处理排序
			orderBys := strings.Split(req.OrderBy, " ")
			orderByColumn := orderBys[0]
			orderByDirection := orderBys[1]
			if orderByColumn == "currencyAmount" {
				sort.SliceStable(list, func(i, j int) bool {
					if list[i] == nil || list[j] == nil {
						return true
					}
					iCurrencyAmount, _ := decimal.NewFromString(list[i].CurrencyAmount)
					jCurrencyAmount, _ := decimal.NewFromString(list[j].CurrencyAmount)
					/*if list[i].Price == "" || list[i].Price == "0" {
						return false
					} else if list[j].Price == "" || list[j].Price == "0" {
						return true
					} else {*/
					if strings.EqualFold(orderByDirection, "asc") {
						if iCurrencyAmount.Equal(jCurrencyAmount) {
							iBalance, _ := decimal.NewFromString(list[i].Balance)
							jBalance, _ := decimal.NewFromString(list[j].Balance)
							return iBalance.LessThan(jBalance)
						}
						return iCurrencyAmount.LessThan(jCurrencyAmount)
					} else {
						if iCurrencyAmount.Equal(jCurrencyAmount) {
							iBalance, _ := decimal.NewFromString(list[i].Balance)
							jBalance, _ := decimal.NewFromString(list[j].Balance)
							return iBalance.GreaterThan(jBalance)
						}
						return iCurrencyAmount.GreaterThan(jCurrencyAmount)
					}
					//}
				})
			} else if orderByColumn == "price" {
				sort.SliceStable(list, func(i, j int) bool {
					if list[i] == nil || list[j] == nil {
						return true
					}
					iPrice, _ := decimal.NewFromString(list[i].Price)
					jPrice, _ := decimal.NewFromString(list[j].Price)
					if list[i].Price == "" || list[i].Price == "0" {
						return false
					} else if list[j].Price == "" || list[j].Price == "0" {
						return true
					} else {
						if strings.EqualFold(orderByDirection, "asc") {
							return iPrice.LessThan(jPrice)
						} else {
							return iPrice.GreaterThan(jPrice)
						}
					}
				})
			}

			//处理分页
			if req.DataDirection == 0 {
				listLen := int32(len(list))
				var offset int32
				var stopIndex int32
				if req.PageNum > 0 {
					offset = (req.PageNum - 1) * req.PageSize
				}
				if offset < listLen {
					stopIndex = offset + req.PageSize
					if stopIndex > listLen {
						stopIndex = listLen
					}
					list = list[offset:stopIndex]
				} else {
					list = nil
				}
			} else {
				list = nil
			}
		}
		result.Total = total
		result.List = list
	}
	return result, err
}

func (s *TransactionUsecase) ClientPageListAsset(ctx context.Context, req *pb.PageListAssetRequest) (*pb.PageListAssetResponse, error) {
	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		req.AddressList = utils.HexToAddress(req.AddressList)
		req.TokenAddressList = utils.HexToAddress(req.TokenAddressList)
	case COSMOS:
		req.TokenAddressList = utils.AddressListIbcToLower(req.TokenAddressList)
	}

	//utils.HexToAddress 会将格式不正确的地址转换成0地址，需要删除0地址
	var tokenList []string
	for _, tokenAddress := range req.TokenAddressList {
		if tokenAddress != "0x0000000000000000000000000000000000000000" {
			tokenList = append(tokenList, tokenAddress)
		}
	}
	req.TokenAddressList = tokenList

	var request = &data.AssetRequest{
		ChainName:        req.ChainName,
		Uid:              req.Uid,
		AddressList:      req.AddressList,
		TokenAddressList: req.TokenAddressList,
		UidType:          req.AddressType,
		AmountType:       req.AmountType,
		TokenType:        req.TokenType,
	}
	var result = &pb.PageListAssetResponse{}
	var total int64
	var sumCurrencyAmount decimal.Decimal
	var list []*pb.AssetResponse
	var err error

	var recordList []*data.UserAsset
	recordMap := make(map[string]*data.UserAsset)
	recordList, err = data.UserAssetRepoClient.List(ctx, request)
	if err == nil {
		if req.AmountType != 2 {
			for _, userAsset := range recordList {
				recordMap[userAsset.TokenAddress] = userAsset
			}
			for _, tokenAddress := range req.TokenAddressList {
				if _, ok := recordMap[tokenAddress]; !ok {
					record := &data.UserAsset{
						ChainName:    req.ChainName,
						TokenAddress: tokenAddress,
					}
					recordList = append(recordList, record)
				}
			}
		}

		if req.Total {
			total = int64(len(recordList))
		}
		err = utils.CopyProperties(recordList, &list)
	}

	if err == nil {
		var chainNameTokenAddressMap = make(map[string][]string)
		var tokenAddressMapMap = make(map[string]map[string]string)
		for _, asset := range recordList {
			tokenAddressMap, ok := tokenAddressMapMap[asset.ChainName]
			if !ok {
				tokenAddressMap = make(map[string]string)
				tokenAddressMapMap[asset.ChainName] = tokenAddressMap
			}
			tokenAddressMap[asset.TokenAddress] = ""
		}

		for chainName, tokenAddressMap := range tokenAddressMapMap {
			tokenAddressList := make([]string, 0, len(tokenAddressMap))
			for key := range tokenAddressMap {
				tokenAddressList = append(tokenAddressList, key)
			}
			chainNameTokenAddressMap[chainName] = tokenAddressList
		}

		resultMap, err := GetTokensPrice(nil, req.Currency, chainNameTokenAddressMap)
		if err != nil {
			return result, err
		}

		if len(list) > 0 {
			for _, record := range list {
				if record == nil {
					continue
				}

				chainName := record.ChainName
				tokenAddress := record.TokenAddress
				var price string
				tokenAddressPriceMap := resultMap[chainName]
				if tokenAddress == "" {
					price = tokenAddressPriceMap[chainName]
				} else {
					price = tokenAddressPriceMap[tokenAddress]
				}
				prices, _ := decimal.NewFromString(price)
				balances, _ := decimal.NewFromString(record.Balance)
				cnyAmount := prices.Mul(balances)
				record.CurrencyAmount = cnyAmount.String()
				record.Price = price

				sumCurrencyAmount = sumCurrencyAmount.Add(cnyAmount)

				/*if strings.Contains(req.OrderBy, "currencyAmount ") {
					record.Cursor = cnyAmount.IntPart()
				} else if strings.Contains(req.OrderBy, "price ") {
					record.Cursor = prices.IntPart()
				}*/
			}

			//处理过滤条件
			if req.CurrencyAmountMoreThan != "" {
				currencyAmountMoreThan, err := decimal.NewFromString(req.CurrencyAmountMoreThan)
				if err == nil {
					var ls []*pb.AssetResponse
					for _, record := range list {
						if record == nil {
							continue
						}

						currencyAmount, _ := decimal.NewFromString(record.CurrencyAmount)
						if currencyAmount.GreaterThan(currencyAmountMoreThan) {
							ls = append(ls, record)
						}
					}
					list = ls
				}
			}

			//处理排序
			orderBys := strings.Split(req.OrderBy, " ")
			orderByColumn := orderBys[0]
			orderByDirection := orderBys[1]
			if orderByColumn == "currencyAmount" {
				sort.SliceStable(list, func(i, j int) bool {
					if list[i] == nil || list[j] == nil {
						return true
					}
					iCurrencyAmount, _ := decimal.NewFromString(list[i].CurrencyAmount)
					jCurrencyAmount, _ := decimal.NewFromString(list[j].CurrencyAmount)
					/*if list[i].Price == "" || list[i].Price == "0" {
						return false
					} else if list[j].Price == "" || list[j].Price == "0" {
						return true
					} else {*/
					if strings.EqualFold(orderByDirection, "asc") {
						if iCurrencyAmount.Equal(jCurrencyAmount) {
							iBalance, _ := decimal.NewFromString(list[i].Balance)
							jBalance, _ := decimal.NewFromString(list[j].Balance)
							return iBalance.LessThan(jBalance)
						}
						return iCurrencyAmount.LessThan(jCurrencyAmount)
					} else {
						if iCurrencyAmount.Equal(jCurrencyAmount) {
							iBalance, _ := decimal.NewFromString(list[i].Balance)
							jBalance, _ := decimal.NewFromString(list[j].Balance)
							return iBalance.GreaterThan(jBalance)
						}
						return iCurrencyAmount.GreaterThan(jCurrencyAmount)
					}
					//}
				})
			} else if orderByColumn == "price" {
				sort.SliceStable(list, func(i, j int) bool {
					if list[i] == nil || list[j] == nil {
						return true
					}
					iPrice, _ := decimal.NewFromString(list[i].Price)
					jPrice, _ := decimal.NewFromString(list[j].Price)
					if list[i].Price == "" || list[i].Price == "0" {
						return false
					} else if list[j].Price == "" || list[j].Price == "0" {
						return true
					} else {
						if strings.EqualFold(orderByDirection, "asc") {
							return iPrice.LessThan(jPrice)
						} else {
							return iPrice.GreaterThan(jPrice)
						}
					}
				})
			}

			//处理分页
			if req.DataDirection == 0 {
				listLen := int32(len(list))
				var offset int32
				var stopIndex int32
				if req.PageNum > 0 {
					offset = (req.PageNum - 1) * req.PageSize
				}
				if offset < listLen {
					stopIndex = offset + req.PageSize
					if stopIndex > listLen {
						stopIndex = listLen
					}
					list = list[offset:stopIndex]
				} else {
					list = nil
				}
			} else {
				list = nil
			}
		}
		result.Total = total
		result.List = list
		result.TotalCurrencyAmount = sumCurrencyAmount.String()
	}
	return result, err
}

func (s *TransactionUsecase) GetBalance(ctx context.Context, req *pb.AssetRequest) (*pb.ListBalanceResponse, error) {
	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		if req.Address != "" {
			req.Address = types2.HexToAddress(req.Address).Hex()
		}
		req.TokenAddressList = utils.HexToAddress(req.TokenAddressList)
	case COSMOS:
		req.TokenAddressList = utils.AddressListIbcToLower(req.TokenAddressList)
	}

	var request = &data.AssetRequest{
		ChainName:        req.ChainName,
		Address:          req.Address,
		TokenAddressList: req.TokenAddressList,
	}
	var result = &pb.ListBalanceResponse{}
	var list []*pb.BalanceResponse
	var err error

	var recordList []*data.UserAsset
	recordList, err = data.UserAssetRepoClient.List(ctx, request)
	if err == nil {
		err = utils.CopyProperties(recordList, &list)
	}
	if err == nil {
		result.List = list
	}

	return result, err
}

func (s *TransactionUsecase) ListAmountUidDimension(ctx context.Context, req *pb.ListAmountUidDimensionRequest) (*pb.ListAmountUidDimensionResponse, error) {
	var request = &data.AssetRequest{
		ChainName:  req.ChainName,
		UidList:    req.UidList,
		AmountType: 2,
	}
	var result = &pb.ListAmountUidDimensionResponse{}
	var list []*pb.AmountUidDimensionResponse
	var amountMap = make(map[string]decimal.Decimal)
	var err error

	var recordList []*data.UserAsset
	recordList, err = data.UserAssetRepoClient.ListBalance(ctx, request)
	if err == nil && len(recordList) > 0 {
		var chainNameTokenAddressMap = make(map[string][]string)
		var tokenAddressMapMap = make(map[string]map[string]string)
		for _, asset := range recordList {
			tokenAddressMap, ok := tokenAddressMapMap[asset.ChainName]
			if !ok {
				tokenAddressMap = make(map[string]string)
				tokenAddressMapMap[asset.ChainName] = tokenAddressMap
			}
			tokenAddressMap[asset.TokenAddress] = ""
		}

		for chainName, tokenAddressMap := range tokenAddressMapMap {
			tokenAddressList := make([]string, 0, len(tokenAddressMap))
			for key := range tokenAddressMap {
				tokenAddressList = append(tokenAddressList, key)
			}
			chainNameTokenAddressMap[chainName] = tokenAddressList
		}

		resultMap, err := GetTokensPrice(nil, req.Currency, chainNameTokenAddressMap)
		if err != nil {
			return result, err
		}

		for _, record := range recordList {
			if record == nil {
				continue
			}

			chainName := record.ChainName
			if IsTestNet(chainName) {
				continue
			}
			tokenAddress := record.TokenAddress
			var price string
			tokenAddressPriceMap := resultMap[chainName]
			if tokenAddress == "" {
				price = tokenAddressPriceMap[chainName]
			} else {
				price = tokenAddressPriceMap[tokenAddress]
			}
			prices, _ := decimal.NewFromString(price)
			balances, _ := decimal.NewFromString(record.Balance)
			cnyAmount := prices.Mul(balances)

			response, ok := amountMap[record.Uid]
			if !ok {
				amountMap[record.Uid] = cnyAmount
			} else {
				amountMap[record.Uid] = response.Add(cnyAmount)
			}
		}

		if len(amountMap) > 0 {
			for key, amount := range amountMap {
				record := &pb.AmountUidDimensionResponse{
					Uid:            key,
					CurrencyAmount: amount.String(),
				}
				list = append(list, record)
			}
		}
		result.List = list
	}
	return result, err
}

func (s *TransactionUsecase) ListHasBalanceUidDimension(ctx context.Context, req *pb.ListHasBalanceUidDimensionRequest) (*pb.ListHasBalanceUidDimensionResponse, error) {
	var request = &data.AssetRequest{
		UidList: req.UidList,
		GroupBy: "uid",
	}
	var result = &pb.ListHasBalanceUidDimensionResponse{}
	var list []*pb.HasBalanceUidDimensionResponse
	var err error

	var recordList []*data.UserAsset
	recordList, err = data.UserAssetRepoClient.ListBalanceGroup(ctx, request)
	if err == nil && len(recordList) > 0 {
		for _, record := range recordList {
			if record == nil {
				continue
			}

			var hasBalance bool
			if record.Balance != "" && record.Balance != "0" {
				hasBalance = true
			}
			hasBalanceUidDimensionResponse := &pb.HasBalanceUidDimensionResponse{
				Uid:        record.Uid,
				HasBalance: hasBalance,
			}
			list = append(list, hasBalanceUidDimensionResponse)
		}
		result.List = list
	}
	return result, err
}

func (s *TransactionUsecase) ListHasBalanceDimension(ctx context.Context, req *pb.ListHasBalanceDimensionRequest) (*pb.ListHasBalanceDimensionResponse, error) {
	var request = &data.AssetRequest{
		ChainName:   req.ChainName,
		UidList:     req.UidList,
		AddressList: req.AddressList,
		GroupBy:     req.GroupBy,
	}
	var result = &pb.ListHasBalanceDimensionResponse{}
	var list []*pb.HasBalanceDimensionResponse
	var err error

	var recordList []*data.UserAsset
	recordList, err = data.UserAssetRepoClient.ListBalanceGroup(ctx, request)
	if err == nil && len(recordList) > 0 {
		for _, record := range recordList {
			if record == nil {
				continue
			}

			var hasBalance bool
			var column string
			if record.Balance != "" && record.Balance != "0" {
				hasBalance = true
			}
			if req.GroupBy == "uid" {
				column = record.Uid
			} else if req.GroupBy == "address" {
				column = record.Address
			}
			hasBalanceDimensionResponse := &pb.HasBalanceDimensionResponse{
				Column:     column,
				HasBalance: hasBalance,
			}
			list = append(list, hasBalanceDimensionResponse)
		}
		result.List = list
	}
	return result, err
}

func (s *TransactionUsecase) AssetHistoryFundAmount(ctx context.Context, req *pb.AssetHistoryRequest) (*pb.AssetHistoryFundAmountListResponse, error) {
	var request = &data.AssetRequest{
		ChainName: req.ChainName,
		UidType:   req.AddressType,
		StartTime: req.StartTime,
		StopTime:  req.StopTime,
		OrderBy:   "dt asc",
	}
	var result = &pb.AssetHistoryFundAmountListResponse{}
	var list []*pb.AssetHistoryFundAmountResponse
	var err error

	var recordList []*data.ChainTypeAsset
	recordList, err = data.ChainTypeAssetRepoClient.List(ctx, request)
	if err == nil {
		err = utils.CopyProperties(recordList, &list)
	}
	if err == nil {
		result.List = list
	}

	return result, err
}

func (s *TransactionUsecase) AssetHistoryAddressAmount(ctx context.Context, req *pb.AssetHistoryRequest) (*pb.AssetHistoryAddressAmountListResponse, error) {
	var request = &data.AssetRequest{
		ChainName: req.ChainName,
		UidType:   req.AddressType,
		StartTime: req.StartTime,
		StopTime:  req.StopTime,
		OrderBy:   "dt asc",
	}
	var result = &pb.AssetHistoryAddressAmountListResponse{}
	var list []*pb.AssetHistoryAddressAmountResponse
	var err error

	var recordList []*data.ChainTypeAddressAmount
	recordList, err = data.ChainTypeAddressAmountRepoClient.List(ctx, request)
	if err == nil {
		err = utils.CopyProperties(recordList, &list)
	}
	if err == nil {
		result.List = list
	}

	return result, err
}

func (s *TransactionUsecase) ClientPageListNftAssetGroup(ctx context.Context, req *pb.PageListNftAssetRequest) (*pb.ClientPageListNftAssetGroupResponse, error) {
	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		req.AddressList = utils.HexToAddress(req.AddressList)
		req.TokenAddressList = utils.HexToAddress(req.TokenAddressList)
	case COSMOS:
		req.TokenAddressList = utils.AddressListIbcToLower(req.TokenAddressList)
	}

	var result = &pb.ClientPageListNftAssetGroupResponse{}
	var total, totalBalance int64
	var list []*pb.ClientNftAssetGroupResponse
	var err error

	var recordList []*data.UserNftAssetGroup
	recordList, total, totalBalance, err = data.UserNftAssetRepoClient.PageListGroup(ctx, req)
	if err == nil {
		err = utils.CopyProperties(recordList, &list)
	}

	if err == nil && len(recordList) > 0 {
		result.Total = total
		result.TotalBalance = totalBalance
		result.List = list

		var nftAddressMap = make(map[string][]string)
		for _, record := range recordList {
			tokenIdList, ok := nftAddressMap[record.TokenAddress]
			if !ok {
				tokenIdList = make([]string, 0)
			}
			tokenIdList = append(tokenIdList, record.TokenId)
			nftAddressMap[record.TokenAddress] = tokenIdList
		}

		resultMap, err := GetNftsInfo(nil, req.ChainName, nftAddressMap)
		if err != nil {
			log.Error("查询用户NFT资产集合，从nodeProxy中获取NFT信息失败", zap.Any("chainName", req.ChainName), zap.Any("addressList", req.AddressList), zap.Any("error", err))
			return result, nil
		}
		nftAddressInfoMap := make(map[string]*v1.GetNftReply_NftInfoResp)
		for _, res := range resultMap {
			nftAddressInfoMap[res.TokenAddress] = res
		}

		if len(list) > 0 {
			for _, record := range list {
				if record == nil {
					continue
				}

				if strings.Contains(req.OrderBy, "balance ") {
					balance, _ := strconv.Atoi(record.Balance)
					record.Cursor = int64(balance)
				} else if strings.Contains(req.OrderBy, "token_id_amount ") {
					record.Cursor = record.TokenIdAmount
				}
				nftInfo := nftAddressInfoMap[record.TokenAddress]
				if nftInfo != nil {
					record.TokenUri = nftInfo.CollectionImageURL
					record.CollectionName = nftInfo.CollectionName
					if strings.HasPrefix(req.ChainName, "Solana") {
						record.TokenAddress = nftInfo.TokenId
					}
				}
			}
		}
	}
	return result, err
}

func (s *TransactionUsecase) ClientPageListNftAsset(ctx context.Context, req *pb.PageListNftAssetRequest) (*pb.ClientPageListNftAssetResponse, error) {
	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		req.AddressList = utils.HexToAddress(req.AddressList)
		req.TokenAddressList = utils.HexToAddress(req.TokenAddressList)
	case COSMOS:
		req.TokenAddressList = utils.AddressListIbcToLower(req.TokenAddressList)
	}

	var request = &data.NftAssetRequest{
		ChainName:                    req.ChainName,
		Uid:                          req.Uid,
		AddressList:                  req.AddressList,
		TokenAddressList:             req.TokenAddressList,
		TokenIdList:                  req.TokenIdList,
		AmountType:                   req.AmountType,
		CollectionNameLike:           req.CollectionNameLike,
		CollectionNameLikeIgnoreCase: req.CollectionNameLikeIgnoreCase,
		NameLikeIgnoreCase:           req.NameLikeIgnoreCase,
		OrderBy:                      req.OrderBy,
		DataDirection:                req.DataDirection,
		StartIndex:                   req.StartIndex,
		PageNum:                      req.PageNum,
		PageSize:                     req.PageSize,
		Total:                        req.Total,
	}
	// No collection name
	if chainType == SUI {
		request.CollectionNameLike = ""
		request.CollectionNameLikeIgnoreCase = ""
	} else {
		request.NameLikeIgnoreCase = ""
	}
	var result = &pb.ClientPageListNftAssetResponse{}
	var total int64
	var list []*pb.ClientNftAssetResponse
	var err error

	var recordList []*data.UserNftAsset
	recordList, total, err = data.UserNftAssetRepoClient.PageList(ctx, request)
	if err == nil {
		err = utils.CopyProperties(recordList, &list)
	}

	if err == nil && len(recordList) > 0 {
		result.Total = total
		result.List = list

		var nftAddressMap = make(map[string][]string)
		for _, record := range recordList {
			tokenIdList, ok := nftAddressMap[record.TokenAddress]
			if !ok {
				tokenIdList = make([]string, 0)
			}
			tokenIdList = append(tokenIdList, record.TokenId)
			nftAddressMap[record.TokenAddress] = tokenIdList
		}

		resultMap, err := GetNftsInfo(nil, req.ChainName, nftAddressMap)
		if err != nil {
			log.Error("查询用户NFT资产，从nodeProxy中获取NFT信息失败", zap.Any("chainName", req.ChainName), zap.Any("addressList", req.AddressList), zap.Any("error", err))
			return result, nil
		}
		nftAddressInfoMap := make(map[string]map[string]*v1.GetNftReply_NftInfoResp)
		for _, res := range resultMap {
			nftIdInfoMap, ok := nftAddressInfoMap[res.TokenAddress]
			if !ok {
				nftIdInfoMap = make(map[string]*v1.GetNftReply_NftInfoResp)
				nftAddressInfoMap[res.TokenAddress] = nftIdInfoMap
			}
			nftIdInfoMap[res.TokenId] = res
		}

		if len(list) > 0 {
			for _, record := range list {
				if record == nil {
					continue
				}

				if strings.Contains(req.OrderBy, "id ") {
					record.Cursor = record.Id
				} else if strings.Contains(req.OrderBy, "created_at ") {
					record.Cursor = record.CreatedAt
				} else if strings.Contains(req.OrderBy, "updated_at ") {
					record.Cursor = record.UpdatedAt
				}
				nftInfo := nftAddressInfoMap[record.TokenAddress][record.TokenId]
				if nftInfo != nil {
					record.TokenUri = nftInfo.CollectionImageURL
					record.CollectionName = nftInfo.CollectionName
					record.ItemName = nftInfo.NftName
					record.ItemUri = nftInfo.ImageURL
					record.ItemOriginalUri = nftInfo.ImageOriginalURL
					record.ItemAnimationUri = nftInfo.AnimationURL
				}
			}
		}
	}
	return result, err
}

func (s *TransactionUsecase) GetNftBalance(ctx context.Context, req *pb.NftAssetRequest) (*pb.NftBalanceResponse, error) {
	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		if req.Address != "" {
			req.Address = types2.HexToAddress(req.Address).Hex()
		}
		if req.TokenAddress != "" {
			req.TokenAddress = types2.HexToAddress(req.TokenAddress).Hex()
		}
	case COSMOS:
		if req.TokenAddress != "" {
			req.TokenAddress = utils.AddressIbcToLower(req.TokenAddress)
		}
	}

	var result = &pb.NftBalanceResponse{}
	var record *data.UserNftAsset
	var err error
	record, err = data.UserNftAssetRepoClient.FindByUniqueKey(ctx, req)
	if err == nil && record != nil {
		result.Balance = record.Balance
	}
	return result, err
}

func (s *TransactionUsecase) PageListStatistic(ctx context.Context, req *pb.PageListStatisticRequest) (*pb.PageListStatisticResponse, error) {
	var result = &pb.PageListStatisticResponse{}
	var total int64
	var list []*pb.StatisticResponse
	var err error

	var recordList []*data.TransactionStatistic
	recordList, total, err = data.TransactionStatisticRepoClient.PageList(ctx, req)
	if err == nil {
		err = utils.CopyProperties(recordList, &list)
	}

	if err == nil {
		result.Total = total
		result.List = list
		if len(list) > 0 {
			for _, record := range list {
				if record == nil {
					continue
				}

				if strings.Contains(req.OrderBy, "id ") {
					record.Cursor = record.Id
				} else if strings.Contains(req.OrderBy, "created_at ") {
					record.Cursor = record.CreatedAt
				} else if strings.Contains(req.OrderBy, "updated_at ") {
					record.Cursor = record.UpdatedAt
				}
			}
		}
	}
	return result, err
}

func (s *TransactionUsecase) StatisticFundAmount(ctx context.Context, req *pb.StatisticFundRequest) (*pb.FundAmountListResponse, error) {
	var result = &pb.FundAmountListResponse{}
	var fundAmountResponse []*pb.FundAmountResponse
	var err error

	fundAmountResponse, err = data.TransactionStatisticRepoClient.StatisticFundAmount(ctx, req)

	if err == nil {
		result.List = fundAmountResponse
	}
	return result, err
}

func (s *TransactionUsecase) StatisticFundRate(ctx context.Context, req *pb.StatisticFundRequest) (*pb.FundRateListResponse, error) {
	var result = &pb.FundRateListResponse{}
	var fundRateResponse []*pb.FundRateResponse
	var err error

	fundRateResponse, err = data.TransactionStatisticRepoClient.StatisticFundRate(ctx, req)

	if err == nil {
		result.List = fundRateResponse
	}
	return result, err
}

func (s *TransactionUsecase) GetUnspentTx(ctx context.Context, req *pb.UnspentReq) (*pb.UnspentResponse, error) {
	var result = &pb.UnspentResponse{}
	if req.ChainName == "Nervos" || req.ChainName == "NervosTEST" {
		var cellList []*pb.CellList
		var tokenCellList []*pb.CellList
		tokenAddress := req.ContractAddress
		result.Ok = true

		req.ContractAddress = ""
		nl, err := data.NervosCellRecordRepoClient.FindByCondition(ctx, req)
		if err != nil {
			result.Ok = false
			return result, err
		}

		if len(nl) != 0 {
			for _, n := range nl {
				c, _ := strconv.Atoi(n.Capacity)
				lockArgs := strings.TrimLeft(strings.Replace(n.LockArgs, "0x", "", 1), "0")
				fla := fmt.Sprintf("%040s", lockArgs)

				typeArgs := strings.TrimLeft(strings.Replace(n.TypeArgs, "0x", "", 1), "0")
				fta := fmt.Sprintf("%040s", typeArgs)
				p := &pb.CellList{
					OutPoint: &pb.OutPoint{
						TxHash: n.TransactionHash,
						Index:  fmt.Sprint(n.Index),
					},
					Lock: &pb.CellLock{
						CodeHash: n.LockCodeHash,
						HashType: n.LockHashType,
						Args:     "0x" + fla,
					},
					Capacity: int64(c),
					Data:     "0x" + fta,
				}

				if n.TypeCodeHash != "" || n.TypeHashType != "" || n.TypeArgs != "" {
					p.Type = &pb.CellLock{
						CodeHash: n.TypeCodeHash,
						HashType: n.TypeHashType,
						Args:     n.TypeArgs,
					}
				}

				cellList = append(cellList, p)
			}
			result.CellList = cellList
		}

		if tokenAddress != "" {
			req.ContractAddress = tokenAddress
			token, err := data.NervosCellRecordRepoClient.FindByCondition(ctx, req)
			if err != nil {
				result.Ok = false
				return result, err
			}

			if len(token) != 0 {
				for _, n := range token {
					c, _ := strconv.Atoi(n.Capacity)
					lockArgs := strings.TrimLeft(strings.Replace(n.LockArgs, "0x", "", 1), "0")
					fla := fmt.Sprintf("%040s", lockArgs)

					typeArgs := strings.TrimLeft(strings.Replace(n.TypeArgs, "0x", "", 1), "0")
					fta := fmt.Sprintf("%040s", typeArgs)

					p := &pb.CellList{
						OutPoint: &pb.OutPoint{
							TxHash: n.TransactionHash,
							Index:  fmt.Sprint(n.Index),
						},
						Lock: &pb.CellLock{
							CodeHash: n.LockCodeHash,
							HashType: n.LockHashType,
							Args:     "0x" + fla,
						},
						Type: &pb.CellLock{
							CodeHash: n.TypeCodeHash,
							HashType: n.TypeHashType,
							Args:     "0x" + fta,
						},
						Capacity: int64(c),
						Data:     "0x" + strings.TrimLeft(strings.Replace(n.Data, "0x", "", 1), "0"),
					}
					tokenCellList = append(tokenCellList, p)
				}
				result.TokenCellList = tokenCellList
			}
		}
		return result, nil
	} else {
		var unspentList []*pb.UnspentList
		dbUnspentRecord, err := data.UtxoUnspentRecordRepoClient.FindByCondition(ctx, req)
		if err != nil {
			result.Ok = false
			return result, err
		}

		//如果库中查出来没有，从链上再获取
		if len(dbUnspentRecord) == 0 && req.IsUnspent == strconv.Itoa(data.UtxoStatusUnSpend) {
			var utxos []*data.UtxoUnspentRecord
			if req.ChainName == "Kaspa" {
				kaspaUTXOS, _ := data.KaspaRepoClient.GetUtxo(req.ChainName, req.Address)
				utxos = convertKaspaUTXO(req.ChainName, kaspaUTXOS)
			} else {
				oklinkUTXOS, _ := data.OklinkRepoClient.GetUtxo(req.ChainName, req.Address)
				utxos = convertOklinkUTXO(req.ChainName, oklinkUTXOS)
			}

			if len(utxos) == 0 {
				result.Ok = true
				return result, nil
			}

			data.UtxoUnspentRecordRepoClient.BatchSaveOrUpdate(nil, utxos)

			for _, utxo := range utxos {
				r := &pb.UnspentList{
					Uid:       utxo.Uid,
					Hash:      utxo.Hash,
					Index:     fmt.Sprint(utxo.N),
					ChainName: utxo.ChainName,
					Address:   utxo.Address,
					Script:    utxo.Script,
					Unspent:   utxo.Unspent,
					Amount:    utxo.Amount,
					TxTime:    utxo.TxTime,
					CreatedAt: utxo.CreatedAt,
					UpdatedAt: utxo.UpdatedAt,
				}

				unspentList = append(unspentList, r)
			}

			result.Ok = true
			result.UtxoList = unspentList
			return result, nil
		}

		for _, db := range dbUnspentRecord {
			var r *pb.UnspentList
			utils.CopyProperties(db, &r)
			r.Index = fmt.Sprint(db.N)
			unspentList = append(unspentList, r)
		}
		result.Ok = true
		result.UtxoList = unspentList
		return result, nil
	}
}

func (s *TransactionUsecase) GetNftRecord(ctx context.Context, req *pb.NftRecordReq) (*pb.NftRecordResponse, error) {
	var result = &pb.NftRecordResponse{}
	var nftHistoryList []*pb.NftHistoryList

	records, err := data.NftRecordHistoryRepoClient.ListByCondition(ctx, req)
	if err != nil {
		result.Ok = false
		return result, err
	}

	for _, db := range records {
		var r *pb.NftHistoryList
		utils.CopyProperties(db, &r)
		if r.FromAddress == "0x0000000000000000000000000000000000000000" {
			r.TransactionType = "mint"
		} else {
			r.TransactionType = "transfer"
		}
		r.TxTime = strings.Replace(r.TxTime, "T", " ", 1)
		nftHistoryList = append(nftHistoryList, r)
	}

	result.Ok = true
	result.Data = nftHistoryList
	return result, nil
}

func (s *TransactionUsecase) JsonRpc(ctx context.Context, req *pb.JsonReq) (*pb.JsonResponse, error) {
	valueS := reflect.TypeOf(s)
	mv, ok := valueS.MethodByName(req.Method)
	if !ok {
		return &pb.JsonResponse{
			Ok:       false,
			ErrorMsg: "not support " + req.Method,
		}, nil
	}
	jsonRpcCtx := &JsonRpcContext{
		Context:   ctx,
		Device:    req.Device,
		Uid:       req.Uid,
		ChainName: req.ChainName,
	}

	args := make([]reflect.Value, 0)
	args = append(args, reflect.ValueOf(s))
	args = append(args, reflect.ValueOf(jsonRpcCtx))

	if len(req.Params) > 0 {
		u := mv.Type.NumIn()
		paseJson := reflect.New(mv.Type.In(u - 1).Elem())
		//reqKey := strings.ReplaceAll(utils.ListToString(req.Params), "\\", "")

		jsonErr := json.Unmarshal([]byte(req.Params), paseJson.Interface())
		if jsonErr == nil {
			args = append(args, reflect.ValueOf(paseJson.Interface()))
		} else {
			return &pb.JsonResponse{
				Ok:       false,
				ErrorMsg: "param error ",
			}, jsonErr
		}
	}
	ss := mv.Func.Call(args)

	// Error handling.
	if len(ss) > 1 {
		if err, ok := ss[1].Interface().(error); ok && err != nil {
			return &pb.JsonResponse{
				Ok:       false,
				ErrorMsg: err.Error(),
			}, nil
		}
	}

	respone := ss[0].Interface()
	ret, _ := utils.JsonEncode(respone)
	return &pb.JsonResponse{
		Ok:       true,
		Response: ret,
	}, nil
}

// 交易资产分布和交易类型 ，前五个后 累加-- 两个 饼图
func (s *TransactionUsecase) GetDappTop(ctx context.Context, req *TransactionTypeReq) (*TopDappResponse, error) {
	startTime, endTime := utils.RangeTimeConvertTime(req.TimeRange)
	dapps, e := data.EvmTransactionRecordRepoClient.ListDappDataStringByTimeRanges(ctx, GetTableName(req.ChainName), req.Address, startTime, endTime)
	if e != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s 链查询用户 %s dapp数据失败", req.ChainName, req.Address)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error(req.ChainName+" : "+req.Address+"dapp数据失败 ", zap.Any("error", e))
		return &TopDappResponse{
			Ok:           false,
			ErrorMessage: e.Error(),
		}, e
	}
	if dapps == nil || len(dapps) == 0 {
		return &TopDappResponse{
			Ok: true,
		}, nil
	}
	var dappInfos []DappInfo

	var dappNameMap = make(map[string]DappInfo)
	//排序用
	var dappCountMap = make(map[int][]DappInfo)
	//有map 则拿出value ，判断orgin 也就是url是不是内部的，如果是 替换。 并数量加一
	var direction = make([]int, 0)
	var checkDirection = make(map[int]int)
	for _, di := range dapps {
		var vs DappInfo
		if jsonErr := json.Unmarshal([]byte(di), &vs); jsonErr == nil {
			name := vs.DappName
			origin := vs.Origin
			if origin == "" || name == "" {
				continue
			}
			if dnm, ok := dappNameMap[name]; ok {
				oldCC := dnm.CheckCount
				nCC := oldCC + 1
				dnm.CheckCount = nCC
				if strings.Contains(origin, "web3app.vip") || strings.Contains(origin, "openblock.com") || strings.Contains(origin, "openblock.vip") {
					dnm.Origin = origin
				}
				dappNameMap[name] = dnm
			} else {
				dappNameMap[name] = DappInfo{
					Origin:     origin,
					Icon:       vs.Icon,
					DappName:   name,
					CheckCount: 1,
				}
			}
		}
	}
	for _, dappDetail := range dappNameMap {
		c := dappDetail.CheckCount
		if dcm, ok := dappCountMap[c]; ok {
			dcm = append(dcm, dappDetail)
			dappCountMap[c] = dcm
		} else {
			var dtf []DappInfo
			dtf = append(dtf, dappDetail)
			dappCountMap[c] = dtf
		}

		direction = append(direction, c)
	}
	result := utils.GetMaxHeap(direction, 10) //由高到低

	for i := len(result) - 1; i >= 0; i-- {
		r := result[i]
		if _, ok := checkDirection[r]; ok {
			continue
		} else {
			checkDirection[r] = r
		}
		dappInfos = append(dappInfos, dappCountMap[r]...)
	}
	//log.Info("dapplog00", zap.Any("DAPPsName", dappNameMap), zap.Any("dappCountMap", dappCountMap), zap.Any("alldirection", direction), zap.Any("result", result), zap.Any("dappInfos", dappInfos))

	return &TopDappResponse{
		Ok:        true,
		DappInfos: dappInfos,
	}, nil
}

func (s *TransactionUsecase) GetAssetDistributionByAddress(ctx context.Context, req *AssetDistributionReq) (*AssetDistributionResponse, error) {
	//tm := time.Now()
	//var dt = utils.GetDayTime(&tm)
	var checkDirection = make(map[int]int)
	var tokenCheck = make(map[string]string)
	platInfo, _ := GetChainPlatInfo(req.ChainName)
	getPriceKey := platInfo.GetPriceKey
	var mcs []*data.MarketCoinHistory
	mcsDb, e := data.MarketCoinHistoryRepoClient.ListByTimeRangesAll(ctx, req.Address, req.ChainName)
	if e != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s 链查询用户 %s 交易资产数据失败", req.ChainName, req.Address)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error(req.ChainName+" : "+req.Address+"交易资产数据失败 ", zap.Any("error", e))
		return &AssetDistributionResponse{
			Ok:           false,
			ErrorMessage: e.Error(),
		}, e
	}
	if mcsDb == nil || len(mcsDb) == 0 {
		return &AssetDistributionResponse{
			Ok: true,
		}, nil
	}

	for _, mmm := range mcsDb {
		kk := mmm.TokenAddress
		if kk == "" {
			kk = getPriceKey
		}
		if _, ok := tokenCheck[kk]; !ok {
			mcs = append(mcs, mmm)
			tokenCheck[kk] = kk
		}
	}
	tokens := make([]*v1.Tokens, 0)
	coinIds := make([]string, 0)
	mchMap := make(map[string]*data.MarketCoinHistory)
	for _, mch := range mcs {
		if mch.TokenAddress == "" {
			coinIds = append(coinIds, getPriceKey)
			mchMap[getPriceKey] = mch
		} else {
			//daibi
			tokens = append(tokens, &v1.Tokens{
				Chain:   req.ChainName,
				Address: mch.TokenAddress,
			})
			mchMap[mch.TokenAddress] = mch
		}
	}

	var abs []AssetBalance
	var orderMap = make(map[int][]AssetBalance)
	var direction = make([]int, 0)
	nowPrice, err := GetPriceFromMarket(tokens, coinIds)

	if err != nil {
		log.Info("查询实时币价失败", zap.Any("error", err))
	} else {
		for _, mch := range mcs {
			var newCnyAmount, newUsdAmount, cnyBalanceAmount, usdBalanceAmount, icon string
			usdBalanceAmountDecimal := decimal.Zero
			cnyBalanceAmountDecimal := decimal.Zero
			if mch.TokenAddress == "" {
				if nowPrice.Coins != nil && len(nowPrice.Coins) == 1 && nowPrice.Coins[0].CoinID == getPriceKey {
					newCnyAmount = strconv.FormatFloat(nowPrice.Coins[0].Price.Cny, 'f', 2, 64)
					newUsdAmount = strconv.FormatFloat(nowPrice.Coins[0].Price.Usd, 'f', 2, 64)
					cpd, _ := decimal.NewFromString(newCnyAmount)
					upd, _ := decimal.NewFromString(newUsdAmount)
					b, _ := decimal.NewFromString(mch.Balance)
					cnyBalanceAmount = b.Mul(cpd).Round(2).String()
					usdBalanceAmount = b.Mul(upd).Round(2).String()
					usdBalanceAmountDecimal = b.Mul(upd).Round(2)
					cnyBalanceAmountDecimal = b.Mul(cpd).Round(2)
					icon = nowPrice.Coins[0].Icon
				}
			} else {
			a:
				for _, t := range nowPrice.Tokens {
					if t.Chain == req.ChainName && mch.TokenAddress == t.Address {
						newCnyAmount = strconv.FormatFloat(t.Price.Cny, 'f', 2, 64)
						newUsdAmount = strconv.FormatFloat(t.Price.Usd, 'f', 2, 64)
						cpd, _ := decimal.NewFromString(newCnyAmount)
						upd, _ := decimal.NewFromString(newUsdAmount)
						b, _ := decimal.NewFromString(mch.Balance)
						cnyBalanceAmount = b.Mul(cpd).Round(2).String()
						usdBalanceAmount = b.Mul(upd).Round(2).String()
						usdBalanceAmountDecimal = b.Mul(upd).Round(2)
						cnyBalanceAmountDecimal = b.Mul(cpd).Round(2)
						icon = t.Icon
						break a
					}
				}
			}

			proportion := "0"
			orgProportion := "0"
			negative := "0"
			usdProceeds := "0"
			cnyProceeds := "0"
			cacUsd := ""
			cacCny := ""
			if usdBalanceAmountDecimal.Cmp(decimal.Zero) != 0 && mch.UsdAmount.Cmp(decimal.Zero) != 0 {
				balanceDecimal, _ := decimal.NewFromString(mch.Balance)
				cacUsd = mch.UsdAmount.Abs().DivRound(balanceDecimal, 2).String()
				cacCny = mch.CnyAmount.Abs().DivRound(balanceDecimal, 2).String()

				v := usdBalanceAmountDecimal.Sub(mch.UsdAmount.Abs())
				if v.Cmp(decimal.Zero) != 0 {
					proportion = v.Abs().DivRound(mch.UsdAmount.Abs(), 2).Mul(decimal.NewFromInt(100)).String()
					orgProportion = v.DivRound(mch.UsdAmount.Abs(), 2).Mul(decimal.NewFromInt(100)).String()
				}
				if v.IsNegative() {
					negative = "1"
					v = v.Abs()
				}
				usdProceeds = v.Round(2).String()

				vv := cnyBalanceAmountDecimal.Sub(mch.CnyAmount)
				cnyProceeds = vv.Abs().Round(2).String()
			}
			if mch.CnyPrice != "0" {
				if len(mch.CnyPrice) >= 1 {
					flag := mch.CnyPrice[0:1] == "-"
					if flag {
						mch.CnyPrice = mch.CnyPrice[1:]
					}
				}
				cacCny = mch.CnyPrice
			}
			if mch.UsdPrice != "0" {
				if len(mch.UsdPrice) >= 1 {
					flag := mch.UsdPrice[0:1] == "-"
					if flag {
						mch.UsdPrice = mch.UsdPrice[1:]
					}
				}
				cacUsd = mch.UsdPrice
			}

			abResult := AssetBalance{
				CnyAmount:        cacCny,
				UsdAmount:        cacUsd,
				NewCnyAmount:     newCnyAmount,
				NewUsdAmount:     newUsdAmount,
				CnyBalanceAmount: cnyBalanceAmount,
				UsdBalanceAmount: usdBalanceAmount,
				Symbol:           mch.Symbol,
				HoldCnyAmount:    cnyProceeds,
				HoldUsdAmount:    usdProceeds,
				Proportion:       proportion,
				Negative:         negative,
				Icon:             icon,
				TokenAddress:     mch.TokenAddress,
			}
			if usdBalanceAmountDecimal.Cmp(decimal.NewFromFloat(10.0)) == 1 {
				if req.OrderField == "1" {
					of := int(usdBalanceAmountDecimal.Mul(decimal.NewFromInt(100)).IntPart())
					if dcm, ok := orderMap[of]; ok {
						dcm = append(dcm, abResult)
						orderMap[of] = dcm
					} else {
						var dtf []AssetBalance
						dtf = append(dtf, abResult)
						orderMap[of] = dtf
					}
					direction = append(direction, of)
				}
				if req.OrderField == "2" {
					of, _ := strconv.Atoi(orgProportion)

					if dcm, ok := orderMap[of]; ok {
						dcm = append(dcm, abResult)
						orderMap[of] = dcm
					} else {
						var dtf []AssetBalance
						dtf = append(dtf, abResult)
						orderMap[of] = dtf
					}
					direction = append(direction, of)
				}
			}
		}

		var result = make([]int, 0)
		if req.DataDirection == "1" {
			result = utils.GetMaxHeap(direction, len(direction)) //由低到高
		} else {
			result = utils.GetMinHeap(direction, len(direction)) //由高到低
		}
		for _, v := range result {
			if _, ok := checkDirection[v]; ok {
				continue
			} else {
				checkDirection[v] = v
			}
			abs = append(abs, orderMap[v]...)
		}
	}

	return &AssetDistributionResponse{
		Ok:               true,
		AssetBalanceList: abs,
	}, nil
}

// GetAssetByAddress 查询用户交易资产价值数据
func (s *TransactionUsecase) GetAssetByAddress(ctx context.Context, req *TransactionTypeReq) (*AddressAssetResponse, error) {
	tm := time.Now()
	var dt = utils.GetDayTime(&tm)
	platInfo, _ := GetChainPlatInfo(req.ChainName)
	getPriceKey := platInfo.GetPriceKey
	startTime, endTime := utils.RangeTimeConvertTime(req.TimeRange)
	uahrs, e := data.UserAssetHistoryRepoClient.ListByRangeTimeAndAddressAndChainName(ctx, startTime, endTime, req.Address, req.ChainName)
	log.Info("xize", zap.Any("", uahrs))
	if e != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链查询用户交易资产价值数据，查询用户交易资产数据库失败，address:%s", req.ChainName, req.Address)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("查询用户交易资产价值数据，查询用户交易资产数据库失败", zap.Any("chainName", req.ChainName), zap.Any("address", req.Address), zap.Any("error", e))
		return &AddressAssetResponse{
			Ok:                   false,
			Proportion:           "0",
			Negative:             "0",
			ProceedsUsd:          "0",
			ProceedsCny:          "0",
			StartTime:            endTime,
			EndTime:              endTime,
			ErrorMessage:         e.Error(),
			AddressAssetTypeList: make([]AddressAssetType, 0),
		}, e
	}
	var request = &data.AssetRequest{
		ChainName:    req.ChainName,
		Address:      req.Address,
		SelectColumn: "id, chain_name, token_address, balance",
		OrderBy:      "id asc",
		Total:        false,
	}
	userAssets, _, err := data.UserAssetRepoClient.PageList(nil, request)
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链查询用户交易资产价值数据，查询用户资产数据库失败，address:%s", req.ChainName, req.Address)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("查询用户交易资产价值数据，查询用户资产数据库失败", zap.Any("chainName", req.ChainName), zap.Any("address", req.Address), zap.Any("error", e))
		return &AddressAssetResponse{
			Ok:                   false,
			Proportion:           "0",
			Negative:             "0",
			ProceedsUsd:          "0",
			ProceedsCny:          "0",
			StartTime:            endTime,
			EndTime:              endTime,
			ErrorMessage:         e.Error(),
			AddressAssetTypeList: make([]AddressAssetType, 0),
		}, e
	}

	tokens := make([]*v1.Tokens, 0)
	coinIds := make([]string, 0)
	dts := make([]int, 0)
	dtsMap := make(map[int]string)
	coinAndBalance := make(map[string]string)
	tokenAndBalance := make(map[string]string)

	//按天 排序 map
	var assetDtMap = make(map[int64]AddressAssetType)
	for _, uahr := range uahrs {
		//已计算过，就继续累加
		if ad, ok := assetDtMap[uahr.Dt]; ok {
			cad := ad.CnyAmountDecimal.Add(uahr.CnyAmount).Round(2)
			ad.CnyAmountDecimal = cad
			ad.CnyAmount = cad.String()

			uad := ad.UsdAmountDecimal.Add(uahr.UsdAmount).Round(2)
			ad.UsdAmountDecimal = uad
			ad.UsdAmount = uad.String()
			assetDtMap[uahr.Dt] = ad
		} else {
			assetDtMap[uahr.Dt] = AddressAssetType{
				CnyAmount:        uahr.CnyAmount.Round(2).String(),
				UsdAmount:        uahr.UsdAmount.Round(2).String(),
				Dt:               int(uahr.Dt),
				CnyAmountDecimal: uahr.CnyAmount.Round(2),
				UsdAmountDecimal: uahr.UsdAmount.Round(2),
			}
		}
		if _, ok := dtsMap[int(uahr.Dt)]; !ok {
			dts = append(dts, int(uahr.Dt))
			dtsMap[int(uahr.Dt)] = uahr.Address
		}
	}
	//zhubi
	for _, ua := range userAssets {
		if ua.TokenAddress == "" {
			coinIds = append(coinIds, getPriceKey)
			coinAndBalance[getPriceKey] = ua.Balance
		} else {
			//daibi
			tokens = append(tokens, &v1.Tokens{
				Chain:   req.ChainName,
				Address: ua.TokenAddress,
			})
			tokenAndBalance[ua.TokenAddress] = ua.Balance
		}
		if _, ok := dtsMap[int(dt)]; !ok {
			dts = append(dts, int(dt))
			dtsMap[int(dt)] = ua.Address
		}
	}

	//查询币价
	nowPrice, err := GetPriceFromMarket(tokens, coinIds)
	if err != nil {
		log.Info("查询用户交易资产价值数据，查询实时币价失败", zap.Any("chainName", req.ChainName), zap.Any("error", err))
	} else {
		totalCnyAmount := decimal.Zero
		totalUsdAmount := decimal.Zero
		//主币价格
		if nowPrice.Coins != nil && len(nowPrice.Coins) == 1 && nowPrice.Coins[0].CoinID == getPriceKey {
			cnyPrice := strconv.FormatFloat(nowPrice.Coins[0].Price.Cny, 'f', 2, 64)
			cpd, _ := decimal.NewFromString(cnyPrice)
			usdPrice := strconv.FormatFloat(nowPrice.Coins[0].Price.Usd, 'f', 2, 64)
			upd, _ := decimal.NewFromString(usdPrice)

			balanceStr := coinAndBalance[getPriceKey]
			balance, _ := decimal.NewFromString(balanceStr)
			//cny 金额
			cnyValue := balance.Mul(cpd).Round(2)
			//usd 金额
			usdValue := balance.Mul(upd).Round(2)
			totalCnyAmount = totalCnyAmount.Add(cnyValue).Round(2)
			totalUsdAmount = totalUsdAmount.Add(usdValue).Round(2)
		}
		for _, t := range nowPrice.Tokens {
			cnyPrice := strconv.FormatFloat(t.Price.Cny, 'f', 2, 64)
			cpd, _ := decimal.NewFromString(cnyPrice)
			usdPrice := strconv.FormatFloat(t.Price.Usd, 'f', 2, 64)
			upd, _ := decimal.NewFromString(usdPrice)

			balanceStr := tokenAndBalance[t.Address]
			balance, _ := decimal.NewFromString(balanceStr)
			//cny 金额
			cnyValue := balance.Mul(cpd).Round(2)
			//usd 金额
			usdValue := balance.Mul(upd).Round(2)
			totalCnyAmount = totalCnyAmount.Add(cnyValue).Round(2)
			totalUsdAmount = totalUsdAmount.Add(usdValue).Round(2)
		}
		assetDtMap[dt] = AddressAssetType{
			CnyAmount:        totalCnyAmount.String(),
			UsdAmount:        totalUsdAmount.String(),
			Dt:               int(dt),
			CnyAmountDecimal: totalCnyAmount,
			UsdAmountDecimal: totalUsdAmount,
		}
	}

	//排序由小到大
	result := utils.GetMaxHeap(dts, len(dts))
	addressAssetTypeList := make([]AddressAssetType, 0)
	for index, dtt := range result {
		if index == 0 {
			startTime = assetDtMap[int64(dtt)].Dt
		}
		addressAssetTypeList = append(addressAssetTypeList, assetDtMap[int64(dtt)])
	}

	proportion := "0"
	negative := "0"
	proceedsUsd := "0"
	proceedsCny := "0"
	if len(result) >= 1 {
		frist := int64(result[0])
		end := int64(result[len(result)-1])
		old := assetDtMap[frist]
		current := assetDtMap[end]
		if old.UsdAmountDecimal.Cmp(decimal.Zero) != 0 {
			proportion = current.UsdAmountDecimal.DivRound(old.UsdAmountDecimal, 4).Mul(decimal.NewFromInt(100)).String()
		}
		v := current.UsdAmountDecimal.Sub(old.UsdAmountDecimal).Round(2)
		vv := current.CnyAmountDecimal.Sub(old.CnyAmountDecimal).Round(2)
		if v.IsNegative() {
			negative = "1"
			v = v.Abs()
		}
		proceedsUsd = v.String()
		proceedsCny = vv.Abs().String()
	}

	return &AddressAssetResponse{
		Ok:                   true,
		Proportion:           proportion,
		Negative:             negative,
		ProceedsUsd:          proceedsUsd,
		ProceedsCny:          proceedsCny,
		StartTime:            startTime,
		EndTime:              endTime,
		AddressAssetTypeList: addressAssetTypeList,
	}, nil
}

func (s *TransactionUsecase) GetTransactionTop(ctx context.Context, req *TransactionTypeReq) (*TransactionTopResponse, error) {
	//查询 from 为 用户地址
	//查询to 为用户地址  native，transfer eventlog
	var checkDirection = make(map[int]int)
	startTime, endTime := utils.RangeTimeConvertTime(req.TimeRange)
	toRecords, _ := data.EvmTransactionRecordRepoClient.FindByAddressCount(ctx, GetTableName(req.ChainName), req.Address, "to_address", startTime, endTime, "from_address")
	fromRecords, _ := data.EvmTransactionRecordRepoClient.FindByAddressCount(ctx, GetTableName(req.ChainName), req.Address, "from_address", startTime, endTime, "to_address")
	//log.Info("huangtugaoyuan",zap.Any())

	var tops = make(map[string]int64)
	var counts = make([]int, 0)
	var result = make(map[int][]string)
	for _, tr := range toRecords {
		tops[tr.Address] = int64(tr.Count)
	}
	for _, tr := range fromRecords {
		if ad, ok := tops[tr.Address]; ok {
			tops[tr.Address] = ad + int64(tr.Count)
		} else {
			tops[tr.Address] = int64(tr.Count)
		}
	}

	for k, v := range tops {
		c := int(v)
		counts = append(counts, c)
		if rr, ok := result[c]; ok {
			rr = append(rr, k)
			result[c] = rr
		} else {
			var nrr []string
			nrr = append(nrr, k)
			result[c] = nrr
		}
	}
	var tts []TransactionTop
	mrs := utils.GetMaxHeap(counts, 10) //由高到低
	for i := len(mrs) - 1; i >= 0; i-- {
		r := mrs[i]
		if _, ok := checkDirection[r]; ok {
			continue
		} else {
			checkDirection[r] = r
		}
		add := result[r]
		for _, rv := range add {
			tts = append(tts, TransactionTop{
				Count:   int64(r),
				Address: rv,
			})
		}
	}

	return &TransactionTopResponse{
		Ok:                 true,
		TransactionTopList: tts,
	}, nil
}

// GetAssetByAddressAndTime 查询交易资产分布数据
func (s *TransactionUsecase) GetAssetByAddressAndTime(ctx context.Context, req *TransactionTypeReq) (*TokenAssetAndCountResponse, error) {
	startTime, endTime := utils.RangeTimeConvertTime(req.TimeRange)
	mcs, e := data.MarketCoinHistoryRepoClient.ListByTimeRanges(ctx, req.Address, req.ChainName, startTime, endTime)
	if e != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链交易资产分布数据，查询交易资产数据库失败，address:%s", req.ChainName, req.Address)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("查询交易资产分布数据，查询交易资产数据库失败", zap.Any("chainName", req.ChainName), zap.Any("address", req.Address), zap.Any("error", e))
		return &TokenAssetAndCountResponse{
			Ok:           false,
			ErrorMessage: e.Error(),
		}, e
	}
	totalTransactionQuantity := 0
	if mcs == nil || len(mcs) == 0 {
		return &TokenAssetAndCountResponse{
			Ok:                       true,
			TotalCnyAmount:           "0",
			TotalUsdAmount:           "0",
			TotalTransactionQuantity: totalTransactionQuantity,
		}, nil
	}
	platInfo, _ := GetChainPlatInfo(req.ChainName)
	symbol := platInfo.NativeCurrency
	getPriceKey := platInfo.GetPriceKey
	totalTransactionq, ctnaa, totalCnyAmount, totalUsdAmount, err := handleCoinPrice(mcs, getPriceKey, symbol, req.OrderField, req.ChainName)
	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链交易资产分布数据，处理币价失败，address:%s", req.ChainName, req.Address)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error("查询交易资产分布数据，处理币价失败", zap.Any("chainName", req.ChainName), zap.Any("address", req.Address), zap.Any("error", e))
		return &TokenAssetAndCountResponse{
			Ok:           false,
			ErrorMessage: e.Error(),
		}, e
	}
	return &TokenAssetAndCountResponse{
		Ok:                        true,
		TotalCnyAmount:            totalCnyAmount.String(),
		TotalUsdAmount:            totalUsdAmount.String(),
		TotalTransactionQuantity:  totalTransactionq,
		ChainTokenNumberAndAssets: ctnaa,
	}, nil
}

func handleCoinPrice(mcs []*data.MarketCoinHistory, getPriceKey string, nativeSymbol string, orderField string, chainName string) (int, []ChainTokenNumberAndAsset, decimal.Decimal, decimal.Decimal, error) {
	//token 交易数量，带小数点的
	tokenAmount := make(map[string]decimal.Decimal)
	coinIdsMap := make(map[string]string)
	tokenIdsMap := make(map[string]string)
	countCoinNum := make(map[string]int)
	orderMap := make(map[int][]ChainTokenNumberAndAsset)
	orderList := make([]int, 0)

	totalCnyAmount := decimal.Zero
	totalUsdAmount := decimal.Zero
	totalTransactionQuantity := 0
	ctnaa := make([]ChainTokenNumberAndAsset, 0)
	ctnaaResult := make([]ChainTokenNumberAndAsset, 0)
	octbaa := ChainTokenNumberAndAsset{
		Symbol:              "other",
		CnyAmount:           "0",
		UsdAmount:           "0",
		TransactionQuantity: 0,
	}
	log.Info("lebenggg", zap.Any("mcs", mcs))
	for _, ta := range mcs {
		if ta.Symbol == "" {
			continue
		}
		tb, _ := decimal.NewFromString(ta.TransactionBalance)
		if ta.TokenAddress == "" {
			//cny
			if v, ok := tokenAmount[getPriceKey]; ok {
				//有记录后 增加
				tokenAmount[getPriceKey] = v.Add(tb)
			} else {
				tokenAmount[getPriceKey] = tb
			}

			if _, ok := coinIdsMap[getPriceKey]; !ok {
				coinIdsMap[getPriceKey] = nativeSymbol
			}

			if n, ok := countCoinNum[getPriceKey]; ok {
				countCoinNum[getPriceKey] = n + int(ta.TransactionQuantity)
			} else {
				countCoinNum[getPriceKey] = int(ta.TransactionQuantity)
			}

		} else {
			//代币
			if v, ok := tokenAmount[ta.TokenAddress]; ok {
				//有记录后 增加
				tokenAmount[ta.TokenAddress] = v.Add(tb)
			} else {
				tokenAmount[ta.TokenAddress] = tb
			}

			if _, ok := tokenIdsMap[ta.TokenAddress]; !ok {
				tokenIdsMap[ta.TokenAddress] = ta.Symbol
			}

			if tn, ok := countCoinNum[ta.TokenAddress]; ok {
				countCoinNum[ta.TokenAddress] = tn + int(ta.TransactionQuantity)
			} else {
				countCoinNum[ta.TokenAddress] = int(ta.TransactionQuantity)
			}
		}
	}
	log.Info("lebenggg1", zap.Any("totalTransactionQuantity", totalTransactionQuantity), zap.Any("tokenAmount", tokenAmount), zap.Any("countCoinNum", countCoinNum))

	tokens := make([]*v1.Tokens, 0)
	for k, _ := range tokenIdsMap {
		tokens = append(tokens, &v1.Tokens{
			Chain:   chainName,
			Address: k,
		})
	}
	coinIds := make([]string, 0)
	for ck, _ := range coinIdsMap {
		coinIds = append(coinIds, ck)
	}

	//查询币价
	nowPrice, err := GetPriceFromMarket(tokens, coinIds)
	if err != nil {
		log.Error("查询币价失败", zap.Any("chainName", chainName), zap.Any("error", err))
		return 0, nil, totalCnyAmount, totalUsdAmount, err
	}
	//主币价格
	if nowPrice.Coins != nil && len(nowPrice.Coins) == 1 && nowPrice.Coins[0].CoinID == getPriceKey {
		cnyPrice := strconv.FormatFloat(nowPrice.Coins[0].Price.Cny, 'f', 2, 64)
		cpd, _ := decimal.NewFromString(cnyPrice)
		usdPrice := strconv.FormatFloat(nowPrice.Coins[0].Price.Usd, 'f', 2, 64)
		upd, _ := decimal.NewFromString(usdPrice)

		balance := tokenAmount[getPriceKey]
		//cny 金额
		cnyValue := balance.Mul(cpd).Round(2)
		//usd 金额
		usdValue := balance.Mul(upd).Round(2)
		hh := ChainTokenNumberAndAsset{
			Symbol:              coinIdsMap[getPriceKey],
			CnyAmount:           cnyValue.String(),
			UsdAmount:           usdValue.String(),
			TransactionQuantity: countCoinNum[getPriceKey],
		}
		if orderField == "1" {
			orderList = append(orderList, countCoinNum[getPriceKey])
			if ov, ok := orderMap[countCoinNum[getPriceKey]]; ok {
				ov = append(ov, hh)
				orderMap[countCoinNum[getPriceKey]] = ov
			} else {
				var newc []ChainTokenNumberAndAsset
				newc = append(newc, hh)
				orderMap[countCoinNum[getPriceKey]] = newc
			}
		}
		if orderField == "2" {
			orderList = append(orderList, int(usdValue.IntPart()))
			if ov, ok := orderMap[int(usdValue.IntPart())]; ok {
				ov = append(ov, hh)
				orderMap[int(usdValue.IntPart())] = ov
			} else {
				var newc []ChainTokenNumberAndAsset
				newc = append(newc, hh)
				orderMap[int(usdValue.IntPart())] = newc
			}
		}

		totalCnyAmount = totalCnyAmount.Add(cnyValue)
		totalUsdAmount = totalUsdAmount.Add(usdValue)
	}
	//拿出币价，计算
	for _, v1 := range nowPrice.Tokens {
		cnyTokenPrice := strconv.FormatFloat(v1.Price.Cny, 'f', 2, 64)
		ctpd, _ := decimal.NewFromString(cnyTokenPrice)
		usdTokenPrice := strconv.FormatFloat(v1.Price.Usd, 'f', 2, 64)
		utpd, _ := decimal.NewFromString(usdTokenPrice)
		tokenBalance := tokenAmount[v1.Address]
		//cny 金额
		cnyTokenValue := tokenBalance.Mul(ctpd).Round(2)
		//usd 金额
		usdTokenValue := tokenBalance.Mul(utpd).Round(2)

		thh := ChainTokenNumberAndAsset{
			TokenAddress:        v1.Address,
			CnyAmount:           cnyTokenValue.String(),
			UsdAmount:           usdTokenValue.String(),
			TransactionQuantity: countCoinNum[v1.Address],
		}
		tokenInfo, e := GetTokenInfoRetryAlert(nil, chainName, v1.Address)
		if e == nil {
			thh.Symbol = tokenInfo.Symbol
		}

		if orderField == "1" {
			orderList = append(orderList, countCoinNum[v1.Address])
			if ov, ok := orderMap[countCoinNum[v1.Address]]; ok {
				ov = append(ov, thh)
				orderMap[countCoinNum[v1.Address]] = ov
			} else {
				var newc []ChainTokenNumberAndAsset
				newc = append(newc, thh)
				orderMap[countCoinNum[v1.Address]] = newc
			}
		}
		if orderField == "2" {
			orderList = append(orderList, int(usdTokenValue.IntPart()))
			if ov, ok := orderMap[int(usdTokenValue.IntPart())]; ok {
				ov = append(ov, thh)
				orderMap[int(usdTokenValue.IntPart())] = ov
			} else {
				var newc []ChainTokenNumberAndAsset
				newc = append(newc, thh)
				orderMap[int(usdTokenValue.IntPart())] = newc
			}
		}

		totalCnyAmount = totalCnyAmount.Add(cnyTokenValue)
		totalUsdAmount = totalUsdAmount.Add(usdTokenValue)
	}

	result := utils.GetMinHeap(orderList, len(orderList)) //由高到低
	log.Info("lebenggg3", zap.Any("orderList", orderList), zap.Any("result", result), zap.Any("orderMap", orderMap))
	var checkDirection = make(map[int]int)

	for _, r := range result {
		if _, ok := checkDirection[r]; ok {
			continue
		} else {
			checkDirection[r] = r
		}
		ctnaa = append(ctnaa, orderMap[r]...)
	}
	for index, rv := range ctnaa {
		if index >= 5 {
			cad, _ := decimal.NewFromString(octbaa.CnyAmount)
			uad, _ := decimal.NewFromString(octbaa.UsdAmount)
			ccad, _ := decimal.NewFromString(rv.CnyAmount)
			cuad, _ := decimal.NewFromString(rv.UsdAmount)
			octbaa.TransactionQuantity = octbaa.TransactionQuantity + rv.TransactionQuantity
			octbaa.CnyAmount = cad.Add(ccad).Round(2).String()
			octbaa.UsdAmount = uad.Add(cuad).Round(2).String()
			totalTransactionQuantity = totalTransactionQuantity + rv.TransactionQuantity
		} else {
			ctnaaResult = append(ctnaaResult, rv)
			totalTransactionQuantity = totalTransactionQuantity + rv.TransactionQuantity

		}
	}
	if octbaa.TransactionQuantity != 0 {
		ctnaaResult = append(ctnaaResult, octbaa)
	}
	return totalTransactionQuantity, ctnaaResult, totalCnyAmount.Round(2), totalUsdAmount.Round(2), nil
}

// GetTransactionTypeDistributionByAddress 交易类型分布
func (s *TransactionUsecase) GetTransactionTypeDistributionByAddress(ctx context.Context, req *TransactionTypeReq) (*TransactionTypeDistributionResponse, error) {
	startTime, endTime := utils.RangeTimeConvertTime(req.TimeRange)
	etcList, e := data.EvmTransactionRecordRepoClient.FindTransactionTypeByAddress(ctx, GetTableName(req.ChainName), req.Address, startTime, endTime)
	if e != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s 链查询用户 %s 交易类型数据失败", req.ChainName, req.Address)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error(req.ChainName+" : "+req.Address+"交易类型数据失败 ", zap.Any("error", e))
		return &TransactionTypeDistributionResponse{
			Ok:           false,
			ErrorMessage: e.Error(),
		}, e
	}
	if etcList == nil || len(etcList) == 0 {
		return &TransactionTypeDistributionResponse{
			Ok:    true,
			Total: 0,
		}, nil
	}
	txTotal := 0
	var ttds []TransactionTypeDistribution
	otherTransactionType := TransactionTypeDistribution{
		TransactionType: OTHER,
		Count:           0,
	}
	for _, evmTransactionCount := range etcList {
		if evmTransactionCount.TransactionType == NATIVE || evmTransactionCount.TransactionType == TRANSFER || evmTransactionCount.TransactionType == CONTRACT || evmTransactionCount.TransactionType == CREATECONTRACT || evmTransactionCount.TransactionType == SWAP {
			ttd := TransactionTypeDistribution{
				TransactionType: evmTransactionCount.TransactionType,
				Count:           evmTransactionCount.Count,
			}
			ttds = append(ttds, ttd)
		} else {
			otherTransactionType.Count = otherTransactionType.Count + evmTransactionCount.Count
		}

		txTotal = txTotal + int(evmTransactionCount.Count)
	}
	ttds = append(ttds, otherTransactionType)

	return &TransactionTypeDistributionResponse{
		Ok:                  true,
		Total:               txTotal,
		TransactionTypeList: ttds,
	}, nil
}

func (s *TransactionUsecase) BatchRouteRpc(ctx context.Context, req *BatchRpcParams) (*pb.JsonResponse, error) {
	if req != nil && len(req.BatchReq) > 0 {
		var rr []RpcResponse
		for _, r := range req.BatchReq {
			jsonrpcReq := &pb.JsonReq{
				Method: r.MethodName,
				Params: r.Params,
			}
			jsonResp, e := s.JsonRpc(ctx, jsonrpcReq)
			if e != nil {
				return &pb.JsonResponse{
					Ok:       false,
					ErrorMsg: e.Error(),
				}, e
			}
			brr := RpcResponse{
				MethodName: r.MethodName,
				Result:     jsonResp.Response,
			}
			if !jsonResp.Ok {
				brr.Result = jsonResp.ErrorMsg
			}
			rr = append(rr, brr)
		}
		ret, _ := utils.JsonEncode(rr)
		return &pb.JsonResponse{
			Ok:       true,
			Response: ret,
		}, nil
	}
	return &pb.JsonResponse{
		Ok:       false,
		ErrorMsg: "param error",
	}, nil
}

// GetDataDictionary 查询数据字典
func (s *TransactionUsecase) GetDataDictionary(ctx context.Context) (*DataDictionary, error) {
	var result = &DataDictionary{}
	var serviceTransactionType = []string{NATIVE, TRANSFER, TRANSFERNFT, APPROVE, APPROVENFT,
		CONTRACT, CREATECONTRACT, EVENTLOG, CREATEACCOUNT, CLOSEACCOUNT, REGISTERTOKEN, DIRECTTRANSFERNFTSWITCH,
		OTHER, SETAPPROVALFORALL, TRANSFERFROM, SAFETRANSFERFROM, SAFEBATCHTRANSFERFROM, MINT, SWAP, ADDLIQUIDITY}
	var serviceStaus = []string{SUCCESS, FAIL, PENDING, NO_STATUS, DROPPED_REPLACED, DROPPED}
	result.Ok = true
	result.ServiceTransactionType = serviceTransactionType
	result.ServiceStatus = serviceStaus

	return result, nil
}

// GetPendingAmount 查询pending状态交易总金额
func (s *TransactionUsecase) GetPendingAmount(ctx context.Context, req *AddressPendingAmountRequest) (*AddressPendingAmountResponse, error) {
	if req == nil || len(req.ChainAndAddressList) == 0 {
		return nil, nil
	}
	userAssetMap := make(map[string]decimal.Decimal)
	userAssetDecimalResult := make(map[string]string)
	userAssetTokenMap := make(map[string]map[string]decimal.Decimal)
	userAssetTokenDecimalResult := make(map[string]map[string]string)

	var result = make(map[string]PendingInfo)
	var list []*pb.TransactionRecord
	for _, apa := range req.ChainAndAddressList {
		add := apa.Address
		chainName := apa.ChainName
		if chainName == "" {
			continue
		}
		addChainName := chainName + "-" + add
		chainType, _ := GetChainNameType(chainName)
		if chainType == "" && strings.Contains(chainName, "evm") {
			chainType = EVM
		}
		switch chainType {
		case EVM:
			if add != "" {
				add = types2.HexToAddress(add).Hex()
			}
		}

		switch chainType {
		case POLKADOT:
			recordList, err := data.DotTransactionRecordRepoClient.PendingByAddress(ctx, GetTableName(chainName), add)
			if err == nil {
				err = utils.CopyProperties(recordList, &list)
			}
			if err != nil {
				return nil, err
			}
		case CASPER:
			recordList, err := data.CsprTransactionRecordRepoClient.PendingByAddress(ctx, GetTableName(chainName), add)
			if err == nil {
				err = utils.CopyProperties(recordList, &list)
			}
			if err != nil {
				return nil, err
			}
		case NERVOS:
			recordList, err := data.CkbTransactionRecordRepoClient.PendingByAddress(ctx, GetTableName(chainName), add)
			if err == nil {
				err = utils.CopyProperties(recordList, &list)
			}
			if err != nil {
				return nil, err
			}
		case BTC:
			recordList, err := data.BtcTransactionRecordRepoClient.PendingByAddress(ctx, GetTableName(chainName), add)
			if err == nil {
				err = utils.CopyProperties(recordList, &list)
			}
			if err != nil {
				return nil, err
			}
		case EVM:
			recordList, err := data.EvmTransactionRecordRepoClient.PendingByAddress(ctx, GetTableName(chainName), add)
			if err == nil {
				err = utils.CopyProperties(recordList, &list)
			}
			if err != nil {
				return nil, err
			}
		case STC:
			recordList, err := data.StcTransactionRecordRepoClient.PendingByAddress(ctx, GetTableName(chainName), add)
			if err == nil {
				err = utils.CopyProperties(recordList, &list)
			}
			if err != nil {
				return nil, err
			}
		case TVM:
			recordList, err := data.TrxTransactionRecordRepoClient.PendingByAddress(ctx, GetTableName(chainName), add)
			if err == nil {
				err = utils.CopyProperties(recordList, &list)
			}
			if err != nil {
				return nil, err
			}
		case APTOS:
			recordList, err := data.AptTransactionRecordRepoClient.PendingByAddress(ctx, GetTableName(chainName), add)
			if err == nil {
				err = utils.CopyProperties(recordList, &list)
			}
			if err != nil {
				return nil, err
			}
		case SUI:
			recordList, err := data.SuiTransactionRecordRepoClient.PendingByAddress(ctx, GetTableName(chainName), add)
			if err == nil {
				err = utils.CopyProperties(recordList, &list)
			}
			if err != nil {
				return nil, err
			}
		case SOLANA:
			recordList, err := data.SolTransactionRecordRepoClient.PendingByAddress(ctx, GetTableName(chainName), add)
			if err == nil {
				err = utils.CopyProperties(recordList, &list)
			}
			if err != nil {
				return nil, err
			}
		case COSMOS:
			recordList, err := data.AtomTransactionRecordRepoClient.PendingByAddress(ctx, GetTableName(chainName), add)
			if err == nil {
				err = utils.CopyProperties(recordList, &list)
			}
			if err != nil {
				return nil, err
			}
		case KASPA:
			recordList, err := data.KasTransactionRecordRepoClient.PendingByAddress(ctx, GetTableName(chainName), add)
			if err == nil {
				err = utils.CopyProperties(recordList, &list)
			}
			if err != nil {
				return nil, err
			}
		}

		// 主币 精度
		platInfo, _ := GetChainPlatInfo(chainName)
		var decimals int32
		if platInfo != nil {
			decimals = platInfo.Decimal
		} else {
			decimals = 18
		}
		if len(list) == 0 {
			result[chainName+"-"+strings.ToLower(add)] = CreatePendingInfo("0", "0", "1", nil)
		}
		for _, record := range list {
			feeAmount, _ := decimal.NewFromString(record.FeeAmount)
			amount, _ := decimal.NewFromString(record.Amount)
			var tokenInfo *types.TokenInfo
			if record.TokenInfo != "" {
				tokenInfo, _ = ConvertGetTokenInfo(chainName, record.TokenInfo)
			} else {
				tokenInfo, _ = ParseGetTokenInfo(chainName, record.ParseData)
			}

			switch record.TransactionType {
			case NATIVE, "":
				var totalAmount decimal.Decimal
				oldTotal := userAssetMap[addChainName]
				if record.FromAddress == add {
					totalAmount = feeAmount.Add(amount)
					totalAmount = oldTotal.Sub(totalAmount)
				} else {
					totalAmount = oldTotal.Add(amount)
				}
				userAssetMap[addChainName] = totalAmount
				total := utils.StringDecimalsValue(totalAmount.String(), int(decimals))
				userAssetDecimalResult[addChainName] = total
			case TRANSFER:
				var totalAmount = decimal.Zero
				var totalTokenAmount decimal.Decimal
				oldTotal := userAssetMap[addChainName]
				tokenAddress := tokenInfo.Address
				tokenDecimals := tokenInfo.Decimals
				ta := tokenInfo.Amount
				tokenAmount, _ := decimal.NewFromString(ta)
				if record.FromAddress == add {
					totalAmount = oldTotal.Sub(feeAmount)
					oldTokenAmount := userAssetTokenMap[add][tokenAddress]
					totalTokenAmount = oldTokenAmount.Sub(tokenAmount)

				} else {
					oldTokenAmount := userAssetTokenMap[add][tokenAddress]
					totalTokenAmount = oldTokenAmount.Add(tokenAmount)
					totalAmount = oldTotal
				}
				userAssetMap[addChainName] = totalAmount
				total := utils.StringDecimalsValue(totalAmount.String(), int(decimals))
				userAssetDecimalResult[addChainName] = total
				if userAssetTokenMap[addChainName] == nil {
					var tv = make(map[string]decimal.Decimal)
					tv[tokenAddress] = totalTokenAmount
					userAssetTokenMap[addChainName] = tv
				} else {
					userAssetTokenMap[addChainName][tokenAddress] = totalTokenAmount
				}
				totalToken := utils.StringDecimalsValue(totalTokenAmount.String(), int(tokenDecimals))
				var utv = make(map[string]string)
				utv[tokenAddress] = totalToken
				userAssetTokenDecimalResult[addChainName] = utv
			case APPROVENFT, CONTRACT, APPROVE, TRANSFERNFT, SAFETRANSFERFROM, SAFEBATCHTRANSFERFROM, SETAPPROVALFORALL, CREATEACCOUNT, CLOSEACCOUNT, REGISTERTOKEN, DIRECTTRANSFERNFTSWITCH, MINT, SWAP, ADDLIQUIDITY:
				if record.FromAddress == add {
					oldTotal := userAssetMap[addChainName]
					totalAmount := oldTotal.Sub(feeAmount)
					userAssetMap[addChainName] = totalAmount
					total := utils.StringDecimalsValue(totalAmount.String(), int(decimals))
					userAssetDecimalResult[addChainName] = total
				}
			default:
				continue
			}
		}
	}
	if len(userAssetMap) == 0 {
		for _, apa := range req.ChainAndAddressList {
			add := apa.Address
			chainName := apa.ChainName
			result[chainName+"-"+strings.ToLower(add)] = CreatePendingInfo("0", "0", "1", nil)
		}
	} else {
		for key, userAsset := range userAssetMap {
			decimalAmount := userAssetDecimalResult[key]
			flag := decimalAmount[0:1] == "-"

			modes := strings.Split(key, "-")
			chainName := modes[0]
			address := strings.ToLower(modes[1])

			tokenMap := userAssetTokenMap[key]
			var tokenList = make(map[string]PendingTokenInfo)
			for tokenAddress, tokenAsset := range tokenMap {
				decimalTokenAsset := userAssetTokenDecimalResult[key][tokenAddress]

				ft := "1"
				tokenFlag := decimalTokenAsset[0:1] == "-"
				tokenAmount := tokenAsset.String()
				if tokenFlag {
					tokenAmount = tokenAmount[1:]
					decimalTokenAsset = decimalTokenAsset[1:]
					ft = "0"
				}
				tokenList[strings.ToLower(tokenAddress)] = CreatePendingTokenInfo(tokenAmount, decimalTokenAsset, ft)
			}
			amount := userAsset.String()
			fat := "1"
			if flag {
				amount = userAsset.String()[1:]
				decimalAmount = decimalAmount[1:]
				fat = "0"
			}
			result[chainName+"-"+address] = CreatePendingInfo(amount, decimalAmount, fat, tokenList)
		}
	}
	return &AddressPendingAmountResponse{
		Result: result,
	}, nil
}

// GetFeeInfoByChainName 查询gasFee相关数据
func (s *TransactionUsecase) GetFeeInfoByChainName(ctx context.Context, req *ChainFeeInfoReq) (*ChainFeeInfoResp, error) {
	chainName := req.ChainName
	if chainName == "SeiTEST" {
		return &ChainFeeInfoResp{
			ChainName: chainName,
			GasPrice:  "150000",
		}, nil
	}
	if chainName == "" || (chainName != "PolygonTEST" && chainName != "BSCTEST" && chainName != "ETHGoerliTEST" && chainName != "ETH" && chainName != "Polygon" && chainName != "ScrollL2TEST" && chainName != "BSC" && chainName != "Optimism" && chainName != "TRX") {
		return nil, errors.New("unsupported chain")
	}

	gasPrice, err := data.RedisClient.Get(TX_FEE_GAS_PRICE + chainName).Result()
	if fmt.Sprintf("%s", err) == REDIS_NIL_KEY && chainName == "TRX" {
		data.RedisClient.Set(TX_FEE_GAS_PRICE+chainName, "420", 0).Err()
		gasPrice = "420"
	}
	maxFeePerGas, err := data.RedisClient.Get(TX_FEE_MAX_FEE_PER_GAS + chainName).Result()
	maxPriorityFeePerGas, err := data.RedisClient.Get(TX_FEE_MAX_PRIORITY_FEE_PER_GAS + chainName).Result()
	if err != nil && fmt.Sprintf("%s", err) != REDIS_NIL_KEY {
		return nil, err
	}

	return &ChainFeeInfoResp{
		ChainName:            chainName,
		GasPrice:             gasPrice,
		MaxFeePerGas:         maxFeePerGas,
		MaxPriorityFeePerGas: maxPriorityFeePerGas,
	}, nil
}

// GetFeeInfoDecimalsByChainName 查询带精度的gasFee相关数据
func (s *TransactionUsecase) GetFeeInfoDecimalsByChainName(ctx context.Context, req *ChainFeeInfoReq) (*ChainFeeInfoResp, error) {
	chainName := req.ChainName

	if chainName == "" || (chainName != "PolygonTEST" && chainName != "BSCTEST" && chainName != "ETHGoerliTEST" && chainName != "ETH" && chainName != "Polygon" && chainName != "BSC") {
		return nil, errors.New("unsupported chain")
	}
	mainDecimals := 18
	if platInfo, ok := GetChainPlatInfo(chainName); ok {
		mainDecimals = int(platInfo.Decimal)
		log.Info(chainName, zap.Any("mainDecimals", mainDecimals))
	}

	gasPrice, err := data.RedisClient.Get(TX_FEE_GAS_PRICE + chainName).Result()
	if fmt.Sprintf("%s", err) == REDIS_NIL_KEY && chainName == "TRX" {
		data.RedisClient.Set(TX_FEE_GAS_PRICE+chainName, "420", 0).Err()
		gasPrice = "420"
	}
	maxFeePerGas, err := data.RedisClient.Get(TX_FEE_MAX_FEE_PER_GAS + chainName).Result()
	maxPriorityFeePerGas, err := data.RedisClient.Get(TX_FEE_MAX_PRIORITY_FEE_PER_GAS + chainName).Result()
	if err != nil && fmt.Sprintf("%s", err) != REDIS_NIL_KEY {
		return nil, err
	}
	gasPrice = utils.StringDecimals(gasPrice, mainDecimals)
	maxFeePerGas = utils.StringDecimals(maxFeePerGas, mainDecimals)
	maxPriorityFeePerGas = utils.StringDecimals(maxPriorityFeePerGas, mainDecimals)

	return &ChainFeeInfoResp{
		ChainName:            chainName,
		GasPrice:             gasPrice,
		MaxFeePerGas:         maxFeePerGas,
		MaxPriorityFeePerGas: maxPriorityFeePerGas,
	}, nil
}

// UpdateUserAsset 客户端上报用户资产变更信息，后端插入pending交易记录
func (s *TransactionUsecase) UpdateUserAsset(ctx context.Context, req *UserAssetUpdateRequest) (interface{}, error) {
	if req.ChainName == "Nervos" || req.ChainName == "Polkadot" {
		return struct{}{}, nil
	}
	isSol := req.ChainName == "Solana"
	onchainAssets := req.Assets
	if len(req.Extra) > 0 {
		var extra *UserAssetExtra
		if err := json.Unmarshal(req.Extra, &extra); err != nil {
			log.Error("PARSE USER ASSET EXTRA ERROR WITH ERROR", zap.Error(err), zap.ByteString("extra", req.Extra))
		} else if extra != nil {
			onchainAssets = append(onchainAssets, extra.AllTokens...)
			log.Info("GOT USER ASSET ALL TOKENS FROM EXTRA", zap.Any("assets", onchainAssets))
			if len(extra.RecentTxns) > 0 {
				batchSaveChaindataTxns(req.ChainName, extra.RecentTxns)
			}
		}
	}

	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		if req.Address != "" {
			req.Address = types2.HexToAddress(req.Address).Hex()
		}
		for i := range onchainAssets {
			onchainAssets[i].TokenAddress = types2.HexToAddress(onchainAssets[i].TokenAddress).Hex()
		}
	case COSMOS:
		for i := range onchainAssets {
			onchainAssets[i].TokenAddress = utils.AddressIbcToLower(onchainAssets[i].TokenAddress)
		}
	}

	dbAssets, err := data.UserAssetRepoClient.List(ctx, &data.AssetRequest{
		ChainName: req.ChainName,
		Address:   req.Address,
	})
	if err != nil {
		return nil, err
	}
	if isSol {
		nftAssets, err := data.UserNftAssetRepoClient.List(ctx, &data.NftAssetRequest{
			ChainName: req.ChainName,
			Address:   req.Address,
		})
		if err != nil {
			log.Error("SOLANA MERGE NFT ASSETS FAILED", zap.Error(err))
		} else {
			for _, a := range nftAssets {
				dbAssets = append(dbAssets, &data.UserAsset{
					ChainName:    a.ChainName,
					Uid:          a.Uid,
					Address:      a.Address,
					TokenAddress: a.TokenAddress,
					TokenUri:     a.TokenUri,
					Balance:      a.Balance,
					Symbol:       a.Symbol,
				})
			}
			log.Info("SOLANA MERGED NFT ASSETS", zap.Int("num", len(nftAssets)))
		}
	}

	assetsGroupByToken := make(map[string]*data.UserAsset)
	for _, item := range dbAssets {
		assetsGroupByToken[item.TokenAddress] = item
	}

	uniqAssets := make(map[string]UserAsset)
	for _, asset := range onchainAssets {
		key := fmt.Sprintf("%s%s%s", req.ChainName, req.Address, asset.TokenAddress)
		uniqAssets[key] = asset
	}

	diffBet := false
	absentNfts := make([]string, 0, 2)
	for _, newItem := range uniqAssets {
		tokenAddress := newItem.TokenAddress
		if req.Address == "0x0000000000000000000000000000000000000000" || req.Address == tokenAddress {
			tokenAddress = ""
		}
		if newItem.Decimals == 0 {
			// all tokens in COSMOS are zero decimals
			s.correctZeroDecimals(ctx, req, &newItem)
		}

		oldItem, ok := assetsGroupByToken[tokenAddress]
		if !ok {
			if newItem.Balance == "0" {
				continue
			}
			if newItem.Balance == "1" && newItem.Decimals == 0 {
				absentNfts = append(absentNfts, newItem.TokenAddress)
			}
			diffBet = true
			continue
		}

		newBalance := newItem.Balance

		// update
		if oldItem.Balance != newBalance {
			if newItem.Balance == "1" && newItem.Decimals == 0 {
				absentNfts = append(absentNfts, newItem.TokenAddress)
			}
			diffBet = true
		}
	}
	if diffBet {
		platInfo, ok := GetChainPlatInfo(req.ChainName)
		if platInfo != nil {
			log.Info("STARTING QUERY TXNS OF ADDRESS", zap.String("chainName", req.ChainName), zap.String("address", req.Address), zap.Strings("urls", platInfo.HttpURL), zap.Strings("absentNfts", absentNfts))
			go GetTxByAddress(req.ChainName, req.Address, platInfo.HttpURL, absentNfts)
		} else {
			log.Warn("QUERY TXNS OF ADDRESS MISSED PLATFORM", zap.String("chainName", req.ChainName), zap.Bool("exists", ok))
		}
	}
	return map[string]interface{}{
		"hasDiff":       diffBet,
		"onchainAssets": uniqAssets,
		"dbAssets":      assetsGroupByToken,
	}, nil
}

func (s *TransactionUsecase) correctZeroDecimals(ctx context.Context, req *UserAssetUpdateRequest, asset *UserAsset) {
	if asset.Decimals != 0 || strings.Contains(asset.Balance, ".") || asset.Balance == "0" {
		return
	}
	tokenInfo, err := s.getTokenInfo(ctx, req.ChainName, req.Address, asset)
	if err != nil {
		log.Error(req.ChainName+"链资产变更，从nodeProxy中获取代币精度失败", zap.Any("address", req.Address), zap.Any("tokenAddress", asset.TokenAddress), zap.Any("error", err))
		return
	}

	if tokenInfo.Decimals != 0 {
		newBalance := utils.StringDecimals(asset.Balance, int(tokenInfo.Decimals))
		log.Info(
			"CORRECTED ZERO DECIMALS",
			zap.String("beforeBalance", asset.Balance),
			zap.String("afterBalance", newBalance),
			zap.String("address", req.Address),
			zap.String("tokenAddress", asset.TokenAddress),
			zap.Int64("decimals", tokenInfo.Decimals),
		)
		asset.Balance = newBalance
	}
}

func (s *TransactionUsecase) getTokenInfo(ctx context.Context, chainName, address string, asset *UserAsset) (types.TokenInfo, error) {
	if asset.TokenAddress == address || asset.TokenAddress == "" {
		platInfo, ok := GetChainPlatInfo(chainName)
		if !ok {
			return *new(types.TokenInfo), errors.New("no such chain")
		}
		return types.TokenInfo{
			Address:  "",
			Decimals: int64(platInfo.Decimal),
			Symbol:   platInfo.NativeCurrency,
		}, nil
		// extract from config
	}
	return GetTokenInfoRetryAlert(ctx, chainName, asset.TokenAddress)
}

// CreateBroadcast 上报上链前交易参数相关
func (s *TransactionUsecase) CreateBroadcast(ctx *JsonRpcContext, req *BroadcastRequest) (*BroadcastResponse, error) {
	device := ctx.ParseDevice()
	var usrhs []*data.UserSendRawHistory

	var userSendRawHistory = &data.UserSendRawHistory{}
	userSendRawHistory.Uid = ctx.Uid
	userSendRawHistory.UserName = req.UserName
	userSendRawHistory.Address = req.Address
	userSendRawHistory.ChainName = req.ChainName
	userSendRawHistory.SessionId = req.SessionId
	userSendRawHistory.BaseTxInput = req.TxInput
	userSendRawHistory.CreatedAt = time.Now().Unix()
	userSendRawHistory.ErrMsg = req.ErrMsg
	userSendRawHistory.DeviceId = device.Id
	if len(device.UserAgent) > 200 {
		userSendRawHistory.UserAgent = device.UserAgent[:200]
	} else {
		userSendRawHistory.UserAgent = device.UserAgent
	}
	if req.ErrMsg != "" && req.Stage != "chaindataSendTx"{
		NotifyBroadcastTxFailed(ctx, req)
	}
	usrhs = append(usrhs, userSendRawHistory)
	result, err := data.UserSendRawHistoryRepoInst.SaveOrUpdate(ctx, usrhs)
	if err != nil {
		return &BroadcastResponse{
			Ok:      false,
			Message: err.Error(),
		}, err
	}

	return &BroadcastResponse{
		Ok: result == 1,
	}, nil
}

// CreateSignRecord 上报签名记录信息
func (s *TransactionUsecase) CreateSignRecord(ctx *JsonRpcContext, req *BroadcastRequest) (*BroadcastResponse, error) {
	sendLock.Lock()
	defer sendLock.Unlock()

	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		if req.Address != "" {
			req.Address = types2.HexToAddress(req.Address).Hex()
		}
	}
	device := ctx.ParseDevice()
	now := time.Now().Unix()
	var usrhs []*data.UserSendRawHistory

	var userSendRawHistory = &data.UserSendRawHistory{}
	userSendRawHistory.Uid = ctx.Uid
	userSendRawHistory.UserName = req.UserName
	userSendRawHistory.Address = req.Address
	userSendRawHistory.ChainName = req.ChainName
	userSendRawHistory.SessionId = req.SessionId
	userSendRawHistory.CreatedAt = now
	userSendRawHistory.UpdatedAt = now
	userSendRawHistory.ErrMsg = req.ErrMsg
	userSendRawHistory.DeviceId = device.Id
	if chainType == BTC {
		userSendRawHistory.TransactionType = NATIVE
	} else {
		userSendRawHistory.TransactionType = req.TransactionType
	}

	userSendRawHistory.SignType = req.SignType
	userSendRawHistory.SignStatus = req.SignStatus
	if req.TransactionHashList != nil && len(req.TransactionHashList) > 0 {
		userSendRawHistory.TransactionHash = strings.Join(req.TransactionHashList, ",")
		info, _ := data.UserSendRawHistoryRepoInst.GetLatestOneBySessionId(ctx, req.SessionId)
		if info != nil {
			if v, _ := GetChainNameType(info.ChainName); v == COSMOS {
				userSendRawHistory.TransactionHash = strings.ToUpper(userSendRawHistory.TransactionHash)
			}
		}
	}
	if len(device.UserAgent) > 200 {
		userSendRawHistory.UserAgent = device.UserAgent[:200]
	} else {
		userSendRawHistory.UserAgent = device.UserAgent
	}
	if req.ErrMsg != "" {
		userSendRawHistory.SignStatus = SIGNRECORD_DROPPED
		NotifyBroadcastTxFailed(ctx, req)
	}
	if req.SignStatus == "" {
		userSendRawHistory.SignStatus = SIGNRECORD_DROPPED
	}
	if req.TxInputList != nil && len(req.TxInputList) > 0 {
		userSendRawHistory.TxInput = strings.Join(req.TxInputList, ",")
		if chainType == EVM && req.SignType == "1" {
			for _, txInput := range req.TxInputList {
				var dec types.EvmTxInput
				if err := json.Unmarshal([]byte(txInput), &dec); err != nil {
					n, _ := strconv.Atoi(dec.Nonce)
					userSendRawHistory.Nonce = int64(n)
				}
			}
		}
	}
	if userSendRawHistory.TransactionHash != "" {
		tt := ""
		if chainType == COSMOS {
			tt = strings.ToUpper(req.TransactionHashList[0])
		} else {
			tt = req.TransactionHashList[0]
		}

		r, _ := data.UserSendRawHistoryRepoInst.SelectByTxHash(nil, tt)
		if r != nil && r.TransactionHash != "" {
			return &BroadcastResponse{
				Ok:      false,
				Message: "duplicate txhash",
			}, nil
		}
	}
	usrhs = append(usrhs, userSendRawHistory)
	result, err := data.UserSendRawHistoryRepoInst.SaveOrUpdate(ctx, usrhs)

	if err != nil {
		return &BroadcastResponse{
			Ok:      false,
			Message: err.Error(),
		}, err
	}

	return &BroadcastResponse{
		Ok: result > 0,
	}, nil
}

// ClearNonce 清除pending状态交易占用的nonce
func (s *TransactionUsecase) ClearNonce(ctx context.Context, req *ClearNonceRequest) (*ClearNonceResponse, error) {
	if req == nil || req.Address == "" || req.ChainName == "" {
		return &ClearNonceResponse{
			Ok:      false,
			Message: "Illegal parameter, address and chainName must not nil",
		}, nil
	}
	//查询 所有 处于pending 和 no_status 的交易记录，并更新 from to 地址 及 from_uid (不用考虑to_uid) 切割字符，然后  把交易状态改成drrop_replace
	//删除所有 该地址 的所有 redis pending的 key
	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		if req.Address != "" {
			req.Address = types2.HexToAddress(req.Address).Hex()
		}
	default:
		return &ClearNonceResponse{
			Ok:      false,
			Message: "Illegal parameter, chainName type must be evm",
		}, nil
	}

	recordList, err := data.EvmTransactionRecordRepoClient.PendingByFromAddress(ctx, GetTableName(req.ChainName), req.Address)
	if err != nil {
		return &ClearNonceResponse{
			Ok:      false,
			Message: err.Error(),
		}, err
	}
	log.Info("++++++++++++++++++++=", zap.Any("", recordList))

	if len(recordList) > 0 {
		//查询 所有 处于pending 和 no_status 的交易记录，并更新 from to 地址 及 from_uid (不用考虑to_uid) 切割字符，然后  把交易状态改成drrop_replace
		//删除所有 该地址 的所有 redis pending的 key
		//更新数据库时注意 如果 状态时 success 或者 fail 不更新
		for _, record := range recordList {
			pn := record.Nonce
			record.FromAddress = utils.StringSpiltByIndex(record.FromAddress, 20)
			record.ToAddress = utils.StringSpiltByIndex(record.ToAddress, 20)
			record.FromUid = utils.StringSpiltByIndex(record.FromUid, 20)
			record.Status = DROPPED_REPLACED
			res, err := data.EvmTransactionRecordRepoClient.UpdateNotSuccessNotFail(ctx, GetTableName(req.ChainName), record)
			log.Info("++++++++++++++++++++=", zap.Any("更新对象", record), zap.Any("更新结果", res))

			if err != nil {
				return &ClearNonceResponse{
					Ok:      false,
					Message: err.Error(),
				}, err
			}

			if res == 1 {
				pNonce := ADDRESS_PENDING_NONCE + req.ChainName + ":" + req.Address + ":" + strconv.Itoa(int(pn))
				data.RedisClient.Del(pNonce)
			}
		}
	}

	return &ClearNonceResponse{
		Ok:      true,
		Message: "",
	}, nil
}

func (s *TransactionUsecase) SigningMessage(ctx context.Context, req *signhash.SignMessageRequest) (string, error) {
	return HashSignMessage(req.ChainName, req)
}

func (s *TransactionUsecase) KanbanSummary(ctx context.Context, req *pb.KanbanSummaryRequest) (*pb.KanbanSummaryResponse, error) {
	return nil, nil
}

// 看板交易数据
func (s *TransactionUsecase) KanbanTxChart(ctx context.Context, req *pb.KanbanChartRequest) (*pb.KanbanChartResponse, error) {
	return nil, nil
}

// 看板合约数据
func (s *TransactionUsecase) KanbanContractChart(ctx context.Context, req *pb.KanbanChartRequest) (*pb.KanbanChartResponse, error) {
	return nil, nil
}

// CountOutTx 统计两个地址之间转账次数
func (s *TransactionUsecase) CountOutTx(ctx context.Context, req *CountOutTxRequest) (*CountOutTxResponse, error) {
	if req.ChainName != "" {
		chainType, _ := GetChainNameType(req.ChainName)
		switch chainType {
		case EVM:
			if req.Address != "" {
				req.Address = types2.HexToAddress(req.Address).Hex()
			}
			if req.ToAddress != "" {
				req.ToAddress = types2.HexToAddress(req.ToAddress).Hex()
			}
		}
	}

	countRequest := &data.CountRequest{
		FromAddress:         req.Address,
		ToAddress:           req.ToAddress,
		TransactionTypeList: []string{NATIVE, TRANSFER, TRANSFERNFT},
	}
	count, err := data.TransactionCountRepoClient.CountTransactionQuantity(ctx, countRequest)
	if err != nil {
		return nil, err
	}
	return &CountOutTxResponse{
		Count: count,
	}, nil
}

// GetTxCount 统计两个地址之间转账次数
func (s *TransactionUsecase) GetTxCount(ctx context.Context, req *data.CountRequest) (*CountOutTxResponse, error) {
	if req.ChainName != "" {
		chainType, _ := GetChainNameType(req.ChainName)
		switch chainType {
		case EVM:
			if req.FromAddress != "" {
				req.FromAddress = types2.HexToAddress(req.FromAddress).Hex()
			}
			if req.ToAddress != "" {
				req.ToAddress = types2.HexToAddress(req.ToAddress).Hex()
			}
			if req.FromOrToAddress != "" {
				req.FromOrToAddress = types2.HexToAddress(req.FromOrToAddress).Hex()
			}
		}
	}

	count, err := data.TransactionCountRepoClient.CountTransactionQuantity(ctx, req)
	if err != nil {
		return nil, err
	}
	return &CountOutTxResponse{
		Count: count,
	}, nil
}

// GetSignRecord 查询签名记录
func (s *TransactionUsecase) GetSignRecord(ctx context.Context, req *SignRecordReq) (*SignRecordResponse, error) {
	chainType, _ := GetChainNameType(req.ChainName)
	switch chainType {
	case EVM:
		if req.Address != "" {
			req.Address = types2.HexToAddress(req.Address).Hex()
		}
	}

	var orderBy string
	if req.TradeTime == 1 {
		orderBy = "updated_at desc"
	}
	if req.TradeTime == 2 {
		orderBy = "updated_at asc"
	}
	signRequest := &data.SignRequest{
		Address:    req.Address,
		ChainName:  req.ChainName,
		SignType:   req.SignType,
		SignStatus: req.SignStatus,
		OrderBy:    orderBy,
		PageNum:    int32(req.Page),
		PageSize:   int32(req.Limit),
		Total:      true,
	}

	if req.TransactionType != "" {
		txType := strings.Split(req.TransactionType, ",")
		for _, transactionType := range txType {
			if transactionType == OTHER {
				txType = append(txType, CONTRACT, CREATEACCOUNT, CLOSEACCOUNT, REGISTERTOKEN, DIRECTTRANSFERNFTSWITCH, CREATECONTRACT, MINT, SWAP, ADDLIQUIDITY)
				break
			}
		}
		signRequest.TransactionTypeList = txType
	}
	result, total, err := data.UserSendRawHistoryRepoInst.PageList(ctx, signRequest)

	if err != nil {
		return &SignRecordResponse{
			Ok:           false,
			ErrorMessage: err.Error(),
			Total:        0,
			Page:         0,
			Limit:        0,
		}, err
	}
	var sis []SignInfo
	for _, v := range result {
		if v.SignType == "1" && v.TransactionHash == "" {
			continue
		}
		var record *pb.TransactionRecord
		var err error
		if v.TransactionType == APPROVE || v.TransactionType == APPROVENFT {
			switch chainType {
			case POLKADOT:
				var oldRecord *data.DotTransactionRecord
				oldRecord, err = data.DotTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case CASPER:
				var oldRecord *data.CsprTransactionRecord
				oldRecord, err = data.CsprTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case NERVOS:
				var oldRecord *data.CkbTransactionRecord
				oldRecord, err = data.CkbTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case BTC:
				var oldRecord *data.BtcTransactionRecord
				oldRecord, err = data.BtcTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case EVM:
				var oldRecord *data.EvmTransactionRecord
				oldRecord, err = data.EvmTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case STC:
				var oldRecord *data.StcTransactionRecord
				oldRecord, err = data.StcTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case TVM:
				var oldRecord *data.TrxTransactionRecord
				oldRecord, err = data.TrxTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case APTOS:
				var oldRecord *data.AptTransactionRecord
				oldRecord, err = data.AptTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case SUI:
				var oldRecord *data.SuiTransactionRecord
				oldRecord, err = data.SuiTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case SOLANA:
				var oldRecord *data.SolTransactionRecord
				oldRecord, err = data.SolTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case COSMOS:
				var oldRecord *data.AtomTransactionRecord
				oldRecord, err = data.AtomTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case KASPA:
				var oldRecord *data.KasTransactionRecord
				oldRecord, err = data.KasTransactionRecordRepoClient.FindByTxHash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			}
			if err == nil && record != nil && (record.Amount == "" || record.Amount == "0") {
				var data = record.Data
				if data == "" {
					if record.ClientData != "" {
						clientData := make(map[string]interface{})
						if jsonErr := json.Unmarshal([]byte(record.ClientData), &clientData); jsonErr == nil {
							dappTxinfoMap, dok := clientData["dappTxinfo"].(map[string]interface{})
							if dok {
								data, _ = dappTxinfoMap["data"].(string)
							}
						}
					}
				}

				if data == "" {
					if record.TransactionType == APPROVE {
						v.TransactionType = "cancelApprove"
					} else if record.TransactionType == APPROVENFT {
						v.TransactionType = "cancelApproveNFT"
					}
				} else {
					switch chainType {
					case EVM, TVM:
						if len(data) == 136 && strings.HasSuffix(data, "0000000000000000000000000000000000000000000000000000000000000000") {
							if record.TransactionType == APPROVE {
								v.TransactionType = "cancelApprove"
							} else if record.TransactionType == APPROVENFT {
								v.TransactionType = "cancelApproveNFT"
							}
						}
					}
				}
			}
		}
		if v.TransactionHash != "" {
			txhash := strings.Split(v.TransactionHash, ",")
			if len(txhash) >= 1 {
				v.TransactionHash = txhash[0]
			}
		}
		sis = append(sis, SignInfo{
			Address:         v.Address,
			ChainName:       v.ChainName,
			SignType:        v.SignType,
			SignStatus:      v.SignStatus,
			SignTxInput:     v.TxInput,
			SignUser:        v.UserName,
			SignTime:        int(v.CreatedAt),
			ConfirmTime:     int(v.TxTime),
			TransactionType: v.TransactionType,
			TransactionHash: v.TransactionHash,
		})
	}
	return &SignRecordResponse{
		Ok:        true,
		SignInfos: sis,
		Total:     int(total),
		Page:      req.Page,
		Limit:     req.Limit,
	}, nil
}

func (s *TransactionUsecase) ChangeUtxoPending(ctx context.Context, req *CreateUtxoPendingReq) (*pb.CreateResponse, error) {
	if req == nil || len(req.CreateUtxoPendingList) == 0 {
		return &pb.CreateResponse{
			Status: true,
			Code:   uint64(200),
			Mes:    "传入参数为空！",
		}, nil
	}

	for _, utxo := range req.CreateUtxoPendingList {
		//更新 状态为pending
		ret, err := data.UtxoUnspentRecordRepoClient.UpdateUnspentToPending(ctx, utxo.ChainName, utxo.Address, utxo.N, utxo.Hash, req.TxHash)
		if err != nil || ret == 0 {
			log.Error(utxo.Hash, zap.Any("更新数据库失败！", err))
			return &pb.CreateResponse{
				Status: false,
				Code:   uint64(200),
				Mes:    "更新数据库失败！",
			}, err
		}
	}
	return &pb.CreateResponse{
		Status: true,
		Code:   uint64(200),
	}, nil
}

// SignMessage2Success 更新签名记录
func (s *TransactionUsecase) SignMessage2Success(ctx context.Context, req *SignTypeMessageRequest) (*BroadcastResponse, error) {
	var rr []*data.UserSendRawHistory
	rr = append(rr, &data.UserSendRawHistory{
		SessionId:  req.SessionId,
		SignStatus: req.SignStatus,
	})

	result, err := data.UserSendRawHistoryRepoInst.SaveOrUpdate(nil, rr)

	if err != nil {
		return &BroadcastResponse{
			Ok:      false,
			Message: err.Error(),
		}, err
	}
	if result == 1 {
		return &BroadcastResponse{
			Ok: true,
		}, nil
	}
	return &BroadcastResponse{
		Ok:      false,
		Message: req.SessionId + "消息签名未更新成功",
	}, nil
}

// SignTXBySessionId 根据sessionId获取txHash
func (s *TransactionUsecase) SignTXBySessionId(ctx context.Context, req *SignTxRequest) (*SignTxResponse, error) {
	infos, err := data.UserSendRawHistoryRepoInst.SelectBySessionIds(ctx, req.SessionIds)
	if err != nil {
		return &SignTxResponse{
			Ok:      false,
			Message: err.Error(),
		}, err
	}
	var sts []SessionTxhashInfo
	if infos == nil || len(infos) == 0 {
		return &SignTxResponse{
			Ok:                    true,
			SessionTxhashInfoList: sts,
		}, nil
	}
	for _, signInfo := range infos {
		if signInfo.TransactionHash != "" {
			sts = append(sts, SessionTxhashInfo{
				SessionId:       signInfo.SessionId,
				TransactionHash: signInfo.TransactionHash,
			})
		}
	}
	return &SignTxResponse{
		Ok:                    true,
		SessionTxhashInfoList: sts,
	}, nil
}

// GetBlockHeight 获取节点块高
func (s *TransactionUsecase) GetBlockHeight(ctx context.Context, req *pb.GetBlockHeightReq) (*pb.GetBlockHeightResponse, error) {

	//ETHW已经停止爬块，并且ChainList中没有，单独处理
	if req.ChainName == "ETHW" {
		client, err := ethclient.Dial("https://mainnet.ethereumpow.org")
		if err != nil {
			return nil, err
		}
		blockNumber, err := client.BlockNumber(context.Background())
		if err != nil {
			return nil, err
		}
		return &pb.GetBlockHeightResponse{Height: int64(blockNumber)}, nil
	}

	//Solana由于出块太快，已经停止爬块，单独处理
	if req.ChainName == "Solana" {
		rpcUrl := "https://solana-mainnet.g.alchemy.com/v2/DLTabdbSnGUoSoFBu-xjjfpplQFdqsP7"
		body := "{\"jsonrpc\": \"2.0\",\"method\": \"getSlot\",\"params\": [],\"id\": 1}"
		resp, err := http.Post(rpcUrl, "application/json", strings.NewReader(body))
		if err != nil {
			return nil, err
		}
		bytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		heightResp := map[string]interface{}{}
		err = json.Unmarshal(bytes, &heightResp)
		if err != nil {
			return nil, err
		}

		height, ok := heightResp["result"].(float64)
		if !ok {
			return nil, errors.New("get solana height error")
		}

		return &pb.GetBlockHeightResponse{Height: int64(height)}, nil
	}

	//如果在爬块的，直接从redis中取
	key := BLOCK_NODE_HEIGHT_KEY + req.ChainName
	heightCmd := data.RedisClient.Get(key)
	height, err := heightCmd.Int()
	if err == nil {
		return &pb.GetBlockHeightResponse{Height: int64(height)}, nil
	}

	//如果从redis未获取到，evm系则从ChainList节点获取块高
	if strings.HasPrefix(req.ChainName, "evm") {
		chainId := strings.TrimPrefix(req.ChainName, "evm")
		resp, err := s.chainListClient.GetChainNodeList(context.Background(), &v1.GetChainNodeListReq{ChainId: chainId})
		if err != nil {
			return nil, err
		}

		for _, rpc := range resp.Data {
			client, err := ethclient.Dial(rpc.Url)
			if err != nil {
				continue
			}
			blockNumber, err := client.BlockNumber(context.Background())
			if err != nil {
				continue
			}
			return &pb.GetBlockHeightResponse{Height: int64(blockNumber)}, nil
		}
	} else if strings.HasPrefix(req.ChainName, "cosmos") {
		chainId := strings.TrimPrefix(req.ChainName, "cosmos")
		resp, err := s.chainListClient.GetChainNodeList(context.Background(), &v1.GetChainNodeListReq{ChainId: chainId})
		if err != nil {
			return nil, err
		}
		for _, rpc := range resp.Data {
			client := &CosmosClient{
				chainName: req.ChainName, legacy: 1, url: rpc.Url,
			}
			blockNumber, err := client.GetBlockNumber()
			if err != nil {
				continue
			}
			return &pb.GetBlockHeightResponse{Height: int64(blockNumber)}, nil
		}
	}

	return nil, errors.New(fmt.Sprintf("can not get the height of the chain:%s", req.ChainName))
}

func convertFeeData(chainName, chainType, reqAddress string, record *pb.TransactionRecord) {
	record.ChainName = chainName
	record.StatusDetail = "{}"
	feeData := make(map[string]string)
	switch chainType {
	case BTC, SOLANA, KASPA:
		feeData = nil
	case EVM:
		feeData["gas_limit"] = record.GasLimit
		feeData["gas_used"] = record.GasUsed
		feeData["gas_price"] = record.GasPrice
		feeData["base_fee"] = record.BaseFee
		feeData["max_fee_per_gas"] = record.MaxFeePerGas
		feeData["max_priority_fee_per_gas"] = record.MaxPriorityFeePerGas

		//ParseData 字段
		//pending时间超过5分钟，交易手续费太低导致，可以尝试加速解决  "gasfeeMsg" :"1"
		//pending时间超过5分钟，有未完成交易正在排队，可以尝试加速取消起该笔之前的未完成交易 "nonceMsg":"1"
		//pending时间超过5分钟，nonce不连续无法上链，请填补空缺nonce交易 "nonceMsg":"2"
		if (record.Status == PENDING || record.Status == NO_STATUS) && time.Now().Unix()-record.TxTime > 300 && (reqAddress == "" || reqAddress == record.FromAddress) {
			evm := make(map[string]interface{})
			statusDetail := make(map[string]interface{})
			if jsonErr := json.Unmarshal([]byte(record.ParseData), &evm); jsonErr == nil {
				if record.Nonce == 0 {
					evm["pendingMsg"] = GAS_FEE_LOW
					statusDetail["pendingMsg"] = GAS_FEE_LOW
				} else {
					ret, err := data.EvmTransactionRecordRepoClient.FindByNonceAndAddress(nil, GetTableName(chainName), record.FromAddress, record.Nonce-1)
					if err == nil {
						if ret == nil {
							//请填补空缺nonce交易 "nonceMsg":"2"
							evm["pendingMsg"] = NONCE_BREAK
							statusDetail["pendingMsg"] = NONCE_BREAK
						} else {
							if ret.Status == SUCCESS || ret.Status == FAIL || ret.Status == DROPPED_REPLACED {
								// "gasfeeMsg" :"1"
								evm["pendingMsg"] = GAS_FEE_LOW
								statusDetail["pendingMsg"] = GAS_FEE_LOW
							}
							if ret.Status == PENDING || ret.Status == NO_STATUS {
								//"nonceMsg":"1"
								evm["pendingMsg"] = NONCE_QUEUE
								statusDetail["pendingMsg"] = NONCE_QUEUE
							}
							if ret.Status == DROPPED {
								//请填补空缺nonce交易 "nonceMsg":"2"
								evm["pendingMsg"] = NONCE_BREAK
								statusDetail["pendingMsg"] = NONCE_BREAK
							}
						}
					}
				}
			}

			parseDataStr, _ := utils.JsonEncode(evm)
			record.ParseData = parseDataStr

			statusDetailStr, _ := utils.JsonEncode(statusDetail)
			record.StatusDetail = statusDetailStr
		}

		if record.Status == FAIL {
			evm := make(map[string]interface{})
			statusDetail := make(map[string]interface{})
			if jsonErr := json.Unmarshal([]byte(record.ParseData), &evm); jsonErr == nil {
				//| 150878    | 149039
				gasLimit, _ := strconv.ParseFloat(record.GasLimit, 64)
				gasUsed, _ := strconv.ParseFloat(record.GasUsed, 64)

				f := gasUsed / gasLimit
				if f > 0.9 {
					evm["failMsg"] = GAS_LIMIT_LOW
					parseDataStr, _ := utils.JsonEncode(evm)
					record.ParseData = parseDataStr

					statusDetail["failMsg"] = GAS_LIMIT_LOW
					statusDetailStr, _ := utils.JsonEncode(statusDetail)
					record.StatusDetail = statusDetailStr

				}
			}
		}
	case TVM:
		feeData["fee_limit"] = record.FeeLimit
		feeData["net_usage"] = record.NetUsage
		feeData["energy_usage"] = record.EnergyUsage
	case SUI:
		feeData["gas_limit"] = record.GasLimit
		feeData["gas_used"] = record.GasUsed
	default:
		feeData["gas_limit"] = record.GasLimit
		feeData["gas_used"] = record.GasUsed
		feeData["gas_price"] = record.GasPrice
	}
	if feeData != nil {
		feeDataStr, _ := utils.JsonEncode(feeData)
		record.FeeData = feeDataStr
	}
}

// 将发送给合约的主币转成一条eventLog
func handleNativeTokenEvent(chainName string, record *pb.TransactionRecord) {
	if record == nil {
		return
	}
	if (record.TransactionType == CONTRACT || record.TransactionType == MINT || record.TransactionType == SWAP) && record.Amount != "" && record.Amount != "0" {
		eventLogStr := handleEventLog(chainName, record.FromAddress, record.ToAddress, record.Amount, record.EventLog)
		record.EventLog = eventLogStr
	}
}

func (s *TransactionUsecase) GetGasCoefficientByChainName(ctx context.Context, req *GasCoefficientReq) (*GasCoefficientResp, error) {
	gasCoefficient := GetGasCoefficient(req.ChainName)
	return &GasCoefficientResp{
		GasCoefficient: gasCoefficient,
	}, nil
}
