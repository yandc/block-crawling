package biz

import (
	pb "block-crawling/api/transaction/v1"
	v1 "block-crawling/internal/client"
	"block-crawling/internal/data"
	"block-crawling/internal/data/kanban"
	"block-crawling/internal/log"
	"block-crawling/internal/signhash"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	types2 "github.com/ethereum/go-ethereum/common"
	"github.com/go-redis/redis"
	"github.com/shopspring/decimal"
	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type TransactionUsecase struct {
	gormDB  *gorm.DB
	lark    Larker
	kBundle *kanban.Bundle
}

var sendLock sync.RWMutex

func NewTransactionUsecase(grom *gorm.DB, lark Larker, bundle *data.Bundle, kBundle *kanban.Bundle) *TransactionUsecase {
	return &TransactionUsecase{
		gormDB:  grom,
		lark:    lark,
		kBundle: kBundle,
	}
}

func (s *TransactionUsecase) GetAllOpenAmount(ctx context.Context, req *pb.OpenAmountReq) (*pb.OpenAmoutResp, error) {
	chainType := ChainNameType[req.ChainName]
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
	chainType := ChainNameType[req.ChainName]
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
		transcationType := ""
		chainType := ChainNameType[da.ChainName]
		switch chainType {
		case EVM:
			evm, err := data.EvmTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(da.ChainName), da.LastTxhash)
			if err == nil && evm != nil {
				dappInfo = evm.DappData
				parseData = evm.ParseData
				transcationType = evm.TransactionType
			}
		case STC:
			stc, err := data.StcTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(da.ChainName), da.LastTxhash)
			if err == nil && stc != nil {
				dappInfo = stc.DappData
				parseData = stc.ParseData
				transcationType = stc.TransactionType
			}
		case TVM:
			tvm, err := data.TrxTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(da.ChainName), da.LastTxhash)
			if err == nil && tvm != nil {
				dappInfo = tvm.DappData
				parseData = tvm.ParseData
				transcationType = tvm.TransactionType
			}
		case APTOS:
			apt, err := data.AptTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(da.ChainName), da.LastTxhash)
			if err == nil && apt != nil {
				dappInfo = apt.DappData
				parseData = apt.ParseData
				transcationType = apt.TransactionType
			}
		case SUI:
			sui, err := data.SuiTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(da.ChainName), da.LastTxhash)
			if err == nil && sui != nil {
				dappInfo = sui.DappData
				parseData = sui.ParseData
				transcationType = sui.TransactionType
			}
		case SOLANA:
			sol, err := data.SolTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(da.ChainName), da.LastTxhash)
			if err == nil && sol != nil {
				dappInfo = sol.DappData
				parseData = sol.ParseData
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
		if parseData != "" {
			tokenInfo, _ := ParseTokenInfo(parseData)
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
	chainType := ChainNameType[pbb.ChainName]

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

	//整理feeDate
	//var maxFeePerGas,maxPriorityFeePerGas string
	paseJson := make(map[string]string)
	if pbb.FeeData != "" {
		if jsonErr := json.Unmarshal([]byte(pbb.FeeData), &paseJson); jsonErr != nil {
			log.Warn("feedata数据解析失败", zap.Any("feeData", pbb.FeeData), zap.Any("error", jsonErr))
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

	switch chainType {
	case CASPER:
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
							log.Error(pbb.ChainName+"链插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
						} else {
							tokenMap["token_uri"] = tokenInfo.TokenUri
							parseData, _ := utils.JsonEncode(parseDataMap)
							pbb.ParseData = parseData
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
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.CsprTransactionRecordRepoClient.Save(ctx, GetTableName(pbb.ChainName), casperRecord)
	case STC:
		parseDataMap := make(map[string]interface{})
		var nonce int64
		if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &parseDataMap); jsonErr == nil {
			evmMap := parseDataMap["stc"]
			ret := evmMap.(map[string]interface{})
			nonceInt, _ := utils.GetInt(ret["sequence_number"])
			nonce = int64(nonceInt)

			tokenMap, ok := parseDataMap["token"].(map[string]interface{})
			if ok {
				tokenAddress, _ := tokenMap["address"].(string)
				if tokenAddress != "" {
					tokenType, _ := tokenMap["token_type"].(string)
					if tokenType == "" {
						tokenInfo, err := GetTokenInfoRetryAlert(context.Background(), pbb.ChainName, tokenAddress)
						if err != nil {
							log.Error(pbb.ChainName+"链插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
						} else {
							tokenMap["token_uri"] = tokenInfo.TokenUri
							parseData, _ := utils.JsonEncode(parseDataMap)
							pbb.ParseData = parseData
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
				}
			}
		}

		stcRecord := &data.StcTransactionRecord{
			BlockHash:       pbb.BlockHash,
			BlockNumber:     int(pbb.BlockNumber),
			TransactionHash: pbb.TransactionHash,
			OriginalHash:    pbb.OriginalHash,
			Nonce:           nonce,
			FromAddress:     pbb.FromAddress,
			ToAddress:       pbb.ToAddress,
			FromUid:         pbb.Uid,
			FeeAmount:       fa,
			Amount:          a,
			Status:          pbb.Status,
			TxTime:          pbb.TxTime,
			ContractAddress: pbb.ContractAddress,
			ParseData:       pbb.ParseData,
			GasLimit:        paseJson["gas_limit"],
			GasUsed:         paseJson["gas_used"],
			GasPrice:        paseJson["gas_price"],
			Data:            pbb.Data,
			EventLog:        pbb.EventLog,
			TransactionType: transactionType,
			OperateType:     pbb.OperateType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.StcTransactionRecordRepoClient.Save(ctx, GetTableName(pbb.ChainName), stcRecord)
		if result == 1 {
			key := pendingNonceKey + strconv.Itoa(int(nonce))
			data.RedisClient.Set(key, pbb.Uid, 6*time.Hour)
		}
	case POLKADOT:
		parseDataMap := make(map[string]interface{})
		var nonce int64
		if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &parseDataMap); jsonErr == nil {
			evmMap := parseDataMap["polkadot"]
			ret := evmMap.(map[string]interface{})
			nonceInt, _ := utils.GetInt(ret["nonce"])
			nonce = int64(nonceInt)

			tokenMap, ok := parseDataMap["token"].(map[string]interface{})
			if ok {
				tokenAddress, _ := tokenMap["address"].(string)
				if tokenAddress != "" {
					tokenType, _ := tokenMap["token_type"].(string)
					if tokenType == "" {
						tokenInfo, err := GetTokenInfoRetryAlert(context.Background(), pbb.ChainName, tokenAddress)
						if err != nil {
							log.Error(pbb.ChainName+"链插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
						} else {
							tokenMap["token_uri"] = tokenInfo.TokenUri
							parseData, _ := utils.JsonEncode(parseDataMap)
							pbb.ParseData = parseData
						}
					}
				}
			}
		}

		dotTransactionRecord := &data.DotTransactionRecord{
			BlockHash:       pbb.BlockHash,
			BlockNumber:     int(pbb.BlockNumber),
			Nonce:           nonce,
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
			TransactionType: pbb.TransactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.DotTransactionRecordRepoClient.Save(ctx, GetTableName(pbb.ChainName), dotTransactionRecord)
	case EVM:
		parseDataMap := make(map[string]interface{})
		var nonce int64
		if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &parseDataMap); jsonErr == nil {
			evmMap := parseDataMap["evm"]
			ret := evmMap.(map[string]interface{})
			nonceInt, _ := utils.GetInt(ret["nonce"])
			nonce = int64(nonceInt)

			tokenMap, ok := parseDataMap["token"].(map[string]interface{})
			if ok {
				tokenAddress, _ := tokenMap["address"].(string)
				if tokenAddress != "" {
					tokenType, _ := tokenMap["token_type"].(string)
					if tokenType == "" || tokenType == "ERC20" {
						tokenInfo, err := GetTokenInfoRetryAlert(context.Background(), pbb.ChainName, tokenAddress)
						if err != nil {
							log.Error(pbb.ChainName+"链插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
						} else {
							tokenMap["token_uri"] = tokenInfo.TokenUri
							parseData, _ := utils.JsonEncode(parseDataMap)
							pbb.ParseData = parseData
						}
					}
				}
			}
		}

		transactionType := pbb.TransactionType
		if transactionType == CONTRACT && ((len(pbb.Data) >= 10 && strings.HasPrefix(pbb.Data, "0x")) || (len(pbb.Data) >= 8 && !strings.HasPrefix(pbb.Data, "0x"))) {
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
				log.Warn(pbb.ChainName+"链查询nodeProxy中合约ABI失败", zap.Any("txHash", pbb.TransactionHash), zap.Any("contractAddress", contractAddress), zap.Any("methodId", methodId), zap.Any("error", err))
			}

			if strings.Contains(methodName, "Mint") || strings.Contains(methodName, "_mint") || strings.HasPrefix(methodName, "mint") {
				transactionType = MINT
			}

			if strings.Contains(methodName, "Swap") || strings.Contains(methodName, "_swap") || strings.HasPrefix(methodName, "swap") {
				transactionType = SWAP
			}
		}

		evmTransactionRecord := &data.EvmTransactionRecord{
			BlockHash:            pbb.BlockHash,
			BlockNumber:          int(pbb.BlockNumber),
			Nonce:                nonce,
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
			GasLimit:             paseJson["gas_limit"],
			GasUsed:              paseJson["gas_used"],
			GasPrice:             paseJson["gas_price"],
			BaseFee:              paseJson["base_fee"],
			MaxFeePerGas:         paseJson["max_fee_per_gas"],
			MaxPriorityFeePerGas: paseJson["max_priority_fee_per_gas"],
			Data:                 pbb.Data,
			EventLog:             pbb.EventLog,
			TransactionType:      transactionType,
			OperateType:          pbb.OperateType,
			DappData:             pbb.DappData,
			ClientData:           pbb.ClientData,
			CreatedAt:            pbb.CreatedAt,
			UpdatedAt:            pbb.UpdatedAt,
		}

		result, err = data.EvmTransactionRecordRepoClient.Save(ctx, GetTableName(pbb.ChainName), evmTransactionRecord)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			result, err = data.EvmTransactionRecordRepoClient.Save(nil, GetTableName(pbb.ChainName), evmTransactionRecord)
		}
		if result == 1 {
			key := pendingNonceKey + strconv.Itoa(int(nonce))
			data.RedisClient.Set(key, pbb.Uid, 6*time.Hour)
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
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.BtcTransactionRecordRepoClient.Save(ctx, GetTableName(pbb.ChainName), btcTransactionRecord)
		if result > 0 {
			go UpdateUtxo(pbb)
		}
	case TVM:
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
							log.Error(pbb.ChainName+"链插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
						} else {
							tokenMap["token_uri"] = tokenInfo.TokenUri
							parseData, _ := utils.JsonEncode(parseDataMap)
							pbb.ParseData = parseData
						}
					}
				}
			}
		}

		transactionType := pbb.TransactionType
		if transactionType == CONTRACT && ((len(pbb.Data) >= 10 && strings.HasPrefix(pbb.Data, "0x")) || (len(pbb.Data) >= 8 && !strings.HasPrefix(pbb.Data, "0x"))) {
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
				log.Warn(pbb.ChainName+"链查询nodeProxy中合约ABI失败", zap.Any("txHash", pbb.TransactionHash), zap.Any("contractAddress", contractAddress), zap.Any("methodId", methodId), zap.Any("error", err))
			}

			if strings.Contains(methodName, "Mint") || strings.Contains(methodName, "_mint") || strings.HasPrefix(methodName, "mint") {
				transactionType = MINT
			}

			if strings.Contains(methodName, "Swap") || strings.Contains(methodName, "_swap") || strings.HasPrefix(methodName, "swap") {
				transactionType = SWAP
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
			NetUsage:        paseJson["net_usage"],
			FeeLimit:        paseJson["fee_limit"],
			EnergyUsage:     paseJson["energy_usage"],
			Data:            pbb.Data,
			EventLog:        pbb.EventLog,
			TransactionType: transactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.TrxTransactionRecordRepoClient.Save(ctx, GetTableName(pbb.ChainName), trxRecord)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			result, err = data.TrxTransactionRecordRepoClient.Save(nil, GetTableName(pbb.ChainName), trxRecord)
		}
	case APTOS:
		parseDataMap := make(map[string]interface{})
		var nonce int64
		if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &parseDataMap); jsonErr == nil {
			evmMap := parseDataMap["aptos"]
			ret := evmMap.(map[string]interface{})
			nonceInt, _ := utils.GetInt(ret["sequence_number"])
			nonce = int64(nonceInt)

			tokenMap, ok := parseDataMap["token"].(map[string]interface{})
			if ok {
				tokenAddress, _ := tokenMap["address"].(string)
				if tokenAddress != "" {
					tokenType, _ := tokenMap["token_type"].(string)
					if tokenType == "" {
						tokenInfo, err := GetTokenInfoRetryAlert(context.Background(), pbb.ChainName, tokenAddress)
						if err != nil {
							log.Error(pbb.ChainName+"链插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
						} else {
							tokenMap["token_uri"] = tokenInfo.TokenUri
							parseData, _ := utils.JsonEncode(parseDataMap)
							pbb.ParseData = parseData
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
				}
			}
		}

		aptRecord := &data.AptTransactionRecord{
			BlockHash:       pbb.BlockHash,
			BlockNumber:     int(pbb.BlockNumber),
			Nonce:           nonce,
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
			GasLimit:        paseJson["gas_limit"],
			GasUsed:         paseJson["gas_used"],
			GasPrice:        paseJson["gas_price"],
			Data:            pbb.Data,
			EventLog:        pbb.EventLog,
			TransactionType: transactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}

		result, err = data.AptTransactionRecordRepoClient.Save(ctx, GetTableName(pbb.ChainName), aptRecord)
		if result == 1 {
			key := pendingNonceKey + strconv.Itoa(int(nonce))
			data.RedisClient.Set(key, pbb.Uid, 6*time.Hour)
		}
	case SUI:
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
							log.Error(pbb.ChainName+"链插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
						} else {
							tokenMap["token_uri"] = tokenInfo.TokenUri
							parseData, _ := utils.JsonEncode(parseDataMap)
							pbb.ParseData = parseData
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
			GasLimit:        paseJson["gas_limit"],
			GasUsed:         paseJson["gas_used"],
			Data:            pbb.Data,
			EventLog:        pbb.EventLog,
			TransactionType: pbb.TransactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.SuiTransactionRecordRepoClient.Save(ctx, GetTableName(pbb.ChainName), suiRecord)
	case SOLANA:
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
							log.Error(pbb.ChainName+"链插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
						} else {
							tokenMap["token_uri"] = tokenInfo.TokenUri
							parseData, _ := utils.JsonEncode(parseDataMap)
							pbb.ParseData = parseData
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
			TransactionType: pbb.TransactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}

		result, err = data.SolTransactionRecordRepoClient.Save(ctx, GetTableName(pbb.ChainName), solRecord)
	case NERVOS:
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
							log.Error(pbb.ChainName+"链插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
						} else {
							tokenMap["token_uri"] = tokenInfo.TokenUri
							parseData, _ := utils.JsonEncode(parseDataMap)
							pbb.ParseData = parseData
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
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.CkbTransactionRecordRepoClient.Save(ctx, GetTableName(pbb.ChainName), ckbTransactionRecord)
		if result > 0 {
			//修改 未花费
			tx, err := GetNervosUTXOTransaction(pbb.TransactionHash)
			if err != nil {
				log.Error(pbb.TransactionHash, zap.Any("查询交易失败", err))
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
						log.Error(pbb.TransactionHash, zap.Any("更新数据库失败！", err))
					}
				}
			}
		}
	case COSMOS:
		parseDataMap := make(map[string]interface{})
		var nonce int64
		if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &parseDataMap); jsonErr == nil {
			evmMap := parseDataMap["cosmos"]
			ret := evmMap.(map[string]interface{})
			nonceInt, _ := utils.GetInt(ret["sequence_number"])
			nonce = int64(nonceInt)

			tokenMap, ok := parseDataMap["token"].(map[string]interface{})
			if ok {
				tokenAddress, _ := tokenMap["address"].(string)
				if tokenAddress != "" {
					tokenType, _ := tokenMap["token_type"].(string)
					if tokenType == "" {
						tokenInfo, err := GetTokenInfoRetryAlert(context.Background(), pbb.ChainName, tokenAddress)
						if err != nil {
							log.Error(pbb.ChainName+"链插入pending记录，从nodeProxy中获取代币精度失败", zap.Any("txHash", pbb.TransactionHash), zap.Any("tokenAddress", tokenAddress), zap.Any("error", err))
						} else {
							tokenMap["token_uri"] = tokenInfo.TokenUri
							parseData, _ := utils.JsonEncode(parseDataMap)
							pbb.ParseData = parseData
						}
					}
				}
			}
		}

		atomRecord := &data.AtomTransactionRecord{
			BlockHash:       pbb.BlockHash,
			BlockNumber:     int(pbb.BlockNumber),
			Nonce:           nonce,
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
			GasLimit:        paseJson["gas_limit"],
			GasUsed:         paseJson["gas_used"],
			GasPrice:        paseJson["gas_price"],
			Data:            pbb.Data,
			EventLog:        pbb.EventLog,
			TransactionType: pbb.TransactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.AtomTransactionRecordRepoClient.Save(ctx, GetTableName(pbb.ChainName), atomRecord)
		if result == 1 {
			key := pendingNonceKey + strconv.Itoa(int(nonce))
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
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.KasTransactionRecordRepoClient.Save(ctx, GetTableName(pbb.ChainName), kasTransactionRecord)
		if result > 0 {
			//修改 未花费
			go KaspaUpdateUtxo(pbb)
		}
	}

	if err != nil {
		// postgres出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链插入pending记录，将数据插入到数据库中失败", pbb.ChainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error(pbb.ChainName+"链插入pending记录，将数据插入到数据库中失败", zap.Any("txHash", pbb.TransactionHash), zap.Any("error", err))
	}

	flag := result == 1
	return &pb.CreateResponse{
		Status: flag,
		Code:   uint64(200),
		Mes:    "",
	}, err
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

	//修改 未花费
	time.Sleep(time.Duration(1) * time.Minute)
	tx, err := GetUTXOByHash[pbb.ChainName](pbb.TransactionHash)
	if err != nil {
		log.Error(pbb.TransactionHash, zap.Any("查询交易失败", err))
	}
	log.Info(pbb.ChainName, zap.Any("更新UTXO状态为pending 4", tx))
	cellInputs := tx.Inputs
	for _, ci := range cellInputs {
		th := ci.PrevHash
		index := ci.OutputIndex
		//更新 状态为pending
		ret, err := data.UtxoUnspentRecordRepoClient.UpdateUnspent(nil, pbb.Uid, pbb.ChainName, pbb.FromAddress, index, th)
		if err != nil || ret == 0 {
			log.Error(pbb.TransactionHash, zap.Any("更新数据库失败！", err))
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
		alarmMsg := fmt.Sprintf("请注意：%s链插入pending交易记录时更新用户UTXO状态，请求节点查询交易记录占用的UTXO失败，txHash:%s", pbb.ChainName, pbb.TransactionHash)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error(pbb.ChainName+"链插入pending交易记录时更新用户UTXO状态，请求节点查询交易记录占用的UTXO失败", zap.Any("txHash", pbb.TransactionHash),
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
			Unspent: 4, //1 未花费 2 已花费 联合索引
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
		alarmMsg := fmt.Sprintf("请注意：%s链插入pending交易记录时更新用户UTXO状态，将UTXO插入到数据库中失败，txHash:%s", pbb.ChainName, pbb.TransactionHash)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Error(pbb.ChainName+"链插入pending交易记录时更新用户UTXO状态，将UTXO插入到数据库中失败", zap.Any("txHash", pbb.TransactionHash),
			zap.Any("fromAddress", pbb.FromAddress), zap.Any("error", err))
	}
}

func (s *TransactionUsecase) PageList(ctx context.Context, req *pb.PageListRequest) (*pb.PageListResponse, error) {
	chainType := ChainNameType[req.ChainName]
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
	request.Nonce = -1

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

				//将发送给合约的主币转成一条eventLog
				if (record.TransactionType == CONTRACT || record.TransactionType == MINT || record.TransactionType == SWAP) && record.Amount != "" && record.Amount != "0" {
					eventLogStr := handleEventLog(req.ChainName, record.FromAddress, record.ToAddress, record.Amount, record.EventLog)
					record.EventLog = eventLogStr
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
			err = utils.CopyProperties(recordList, &list)
		}
		if len(list) > 0 {
			for _, record := range list {
				//将发送给合约的主币转成一条eventLog
				if (record.TransactionType == CONTRACT || record.TransactionType == MINT || record.TransactionType == SWAP) && record.Amount != "" && record.Amount != "0" {
					eventLogStr := handleEventLog(req.ChainName, record.FromAddress, record.ToAddress, record.Amount, record.EventLog)
					record.EventLog = eventLogStr
				}
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
			now := time.Now().Unix()
			for _, record := range list {
				if record == nil {
					continue
				}

				record.ChainName = req.ChainName
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

				feeData := make(map[string]string)
				switch chainType {
				case BTC:
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

					if (record.Status == PENDING || record.Status == NO_STATUS) && now-record.TxTime > 300 && req.Address == record.FromAddress {
						evm := make(map[string]interface{})
						if jsonErr := json.Unmarshal([]byte(record.ParseData), &evm); jsonErr == nil {
							if record.Nonce == 0 {
								evm["pendingMsg"] = GAS_FEE_LOW
								parseDataStr, _ := utils.JsonEncode(evm)
								record.ParseData = parseDataStr
							} else {
								ret, err := data.EvmTransactionRecordRepoClient.FindByNonceAndAddress(nil, GetTableName(req.ChainName), record.FromAddress, record.Nonce-1)
								if err == nil {
									if ret == nil {
										//请填补空缺nonce交易 "nonceMsg":"2"
										evm["pendingMsg"] = NONCE_BREAK
									} else {
										if ret.Status == SUCCESS || ret.Status == FAIL || ret.Status == DROPPED_REPLACED {
											// "gasfeeMsg" :"1"
											evm["pendingMsg"] = GAS_FEE_LOW
										}
										if ret.Status == PENDING || ret.Status == NO_STATUS {
											//"nonceMsg":"1"
											evm["pendingMsg"] = NONCE_QUEUE
										}
										if ret.Status == DROPPED {
											//请填补空缺nonce交易 "nonceMsg":"2"
											evm["pendingMsg"] = NONCE_BREAK
										}
									}
									parseDataStr, _ := utils.JsonEncode(evm)
									record.ParseData = parseDataStr
								}
							}
						}
					}

					if record.Status == FAIL {
						evm := make(map[string]interface{})
						if jsonErr := json.Unmarshal([]byte(record.ParseData), &evm); jsonErr == nil {
							//| 150878    | 149039
							gasLimit, _ := strconv.ParseFloat(record.GasLimit, 64)
							gasUsed, _ := strconv.ParseFloat(record.GasUsed, 64)

							f := gasUsed / gasLimit
							if f > 0.9 {
								evm["failMsg"] = GAS_LIMIT_LOW
								parseDataStr, _ := utils.JsonEncode(evm)
								record.ParseData = parseDataStr
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
				case SOLANA:
					feeData = nil
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
			log.Error("parse EventLog failed", zap.Any("eventLog", eventLogStr), zap.Any("error", err))
			return eventLogStr
		}

		var hasMain bool
		var mainTotal int
		for _, eventLog := range eventLogs {
			if recordFromAddress == eventLog.From {
				if eventLog.Token.Address == "" {
					mainTotal++
					if recordToAddress == eventLog.To || recordAmount == eventLog.Amount.String() {
						hasMain = true
						break
					}
				} else {
					var mainSymbol string
					if platInfo, ok := PlatInfoMap[chainName]; ok {
						mainSymbol = platInfo.NativeCurrency
					}
					if recordToAddress == eventLog.To && recordAmount == eventLog.Amount.String() && eventLog.Token.Symbol == mainSymbol {
						hasMain = true
						break
					}
				}
			}
		}
		if !hasMain && mainTotal == 1 {
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

	chainType := ChainNameType[req.ChainName]
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
	chainType := ChainNameType[req.ChainName]
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
			chainType := ChainNameType[value.ChainName]
			feeData := make(map[string]string)
			switch chainType {
			case BTC:
				btc, err := data.BtcTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(value.ChainName), value.LastTxhash)
				if err == nil && btc != nil {
					var r *pb.TransactionRecord
					utils.CopyProperties(btc, &r)
					r.ChainName = value.ChainName
					r.Cursor = value.TxTime
					r.Amount = value.Amount
					trs = append(trs, r)
				}
			case EVM:
				evm, err := data.EvmTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(value.ChainName), value.LastTxhash)
				if err == nil && evm != nil {
					var r *pb.TransactionRecord
					utils.CopyProperties(evm, &r)

					eventLogInfo, err1 := data.EvmTransactionRecordRepoClient.FindParseDataByTxHashAndToken(ctx, GetTableName(value.ChainName), value.LastTxhash, tokenAddress)
					if err1 == nil && eventLogInfo != nil {
						r.ParseData = eventLogInfo.ParseData
					}
					feeData["gas_limit"] = r.GasLimit
					feeData["gas_used"] = r.GasUsed
					feeData["gas_price"] = r.GasPrice
					feeData["base_fee"] = r.BaseFee
					feeData["max_fee_per_gas"] = r.MaxFeePerGas
					feeData["max_priority_fee_per_gas"] = r.MaxPriorityFeePerGas
					r.ChainName = value.ChainName
					feeDataStr, _ := utils.JsonEncode(feeData)
					r.FeeData = feeDataStr
					r.Cursor = value.TxTime
					r.Amount = value.Amount
					trs = append(trs, r)
				}
			case STC:
				stc, err := data.StcTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(value.ChainName), value.LastTxhash)
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
				tvm, err := data.TrxTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(value.ChainName), value.LastTxhash)
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
						//r.ParseData =
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
					}

					trs = append(trs, r)
				}
			case APTOS:
				apt, err := data.AptTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(value.ChainName), value.LastTxhash)
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
				sui, err := data.SuiTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(value.ChainName), value.LastTxhash)
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
				sol, err := data.SolTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(value.ChainName), value.LastTxhash)
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

func (s *TransactionUsecase) GetNonce(ctx context.Context, req *pb.NonceReq) (*pb.NonceResp, error) {
	chainType := ChainNameType[req.ChainName]
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
		alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户done,nonce失败", req.ChainName)
		alarmOpts := WithMsgLevel("FATAL")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		log.Info("查询redis缓存报错：获取用户done,nonce失败", zap.Any(req.ChainName, req.Address), zap.Any("error", err))
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
			alarmMsg := fmt.Sprintf("请注意：%s链从redis获取用户done,nonce失败", req.ChainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Info("查询redis缓存报错：获取用户done,nonce失败", zap.Any(req.ChainName, req.Address), zap.Any("error", err1))
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
	chainType := ChainNameType[req.ChainName]
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
		AmountType:       req.AmountType,
		OrderBy:          req.OrderBy,
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
		result.TotalCurrencyAmount = totalCurrencyAmount.String()
	}
	return result, err
}

func (s *TransactionUsecase) ClientPageListAsset(ctx context.Context, req *pb.PageListAssetRequest) (*pb.PageListAssetResponse, error) {
	chainType := ChainNameType[req.ChainName]
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
		AmountType:       req.AmountType,
	}
	var result = &pb.PageListAssetResponse{}
	var total int64
	var totalCurrencyAmount decimal.Decimal
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
			for key, _ := range tokenAddressMap {
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

				totalCurrencyAmount = totalCurrencyAmount.Add(cnyAmount)
			}

			//处理排序
			orderBy := req.OrderBy
			orderBys := strings.Split(orderBy, " ")
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
		result.TotalCurrencyAmount = totalCurrencyAmount.String()
	}
	return result, err
}

func (s *TransactionUsecase) GetBalance(ctx context.Context, req *pb.AssetRequest) (*pb.ListBalanceResponse, error) {
	chainType := ChainNameType[req.ChainName]
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
			for key, _ := range tokenAddressMap {
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
	}
	var result = &pb.ListHasBalanceUidDimensionResponse{}
	var list []*pb.HasBalanceUidDimensionResponse
	var err error

	var recordList []*data.UserAsset
	recordList, err = data.UserAssetRepoClient.ListBalanceGroupByUid(ctx, request)
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

func (s *TransactionUsecase) ClientPageListNftAssetGroup(ctx context.Context, req *pb.PageListNftAssetRequest) (*pb.ClientPageListNftAssetGroupResponse, error) {
	chainType := ChainNameType[req.ChainName]
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
			log.Error(req.ChainName+"链查询用户NFT资产集合，从nodeProxy中获取NFT信息失败", zap.Any("addressList", req.AddressList), zap.Any("error", err))
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
	chainType := ChainNameType[req.ChainName]
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
		OrderBy:                      req.OrderBy,
		DataDirection:                req.DataDirection,
		StartIndex:                   req.StartIndex,
		PageNum:                      req.PageNum,
		PageSize:                     req.PageSize,
		Total:                        req.Total,
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
			log.Error(req.ChainName+"链查询用户NFT资产，从nodeProxy中获取NFT信息失败", zap.Any("addressList", req.AddressList), zap.Any("error", err))
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
	chainType := ChainNameType[req.ChainName]
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
		Context: ctx,
		Device:  req.Device,
	}

	args := make([]reflect.Value, 0)
	args = append(args, reflect.ValueOf(s))
	args = append(args, reflect.ValueOf(jsonRpcCtx))

	if len(req.Params) > 0 {
		u := mv.Type.NumIn()
		paseJson := reflect.New(mv.Type.In(u - 1).Elem())

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

func (s *TransactionUsecase) BatchRouteRpc(ctx context.Context, req *BatchRpcParams) (*pb.JsonResponse, error) {
	//log.Info("==4",zap.Any("4",req))

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

func (s *TransactionUsecase) GetDataDictionary(ctx context.Context) (*DataDictionary, error) {
	var result = &DataDictionary{}
	var serviceTransactionType = []string{NATIVE, TRANSFER, TRANSFERNFT, APPROVE, APPROVENFT,
		CONTRACT, CREATECONTRACT, EVENTLOG, CREATEACCOUNT, CLOSEACCOUNT, REGISTERTOKEN, DIRECTTRANSFERNFTSWITCH,
		OTHER, SETAPPROVALFORALL, TRANSFERFROM, SAFETRANSFERFROM, SAFEBATCHTRANSFERFROM, MINT, SWAP}
	var serviceStaus = []string{SUCCESS, FAIL, PENDING, NO_STATUS, DROPPED_REPLACED, DROPPED}
	result.Ok = true
	result.ServiceTransactionType = serviceTransactionType
	result.ServiceStatus = serviceStaus

	return result, nil
}

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
		chainType := ChainNameType[chainName]

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
		platInfo := PlatInfoMap[chainName]
		decimals := platInfo.Decimal
		if len(list) == 0 {
			result[chainName+"-"+strings.ToLower(add)] = CreatePendingInfo("0", "0", "1", nil)
		}
		for _, record := range list {
			feeAmount, _ := decimal.NewFromString(record.FeeAmount)
			amount, _ := decimal.NewFromString(record.Amount)
			tokenInfo, _ := ParseGetTokenInfo(chainName, record.ParseData)

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
			case APPROVENFT, CONTRACT, APPROVE, TRANSFERNFT, SAFETRANSFERFROM, SAFEBATCHTRANSFERFROM, SETAPPROVALFORALL, CREATEACCOUNT, CLOSEACCOUNT, REGISTERTOKEN, DIRECTTRANSFERNFTSWITCH, MINT, SWAP:
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

func (s *TransactionUsecase) GetFeeInfoByChainName(ctx context.Context, req *ChainFeeInfoReq) (*ChainFeeInfoResp, error) {
	chainName := req.ChainName
	if chainName == "" || (chainName != "ETH" && chainName != "Polygon" && chainName != "ScrollL2TEST") {
		return nil, nil
	}
	gasPrice, err := data.RedisClient.Get(TX_FEE_GAS_PRICE + chainName).Result()
	maxFeePerGas, err := data.RedisClient.Get(TX_FEE_MAX_FEE_PER_GAS + chainName).Result()
	maxPriorityFeePerGas, err := data.RedisClient.Get(TX_FEE_MAX_PRIORITY_FEE_PER_GAS + chainName).Result()
	if err != nil {
		return nil, err
	}
	return &ChainFeeInfoResp{
		ChainName:            chainName,
		GasPrice:             gasPrice,
		MaxFeePerGas:         maxFeePerGas,
		MaxPriorityFeePerGas: maxPriorityFeePerGas,
	}, nil
}

// JsonRPC
func (s *TransactionUsecase) UpdateUserAsset(ctx context.Context, req *UserAssetUpdateRequest) (interface{}, error) {
	if req.ChainName == "Nervos" || req.ChainName == "Polkadot" {
		return struct{}{}, nil
	}

	chainType := ChainNameType[req.ChainName]
	switch chainType {
	case EVM:
		if req.Address != "" {
			req.Address = types2.HexToAddress(req.Address).Hex()
		}
		for i := range req.Assets {
			req.Assets[i].TokenAddress = types2.HexToAddress(req.Assets[i].TokenAddress).Hex()
		}
	case COSMOS:
		for i := range req.Assets {
			req.Assets[i].TokenAddress = utils.AddressIbcToLower(req.Assets[i].TokenAddress)
		}
	}

	assets, err := data.UserAssetRepoClient.List(ctx, &data.AssetRequest{
		ChainName: req.ChainName,
		Address:   req.Address,
	})
	if err != nil {
		return nil, err
	}
	assetsGroupByToken := make(map[string]*data.UserAsset)
	for _, item := range assets {
		assetsGroupByToken[item.TokenAddress] = item
	}

	uniqAssets := make(map[string]UserAsset)
	for _, asset := range req.Assets {
		key := fmt.Sprintf("%s%s%s", req.ChainName, req.Address, asset.TokenAddress)
		uniqAssets[key] = asset
	}

	needUpdateAssets := make([]*data.UserAsset, 0, len(assets))
	needPush := make([]UserTokenPush, 0, len(assets))
	for _, newItem := range uniqAssets {
		tokenAddress := newItem.TokenAddress
		if req.Address == "0x0000000000000000000000000000000000000000" || req.Address == tokenAddress {
			tokenAddress = ""
		}

		oldItem, ok := assetsGroupByToken[tokenAddress]
		if !ok {
			if newItem.Balance == "0" {
				continue
			}
			uidOk, uid, err := UserAddressSwitchRetryAlert(req.ChainName, req.Address)
			if err != nil {
				return nil, err
			}

			if !uidOk {
				return nil, errors.New("unknown address")
			}
			tokenInfo, err := s.getTokenInfo(ctx, req.ChainName, req.Address, &newItem)
			if err != nil {
				log.Error(req.ChainName+"链资产变更，从nodeProxy中获取代币精度失败", zap.Any("address", req.Address), zap.Any("tokenAddress", newItem.TokenAddress), zap.Any("error", err))
				return nil, err
			}
			tokenInfo.Address = newItem.TokenAddress
			s.attemptFixZeroDecimals(ctx, req, &newItem, &tokenInfo)

			needUpdateAssets = append(needUpdateAssets, &data.UserAsset{
				ChainName:    req.ChainName,
				Uid:          uid,
				Address:      req.Address,
				TokenAddress: tokenInfo.Address,
				Balance:      newItem.Balance,
				Decimals:     int32(tokenInfo.Decimals),
				Symbol:       tokenInfo.Symbol,
				CreatedAt:    time.Now().Unix(),
				UpdatedAt:    time.Now().Unix(),
			})
			if !IsNative(req.ChainName, tokenAddress) {
				needPush = append(needPush, UserTokenPush{
					ChainName:    req.ChainName,
					Uid:          uid,
					Address:      req.Address,
					TokenAddress: tokenAddress,
					Decimals:     int32(tokenInfo.Decimals),
					Symbol:       tokenInfo.Symbol,
				})
			}
			continue
		}
		s.attemptFixZeroDecimals(ctx, req, &newItem, nil)

		newBalance := newItem.Balance
		if oldItem.Balance != "0" && newBalance == "0" && tokenAddress == "" {
			// Double check zero balance.
			balance, err := s.getBalance(ctx, req.ChainName, req.Address)
			if err != nil {
				continue
			}
			newBalance = balance
			log.Info(
				"FIX ZERO BALANCE",
				zap.String("chainName", req.ChainName),
				zap.String("address", req.Address),
				zap.String("balance", newBalance),
			)
		}

		// update
		if oldItem.Balance != newBalance {
			// XXX: only update balance here.
			oldItem.Balance = newBalance
			needUpdateAssets = append(needUpdateAssets, oldItem)
		}
	}
	if len(needUpdateAssets) > 0 {
		_, err := data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, needUpdateAssets, PAGE_SIZE)
		for i := 0; i < 3 && err != nil; i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.UserAssetRepoClient.PageBatchSaveOrUpdate(nil, needUpdateAssets, PAGE_SIZE)
		}
		if err != nil {
			// postgres出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链资产变更，将数据插入到数据库中失败", req.ChainName)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			log.Error(req.ChainName+"链资产变更，将数据插入到数据库中失败", zap.Any("error", err))
			return nil, err
		}

		platInfo := PlatInfoMap[req.ChainName]
		if platInfo != nil {
			go GetTxByAddress(req.ChainName, req.Address, platInfo.HttpURL)
		}
	}
	if len(needPush) > 0 {
		HandleTokenPush(req.ChainName, needPush)
	}
	return struct{}{}, nil
}

func (s *TransactionUsecase) attemptFixZeroDecimals(ctx context.Context, req *UserAssetUpdateRequest, asset *UserAsset, tokenInfo *types.TokenInfo) {
	if asset.Decimals != 0 || strings.Contains(asset.Balance, ".") || asset.Balance == "0" {
		return
	}
	// Decimals is zero which may be incorrect.
	if tokenInfo == nil {
		token, err := s.getTokenInfo(ctx, req.ChainName, req.Address, asset)
		if err != nil {
			log.Error(req.ChainName+"链资产变更，从nodeProxy中获取代币精度失败", zap.Any("address", req.Address), zap.Any("tokenAddress", asset.TokenAddress), zap.Any("error", err))
			return
		}
		tokenInfo = &token
	}

	// Ignore native currency
	if tokenInfo.Address == "" {
		return
	}

	if tokenInfo.Decimals != 0 {
		newBalance := utils.StringDecimals(asset.Balance, int(tokenInfo.Decimals))
		log.Info(
			"INCORRECT ZERO DECIMALS",
			zap.String("beforeBalance", asset.Balance),
			zap.String("afterBalance", newBalance),
			zap.String("address", req.Address),
			zap.String("tokenAddress", asset.TokenAddress),
			zap.Int64("decimals", tokenInfo.Decimals),
		)
		asset.Balance = newBalance
	}
}

func (s *TransactionUsecase) getBalance(ctx context.Context, chainName, address string) (string, error) {
	result, err := ExecuteRetry(chainName, func(client chain.Clienter) (interface{}, error) {
		if c, ok := client.(RPCNodeBalancer); ok {
			return c.GetBalance(address)
		}
		return 0, errors.New("not supported")
	})

	if err != nil {
		// We don't know, returns false is safer.
		log.Info(
			"CHECK ZERO BALANCE FAILED",
			zap.String("chainName", chainName),
			zap.String("address", address),
			zap.Error(err),
		)
		return "", err
	}
	return result.(string), nil
}

func (s *TransactionUsecase) getTokenInfo(ctx context.Context, chainName, address string, asset *UserAsset) (types.TokenInfo, error) {
	if asset.TokenAddress == address || asset.TokenAddress == "" {
		platInfo, ok := PlatInfoMap[chainName]
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

func (s *TransactionUsecase) CreateBroadcast(ctx *JsonRpcContext, req *BroadcastRequest) (*BroadcastResponse, error) {
	device := ctx.ParseDevice()
	var usrhs []*data.UserSendRawHistory

	var userSendRawHistory = &data.UserSendRawHistory{}
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
	if req.ErrMsg != "" {
		NotifyBroadcastTxFailed(ctx, req, ctx.ParseDevice())
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

func (s *TransactionUsecase) CreateSignRecord(ctx *JsonRpcContext, req *BroadcastRequest) (*BroadcastResponse, error) {
	sendLock.Lock()
	defer sendLock.Unlock()

	device := ctx.ParseDevice()
	now := time.Now().Unix()
	var usrhs []*data.UserSendRawHistory

	chainType := ChainNameType[req.ChainName]
	var userSendRawHistory = &data.UserSendRawHistory{}
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
			if ChainNameType[info.ChainName] == COSMOS {
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
		userSendRawHistory.SignStatus = "3"
		NotifyBroadcastTxFailed(ctx, req, ctx.ParseDevice())
	}
	if req.SignStatus == "" {
		userSendRawHistory.SignStatus = "3"
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
func (s *TransactionUsecase) ClearNonce(ctx context.Context, req *ClearNonceRequest) (*ClearNonceResponse, error) {
	if req == nil || req.Address == "" || req.ChainName == "" {
		return &ClearNonceResponse{
			Ok:      false,
			Message: "Illegal parameter, address and chainName must not nil",
		}, nil
	}
	//查询 所有 处于pending 和 no_status 的交易记录，并更新 from to 地址 及 from_uid (不用考虑to_uid) 切割字符，然后  把交易状态改成drrop_replace
	//删除所有 该地址 的所有 redis pending的 key
	chainType := ChainNameType[req.ChainName]
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
	summary, err := s.kBundle.Wallet.LoadSummary(ctx, req.ChainName, req.Address)
	if err != nil {

		if err != gorm.ErrRecordNotFound {
			return nil, err
		}
		summary = &kanban.WalletSummaryRecord{}
	}

	price, err := GetTokenPriceRetryAlert(ctx, "ETH", "USD", "")
	if err != nil {
		return nil, err
	}

	totalTxsRanks, totalTxTp, err := s.loadRanks(ctx, req.ChainName, "total_tx_num", decimal.New(summary.TotalTxNum, 0), price)
	if err != nil {
		return nil, err
	}
	totalTxAmountRanks, totalTxAmountTp, err := s.loadRanks(ctx, req.ChainName, "total_tx_amount", summary.TotalTxAmount, price)
	if err != nil {
		return nil, err
	}
	totalContractRanks, totalContractTp, err := s.loadRanks(ctx, req.ChainName, "total_contract_num", decimal.New(summary.TotalContractNum, 0), price)
	if err != nil {
		return nil, err
	}
	totalTxInAmountRanks, totalTxInAmountTp, err := s.loadRanks(ctx, req.ChainName, "total_tx_in_amount", summary.TotalTxInAmount, price)
	if err != nil {
		return nil, err
	}

	return &pb.KanbanSummaryResponse{
		FirstTxTime:          uint64(summary.FirstTradeTime) * 1000,
		TotalTxNum:           uint64(summary.TotalTxNum),
		TotalTxAmount:        s.currencyAmount(utils.StringDecimals(summary.TotalTxAmount.String(), 18), price),
		TotalContract:        uint64(summary.TotalContractNum),
		TotalTxInAmount:      s.currencyAmount(utils.StringDecimals(summary.TotalTxInAmount.String(), 18), price),
		TotalTxsRanks:        totalTxsRanks,
		TotalTxAmountRanks:   totalTxAmountRanks,
		TotalContractRanks:   totalContractRanks,
		TotalTxInAmountRanks: totalTxInAmountRanks,
		TopPercents: &pb.KanbanTopPercent{
			TotalTx:         totalTxTp,
			TotalTxAmount:   totalTxAmountTp,
			TotalContract:   totalContractTp,
			TotalTxInAmount: totalTxInAmountTp,
		},
	}, nil
}

func (s *TransactionUsecase) currencyAmount(amount string, price string) string {
	prices, _ := decimal.NewFromString(price)
	balances, _ := decimal.NewFromString(amount)
	cAmount := prices.Mul(balances)
	return cAmount.String()
}

func (s *TransactionUsecase) loadRanks(ctx context.Context, chainName, key string, value decimal.Decimal, price string) ([]*pb.KanbanRank, int32, error) {
	trendings, err := s.kBundle.Trending.Load(ctx, chainName, key)
	if err != nil {
		return nil, 0, err
	}
	rankTopPercents := map[int]bool{
		10: true,
		20: true,
		30: true,
		50: true,
	}
	topPercent := 99
	results := make([]*pb.KanbanRank, 0, 10)
	for _, item := range trendings {
		if _, ok := rankTopPercents[item.TopPercent]; ok {
			lower_bound := item.Rank.String()
			if strings.HasSuffix(key, "_amount") {
				lower_bound = s.currencyAmount(utils.StringDecimals(lower_bound, 18), price)
			}
			results = append(results, &pb.KanbanRank{
				TopPercent: int32(item.TopPercent),
				LowerBound: lower_bound,
			})
		}
		if value.Cmp(item.Rank) >= 0 {
			topPercent = item.TopPercent
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return uint64(results[i].TopPercent) < uint64(results[j].TopPercent)
	})
	return results, int32(topPercent), nil
}

// 看板交易数据
func (s *TransactionUsecase) KanbanTxChart(ctx context.Context, req *pb.KanbanChartRequest) (*pb.KanbanChartResponse, error) {
	return s.kanbanChart(ctx, req, func(item *kanban.WalletDaySummaryRecord) uint64 {
		return uint64(item.TotalTxNum)
	})
}

// 看板合约数据
func (s *TransactionUsecase) KanbanContractChart(ctx context.Context, req *pb.KanbanChartRequest) (*pb.KanbanChartResponse, error) {
	return s.kanbanChart(ctx, req, func(item *kanban.WalletDaySummaryRecord) uint64 {
		return uint64(item.TotalTxContract)
	})
}

func (s *TransactionUsecase) kanbanChart(ctx context.Context, req *pb.KanbanChartRequest, accessor func(*kanban.WalletDaySummaryRecord) uint64) (*pb.KanbanChartResponse, error) {
	byDay, err := s.kBundle.Wallet.RangeDaySummary(ctx, req.ChainName, req.Address, int64(req.StartTime/1000), int64(req.EndTime/1000))
	if err != nil {
		return nil, err
	}

	result := &pb.KanbanChartResponse{
		NumsByDay:       make([]*pb.KanbanBar, 0, len(byDay)),
		AccumulatedNums: make([]*pb.KanbanBar, 0, len(byDay)),
	}
	var acc uint64
	for _, item := range byDay {
		v := accessor(item)
		if v == 0 {
			continue
		}
		acc += v
		result.NumsByDay = append(result.NumsByDay, &pb.KanbanBar{
			Time:  uint64(item.Sharding) * 1000,
			Value: v,
		})
		result.AccumulatedNums = append(result.AccumulatedNums, &pb.KanbanBar{
			Time:  uint64(item.Sharding) * 1000,
			Value: acc,
		})
	}
	return result, nil
}

func (s *TransactionUsecase) CountOutTx(ctx context.Context, req *CountOutTxRequest) (*CountOutTxResponse, error) {
	chainType := ChainNameType[req.ChainName]
	var counter data.OutTxCounter
	switch chainType {
	case STC:
		counter = data.StcTransactionRecordRepoClient
	case BTC:
		counter = data.BtcTransactionRecordRepoClient
	case EVM:
		counter = data.EvmTransactionRecordRepoClient
	case TVM:
		counter = data.TrxTransactionRecordRepoClient
	case APTOS:
		counter = data.AptTransactionRecordRepoClient
	case SUI:
		counter = data.SuiTransactionRecordRepoClient
	case SOLANA:
		counter = data.SolTransactionRecordRepoClient
	case NERVOS:
		counter = data.CkbTransactionRecordRepoClient
	case CASPER:
		counter = data.CsprTransactionRecordRepoClient
	case COSMOS:
		counter = data.AtomTransactionRecordRepoClient
	case POLKADOT:
		counter = data.DotTransactionRecordRepoClient
	case KASPA:
		counter = data.KasTransactionRecordRepoClient
	}
	if counter == nil {
		return nil, errors.New("Unsupported chain type")
	}
	count, err := counter.CountOut(ctx, data.GetTableName(req.ChainName), req.Address, req.ToAddress)
	if err != nil {
		return nil, err
	}
	return &CountOutTxResponse{
		Count: count,
	}, nil
}

func (s *TransactionUsecase) GetSignRecord(ctx context.Context, req *SignRecordReq) (*SignRecordResponse, error) {

	chainType := ChainNameType[req.ChainName]
	switch chainType {
	case EVM:
		if req.Address != "" {
			req.Address = types2.HexToAddress(req.Address).Hex()
		}
	}

	r := data.SignReqPage{
		Address:    req.Address,
		ChainName:  req.ChainName,
		SignType:   req.SignType,
		SignStatus: req.SignStatus,
		PageNum:    req.Page,
		PageSize:   req.Limit,
		TradeTime:  req.TradeTime,
	}

	if req.TransactionType != "" {
		txType := strings.Split(req.TransactionType, ",")
		for _, transactionType := range txType {
			if transactionType == OTHER {
				txType = append(txType, CONTRACT, CREATEACCOUNT, CLOSEACCOUNT, REGISTERTOKEN, DIRECTTRANSFERNFTSWITCH, CREATECONTRACT, MINT, SWAP)
				break
			}
		}
		r.TransactionTypeList = txType
	}
	result, total, err := data.UserSendRawHistoryRepoInst.PageList(ctx, r)

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
				oldRecord, err = data.DotTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case CASPER:
				var oldRecord *data.CsprTransactionRecord
				oldRecord, err = data.CsprTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case NERVOS:
				var oldRecord *data.CkbTransactionRecord
				oldRecord, err = data.CkbTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case BTC:
				var oldRecord *data.BtcTransactionRecord
				oldRecord, err = data.BtcTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case EVM:
				var oldRecord *data.EvmTransactionRecord
				oldRecord, err = data.EvmTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case STC:
				var oldRecord *data.StcTransactionRecord
				oldRecord, err = data.StcTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case TVM:
				var oldRecord *data.TrxTransactionRecord
				oldRecord, err = data.TrxTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case APTOS:
				var oldRecord *data.AptTransactionRecord
				oldRecord, err = data.AptTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case SUI:
				var oldRecord *data.SuiTransactionRecord
				oldRecord, err = data.SuiTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case SOLANA:
				var oldRecord *data.SolTransactionRecord
				oldRecord, err = data.SolTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case COSMOS:
				var oldRecord *data.AtomTransactionRecord
				oldRecord, err = data.AtomTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			case KASPA:
				var oldRecord *data.KasTransactionRecord
				oldRecord, err = data.KasTransactionRecordRepoClient.FindByTxhash(ctx, GetTableName(req.ChainName), v.TransactionHash)
				if err == nil {
					err = utils.CopyProperties(oldRecord, &record)
				}
			}
			if err == nil && (record.Amount == "" || record.Amount == "0") {
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
			if len(txhash) >= 1{
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
