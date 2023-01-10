package biz

import (
	pb "block-crawling/api/transaction/v1"
	v1 "block-crawling/internal/client"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"context"
	"encoding/json"
	"fmt"
	types2 "github.com/ethereum/go-ethereum/common"
	"github.com/go-redis/redis"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"gorm.io/gorm"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var grpcPlatformInfos []*GrpcPlatformInfo
var chain2Type = make(map[string]string)

type GrpcPlatformInfo struct {
	Chain       string
	GetPriceKey string
	Handler     string
	Type        string
}

func Init(handler string, getPriceKey string, chainName string, chainType string) []*GrpcPlatformInfo {
	gpi := &GrpcPlatformInfo{
		Handler:     handler,
		Chain:       chainName,
		GetPriceKey: getPriceKey,
		Type:        chainType,
	}
	grpcPlatformInfos = append(grpcPlatformInfos, gpi)

	chain2Type[chainName] = chainType
	return grpcPlatformInfos

}

type TransactionUsecase struct {
	gormDB *gorm.DB
	lark   *Lark
}

func NewTransactionUsecase(grom *gorm.DB, lark *Lark) *TransactionUsecase {
	return &TransactionUsecase{
		gormDB: grom,
		lark:   lark,
	}
}

func (s *TransactionUsecase) GetAllOpenAmount(ctx context.Context, req *pb.OpenAmountReq) (*pb.OpenAmoutResp, error) {
	if req.Address != "" {
		req.Address = types2.HexToAddress(req.Address).Hex()
	}
	if req.ContractAddress != "" {
		req.ContractAddress = types2.HexToAddress(req.ContractAddress).Hex()
	}
	var oai = &pb.OpenAmountInfo{}
	amountTotal := decimal.Zero
	//根据条件去数据库查询list，
	dapps, err := data.DappApproveRecordRepoClient.GetAmountList(ctx, req)
	if err != nil {
		log.Error("查询授权dapp列表报错！", zap.Any("param", req), zap.Any("error", err))
		return &pb.OpenAmoutResp{
			Ok: false,
		}, err
	}
	//返回空列表
	if dapps == nil {
		oai.RiskExposureAmount = amountTotal.String()
		oai.DappCount = 0
		return &pb.OpenAmoutResp{
			Ok:   true,
			Data: oai,
		}, err
	} else {
		var keys []string
		var reqKey string
		dapMap := make(map[*data.DappApproveRecord]string)

		for _, dapp := range dapps {
			if len(dapp.Amount) >= 40 {
				continue
			}
			for _, g := range grpcPlatformInfos {
				if g.Chain == dapp.ChainName {
					keys = append(keys, g.Handler+"_"+dapp.Token)
					dapMap[dapp] = g.Handler + "_" + dapp.Token
					continue
				}
			}
			reqKey = strings.ReplaceAll(utils.ListToString(keys), "\"", "")
		}
		conn, err := grpc.Dial(AppConfig.Addr, grpc.WithInsecure())
		if err != nil {
			log.Fatale("did not token price connect", err)
		}
		defer conn.Close()
		tokenList := v1.NewTokenlistClient(conn)
		priceResp, err2 := tokenList.GetPrice(ctx, &v1.PriceReq{
			CoinAddresses: reqKey,
			Currency:      req.Currency,
		})
		if err2 != nil {
			log.Error("调用tokenPrice报错!", zap.Any("error", err2), zap.Any("请求key", reqKey))
			return &pb.OpenAmoutResp{
				Ok: true,
			}, err2
		}
		result := make(map[string]map[string]string)
		err1 := json.Unmarshal(priceResp.Data, &result)
		if err1 != nil {
			log.Error("调用币价报错!", zap.Any("error", err1), zap.Any("priceResp.Data", result))
			return &pb.OpenAmoutResp{
				Ok: true,
			}, err1
		}
		for key, value := range result {
			amount := value[req.Currency]
			a, _ := decimal.NewFromString(amount)
			for da, resultKey := range dapMap {
				if key == resultKey {
					bal := utils.StringDecimals(da.Amount, int(da.Decimals))
					balance, _ := decimal.NewFromString(bal)
					amountTotal = amountTotal.Add(a.Mul(balance))
				}
			}
		}
		return &pb.OpenAmoutResp{
			Ok: true,
			Data: &pb.OpenAmountInfo{
				RiskExposureAmount: amountTotal.String(),
				DappCount:          int64(len(dapps)),
			},
		}, err
	}
}

func (s *TransactionUsecase) GetDappList(ctx context.Context, req *pb.DappListReq) (*pb.DappListResp, error) {
	if len(req.Addresses) > 0 {
		var addresses []string
		for _, addr := range req.Addresses {
			addresses = append(addresses, types2.HexToAddress(addr).Hex())
		}
		req.Addresses = addresses
	}
	if req.ContractAddress != "" {
		req.ContractAddress = types2.HexToAddress(req.ContractAddress).Hex()
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
		chainType := chain2Type[da.ChainName]
		switch chainType {
		case EVM:
			evm, err := data.EvmTransactionRecordRepoClient.FindByTxhash(ctx, GetTalbeName(da.ChainName), da.LastTxhash)
			if err == nil && evm != nil {
				dappInfo = evm.DappData
				parseData = evm.ParseData
				transcationType = evm.TransactionType
			}
		case STC:
			stc, err := data.StcTransactionRecordRepoClient.FindByTxhash(ctx, GetTalbeName(da.ChainName), da.LastTxhash)
			if err == nil && stc != nil {
				dappInfo = stc.DappData
				parseData = stc.ParseData
				transcationType = stc.TransactionType
			}
		case TVM:
			tvm, err := data.TrxTransactionRecordRepoClient.FindByTxhash(ctx, GetTalbeName(da.ChainName), da.LastTxhash)
			if err == nil && tvm != nil {
				dappInfo = tvm.DappData
				parseData = tvm.ParseData
				transcationType = tvm.TransactionType
			}
		case APTOS:
			apt, err := data.AptTransactionRecordRepoClient.FindByTxhash(ctx, GetTalbeName(da.ChainName), da.LastTxhash)
			if err == nil && apt != nil {
				dappInfo = apt.DappData
				parseData = apt.ParseData
				transcationType = apt.TransactionType
			}
		case SUI:
			sui, err := data.SuiTransactionRecordRepoClient.FindByTxhash(ctx, GetTalbeName(da.ChainName), da.LastTxhash)
			if err == nil && sui != nil {
				dappInfo = sui.DappData
				parseData = sui.ParseData
				transcationType = sui.TransactionType
			}
		case SOLANA:
			sol, err := data.SolTransactionRecordRepoClient.FindByTxhash(ctx, GetTalbeName(da.ChainName), da.LastTxhash)
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
			tokenInfo, _ := PaseTokenInfo(parseData)
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
	chainType := chain2Type[pbb.ChainName]

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
			log.Info("feedata数据解析失败！")
		}
	}

	//pendingNonceKey := ADDRESS_PENDING_NONCE + pbb.ChainName + ":" + pbb.FromAddress + ":" + strconv.Itoa(int(pbb.Nonce))
	pendingNonceKey := ADDRESS_PENDING_NONCE + pbb.ChainName + ":" + pbb.FromAddress + ":"
	switch chainType {
	case CASPER:
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
		result, err = data.CsprTransactionRecordRepoClient.Save(ctx, GetTalbeName(pbb.ChainName), casperRecord)
	case STC:

		//{"stc":{"sequence_number":53}

		stc := make(map[string]interface{})
		nonce := ""
		if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &stc); jsonErr == nil {
			evmMap := stc["stc"]
			ret := evmMap.(map[string]interface{})
			if _, ok := ret["sequence_number"].(float64); ok {
				decimal := ret["sequence_number"].(float64)
				nonce = strconv.FormatFloat(decimal, 'f', 0, 64)
			}
			if _, ok := ret["sequence_number"].(string); ok {
				nonce = ret["sequence_number"].(string)
			}
		}
		dbNonce, _ := strconv.ParseUint(nonce, 10, 64)
		stcRecord := &data.StcTransactionRecord{
			BlockHash:       pbb.BlockHash,
			BlockNumber:     int(pbb.BlockNumber),
			TransactionHash: pbb.TransactionHash,
			Nonce:           int64(dbNonce),
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
			TransactionType: pbb.TransactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}

		result, err = data.StcTransactionRecordRepoClient.Save(ctx, GetTalbeName(pbb.ChainName), stcRecord)
		if result == 1 {
			//插入redis 并设置过期时间为6个小时
			key := pendingNonceKey + nonce
			data.RedisClient.Set(key, pbb.Uid, 6*time.Hour)
		}

	case POLKADOT:
		evm := make(map[string]interface{})
		var dbNonce uint64
		if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &evm); jsonErr == nil {
			evmMap := evm["polkadot"]
			ret := evmMap.(map[string]interface{})
			nonceStr := ret["nonce"].(string)
			dbNonce, _ = strconv.ParseUint(nonceStr, 10, 64)
		}
		dotTransactionRecord := &data.DotTransactionRecord{
			BlockHash:            pbb.BlockHash,
			BlockNumber:          int(pbb.BlockNumber),
			Nonce:                int64(dbNonce),
			TransactionHash:      pbb.TransactionHash,
			FromAddress:          pbb.FromAddress,
			ToAddress:            pbb.ToAddress,
			FromUid:              pbb.Uid,
			FeeAmount:            fa,
			Amount:               a,
			Status:               pbb.Status,
			TxTime:               pbb.TxTime,
			ContractAddress:      pbb.ContractAddress,
			ParseData:            pbb.ParseData,
			Data:                 pbb.Data,
			EventLog:             pbb.EventLog,
			TransactionType:      pbb.TransactionType,
			DappData:             pbb.DappData,
			ClientData:           pbb.ClientData,
			CreatedAt:            pbb.CreatedAt,
			UpdatedAt:            pbb.UpdatedAt,
		}
		result, err = data.DotTransactionRecordRepoClient.Save(ctx, GetTalbeName(pbb.ChainName), dotTransactionRecord)


	case EVM:
		evm := make(map[string]interface{})
		var dbNonce uint64
		if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &evm); jsonErr == nil {
			evmMap := evm["evm"]
			ret := evmMap.(map[string]interface{})
			nonceStr := ret["nonce"].(string)
			dbNonce, _ = strconv.ParseUint(nonceStr, 10, 64)
		}

		if pbb.ContractAddress != "" {
			pbb.ContractAddress = types2.HexToAddress(pbb.ContractAddress).Hex()
		}
		if pbb.FromAddress != "" {
			pbb.FromAddress = types2.HexToAddress(pbb.FromAddress).Hex()
		}
		if strings.HasPrefix(pbb.ToAddress, "0x") {
			pbb.ToAddress = types2.HexToAddress(pbb.ToAddress).Hex()
		}

		evmTransactionRecord := &data.EvmTransactionRecord{
			BlockHash:            pbb.BlockHash,
			BlockNumber:          int(pbb.BlockNumber),
			Nonce:                int64(dbNonce),
			TransactionHash:      pbb.TransactionHash,
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
			TransactionType:      pbb.TransactionType,
			DappData:             pbb.DappData,
			ClientData:           pbb.ClientData,
			CreatedAt:            pbb.CreatedAt,
			UpdatedAt:            pbb.UpdatedAt,
		}

		result, err = data.EvmTransactionRecordRepoClient.Save(ctx, GetTalbeName(pbb.ChainName), evmTransactionRecord)
		if result == 1 {
			//插入redis 并设置过期时间为6个小时
			key := pendingNonceKey + strconv.Itoa(int(dbNonce))
			log.Info("asdf", zap.Any("插入缓存", key), zap.Any("result", pbb.Uid))
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
		result, err = data.BtcTransactionRecordRepoClient.Save(ctx, GetTalbeName(pbb.ChainName), btcTransactionRecord)
		if result > 0 {
			//修改 未花费
			tx, err := GetUTXOByHash[pbb.ChainName](pbb.TransactionHash)
			if err != nil {
				log.Error(pbb.TransactionHash, zap.Any("查询交易失败", err))
			}

			cellInputs := tx.Inputs
			for _, ci := range cellInputs {
				th := ci.PrevHash
				index := ci.OutputIndex
				//更新 状态为pending
				ret, err := data.UtxoUnspentRecordRepoClient.UpdateUnspent(ctx, pbb.Uid, pbb.ChainName, pbb.FromAddress, index, th)
				if err != nil || ret == 0 {
					log.Error(pbb.TransactionHash, zap.Any("更新数据库失败！", err))
				}
			}

		}
	case TVM:
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
			TransactionType: pbb.TransactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}
		result, err = data.TrxTransactionRecordRepoClient.Save(ctx, GetTalbeName(pbb.ChainName), trxRecord)
	case APTOS:
		apt := make(map[string]interface{})
		nonce := ""
		if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &apt); jsonErr == nil {
			evmMap := apt["aptos"]
			ret := evmMap.(map[string]interface{})
			if _, ok := ret["sequence_number"].(float64); ok {
				decimal := ret["sequence_number"].(float64)
				nonce = strconv.FormatFloat(decimal, 'f', 0, 64)
			}
			if _, ok := ret["sequence_number"].(string); ok {
				nonce = ret["sequence_number"].(string)
			}

		}
		dbNonce, _ := strconv.ParseUint(nonce, 10, 64)

		aptRecord := &data.AptTransactionRecord{
			BlockHash:   pbb.BlockHash,
			BlockNumber: int(pbb.BlockNumber),
			Nonce:       int64(dbNonce),
			//TransactionVersion: int(pbb.BlockNumber),
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
			TransactionType: pbb.TransactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}

		result, err = data.AptTransactionRecordRepoClient.Save(ctx, GetTalbeName(pbb.ChainName), aptRecord)
		if result == 1 {
			key := pendingNonceKey + nonce
			log.Info("asdf", zap.Any("插入缓存", key), zap.Any("result", pbb.Uid))
			data.RedisClient.Set(key, pbb.Uid, 6*time.Hour)
		}
	case SUI:
		suiRecord := &data.SuiTransactionRecord{
			//TransactionVersion: int(pbb.BlockNumber),
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
			TransactionType: pbb.TransactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}

		result, err = data.SuiTransactionRecordRepoClient.Save(ctx, GetTalbeName(pbb.ChainName), suiRecord)
	case SOLANA:
		solRecord := &data.SolTransactionRecord{
			//SlotNumber:   int(pbb.BlockNumber),
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
			TransactionType: pbb.TransactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}

		result, err = data.SolTransactionRecordRepoClient.Save(ctx, GetTalbeName(pbb.ChainName), solRecord)
	case NERVOS:
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
		result, err = data.CkbTransactionRecordRepoClient.Save(ctx, GetTalbeName(pbb.ChainName), ckbTransactionRecord)
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
		cosmos := make(map[string]interface{})
		nonce := ""
		if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &cosmos); jsonErr == nil {
			evmMap := cosmos["cosmos"]
			ret := evmMap.(map[string]interface{})
			if _, ok := ret["sequence_number"].(float64); ok {
				decimal := ret["sequence_number"].(float64)
				nonce = strconv.FormatFloat(decimal, 'f', 0, 64)
			}
			if _, ok := ret["sequence_number"].(string); ok {
				nonce = ret["sequence_number"].(string)
			}
		}
		dbNonce, _ := strconv.ParseUint(nonce, 10, 64)

		atomRecord := &data.AtomTransactionRecord{
			BlockHash:       pbb.BlockHash,
			BlockNumber:     int(pbb.BlockNumber),
			Nonce:           int64(dbNonce),
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
			TransactionType: pbb.TransactionType,
			DappData:        pbb.DappData,
			ClientData:      pbb.ClientData,
			CreatedAt:       pbb.CreatedAt,
			UpdatedAt:       pbb.UpdatedAt,
		}

		result, err = data.AtomTransactionRecordRepoClient.Save(ctx, GetTalbeName(pbb.ChainName), atomRecord)
		if result == 1 {
			key := pendingNonceKey + nonce
			log.Info("asdf", zap.Any("插入缓存", key), zap.Any("result", pbb.Uid))
			data.RedisClient.Set(key, pbb.Uid, 6*time.Hour)
		}
	}

	flag := result == 1
	return &pb.CreateResponse{
		Status: flag,
		Code:   uint64(200),
		Mes:    "",
	}, err
}

func (s *TransactionUsecase) PageList(ctx context.Context, req *pb.PageListRequest) (*pb.PageListResponse, error) {
	var result = &pb.PageListResponse{}
	var total int64
	var list []*pb.TransactionRecord
	var err error

	chainType := chain2Type[req.ChainName]
	switch chainType {
	case POLKADOT:
		var recordList []*data.DotTransactionRecord
		recordList, total, err = data.DotTransactionRecordRepoClient.PageList(ctx,GetTalbeName(req.ChainName), req)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case CASPER:
		var recordList []*data.CsprTransactionRecord
		recordList, total, err = data.CsprTransactionRecordRepoClient.PageList(ctx, GetTalbeName(req.ChainName), req)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case NERVOS:
		var recordList []*data.CkbTransactionRecord
		recordList, total, err = data.CkbTransactionRecordRepoClient.PageList(ctx, GetTalbeName(req.ChainName), req)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case BTC:
		var recordList []*data.BtcTransactionRecord
		recordList, total, err = data.BtcTransactionRecordRepoClient.PageList(ctx, GetTalbeName(req.ChainName), req)
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
		if req.ContractAddress != "" {
			req.ContractAddress = types2.HexToAddress(req.ContractAddress).Hex()
		}
		req.FromAddressList = utils.HexToAddress(req.FromAddressList)
		req.ToAddressList = utils.HexToAddress(req.ToAddressList)
		if req.Address != "" {
			req.Address = types2.HexToAddress(req.Address).Hex()
		}

		var recordList []*data.EvmTransactionRecord
		recordList, total, err = data.EvmTransactionRecordRepoClient.PageList(ctx, GetTalbeName(req.ChainName), req)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case STC:
		var recordList []*data.StcTransactionRecord
		recordList, total, err = data.StcTransactionRecordRepoClient.PageList(ctx, GetTalbeName(req.ChainName), req)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case TVM:
		var recordList []*data.TrxTransactionRecord
		recordList, total, err = data.TrxTransactionRecordRepoClient.PageList(ctx, GetTalbeName(req.ChainName), req)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case APTOS:
		var recordList []*data.AptTransactionRecord
		recordList, total, err = data.AptTransactionRecordRepoClient.PageList(ctx, GetTalbeName(req.ChainName), req)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case SUI:
		var recordList []*data.SuiTransactionRecord
		recordList, total, err = data.SuiTransactionRecordRepoClient.PageList(ctx, GetTalbeName(req.ChainName), req)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case SOLANA:
		var recordList []*data.SolTransactionRecord
		recordList, total, err = data.SolTransactionRecordRepoClient.PageList(ctx, GetTalbeName(req.ChainName), req)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
		}
	case COSMOS:
		var recordList []*data.AtomTransactionRecord
		recordList, total, err = data.AtomTransactionRecordRepoClient.PageList(ctx, GetTalbeName(req.ChainName), req)
		if err == nil {
			err = utils.CopyProperties(recordList, &list)
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

				record.ChainName = req.ChainName

				if strings.Contains(req.OrderBy, "id ") {
					record.Cursor = record.Id
				} else if strings.Contains(req.OrderBy, "block_number ") {
					record.Cursor = record.BlockNumber
				} else if strings.Contains(req.OrderBy, "nonce ") {
					record.Cursor = record.Nonce
				} else if strings.Contains(req.OrderBy, "tx_time ") {
					record.Cursor = record.TxTime
				} else if strings.Contains(req.OrderBy, "created_at ") {
					record.Cursor = record.CreatedAt
				} else if strings.Contains(req.OrderBy, "updated_at ") {
					record.Cursor = record.UpdatedAt
				}

				if record.Status == DROPPED_REPLACED || record.Status == DROPPED {
					record.Status = FAIL
				}
				if record.Status == NO_STATUS {
					record.Status = PENDING
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

func (s *TransactionUsecase) GetAmount(ctx context.Context, req *pb.AmountRequest) (*pb.AmountResponse, error) {
	var result = &pb.AmountResponse{}
	var amount string
	var err error

	chainType := chain2Type[req.ChainName]
	switch chainType {
	case BTC:
		amount, err = data.BtcTransactionRecordRepoClient.GetAmount(ctx, GetTalbeName(req.ChainName), req, PENDING)
	case EVM:
		req.FromAddressList = utils.HexToAddress(req.FromAddressList)
		req.ToAddressList = utils.HexToAddress(req.ToAddressList)

		amount, err = data.EvmTransactionRecordRepoClient.GetAmount(ctx, GetTalbeName(req.ChainName), req, PENDING)
	case STC:
		amount, err = data.StcTransactionRecordRepoClient.GetAmount(ctx, GetTalbeName(req.ChainName), req, PENDING)
	case TVM:
		amount, err = data.TrxTransactionRecordRepoClient.GetAmount(ctx, GetTalbeName(req.ChainName), req, PENDING)
	case APTOS:
		amount, err = data.AptTransactionRecordRepoClient.GetAmount(ctx, GetTalbeName(req.ChainName), req, PENDING)
	}

	if err == nil {
		result.Amount = amount
	}
	return result, err
}

func (s *TransactionUsecase) GetDappListPageList(ctx context.Context, req *pb.DappPageListReq) (*pb.DappPageListResp, error) {
	if req.Page == 0 {
		req.Page = 1
	}
	if req.Limit == 0 {
		req.Limit = 20
	}
	if req.ContractAddress != "" {
		req.ContractAddress = types2.HexToAddress(req.ContractAddress).Hex()
	}
	if req.FromAddress != "" {
		req.FromAddress = types2.HexToAddress(req.FromAddress).Hex()
	}
	//if req.OrderBy == "" {
	//	req.OrderBy = "tx_time desc"
	//}

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
			chainType := chain2Type[value.ChainName]
			feeData := make(map[string]string)
			switch chainType {
			case BTC:
				btc, err := data.BtcTransactionRecordRepoClient.FindByTxhash(ctx, GetTalbeName(value.ChainName), value.LastTxhash)
				if err == nil && btc != nil {
					var r *pb.TransactionRecord
					utils.CopyProperties(btc, &r)
					r.ChainName = value.ChainName
					r.Cursor = value.TxTime
					r.Amount = value.Amount
					trs = append(trs, r)
				}
			case EVM:
				if req.ContractAddress != "" {
					req.ContractAddress = types2.HexToAddress(req.ContractAddress).Hex()
				}
				if req.FromAddress != "" {
					req.FromAddress = types2.HexToAddress(req.FromAddress).Hex()
				}

				evm, err := data.EvmTransactionRecordRepoClient.FindByTxhash(ctx, GetTalbeName(value.ChainName), value.LastTxhash)

				if err == nil && evm != nil {
					var r *pb.TransactionRecord
					utils.CopyProperties(evm, &r)

					eventLogInfo, err1 := data.EvmTransactionRecordRepoClient.FindParseDataByTxHashAndToken(ctx, GetTalbeName(value.ChainName), value.LastTxhash, tokenAddress)
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
				stc, err := data.StcTransactionRecordRepoClient.FindByTxhash(ctx, GetTalbeName(value.ChainName), value.LastTxhash)
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
				tvm, err := data.TrxTransactionRecordRepoClient.FindByTxhash(ctx, GetTalbeName(value.ChainName), value.LastTxhash)
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
					trs = append(trs, r)
				}
			case APTOS:
				apt, err := data.AptTransactionRecordRepoClient.FindByTxhash(ctx, GetTalbeName(value.ChainName), value.LastTxhash)
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
				sui, err := data.SuiTransactionRecordRepoClient.FindByTxhash(ctx, GetTalbeName(value.ChainName), value.LastTxhash)
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
				sol, err := data.SolTransactionRecordRepoClient.FindByTxhash(ctx, GetTalbeName(value.ChainName), value.LastTxhash)
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
	chainType := chain2Type[req.ChainName]
	switch chainType {
	case EVM:
		req.AddressList = utils.HexToAddress(req.AddressList)
		req.TokenAddressList = utils.HexToAddress(req.TokenAddressList)
	}

	var result = &pb.PageListAssetResponse{}
	var total int64
	var totalCurrencyAmount decimal.Decimal
	var list []*pb.AssetResponse
	var err error

	var recordList []*data.UserAsset
	recordList, total, err = data.UserAssetRepoClient.PageList(ctx, req)
	if err == nil {
		err = utils.CopyProperties(recordList, &list)
	}

	var recordGroupList []*data.UserAsset
	if err == nil {
		recordGroupList, err = data.UserAssetRepoClient.GroupListBalance(ctx, req)
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
			for key, _ := range tokenAddressMap {
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

func (s *TransactionUsecase) GetBalance(ctx context.Context, req *pb.AssetRequest) (*pb.ListBalanceResponse, error) {
	chainType := chain2Type[req.ChainName]
	switch chainType {
	case EVM:
		if req.Address != "" {
			req.Address = types2.HexToAddress(req.Address).Hex()
		}
		req.TokenAddressList = utils.HexToAddress(req.TokenAddressList)
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
	chainType := chain2Type[req.ChainName]
	switch chainType {
	case EVM:
		req.AddressList = utils.HexToAddress(req.AddressList)
		req.TokenAddressList = utils.HexToAddress(req.TokenAddressList)
	}

	var result = &pb.ClientPageListNftAssetGroupResponse{}
	var total int64
	var list []*pb.ClientNftAssetGroupResponse
	var err error

	var recordList []*data.UserNftAssetGroup
	recordList, total, err = data.UserNftAssetRepoClient.PageListGroup(ctx, req)
	if err == nil {
		err = utils.CopyProperties(recordList, &list)
	}

	if err == nil && len(recordList) > 0 {
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
			return result, err
		}
		nftAddressInfoMap := make(map[string]*v1.GetNftReply_NftInfoResp)
		for _, res := range resultMap {
			nftAddressInfoMap[res.TokenAddress] = res
		}

		result.Total = total
		result.List = list
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
				}
			}
		}
	}
	return result, err
}

func (s *TransactionUsecase) ClientPageListNftAsset(ctx context.Context, req *pb.PageListNftAssetRequest) (*pb.ClientPageListNftAssetResponse, error) {
	chainType := chain2Type[req.ChainName]
	switch chainType {
	case EVM:
		req.AddressList = utils.HexToAddress(req.AddressList)
		req.TokenAddressList = utils.HexToAddress(req.TokenAddressList)
	}

	var result = &pb.ClientPageListNftAssetResponse{}
	var total int64
	var list []*pb.ClientNftAssetResponse
	var err error

	var recordList []*data.UserNftAsset
	recordList, total, err = data.UserNftAssetRepoClient.PageList(ctx, req)
	if err == nil {
		err = utils.CopyProperties(recordList, &list)
	}

	if err == nil && len(recordList) > 0 {
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
			return result, err
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
	chainType := chain2Type[req.ChainName]
	switch chainType {
	case EVM:
		if req.Address != "" {
			req.Address = types2.HexToAddress(req.Address).Hex()
		}
		if req.TokenAddress != "" {
			req.TokenAddress = types2.HexToAddress(req.TokenAddress).Hex()
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
		//var utxoList []*pb.UtxoList
		//dbUnspentRecord, err := data.UtxoUnspentRecordRepoClient.FindByCondition(ctx, req)
		//if err != nil {
		//	result.Ok = false
		//	return result, err
		//}
		//for _, db := range dbUnspentRecord {
		//	var lt []*pb.UnspentList
		//	amount := make(map[string]string)
		//	amount["amount"] = db.Amount
		//	a := &pb.UnspentList{
		//		Data: amount,
		//	}
		//	lt = append(lt, a)
		//
		//	hash := make(map[string]string)
		//	hash["hash"] = db.Hash
		//	b := &pb.UnspentList{
		//		Data: hash,
		//	}
		//	lt = append(lt, b)
		//
		//	index := make(map[string]string)
		//	index["index"] = strconv.Itoa(db.N)
		//	c := &pb.UnspentList{
		//		Data: index,
		//	}
		//	lt = append(lt, c)
		//
		//	script := make(map[string]string)
		//	script["script"] = db.Script
		//	d := &pb.UnspentList{
		//		Data: script,
		//	}
		//	lt = append(lt, d)
		//
		//	r := &pb.UtxoList{
		//		List: lt,
		//	}
		//	utxoList = append(utxoList, r)
		//}
		//
		//result.Ok = true
		//result.UtxoList = utxoList
		//return result, nil

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
	args := make([]reflect.Value, 0)
	args = append(args, reflect.ValueOf(s))
	args = append(args, reflect.ValueOf(ctx))

	if len(req.Params) > 0 {
		u := mv.Type.NumIn()
		paseJson := reflect.New(mv.Type.In(u - 1).Elem())
		reqKey := strings.ReplaceAll(utils.ListToString(req.Params), "\\", "")

		jsonErr := json.Unmarshal([]byte(reqKey), paseJson.Interface())
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
	respone := ss[0].Interface()
	ret, _ := json.Marshal(respone)
	return &pb.JsonResponse{
		Ok:       true,
		Response: string(ret),
	}, nil
}

func (s *TransactionUsecase) GetDataDictionary(ctx context.Context) (*DataDictionary, error) {
	var result = &DataDictionary{}
	var serviceTransactionType = []string{CANCEL, SPEED_UP, NATIVE, TRANSFERNFT, CONTRACT, EVENTLOG, CREATEACCOUNT, REGISTERTOKEN, DIRECTTRANSFERNFTSWITCH, APPROVE, TRANSFER}
	var serviceStaus = []string{SUCCESS, FAIL, PENDING, NO_STATUS, DROPPED_REPLACED, DROPPED}
	result.Ok = true
	result.ServiceTransactionType = serviceTransactionType
	result.ServiceStatus = serviceStaus

	return result, nil
}
