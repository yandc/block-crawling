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
	var dappALl []*pb.DappInfo
	dapps, err := data.DappApproveRecordRepoClient.ListByCondition(ctx, req)
	if err != nil {
		log.Errore("返回授权dapp列表报错！", err)
		return &pb.DappListResp{
			Ok:   false,
			Data: dappALl,
		}, err
	}
	if dapps == nil {
		return &pb.DappListResp{
			Ok:   true,
			Data: dappALl,
		}, err

	}
	for _, da := range dapps {
		//查询
		dappInfo := ""
		chainType := chain2Type[da.ChainName]
		switch chainType {
		case EVM:
			evm, err := data.EvmTransactionRecordRepoClient.FindByTxhash(ctx, GetTalbeName(da.ChainName), da.LastTxhash)
			if err == nil && evm != nil {
				dappInfo = evm.DappData
			}
		case STC:
			stc, err := data.StcTransactionRecordRepoClient.FindByTxhash(ctx, GetTalbeName(da.ChainName), da.LastTxhash)
			if err == nil && stc != nil {
				dappInfo = stc.DappData
			}
		case TVM:
			tvm, err := data.TrxTransactionRecordRepoClient.FindByTxhash(ctx, GetTalbeName(da.ChainName), da.LastTxhash)
			if err == nil && tvm != nil {
				dappInfo = tvm.DappData
			}
		case APTOS:
			apt, err := data.AptTransactionRecordRepoClient.FindByTxhash(ctx, GetTalbeName(da.ChainName), da.LastTxhash)
			if err == nil && apt != nil {
				dappInfo = apt.DappData
			}
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
		dappALl = append(dappALl, dif)
	}
	return &pb.DappListResp{
		Ok:   true,
		Data: dappALl,
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
	case STC:

		//{"stc":{"sequence_number":53}

		stc := make(map[string]interface{})
		nonce := ""
		if jsonErr := json.Unmarshal([]byte(pbb.ParseData), &stc); jsonErr == nil {
			evmMap := stc["stc"]
			ret := evmMap.(map[string]interface{})
			nonce = ret["sequence_number"].(string)

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
			key := pendingNonceKey+nonce
			data.RedisClient.Set(key, pbb.Uid, 6*time.Hour)
		}
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
		if pbb.ToAddress != "" {
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
		log.Info("asdf",zap.Any("插入数据库结果",result))
		if result == 1 {
			//插入redis 并设置过期时间为6个小时
			key := pendingNonceKey+strconv.Itoa(int(dbNonce))
			log.Info("asdf",zap.Any("插入缓存",key),zap.Any("result",pbb.Uid))
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
		stcRecord := &data.AptTransactionRecord{
			Nonce:              pbb.Nonce,
			TransactionVersion: int(pbb.BlockNumber),
			TransactionHash:    pbb.TransactionHash,
			FromAddress:        pbb.FromAddress,
			ToAddress:          pbb.ToAddress,
			FromUid:            pbb.Uid,
			FeeAmount:          fa,
			Amount:             a,
			Status:             pbb.Status,
			TxTime:             pbb.TxTime,
			ContractAddress:    pbb.ContractAddress,
			ParseData:          pbb.ParseData,
			GasLimit:           pbb.GasLimit,
			GasUsed:            pbb.GasUsed,
			GasPrice:           pbb.GasPrice,
			Data:               pbb.Data,
			TransactionType:    pbb.TransactionType,
			DappData:           pbb.DappData,
			ClientData:         pbb.ClientData,
			CreatedAt:          pbb.CreatedAt,
			UpdatedAt:          pbb.UpdatedAt,
		}

		result, err = data.AptTransactionRecordRepoClient.Save(ctx, GetTalbeName(pbb.ChainName), stcRecord)
		if result == 1 {
			//插入redis 并设置过期时间为6个小时
			key := pendingNonceKey+strconv.Itoa(int(pbb.Nonce))
			log.Info("asdf",zap.Any("插入缓存",key),zap.Any("result",pbb.Uid))
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
				} else if strings.Contains(req.OrderBy, "create_at ") {
					record.Cursor = record.CreatedAt
				} else if strings.Contains(req.OrderBy, "updated_at ") {
					record.Cursor = record.UpdatedAt
				}

				if record.Status == DROPPED_REPLACED {
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
	dapps, err := data.DappApproveRecordRepoClient.GetDappListPageList(ctx, req)
	log.Info("yd", zap.Any("dapp", dapps))
	if err != nil {
		log.Errore("返回授权dapp列表报错！", err)
		return &pb.DappPageListResp{
			Ok: false,
		}, err
	}
	//分组 根据 token 然后sum出结果 过滤出amount len > 40的
	total := data.DappApproveRecordRepoClient.GetDappListPageCount(ctx, req)
	log.Info("yd", zap.Any("dapptotal", len(dapps)))

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
			chainType := chain2Type[value.ChainName]
			feeData := make(map[string]string)
			switch chainType {
			case BTC:
				btc, err := data.BtcTransactionRecordRepoClient.FindByTxhash(ctx, GetTalbeName(value.ChainName), value.LastTxhash)
				if err == nil && btc != nil {
					var r *pb.TransactionRecord
					utils.CopyProperties(btc, &r)
					r.ChainName = value.ChainName
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
					feeData["gas_limit"] = r.GasLimit
					feeData["gas_used"] = r.GasUsed
					feeData["gas_price"] = r.GasPrice
					feeData["base_fee"] = r.BaseFee
					feeData["max_fee_per_gas"] = r.MaxFeePerGas
					feeData["max_priority_fee_per_gas"] = r.MaxPriorityFeePerGas
					r.ChainName = value.ChainName
					feeDataStr, _ := utils.JsonEncode(feeData)
					r.FeeData = feeDataStr
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
