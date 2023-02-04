package service

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform"
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
)

type TransactionService struct {
	pb.UnimplementedTransactionServer
	ts *biz.TransactionUsecase
}

func NewTransactionService(ts *biz.TransactionUsecase, p platform.PlatformContainer, ip platform.InnerPlatformContainer) *TransactionService {
	return &TransactionService{ts: ts}
}

func (s *TransactionService) CreateRecordFromWallet(ctx context.Context, req *pb.TransactionReq) (*pb.CreateResponse, error) {
	log.Info("request", zap.Any("request", req))
	subctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	result, err := s.ts.CreateRecordFromWallet(subctx, req)
	return result, err
}

func (s *TransactionService) PageList(ctx context.Context, req *pb.PageListRequest) (*pb.PageListResponse, error) {
	subctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if req.Platform == biz.WEB {
		req.TransactionTypeNotInList = []string{biz.EVENTLOG}
	} else if req.Platform == biz.ANDROID {
		if req.OsVersion > 2022101201 {
			req.TransactionTypeNotInList = []string{biz.EVENTLOG}
		} else {
			req.TransactionTypeNotInList = []string{biz.CONTRACT}
		}
	} else if req.Platform == biz.IOS {
		if req.OsVersion >= 2022101501 {
			req.TransactionTypeNotInList = []string{biz.EVENTLOG}
		} else {
			req.TransactionTypeNotInList = []string{biz.CONTRACT}
		}
	}
	for _, transactionType := range req.TransactionTypeList {
		if transactionType == biz.OTHER {
			req.TransactionTypeList = append(req.TransactionTypeList, biz.CONTRACT, biz.CREATEACCOUNT, biz.REGISTERTOKEN, biz.DIRECTTRANSFERNFTSWITCH)
			break
		}
	}

	if req.OrderBy == "" {
		req.OrderBy = "tx_time desc"
	}

	if req.PageSize <= 0 {
		req.PageSize = data.PAGE_SIZE
	} else if req.PageSize > data.MAX_PAGE_SIZE {
		req.PageSize = data.MAX_PAGE_SIZE
	}

	if len(req.StatusList) > 0 {
		for _, status := range req.StatusList {
			if status == biz.PENDING {
				req.StatusList = append(req.StatusList, biz.NO_STATUS)
			} else if status == biz.FAIL {
				req.StatusList = append(req.StatusList, biz.DROPPED_REPLACED, biz.DROPPED)
			}
		}
	}

	result, err := s.ts.PageList(subctx, req)
	if result != nil && len(result.List) > 0 {
		for _, record := range result.List {
			if record == nil {
				continue
			}

			if record.Status == biz.DROPPED_REPLACED || record.Status == biz.DROPPED {
				record.Status = biz.FAIL
			}
			if record.Status == biz.NO_STATUS {
				record.Status = biz.PENDING
			}

			if req.Platform == biz.ANDROID || req.Platform == biz.IOS {
				if req.OsVersion < 2023011001 && record.OperateType != "" {
					record.TransactionType = record.OperateType
				}
			}
		}
	}
	return result, err
}

func (s *TransactionService) GetAmount(ctx context.Context, req *pb.AmountRequest) (*pb.AmountResponse, error) {
	result, err := s.ts.GetAmount(ctx, req)
	return result, err
}

func (s *TransactionService) GetDappList(ctx context.Context, req *pb.DappListReq) (*pb.DappListResp, error) {
	subctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	result, err := s.ts.GetDappList(subctx, req)
	return result, err
}

func (s *TransactionService) GetAllOpenAmount(ctx context.Context, req *pb.OpenAmountReq) (*pb.OpenAmoutResp, error) {
	subctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	result, err := s.ts.GetAllOpenAmount(subctx, req)
	return result, err
}

func (s *TransactionService) GetNonce(ctx context.Context, req *pb.NonceReq) (*pb.NonceResp, error) {
	subctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	result, err := s.ts.GetNonce(subctx, req)
	return result, err
}

func (s *TransactionService) GetDappListPageList(ctx context.Context, req *pb.DappPageListReq) (*pb.DappPageListResp, error) {
	subctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	result, err := s.ts.GetDappListPageList(subctx, req)
	return result, err
}

func (s *TransactionService) PageListAsset(ctx context.Context, req *pb.PageListAssetRequest) (*pb.PageListAssetResponse, error) {
	if req.Uid == "" && len(req.AddressList) == 0 {
		return nil, errors.New("uid or addressList is required")
	}
	if req.Currency != "CNY" && req.Currency != "USD" {
		return nil, errors.New("currency must be CNY or USD")
	}

	if req.OrderBy == "" {
		req.OrderBy = "id desc"
	}

	if req.PageSize <= 0 {
		req.PageSize = data.PAGE_SIZE
	} else if req.PageSize > data.MAX_PAGE_SIZE {
		req.PageSize = data.MAX_PAGE_SIZE
	}

	result, err := s.ts.PageListAsset(ctx, req)
	return result, err
}

func (s *TransactionService) ClientPageListAsset(ctx context.Context, req *pb.PageListAssetRequest) (*pb.PageListAssetResponse, error) {
	if req.ChainName == "" {
		return nil, errors.New("chainName is required")
	}
	if req.Uid == "" && len(req.AddressList) == 0 {
		return nil, errors.New("uid or addressList is required")
	}
	if req.Currency != "CNY" && req.Currency != "USD" {
		return nil, errors.New("currency must be CNY or USD")
	}

	if req.OrderBy == "" {
		req.OrderBy = "currencyAmount desc"
	}

	if req.PageSize <= 0 {
		req.PageSize = data.PAGE_SIZE
	} else if req.PageSize > data.MAX_PAGE_SIZE {
		req.PageSize = data.MAX_PAGE_SIZE
	}

	result, err := s.ts.ClientPageListAsset(ctx, req)
	return result, err
}

func (s *TransactionService) GetBalance(ctx context.Context, req *pb.AssetRequest) (*pb.ListBalanceResponse, error) {
	result, err := s.ts.GetBalance(ctx, req)
	return result, err
}

func (s *TransactionService) ListAmountUidDimension(ctx context.Context, req *pb.ListAmountUidDimensionRequest) (*pb.ListAmountUidDimensionResponse, error) {
	if len(req.UidList) == 0 {
		return nil, errors.New("uidList is required")
	}
	if req.Currency != "CNY" && req.Currency != "USD" {
		return nil, errors.New("currency must be CNY or USD")
	}

	result, err := s.ts.ListAmountUidDimension(ctx, req)
	return result, err
}

func (s *TransactionService) ListHasBalanceUidDimension(ctx context.Context, req *pb.ListHasBalanceUidDimensionRequest) (*pb.ListHasBalanceUidDimensionResponse, error) {
	if len(req.UidList) == 0 {
		return nil, errors.New("uidList is required")
	}

	result, err := s.ts.ListHasBalanceUidDimension(ctx, req)
	return result, err
}

func (s *TransactionService) ClientPageListNftAssetGroup(ctx context.Context, req *pb.PageListNftAssetRequest) (*pb.ClientPageListNftAssetGroupResponse, error) {
	if req.Uid == "" && len(req.AddressList) == 0 {
		return nil, errors.New("uid or addressList is required")
	}

	if req.OrderBy == "" {
		req.OrderBy = "token_id_amount desc"
	}

	if req.PageSize <= 0 {
		req.PageSize = data.PAGE_SIZE
	} else if req.PageSize > data.MAX_PAGE_SIZE {
		req.PageSize = data.MAX_PAGE_SIZE
	}

	result, err := s.ts.ClientPageListNftAssetGroup(ctx, req)
	return result, err
}

func (s *TransactionService) ClientPageListNftAsset(ctx context.Context, req *pb.PageListNftAssetRequest) (*pb.ClientPageListNftAssetResponse, error) {
	if req.Uid == "" && len(req.AddressList) == 0 {
		return nil, errors.New("uid or addressList is required")
	}

	if req.OrderBy == "" {
		req.OrderBy = "id desc"
	}

	if req.PageSize <= 0 {
		req.PageSize = data.PAGE_SIZE
	} else if req.PageSize > data.MAX_PAGE_SIZE {
		req.PageSize = data.MAX_PAGE_SIZE
	}

	result, err := s.ts.ClientPageListNftAsset(ctx, req)
	return result, err
}

func (s *TransactionService) GetNftBalance(ctx context.Context, req *pb.NftAssetRequest) (*pb.NftBalanceResponse, error) {
	result, err := s.ts.GetNftBalance(ctx, req)
	return result, err
}

func (s *TransactionService) PageListStatistic(ctx context.Context, req *pb.PageListStatisticRequest) (*pb.PageListStatisticResponse, error) {
	if req.StartTime >= req.StopTime {
		return nil, errors.New("startTime is greater than stopTime")
	}
	result, err := s.ts.PageListStatistic(ctx, req)
	return result, err
}

func (s *TransactionService) StatisticFundAmount(ctx context.Context, req *pb.StatisticFundRequest) (*pb.FundAmountListResponse, error) {
	if req.StartTime >= req.StopTime {
		return nil, errors.New("startTime is greater than stopTime")
	}
	result, err := s.ts.StatisticFundAmount(ctx, req)
	return result, err
}

func (s *TransactionService) StatisticFundRate(ctx context.Context, req *pb.StatisticFundRequest) (*pb.FundRateListResponse, error) {
	if req.StartTime >= req.StopTime {
		return nil, errors.New("startTime is greater than stopTime")
	}
	result, err := s.ts.StatisticFundRate(ctx, req)
	return result, err
}
func (s *TransactionService) GetUnspentTx(ctx context.Context, req *pb.UnspentReq) (*pb.UnspentResponse, error) {
	subctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	result, err := s.ts.GetUnspentTx(subctx, req)
	return result, err
}
func (s *TransactionService) GetNftRecord(ctx context.Context, req *pb.NftRecordReq) (*pb.NftRecordResponse, error) {
	subctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	result, err := s.ts.GetNftRecord(subctx, req)
	return result, err
}

func (s *TransactionService) JsonRpc(ctx context.Context, req *pb.JsonReq) (*pb.JsonResponse, error) {
	subctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	result, err := s.ts.JsonRpc(subctx, req)
	log.Info(
		"JsonRpc",
		zap.String("method", req.Method),
		zap.String("uid", req.Uid),
		zap.String("chainName", req.ChainName),
		zap.String("params", req.Params),
		zap.Any("response", result),
		zap.Error(err),
	)
	return result, err
}
