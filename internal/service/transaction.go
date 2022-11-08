package service

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"context"
	"errors"
	"go.uber.org/zap"
	"time"
)

type TransactionService struct {
	pb.UnimplementedTransactionServer
	ts *biz.TransactionUsecase
}

func NewTransactionService(ts *biz.TransactionUsecase) *TransactionService {
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
	} else if req.Platform == biz.ANDROID || req.Platform == biz.IOS {
		req.TransactionTypeNotInList = []string{biz.CONTRACT}
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

func (s *TransactionService) GetBalance(ctx context.Context, req *pb.AssetRequest) (*pb.ListBalanceResponse, error) {
	result, err := s.ts.GetBalance(ctx, req)
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
