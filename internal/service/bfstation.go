package service

import (
	pb "block-crawling/api/bfstation/v1"
	"block-crawling/internal/biz"
	"context"
)

type BFStationService struct {
	pb.UnimplementedBFStationServer

	usecase *biz.BFStationUsecase
}

func (s *BFStationService) PageListTxns(ctx context.Context, req *pb.PageListTxnsRequest) (*pb.PageListTxnsResponse, error) {
	return s.usecase.PageListTxns(ctx, req)
}

func (s *BFStationService) CountPoolHolders(ctx context.Context, req *pb.PoolHolderRequest) (*pb.CountResponse, error) {
	total, err := s.usecase.CountPoolHolders(ctx, req.ChainName, req.PoolId)
	if err != nil {
		return nil, err
	}
	return &pb.CountResponse{
		Total: total,
	}, nil
}

func (s *BFStationService) CountTokenHolders(ctx context.Context, req *pb.TokenHolderRequest) (*pb.CountResponse, error) {
	total, err := s.usecase.CountTokenHolders(ctx, req.ChainName, req.CoinType)
	if err != nil {
		return nil, err
	}
	return &pb.CountResponse{
		Total: total,
	}, err
}

func (s *BFStationService) PageListCollectFees(ctx context.Context, req *pb.PageListFeesRequest) (*pb.PageListFeesResponse, error) {
	return s.usecase.PageListCollectFees(ctx, req)
}

func NewBFStationService(usecase *biz.BFStationUsecase) *BFStationService {
	return &BFStationService{
		usecase: usecase,
	}
}
