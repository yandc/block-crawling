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

func NewBFStationService(usecase *biz.BFStationUsecase) *BFStationService {
	return &BFStationService{
		usecase: usecase,
	}
}
