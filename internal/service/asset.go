package service

import (
	pb "block-crawling/api/userWalletAsset/v1"
	"block-crawling/internal/biz"
	"block-crawling/internal/log"
	"context"
	"go.uber.org/zap"
)

type UserWalletAssetService struct {
	pb.UnimplementedUserWalletAssetServer
	uc *biz.UserWalletAssetUsecase
}

func NewUserWalletAssetService(uc *biz.UserWalletAssetUsecase) *UserWalletAssetService {
	return &UserWalletAssetService{uc: uc}
}

func (s *UserWalletAssetService) UserWalletAssetTotal(ctx context.Context, req *pb.UserWalletAssetTotalReq) (*pb.UserWalletAssetTotalResp, error) {
	log.Info("request", zap.Any("request", req))
	//biz.ChainTypeAdd(req.ChainName)
	//subctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	//defer cancel()
	return s.uc.UserWalletAssetTotal(ctx, req)
}

func (s *UserWalletAssetService) UserWalletAssetHistory(ctx context.Context, req *pb.UserWalletAssetHistoryReq) (*pb.UserWalletAssetHistoryResp, error) {
	log.Info("request", zap.Any("request", req))
	//biz.ChainTypeAdd(req.ChainName)
	//subctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	//defer cancel()
	return s.uc.UserWalletAssetHistory(ctx, req)
}

func (s *UserWalletAssetService) UserWalletIncomeHistory(ctx context.Context, req *pb.UserWalletIncomeHistoryReq) (*pb.UserWalletIncomeHistoryResp, error) {
	log.Info("request", zap.Any("request", req))
	//biz.ChainTypeAdd(req.ChainName)
	//subctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	//defer cancel()
	return s.uc.UserWalletIncomeHistory(ctx, req)
}

func (s *UserWalletAssetService) UserWallet(ctx context.Context, req *pb.UserWalletReq) (*pb.UserWalletResp, error) {
	log.Info("request", zap.Any("request", req))
	//biz.ChainTypeAdd(req.ChainName)
	//subctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	//defer cancel()
	return s.uc.UserWallet(ctx, req)
}

func (s *UserWalletAssetService) UserChain(ctx context.Context, req *pb.UserChainReq) (*pb.UserChainResp, error) {
	log.Info("request", zap.Any("request", req))
	//biz.ChainTypeAdd(req.ChainName)
	//subctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	//defer cancel()
	return s.uc.UserChain(ctx, req)
}

func (s *UserWalletAssetService) UserChainAmount(ctx context.Context, req *pb.UserChainAmountReq) (*pb.UserChainAmountResp, error) {
	log.Info("request", zap.Any("request", req))
	//biz.ChainTypeAdd(req.ChainName)
	//subctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	//defer cancel()
	return s.uc.UserChainAmount(ctx, req)
}

func (s *UserWalletAssetService) UserToken(ctx context.Context, req *pb.UserTokenReq) (*pb.UserTokenResp, error) {
	log.Info("request", zap.Any("request", req))
	//biz.ChainTypeAdd(req.ChainName)
	//subctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	//defer cancel()
	return s.uc.UserToken(ctx, req)
}

func (s *UserWalletAssetService) UserAssetList(ctx context.Context, req *pb.UserAssetListReq) (*pb.UserAssetListResp, error) {
	log.Info("request", zap.Any("request", req))
	//biz.ChainTypeAdd(req.ChainName)
	//subctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	//defer cancel()
	return s.uc.UserAssetList(ctx, req)
}

func (s *UserWalletAssetService) UserAssetDistribution(ctx context.Context, req *pb.UserAssetDistributionReq) (*pb.UserAssetDistributionResp, error) {
	log.Info("request", zap.Any("request", req))
	//biz.ChainTypeAdd(req.ChainName)
	//subctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	//defer cancel()
	return s.uc.UserAssetDistribution(ctx, req)
}

func (s *UserWalletAssetService) UserChainAssetDistribution(ctx context.Context, req *pb.UserChainAssetDistributionReq) (*pb.UserChainAssetDistributionResp, error) {
	log.Info("request", zap.Any("request", req))
	//biz.ChainTypeAdd(req.ChainName)
	//subctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	//defer cancel()
	return s.uc.UserChainAssetDistribution(ctx, req)
}

func (s *UserWalletAssetService) UserChainDistribution(ctx context.Context, req *pb.UserChainDistributionReq) (*pb.UserChainDistributionResp, error) {
	log.Info("request", zap.Any("request", req))
	//biz.ChainTypeAdd(req.ChainName)
	//subctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	//defer cancel()
	return s.uc.UserChainDistribution(ctx, req)
}

func (s *UserWalletAssetService) UserWalletDistribution(ctx context.Context, req *pb.UserWalletDistributionReq) (*pb.UserWalletDistributionResp, error) {
	log.Info("request", zap.Any("request", req))
	//biz.ChainTypeAdd(req.ChainName)
	//subctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	//defer cancel()
	return s.uc.UserWalletDistribution(ctx, req)
}

func (s *UserWalletAssetService) UserChainAssetFilter(ctx context.Context, req *pb.UserChainAssetFilterReq) (*pb.UserChainAssetFilterResp, error) {
	log.Info("request", zap.Any("request", req))
	//biz.ChainTypeAdd(req.ChainName)
	//subctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	//defer cancel()
	return s.uc.UserChainAssetFilter(ctx, req)
}
