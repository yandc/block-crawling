package biz

import (
	pb "block-crawling/api/bfstation/v1"
	"block-crawling/internal/data"
	"context"

	"gorm.io/gorm"
)

type BFStationUsecase struct {
	gormDB *gorm.DB
}

func (c *BFStationUsecase) PageListTxns(ctx context.Context, req *pb.PageListTxnsRequest) (*pb.PageListTxnsResponse, error) {
	records, total, err := data.BFCStationRepoIns.PageList(ctx, req.ChainName, req)
	if err != nil {
		return nil, err
	}
	response := &pb.PageListTxnsResponse{
		Total: total,
		List:  make([]*pb.BFStationTxRecord, 0, len(records)),
	}
	for _, item := range records {
		response.List = append(response.List, &pb.BFStationTxRecord{
			Id:              item.Id,
			BlockHash:       item.BlockHash,
			BlockNumber:     int64(item.BlockNumber),
			TxTime:          item.TxTime,
			TransactionHash: item.TransactionHash,
			WalletAddress:   item.WalletAddress,
			Type:            string(item.Type),
			PoolId:          item.PoolID,
			Vault:           item.Vault,
			TokenAmountIn:   item.TokenAmountIn.String(),
			TokenAmountOut:  item.TokenAmountOut.String(),
			CoinTypeIn:      item.CoinTypeIn,
			CoinTypeOut:     item.CoinTypeOut,
			ParsedJson:      item.ParsedJson,
			Status:          item.Status,
			CreatedAt:       item.CreatedAt,
			WalletUid:       item.WalletUID,
			FeeAmount:       item.FeeAmount.String(),
			GasLimit:        item.GasLimit,
			GasUsed:         item.GasUsed,
			FromOBWallet:    item.WalletAddress != "",
			CoinInfoIn:      item.CoinInfoIn,
			CoinInfoOut:     item.CoinInfoOut,
		})
	}
	return response, nil
}

func (c *BFStationUsecase) CountPoolHolders(ctx context.Context, chainName, poolID string) (int64, error) {
	return data.BFCStationRepoIns.CountPoolHolders(ctx, chainName, poolID)
}

func (c *BFStationUsecase) CountTokenHolders(ctx context.Context, chainName, coinType string) (int64, error) {
	return data.BFCStationRepoIns.CountTokenHolders(ctx, chainName, coinType)
}

func (c *BFStationUsecase) PageListCollectFees(ctx context.Context, req *pb.PageListFeesRequest) (*pb.PageListFeesResponse, error) {
	records, total, err := data.BFCStationRepoIns.PageListCollectFees(ctx, req.ChainName, req)
	if err != nil {
		return nil, err
	}
	results := make([]*pb.BFStationCollectFeeRecord, 0, len(records))
	for _, record := range records {
		results = append(results, &pb.BFStationCollectFeeRecord{
			Id:              record.Id,
			TransactionHash: record.TxHash,
			TxTime:          record.TxTime,
			WalletAddress:   record.Address,
			PoolId:          record.PoolID,
			PositionId:      record.Position,
			AmountA:         record.AmountA.String(),
			AmountB:         record.AmountB.String(),
			CoinTypeA:       record.CoinTypeA,
			CoinTypeB:       record.CoinTypeB,
			CreatedAt:       record.CreatedAt,
		})
	}
	return &pb.PageListFeesResponse{
		Total: total,
		List:  results,
	}, nil
}

func NewBFStationUsecase(gorm *gorm.DB) *BFStationUsecase {
	return &BFStationUsecase{
		gormDB: gorm,
	}
}
