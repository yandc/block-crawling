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
			Vault:           item.Vault,
			TokenAmountIn:   item.TokenAmountIn.String(),
			TokenAmountOut:  item.TokenAmountOut.String(),
			CoinTypeIn:      item.CoinTypeIn,
			CoinTypeOut:     item.CoinTypeOut,
			ParsedJson:      item.ParsedJson,
			Status:          item.Status,
			CreatedAt:       item.CreatedAt,
		})
	}
	return response, nil
}

func NewBFStationUsecase(gorm *gorm.DB) *BFStationUsecase {
	return &BFStationUsecase{
		gormDB: gorm,
	}
}
