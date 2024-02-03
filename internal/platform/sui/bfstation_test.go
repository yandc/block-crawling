package sui

import (
	pb "block-crawling/api/bfstation/v1"
	"block-crawling/internal/biz"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	log.BootstrapLogger(&conf.Logger{
		DEBUG:    false,
		FileName: "stderr",
		Level:    "debug",
	})
	os.Exit(m.Run())
}

type mockBFStationRepo struct {
	records     []data.BFCStationRecord
	tokens      []data.BFStationToken
	pools       []data.BFStationPool
	accTokens   []data.BFStationAccountToken
	accPools    []data.BFStationAccountPool
	collectFees []data.BFStationCollectFee
}

// ListAllAccountTokens implements data.BFCStationRepo
func (*mockBFStationRepo) ListAllAccountTokens(ctx context.Context, chainName string) ([]data.BFStationAccountToken, error) {
	panic("unimplemented")
}

// SaveAccountToken implements data.BFCStationRepo
func (*mockBFStationRepo) SaveAccountToken(ctx context.Context, chainName string, record *data.BFStationAccountToken) error {
	panic("unimplemented")
}

// BatchSaveCollectFees implements data.BFCStationRepo
func (*mockBFStationRepo) BatchSaveCollectFees(ctx context.Context, chainName string, records []data.BFStationCollectFee) error {
	panic("unimplemented")
}

// GetPool implements data.BFCStationRepo
func (*mockBFStationRepo) GetPool(ctx context.Context, chainName string, poolID string) (*data.BFStationPool, error) {
	panic("unimplemented")
}

// PageListCollectFees implements data.BFCStationRepo
func (*mockBFStationRepo) PageListCollectFees(ctx context.Context, chainName string, req *pb.PageListFeesRequest) ([]data.BFStationCollectFee, int64, error) {
	panic("unimplemented")
}

// CountPoolHolders implements data.BFCStationRepo
func (*mockBFStationRepo) CountPoolHolders(ctx context.Context, chainName string, poolID string) (int64, error) {
	panic("unimplemented")
}

// CountTokenHolders implements data.BFCStationRepo
func (*mockBFStationRepo) CountTokenHolders(ctx context.Context, chainName string, coinType string) (int64, error) {
	panic("unimplemented")
}

// BatchSave implements data.BFCStationRepo
func (m *mockBFStationRepo) BatchSave(ctx context.Context, chainName string, records []data.BFCStationRecord) error {
	m.records = append(m.records, records...)
	return nil
}

// BatchSaveAccountPools implements data.BFCStationRepo
func (m *mockBFStationRepo) BatchSaveAccountPools(ctx context.Context, chainName string, records []data.BFStationAccountPool) error {
	m.accPools = append(m.accPools, records...)
	return nil
}

// BatchSaveAccountTokens implements data.BFCStationRepo
func (m *mockBFStationRepo) BatchSaveAccountTokens(ctx context.Context, chainName string, records []data.BFStationAccountToken) error {
	m.accTokens = append(m.accTokens, records...)
	return nil
}

// BatchSavePools implements data.BFCStationRepo
func (m *mockBFStationRepo) BatchSavePools(ctx context.Context, chainName string, records []data.BFStationPool) error {
	m.pools = append(m.pools, records...)
	return nil
}

// BatchSaveTokens implements data.BFCStationRepo
func (m *mockBFStationRepo) BatchSaveTokens(ctx context.Context, chainName string, records []data.BFStationToken) error {
	m.tokens = append(m.tokens, records...)
	return nil
}

// LoadTimeoutRecords implements data.BFCStationRepo
func (*mockBFStationRepo) LoadTimeoutRecords(ctx context.Context, chainName string, timeout time.Duration) ([]data.BFCStationRecord, error) {
	panic("unimplemented")
}

// PageList implements data.BFCStationRepo
func (*mockBFStationRepo) PageList(ctx context.Context, chainName string, req *pb.PageListTxnsRequest) ([]data.BFCStationRecord, int64, error) {
	panic("unimplemented")
}

func newMockBFStationRepo() *mockBFStationRepo {
	return &mockBFStationRepo{}
}

// https://obc.openblock.vip/txblock/8Mte23TnCAZnA3Nf1EfFNd2bTnGFh2Cw117DXN4Vrbj5?network=testnet
func TestCollectFee(t *testing.T) {
	repo := handleTx(t, "8Mte23TnCAZnA3Nf1EfFNd2bTnGFh2Cw117DXN4Vrbj5")
	assert.Len(t, repo.collectFees, 1)
}

func handleTx(t *testing.T, hash string) *mockBFStationRepo {
	client := NewClient("https://obcrpc.openblock.vip", "BenfenTEST")
	txInfo, err := client.GetTransactionByHash(hash)
	assert.NoError(t, err)
	repo := newMockBFStationRepo()
	h := &bfstationHandler{
		chainName: "BenfenTEST",
		repo:      repo,
	}
	h.Handle(txInfo, nil, biz.SUCCESS)
	return repo
}
