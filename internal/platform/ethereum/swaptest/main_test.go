package swap

import (
	"block-crawling/internal/data"
	"context"
	"os"
	"testing"
)

type dummySwapContractRepo struct {
}

// Is implements data.SwapContractRepo
func (*dummySwapContractRepo) Is(chainName string, contract string) bool {
	return false
}

// BatchSave implements data.SwapContractRepo
func (*dummySwapContractRepo) BatchSave(ctx context.Context, chainName string, records []*data.SwapContract) error {
	panic("unimplemented")
}

// FindOne implements data.SwapContractRepo
func (*dummySwapContractRepo) FindOne(ctx context.Context, chainName string, contract string) (*data.SwapContract, error) {
	return nil, nil
}

func TestMain(m *testing.M) {
	data.SwapContractRepoClient = &dummySwapContractRepo{}
	os.Exit(m.Run())
}
