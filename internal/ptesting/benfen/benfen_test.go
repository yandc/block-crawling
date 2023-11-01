package benfen

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/ptesting"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	chainName  = "BenfenTEST"
	configPath = "../../..//configs"

	// Unique queue to avoid conflicts as tests in different packages will run in parallel.
	TOKEN_INFO_QUEUE_TOPIC = biz.TOKEN_INFO_QUEUE_TOPIC + ":benfen"
)

func TestWhyContract(t *testing.T) {
	ptesting.RunTest(ptesting.Preparation{
		Configs:   configPath,
		ChainName: chainName,
		IndexBlockNumbers: []uint64{
			79917,
		},
		Users: map[string]string{
			"BFC4812ec9d87678e94747d6ea092d4f76ec92b8bfc01aac509394204e4d4150af92999": "1",
		},
		Assert: func() {
			record, err := data.SuiTransactionRecordRepoClient.FindByTxHash(
				context.Background(),
				biz.GetTableName(chainName),
				"CwYyC5NhULTuzEb1DGeAGtBnzg9rdmKa7HGVXZsS1bCn",
			)
			assert.NoError(t, err)
			assert.Equal(t, "BFC9e5f7db2f8384b1c88f6b0b8cd2bd79235591ab474fcdcc8856a51653f32a4326f2c", record.ToAddress)
			assert.Equal(t, "BFC4812ec9d87678e94747d6ea092d4f76ec92b8bfc01aac509394204e4d4150af92999", record.FromAddress)
			assert.Equal(t, "500000000000", record.Amount.String())
			assert.Equal(t, "142027", record.FeeAmount.String())
			assert.Equal(t, biz.NATIVE, record.TransactionType)
			assert.Equal(t, biz.SUCCESS, record.Status)
		},
		AssertErrs: func(errs []error) {
			ei := make([]interface{}, 0, len(errs))
			for _, e := range errs {
				ei = append(ei, e)
			}
			assert.Equal(t, 0, len(errs), ei...)
		},
	})
}

func TestFaucet(t *testing.T) {
	ptesting.RunTest(ptesting.Preparation{
		Configs:   configPath,
		ChainName: chainName,
		IndexBlockNumbers: []uint64{
			79593,
		},
		Users: map[string]string{
			"BFC4812ec9d87678e94747d6ea092d4f76ec92b8bfc01aac509394204e4d4150af92999": "1",
		},
		Assert: func() {
			record, err := data.SuiTransactionRecordRepoClient.FindByTxHash(
				context.Background(),
				biz.GetTableName(chainName),
				"J8mGwLDjskz3Pzu9M7SKi14UiuD4JjQuQAB2hmSoXLkv",
			)
			assert.NoError(t, err)
			assert.Equal(t, "BFCd33efeeeabe879697e7a417dff281f9a89cf176a707ea7b354e04480e0c1895d727a", record.FromAddress)
			assert.Equal(t, "BFC4812ec9d87678e94747d6ea092d4f76ec92b8bfc01aac509394204e4d4150af92999", record.ToAddress)
			assert.Equal(t, "1000000000000", record.Amount.String())
			assert.Equal(t, "271440", record.FeeAmount.String())
			assert.Equal(t, biz.NATIVE, record.TransactionType)
			assert.Equal(t, biz.SUCCESS, record.Status)
		},
		AssertErrs: func(errs []error) {
			ei := make([]interface{}, 0, len(errs))
			for _, e := range errs {
				ei = append(ei, e)
			}
			assert.Equal(t, 0, len(errs), ei...)
		},
	})
}

func TestSelfTransfer(t *testing.T) {
	ptesting.RunTest(ptesting.Preparation{
		Configs:   configPath,
		ChainName: chainName,
		IndexBlockNumbers: []uint64{
			128168,
		},
		Users: map[string]string{
			"BFC4812ec9d87678e94747d6ea092d4f76ec92b8bfc01aac509394204e4d4150af92999": "1",
		},
		Assert: func() {
			record, err := data.SuiTransactionRecordRepoClient.FindByTxHash(
				context.Background(),
				biz.GetTableName(chainName),
				"2gvwFGphN1HUau3vyDNJk5XLDw6AgHWvwv3PnrkX4vdb",
			)
			assert.NoError(t, err)
			assert.NotNil(t, record, "not found such tx")
			assert.Equal(t, "BFC4812ec9d87678e94747d6ea092d4f76ec92b8bfc01aac509394204e4d4150af92999", record.FromAddress)
			assert.Equal(t, "BFC4812ec9d87678e94747d6ea092d4f76ec92b8bfc01aac509394204e4d4150af92999", record.ToAddress)
			assert.Equal(t, "1000000000", record.Amount.String())
			assert.Equal(t, "142027", record.FeeAmount.String())
			assert.Equal(t, biz.NATIVE, record.TransactionType)
			assert.Equal(t, biz.SUCCESS, record.Status)
		},
		AssertErrs: func(errs []error) {
			ei := make([]interface{}, 0, len(errs))
			for _, e := range errs {
				ei = append(ei, e)
			}
			assert.Equal(t, 0, len(errs), ei...)
		},
	})
}
