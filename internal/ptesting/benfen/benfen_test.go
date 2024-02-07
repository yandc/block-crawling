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
			798_428,
		},
		Users: map[string]string{
			"BFC20a11b40318a657df78e2ef32ee8bbe88529e545207c259d2cda659d182948f9e20a": "1",
		},
		Assert: func() {
			record, err := data.SuiTransactionRecordRepoClient.FindByTxHash(
				context.Background(),
				biz.GetTableName(chainName),
				"8e87tV5suA5F4BcwEFMja12owey3BP5XhhpKqkqZqdBT",
			)
			assert.NoError(t, err)
			assert.Equal(t, "10000000", record.Amount.String())
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
