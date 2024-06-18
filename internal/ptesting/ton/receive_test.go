package ton

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/ptesting"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	chainName  = "Ton"
	configPath = "../../..//configs"

	// Unique queue to avoid conflicts as tests in different packages will run in parallel.
	TOKEN_INFO_QUEUE_TOPIC = biz.TOKEN_INFO_QUEUE_TOPIC + ":ton"
)

func TestReceiveTon(t *testing.T) {
	ptesting.RunTest(ptesting.Preparation{
		Configs:   configPath,
		ChainName: chainName,
		IndexBlockNumbers: []uint64{
			38_297_645,
		},
		Users: map[string]string{
			"EQANI-Z3j12eqNKpulhOvRsZGmVFjZxMKo3ziyFHf49bR7BE": "1",
		},
		Assert: func() {
			record, err := data.SuiTransactionRecordRepoClient.FindByTxHash(
				context.Background(),
				biz.GetTableName(chainName),
				"I/tI7SEFAticcS5aEyUP9QRKNulZM+ML2e4E8WsxuCo=",
			)
			assert.NoError(t, err)
			assert.Equal(t, "190000000", record.Amount.String())
			assert.Equal(t, "1937348", record.FeeAmount.String())
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
