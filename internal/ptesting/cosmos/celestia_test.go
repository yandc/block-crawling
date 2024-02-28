package cosmos

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/ptesting"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWhyMissed(t *testing.T) {
	const (
		chainName  = "Celestia"
		configPath = "../../../configs"

		// Unique queue to avoid conflicts as tests in different packages will run in parallel.
		TOKEN_INFO_QUEUE_TOPIC = biz.TOKEN_INFO_QUEUE_TOPIC + ":benfen"
	)

	ptesting.RunTest(ptesting.Preparation{
		Configs:   configPath,
		ChainName: chainName,
		IndexBlockNumbers: []uint64{
			884_152,
		},
		Users: map[string]string{
			"celestia1d0zaj83ac7lw9l4g6w740rpct5zawjmfzr8upq": "1",
		},
		Assert: func() {
			record, err := data.AtomTransactionRecordRepoClient.FindByTxHash(
				context.Background(),
				biz.GetTableName(chainName),
				"6A074876F488895CFB95BB8A3B99AB3F6D7E6FE587A5C9BDD3D102F1A916CA67",
			)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			// assert.Equal(t, biz.TRANSFER, record.TransactionType)
			// assert.Equal(t, biz.SUCCESS, record.Status)
		},
		AssertErrs: func(errs []error) {
			ei := make([]interface{}, 0, len(errs))
			for _, e := range errs {
				println("Got Error", e.Error())
				ei = append(ei, e.Error())
			}
			assert.Equal(t, 0, len(errs), ei...)
		},
	})
}
