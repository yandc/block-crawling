package nervos

import (
	v1 "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/ptesting"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const CHAIN_NAME = "Nervos"
const CONFIG_PATH = "../../../configs"

// Unique queue to avoid conflicts as tests in different packages will run in parallel.
const TOKEN_INFO_QUEUE_TOPIC = biz.TOKEN_INFO_QUEUE_TOPIC + ":nervos"

const TIMEOUT = 5 * time.Second

func TestPendingTx(t *testing.T) {
	txHash := "0x1ac2060e11796f9d27c218747d6a4f7bde30589172af2a1266f1936dcbe0f0fe"
	tableName := biz.GetTableName(CHAIN_NAME)

	ptesting.RunTest(ptesting.Preparation{
		ChainName:         CHAIN_NAME,
		Configs:           CONFIG_PATH,
		IndexBlockNumbers: []uint64{},
		Users: map[string]string{
			"ckb1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsq2qfvrssmvrdjql4tjaefega3pdegfftzgamcexw": "1",
		},
		PendingTxs: []*v1.TransactionReq{
			{
				Uid:             "1",
				ChainName:       CHAIN_NAME,
				TransactionHash: txHash,
				Status:          biz.PENDING,
				FromAddress:     "ckb1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsq2qfvrssmvrdjql4tjaefega3pdegfftzgamcexw",
				ToAddress:       "0x403750780e687add224adbc9a42bf806aa96deaeafcfc97f2357053a4b8bf31f",
				FeeAmount:       "5922173296656",
				Amount:          "105807",
				TransactionType: biz.NATIVE,
			},
		},
		BeforePrepare: func() {
		},
		AfterPrepare: func() {
			record, err := data.CkbTransactionRecordRepoClient.FindByTxhash(context.TODO(), tableName, txHash)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, biz.PENDING, record.Status)
		},
		Assert: func() {
			record, err := data.CkbTransactionRecordRepoClient.FindByTxhash(context.TODO(), tableName, txHash)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, biz.SUCCESS, record.Status)
		},
	})
}
