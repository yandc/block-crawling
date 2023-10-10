package sui

import (
	v1 "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/platform"
	"block-crawling/internal/platform/sui"
	"block-crawling/internal/ptesting"
	"block-crawling/internal/types"
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

const (
	chainName  = "SUITEST"
	configPath = "./configs"

	// Unique queue to avoid conflicts as tests in different packages will run in parallel.
	TOKEN_INFO_QUEUE_TOPIC = biz.TOKEN_INFO_QUEUE_TOPIC + ":sui"
)

// native
func TestIndeBlock1655093(t *testing.T) {
	ptesting.RunTest(ptesting.Preparation{
		Configs:   configPath,
		ChainName: chainName,
		IndexBlockNumbers: []uint64{
			1655093,
		},
		Users: map[string]string{
			"0xa1a00d9468e3276a3964a973f0054f1744667251f0b38e9ff22b6a5c77b765c9": "1",
		},
		Assert: func() {
			record, err := data.SuiTransactionRecordRepoClient.FindByTxHash(
				context.Background(),
				biz.GetTableName(chainName),
				"5KKEACtpLHvNeQGxRWv6cGDCjdjBhwfb7Nj3u2LivdpN",
			)
			assert.NoError(t, err)
			assert.Equal(t, "0xa1a00d9468e3276a3964a973f0054f1744667251f0b38e9ff22b6a5c77b765c9", record.ToAddress)
			assert.Equal(t, "0x9835489cd4a917a50ef7eac4372211f7f4b1cb60438f427f870fd656061ee816", record.FromAddress)
			assert.Equal(t, "998990120", record.Amount.String())
			assert.Equal(t, "1009880", record.FeeAmount.String())
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

// contract
func TestIndeBlock1733650(t *testing.T) {
	ptesting.RunTest(ptesting.Preparation{
		Configs:   configPath,
		ChainName: chainName,
		IndexBlockNumbers: []uint64{
			1733650,
		},
		Users: map[string]string{
			"0x8041dcb0f0d4d528c0eef2693e02b6df39d50c7c4dd171c208b2394ceaff42b7": "1",
		},
		Assert: func() {
			record, err := data.SuiTransactionRecordRepoClient.FindByTxHash(
				context.Background(),
				biz.GetTableName(chainName),
				"3ZCqELu7YoVSCXBxWL3ijLX371adSzPmJjXfwuE7GVep",
			)
			assert.NoError(t, err)
			assert.Equal(t, "", record.ToAddress)
			assert.Equal(t, "0x8041dcb0f0d4d528c0eef2693e02b6df39d50c7c4dd171c208b2394ceaff42b7", record.FromAddress)
			assert.Equal(t, "0", record.Amount.String())
			assert.Equal(t, "9048252", record.FeeAmount.String())
			assert.Equal(t, biz.CONTRACT, record.TransactionType)
			assert.Equal(t, biz.FAIL, record.Status)
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

// transfer
func TestIndeBlock1878153(t *testing.T) {
	ptesting.RunTest(ptesting.Preparation{
		Configs:   configPath,
		ChainName: chainName,
		IndexBlockNumbers: []uint64{
			1878153,
		},
		Users: map[string]string{
			"0xa1a00d9468e3276a3964a973f0054f1744667251f0b38e9ff22b6a5c77b765c9": "1",
		},
		Tokens: []types.TokenInfo{
			{
				Address:  "0x39ad74e0e1cc61c60c0ca0ebc3f6333b97ad2895bc9ebaa357faee721f7ea003",
				Amount:   "1",
				Decimals: 0,
				Symbol:   "TestUSDT",
			},
		},
		Assert: func() {
			record, err := data.SuiTransactionRecordRepoClient.FindByTxHash(
				context.Background(),
				biz.GetTableName(chainName),
				"A2Tfv7ujDBMYBsuDr2Df7JTod4n4KLzprVxuobY9xv3V",
			)
			assert.NoError(t, err)
			assert.Equal(t, "0xa1a00d9468e3276a3964a973f0054f1744667251f0b38e9ff22b6a5c77b765c9", record.ToAddress)
			assert.Equal(t, "0x9835489cd4a917a50ef7eac4372211f7f4b1cb60438f427f870fd656061ee816", record.FromAddress)
			assert.Equal(t, "0xe158e6df182971bb6c85eb9de9fbfb460b68163d19afc45873c8672b5cc521b2::TOKEN::TestUSDT", record.ContractAddress)
			assert.Equal(t, "52632005", record.Amount.String())
			assert.Equal(t, "2383884", record.FeeAmount.String())
			assert.Equal(t, biz.TRANSFER, record.TransactionType)
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

func TestPendingTx(t *testing.T) {
	ptesting.RunTest(ptesting.Preparation{
		Configs:   configPath,
		ChainName: chainName,
		PendingTxs: []*v1.TransactionReq{
			{
				ChainName:       chainName,
				TransactionHash: "D1oPWQvUqEjZHkBLLXSff35sq8U9eKZzJtLLf94scZ6Y",
				Status:          biz.PENDING,
				FromAddress:     "0x9835489cd4a917a50ef7eac4372211f7f4b1cb60438f427f870fd656061ee816",
				ToAddress:       "0xa1a00d9468e3276a3964a973f0054f1744667251f0b38e9ff22b6a5c77b765c9",
				Amount:          "0.99899012",
			},
		},
		Users: map[string]string{
			"0xa1a00d9468e3276a3964a973f0054f1744667251f0b38e9ff22b6a5c77b765c9": "1",
		},
		AfterPrepare: func() {
			record, err := data.SuiTransactionRecordRepoClient.FindByTxHash(
				context.Background(),
				biz.GetTableName(chainName),
				"D1oPWQvUqEjZHkBLLXSff35sq8U9eKZzJtLLf94scZ6Y",
			)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, biz.PENDING, record.Status)
		},
		Assert: func() {
			record, err := data.SuiTransactionRecordRepoClient.FindByTxHash(
				context.Background(),
				biz.GetTableName(chainName),
				"D1oPWQvUqEjZHkBLLXSff35sq8U9eKZzJtLLf94scZ6Y",
			)
			assert.NoError(t, err)

			assert.Equal(t, "0xa1a00d9468e3276a3964a973f0054f1744667251f0b38e9ff22b6a5c77b765c9", record.ToAddress)
			assert.Equal(t, "0x9835489cd4a917a50ef7eac4372211f7f4b1cb60438f427f870fd656061ee816", record.FromAddress)
			assert.Equal(t, "998990120", record.Amount.String())
			assert.Equal(t, "1009880", record.FeeAmount.String())
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

var txRecords = []*data.SuiTransactionRecord{
	{
		BlockNumber:     1879344,
		TransactionHash: "EZ4QLGr724qxhVYPZxFiizBheHoBmP4LxsUXt6LLoJ3o",
		FromAddress:     "0x9835489cd4a917a50ef7eac4372211f7f4b1cb60438f427f870fd656061ee816",
		ToAddress:       "0xa1a00d9468e3276a3964a973f0054f1744667251f0b38e9ff22b6a5c77b765c9",
		FromUid:         "1",
		ToUid:           "1",
		FeeAmount:       decimal.NewFromInt(1037088),
		Amount:          decimal.NewFromInt(100),
		Status:          biz.SUCCESS,
		TxTime:          0,
		ContractAddress: "0xe158e6df182971bb6c85eb9de9fbfb460b68163d19afc45873c8672b5cc521b2::TOKEN::TestUSDT",
		ParseData:       "{\"token\":{\"address\":\"0xe158e6df182971bb6c85eb9de9fbfb460b68163d19afc45873c8672b5cc521b2::TOKEN::TestUSDT\",\"amount\":\"52632005\",\"decimals\":0,\"symbol\":\"TestUSDT\"}}",
	},
}

func TestTokenPush(t *testing.T) {
	req := &data.AssetRequest{
		ChainName: chainName,
		Address:   "0xa1a00d9468e3276a3964a973f0054f1744667251f0b38e9ff22b6a5c77b765c9",
		TokenAddressList: []string{
			"0xe158e6df182971bb6c85eb9de9fbfb460b68163d19afc45873c8672b5cc521b2::TOKEN::TestUSDT",
		},
	}

	ptesting.RunTest(ptesting.Preparation{
		ChainName: chainName,
		Configs:   configPath,
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
			_, err = data.RedisQueueManager.QueueDel(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION)
			assert.NoError(t, err)
		},
		Tokens: []types.TokenInfo{
			{
				Address:  "0xe158e6df182971bb6c85eb9de9fbfb460b68163d19afc45873c8672b5cc521b2::TOKEN::TestUSDT",
				Amount:   "1",
				Decimals: 5,
				Symbol:   "TestUSDT",
			},
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			reset := ptesting.StubTokenPushQueue(TOKEN_INFO_QUEUE_TOPIC)
			defer reset()

			sui.HandleTokenPush(chainName, *client.(*sui.Client), txRecords)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, time.Second*5)
			assert.NoError(t, err)
			assert.Equal(t, result, "{\"chainName\":\"SUITEST\",\"uid\":\"1\",\"address\":\"0xa1a00d9468e3276a3964a973f0054f1744667251f0b38e9ff22b6a5c77b765c9\",\"tokenAddress\":\"0xe158e6df182971bb6c85eb9de9fbfb460b68163d19afc45873c8672b5cc521b2::TOKEN::TestUSDT\",\"decimals\":0,\"symbol\":\"TestUSDT\"}")
		},
	})
}

func TestUserAsset(t *testing.T) {
	req := &data.AssetRequest{
		ChainName: chainName,
		Address:   "0xa1a00d9468e3276a3964a973f0054f1744667251f0b38e9ff22b6a5c77b765c9",
		TokenAddressList: []string{
			"0xe158e6df182971bb6c85eb9de9fbfb460b68163d19afc45873c8672b5cc521b2::TOKEN::TestUSDT",
		},
	}

	ptesting.RunTest(ptesting.Preparation{
		ChainName: chainName,
		Configs:   configPath,
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			c := client.(*sui.Client)
			outputs := []gomonkey.OutputCell{
				{
					Values: gomonkey.Params{"5", nil},
				},
				{
					Values: gomonkey.Params{"5", nil},
				},
				{
					Values: gomonkey.Params{"5", nil},
				},
			}
			patches := gomonkey.ApplyMethodSeq(reflect.TypeOf(c), "GetTokenBalance", outputs)
			defer patches.Reset()

			reset := ptesting.StubTokenPushQueue(TOKEN_INFO_QUEUE_TOPIC)
			defer reset()
			sui.HandleUserAsset(chainName, *client.(*sui.Client), txRecords)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 1)
		},
	})
}

func TestUserStatistic(t *testing.T) {
	req := &data.StatisticRequest{
		ChainName: chainName,
		TokenAddressList: []string{
			"0xe158e6df182971bb6c85eb9de9fbfb460b68163d19afc45873c8672b5cc521b2::TOKEN::TestUSDT",
		},
	}

	ptesting.RunTest(ptesting.Preparation{
		ChainName: chainName,
		Configs:   configPath,
		BeforePrepare: func() {
			_, err := data.TransactionStatisticRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			sui.HandleUserStatistic(chainName, *client.(*sui.Client), txRecords)
			list, err := data.TransactionStatisticRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 1)
		},
	})
}
