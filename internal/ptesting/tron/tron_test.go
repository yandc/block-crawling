package tron

import (
	v1 "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/platform"
	"block-crawling/internal/platform/tron"
	"block-crawling/internal/ptesting"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/go-redis/redis"
	"github.com/shopspring/decimal"

	"github.com/stretchr/testify/assert"
)

const CHAIN_NAME = "TRX"
const CONFIG_PATH = "../../../configs"

// Unique queue to avoid conflicts as tests in different packages will run in parallel.
const TOKEN_INFO_QUEUE_TOPIC = biz.TOKEN_INFO_QUEUE_TOPIC + ":tron"

const TIMEOUT = 5 * time.Second

var tokens []types.TokenInfo = []types.TokenInfo{
	{
		Address:  "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
		Amount:   "100000",
		Decimals: 6,
		Symbol:   "TETHER USD",
	},
	{
		Address:  "TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA",
		Amount:   "1000",
		Decimals: 18,
		Symbol:   "Fake",
	},
}

func TestIndexBlock(t *testing.T) {
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		Prefetch:  false,
		Users: map[string]string{
			"TWaX2e4MFVFUKo3LKwVbiBvrkSGheAWSep": "1",
			"TZDox8ATvSQsEnht4yrP4h2XsrVMCQk68t": "1",
		},
		Tokens: tokens,
		IndexBlockNumbers: []uint64{
			41826081,
		},
		Assert: func() {
			record, err := data.TrxTransactionRecordRepoClient.FindByTxhash(context.Background(), biz.GetTableName(CHAIN_NAME), "69d3be75038b8a5174e7294413ae4d8d9132941b06a46f7294a8119a9c59eb18")
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, record.BlockNumber, 41826081)
			assert.Equal(t, record.TransactionHash, "69d3be75038b8a5174e7294413ae4d8d9132941b06a46f7294a8119a9c59eb18")
			assert.Equal(t, record.FromAddress, "TWaX2e4MFVFUKo3LKwVbiBvrkSGheAWSep")
			assert.Equal(t, record.ToAddress, "TZDox8ATvSQsEnht4yrP4h2XsrVMCQk68t")
			assert.Equal(t, record.FromUid, "1")
			assert.Equal(t, record.ToUid, "1")
			assert.Equal(t, record.FeeAmount, decimal.NewFromInt(8296680))
			assert.Equal(t, record.Amount, decimal.NewFromInt(0))
			assert.Equal(t, record.Status, biz.SUCCESS)
			assert.Equal(t, record.ContractAddress, "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", record)
			assert.Equal(t, record.TransactionType, biz.TRANSFER)
			// 异步任务重补全，不在这里进行验证。
			// assert.Equal(t, record.ParseData, "{\"token\":{\"address\":\"TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t\",\"amount\":\"100000\",\"decimals\":6,\"symbol\":\"TETHER USD\"},\"tvm\":{}}")
		},
	})
}

func TestPendingTx(t *testing.T) {
	txHash := "69d3be75038b8a5174e7294413ae4d8d9132941b06a46f7294a8119a9c59eb18"
	tableName := biz.GetTableName(CHAIN_NAME)
	req := &data.TransactionRequest{
		TransactionHash: txHash,
		Nonce:           -1,
	}

	ptesting.RunTest(ptesting.Preparation{
		ChainName:         CHAIN_NAME,
		Configs:           CONFIG_PATH,
		IndexBlockNumbers: []uint64{},
		Users: map[string]string{
			"TWaX2e4MFVFUKo3LKwVbiBvrkSGheAWSep": "1",
		},
		PendingTxs: []*v1.TransactionReq{
			{
				Uid:             "1",
				ChainName:       CHAIN_NAME,
				TransactionHash: txHash,
				Status:          biz.PENDING,
				FromAddress:     "TWaX2e4MFVFUKo3LKwVbiBvrkSGheAWSep",
				ToAddress:       "TZDox8ATvSQsEnht4yrP4h2XsrVMCQk68t",
				FeeAmount:       "364000",
				Amount:          "0",
				TransactionType: biz.TRANSFER,
			},
		},
		BeforePrepare: func() {
			_, err := data.TrxTransactionRecordRepoClient.Delete(context.TODO(), tableName, req)
			assert.NoError(t, err)
		},
		AfterPrepare: func() {
			record, err := data.TrxTransactionRecordRepoClient.FindByTxhash(context.TODO(), tableName, txHash)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, biz.PENDING, record.Status)
		},
		Assert: func() {
			record, err := data.TrxTransactionRecordRepoClient.FindByTxhash(context.TODO(), tableName, txHash)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, biz.SUCCESS, record.Status)
		},
	})
}

var amount, _ = decimal.NewFromString("0")
var txRecords = []*data.TrxTransactionRecord{{
	BlockNumber:     41826081,
	TransactionHash: "c4963f6e8f55540f0530f8a739480cd602f52043c2e3d982428e76f1bc0429c0",
	FromAddress:     "TSPjKkzNCn61wp3ntg7G1YpmaAmnYP3oJ1",
	ToAddress:       "TDEWa49dAgsdR4g7Vt4qzo6sJ2fs283bR9",
	FromUid:         "1",
	ToUid:           "1",
	FeeAmount:       decimal.NewFromInt(364000),
	Amount:          amount,
	Status:          biz.SUCCESS,
	TxTime:          1656038897,
	ContractAddress: "TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA",
	TransactionType: biz.TRANSFER,
	ParseData:       "{\"token\":{\"address\":\"TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA\",\"amount\":\"8888000000000000000000\",\"decimals\":18,\"symbol\":\"L71.COM\"},\"tvm\":{}}",
	ClientData:      "{\"sendTime\":1677039146259,\"txInput\":\"\"}",
}}

var txNftRecords = []*data.TrxTransactionRecord{{
	BlockNumber:     16073725,
	TransactionHash: "0xcf9db3314b103eb91e49346b6b65dd5594c2cfab8a113fa575d80ab3219a860a",
	FromAddress:     "0x580Bad3785E99E679e8F128Aad8F47DD85b26A0D",
	ToAddress:       "0x11C2254143310e834640F0FDafd6e44516340E40",
	FromUid:         "1",
	ToUid:           "1",
	FeeAmount:       decimal.NewFromInt(551454279931832),
	Amount:          decimal.NewFromInt(1),
	Status:          biz.SUCCESS,
	TxTime:          1669701707,
	ContractAddress: "0x495f947276749Ce646f68AC8c248420045cb7b5e",
	TransactionType: biz.TRANSFERNFT,
	ParseData:       "{\"stc\":{\"sequence_number\":\"10\",\"type\":\"2\"},\"token\":{\"address\":\"0x495f947276749Ce646f68AC8c248420045cb7b5e\",\"amount\":\"1\",\"decimals\":0,\"symbol\":\"\",\"collection_name\":\"Red Panda Pals NFT Collection\",\"token_type\":\"ERC1155\",\"token_id\":\"113131626276964395896987417252704130741588147670481760275076171533867519311873\",\"item_name\":\"Red Panda Pal #255\",\"item_uri\":\"https://i.seadn.io/gae/z1cmnY8oAYmUtnbpdC_77YIzdbj4J8t_Sut4WtPrHCrS74D7JVYYNLRN0C4UgIwGM4V-lSiwyaWM2waOv36GDFhxQkxtJ5GnP5OtaZU?w=500&auto=format\"}}",
}}

var txPendingRecords = []*data.TrxTransactionRecord{{
	TransactionHash: "c4963f6e8f55540f0530f8a739480cd602f52043c2e3d982428e76f1bc0429c3",
	FromAddress:     "TSPjKkzNCn61wp3ntg7G1YpmaAmnYP3oJ1",
	ToAddress:       "TDEWa49dAgsdR4g7Vt4qzo6sJ2fs283bR9",
	FromUid:         "1",
	ToUid:           "1",
	FeeAmount:       decimal.NewFromInt(364000),
	Amount:          amount,
	Status:          biz.PENDING,
	ContractAddress: "TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA",
	TransactionType: biz.TRANSFER,
	ClientData:      "{\"sendTime\":1677039146259,\"txInput\":\"\"}",
}, {
	TransactionHash: "c4963f6e8f55540f0530f8a739480cd602f52043c2e3d982428e76f1bc0429c4",
	FromAddress:     "TSPjKkzNCn61wp3ntg7G1YpmaAmnYP3oJ1",
	ToAddress:       "TDEWa49dAgsdR4g7Vt4qzo6sJ2fs283bR9",
	FromUid:         "1",
	ToUid:           "1",
	FeeAmount:       decimal.NewFromInt(364000),
	Amount:          decimal.NewFromInt(0),
	Status:          biz.PENDING,
	ContractAddress: "TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA",
	TransactionType: biz.TRANSFER,
	ClientData:      "{\"sendTime\":1677039146259,\"txInput\":\"\"}",
}}

func TestHandleTokenPush(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "TDEWa49dAgsdR4g7Vt4qzo6sJ2fs283bR9",
		TokenAddressList: []string{"TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA"},
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
			_, err = data.RedisQueueManager.QueueDel(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			reset := ptesting.StubTokenPushQueue(TOKEN_INFO_QUEUE_TOPIC)
			defer reset()
			tron.HandleTokenPush(CHAIN_NAME, *client.(*tron.Client), txRecords)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.NoError(t, err)
			assert.Equal(t, result, "{\"chainName\":\"TRX\",\"uid\":\"1\",\"address\":\"TDEWa49dAgsdR4g7Vt4qzo6sJ2fs283bR9\",\"tokenAddress\":\"TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA\",\"decimals\":18,\"symbol\":\"L71.COM\"}")
			assert.Equal(t, strings.Contains(result, CHAIN_NAME), true)
			assert.Equal(t, strings.Contains(result, "TDEWa49dAgsdR4g7Vt4qzo6sJ2fs283bR9"), true)
			assert.Equal(t, strings.Contains(result, "TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA"), true)
		},
	})
}

func TestHandleTokenPush1(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "TDEWa49dAgsdR4g7Vt4qzo6sJ2fs283bR9",
		TokenAddressList: []string{"TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA"},
	}
	var txRecordList []*data.TrxTransactionRecord
	utils.CopyProperties(txRecords, &txRecordList)
	for _, record := range txRecordList {
		record.TransactionType = biz.CONTRACT
		index := strings.Index(record.ParseData, "\"token\"")
		record.ParseData = record.ParseData[:index] + "\"token\":{\"address\":\"\",\"amount\":\"\",\"decimals\":0,\"symbol\":\"\"}}"
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
			_, err = data.RedisQueueManager.QueueDel(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			reset := ptesting.StubTokenPushQueue(TOKEN_INFO_QUEUE_TOPIC)
			defer reset()
			tron.HandleTokenPush(CHAIN_NAME, *client.(*tron.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleTokenPush2(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "TDEWa49dAgsdR4g7Vt4qzo6sJ2fs283bR9",
		TokenAddressList: []string{"TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA"},
	}
	var txRecordList []*data.TrxTransactionRecord
	utils.CopyProperties(txRecords, &txRecordList)
	for _, record := range txRecordList {
		record.Status = biz.FAIL
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
			_, err = data.RedisQueueManager.QueueDel(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			reset := ptesting.StubTokenPushQueue(TOKEN_INFO_QUEUE_TOPIC)
			defer reset()
			tron.HandleTokenPush(CHAIN_NAME, *client.(*tron.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleTokenPush3(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "TDEWa49dAgsdR4g7Vt4qzo6sJ2fs283bR9",
		TokenAddressList: []string{"TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA"},
	}
	var txRecordList []*data.TrxTransactionRecord
	utils.CopyProperties(txRecords, &txRecordList)
	for _, record := range txRecordList {
		record.ToAddress = ""
		record.ToUid = ""
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
			_, err = data.RedisQueueManager.QueueDel(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			reset := ptesting.StubTokenPushQueue(TOKEN_INFO_QUEUE_TOPIC)
			defer reset()
			tron.HandleTokenPush(CHAIN_NAME, *client.(*tron.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleTokenPush4(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "TDEWa49dAgsdR4g7Vt4qzo6sJ2fs283bR9",
		TokenAddressList: []string{"TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA"},
	}
	var txRecordList []*data.TrxTransactionRecord
	utils.CopyProperties(txRecords, &txRecordList)
	for _, record := range txRecordList {
		index := strings.Index(record.ParseData, "\"decimals\"")
		record.ParseData = record.ParseData[:index] + "\"decimals\":0,\"symbol\":\"Unknown Token\"}}"
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
			_, err = data.RedisQueueManager.QueueDel(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			reset := ptesting.StubTokenPushQueue(TOKEN_INFO_QUEUE_TOPIC)
			defer reset()
			tron.HandleTokenPush(CHAIN_NAME, *client.(*tron.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleTokenPush5(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "0x11C2254143310e834640F0FDafd6e44516340E40",
		TokenAddressList: []string{"0x495f947276749Ce646f68AC8c248420045cb7b5e"},
	}
	var txRecordList []*data.TrxTransactionRecord
	utils.CopyProperties(txNftRecords, &txRecordList)
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
			_, err = data.RedisQueueManager.QueueDel(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			reset := ptesting.StubTokenPushQueue(TOKEN_INFO_QUEUE_TOPIC)
			defer reset()
			tron.HandleTokenPush(CHAIN_NAME, *client.(*tron.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleUserAsset(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"TSPjKkzNCn61wp3ntg7G1YpmaAmnYP3oJ1", "TDEWa49dAgsdR4g7Vt4qzo6sJ2fs283bR9"},
		TokenAddressList: []string{"", "TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA"},
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[len(plat.Conf.RpcURL)-1])
			c := client.(*tron.Client)

			outputs := []gomonkey.OutputCell{
				{
					Values: gomonkey.Params{"5", nil},
				},
				{
					Values: gomonkey.Params{"10", nil},
				},
			}
			patches := gomonkey.ApplyMethodSeq(reflect.TypeOf(c), "GetTokenBalance", outputs)
			patches.ApplyMethod(reflect.TypeOf(c), "GetBalance", func(_ *tron.Client, _ string) (string, error) {
				return "1", nil
			})
			// 执行完毕后释放桩序列
			defer patches.Reset()

			tron.HandleUserAsset(CHAIN_NAME, *c, txRecords)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 3)
			for _, asset := range list {
				if asset.Address == "TSPjKkzNCn61wp3ntg7G1YpmaAmnYP3oJ1" {
					if asset.TokenAddress == "" {
						assert.Equal(t, asset.Balance, "1")
						assert.Equal(t, asset.Symbol, "TRX")
						assert.Equal(t, asset.Decimals, int32(6))
					} else if asset.TokenAddress == "TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA" {
						assert.Equal(t, asset.Balance, "5")
						assert.Equal(t, asset.Symbol, "L71.COM")
						assert.Equal(t, asset.Decimals, int32(18))
					}
				} else if asset.Address == "TDEWa49dAgsdR4g7Vt4qzo6sJ2fs283bR9" {
					assert.Equal(t, asset.TokenAddress, "TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA")
					assert.Equal(t, asset.Balance, "10")
					assert.Equal(t, asset.Symbol, "L71.COM")
					assert.Equal(t, asset.Decimals, int32(18))
				}

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
			}
		},
	})
}

func TestHandleUserAsset1(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"TSPjKkzNCn61wp3ntg7G1YpmaAmnYP3oJ1", "TDEWa49dAgsdR4g7Vt4qzo6sJ2fs283bR9"},
		TokenAddressList: []string{"", "TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA"},
	}
	var txRecordList []*data.TrxTransactionRecord
	utils.CopyProperties(txRecords, &txRecordList)
	for _, record := range txRecordList {
		record.Status = biz.DROPPED
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[len(plat.Conf.RpcURL)-1])
			c := client.(*tron.Client)

			tron.HandleUserAsset(CHAIN_NAME, *c, txRecordList)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 0)
		},
	})
}

func TestHandleUserAsset2(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"TSPjKkzNCn61wp3ntg7G1YpmaAmnYP3oJ1", "TDEWa49dAgsdR4g7Vt4qzo6sJ2fs283bR9"},
		TokenAddressList: []string{"", "TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA"},
	}
	var txRecordList []*data.TrxTransactionRecord
	utils.CopyProperties(txRecords, &txRecordList)
	for _, record := range txRecordList {
		record.TransactionType = biz.CONTRACT
		index := strings.Index(record.ParseData, "\"token\"")
		record.ParseData = record.ParseData[:index] + "\"token\":{\"address\":\"\",\"amount\":\"\",\"decimals\":0,\"symbol\":\"\"}}"
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[len(plat.Conf.RpcURL)-1])
			c := client.(*tron.Client)

			patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetBalance", func(_ *tron.Client, _ string) (string, error) {
				return "1", nil
			})
			// 执行完毕后释放桩序列
			defer patches.Reset()

			tron.HandleUserAsset(CHAIN_NAME, *c, txRecordList)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 1)
			for _, asset := range list {
				assert.Equal(t, asset.Address, "TSPjKkzNCn61wp3ntg7G1YpmaAmnYP3oJ1")
				assert.Equal(t, asset.TokenAddress, "")
				assert.Equal(t, asset.Balance, "1")
				assert.Equal(t, asset.Symbol, "TRX")

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
				assert.Equal(t, asset.Decimals, int32(6))
			}
		},
	})
}

/*func TestHandleUserAsset3(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"0x580Bad3785E99E679e8F128Aad8F47DD85b26A0D", "0x11C2254143310e834640F0FDafd6e44516340E40"},
		TokenAddressList: []string{"", "0x495f947276749Ce646f68AC8c248420045cb7b5e"},
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[len(plat.Conf.RpcURL)-1])
			c := client.(*tron.Client)

			patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetBalance", func(_ *tron.Client, _ string) (string, error) {
				return "1", nil
			})
			// 执行完毕后释放桩序列
			defer patches.Reset()

			tron.HandleUserAsset(CHAIN_NAME, *c, txNftRecords)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 1)
			for _, asset := range list {
				assert.Equal(t, asset.Address, "0x580Bad3785E99E679e8F128Aad8F47DD85b26A0D")
				assert.Equal(t, asset.TokenAddress, "")
				assert.Equal(t, asset.Balance, "1")
				assert.Equal(t, asset.Symbol, "TRX")

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
				assert.Equal(t, asset.Decimals, int32(6))
			}
		},
	})
}*/

func TestHandleUserStatistic(t *testing.T) {
	req := &data.StatisticRequest{
		ChainName:         CHAIN_NAME,
		TokenAddressList:  []string{"TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA"},
		FundDirectionList: []int16{3},
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		Tokens:    tokens,
		TokenPrices: []ptesting.TokenPrice{
			{
				TokenAddress: "TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA",
				Currency:     "CNY",
				Price:        "1",
			},
		},
		BeforePrepare: func() {
			_, err := data.TransactionStatisticRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			tron.HandleUserStatistic(CHAIN_NAME, *client.(*tron.Client), txRecords)
			list, err := data.TransactionStatisticRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Greater(t, len(list), 0)
			for _, asset := range list {
				assert.Equal(t, asset.TokenAddress, "TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA")
				assert.Equal(t, asset.FundDirection, int16(3))

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
			}
		},
	})
}

func TestHandleUserStatistic1(t *testing.T) {
	req := &data.StatisticRequest{
		ChainName:         CHAIN_NAME,
		TokenAddressList:  []string{"TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA"},
		FundDirectionList: []int16{3},
	}
	var txRecordList []*data.TrxTransactionRecord
	utils.CopyProperties(txRecords, &txRecordList)
	for _, record := range txRecordList {
		record.TransactionType = biz.CONTRACT
		index := strings.Index(record.ParseData, "\"token\"")
		record.ParseData = record.ParseData[:index] + "\"token\":{\"address\":\"\",\"amount\":\"\",\"decimals\":0,\"symbol\":\"\"}}"
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.TransactionStatisticRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			tron.HandleUserStatistic(CHAIN_NAME, *client.(*tron.Client), txRecordList)
			list, err := data.TransactionStatisticRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 0)
		},
	})
}

func TestHandleUserStatistic2(t *testing.T) {
	req := &data.StatisticRequest{
		ChainName:         CHAIN_NAME,
		TokenAddressList:  []string{"TYNebDWrrqw4Tz6kwN2GkRqt9H2f195LxA"},
		FundDirectionList: []int16{3},
	}
	var txRecordList []*data.TrxTransactionRecord
	utils.CopyProperties(txRecords, &txRecordList)
	for _, record := range txRecordList {
		record.Status = biz.FAIL
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.TransactionStatisticRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			tron.HandleUserStatistic(CHAIN_NAME, *client.(*tron.Client), txRecordList)
			list, err := data.TransactionStatisticRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 0)
		},
	})
}
