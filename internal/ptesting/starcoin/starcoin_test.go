package starcoin

import (
	v1 "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/platform"
	"block-crawling/internal/platform/starcoin"
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

const CHAIN_NAME = "STC"
const CONFIG_PATH = "../../../configs"

// Unique queue to avoid conflicts as tests in different packages will run in parallel.
const TOKEN_INFO_QUEUE_TOPIC = biz.TOKEN_INFO_QUEUE_TOPIC + ":starcoin"

const TIMEOUT = 5 * time.Second

func TestIndexBlock(t *testing.T) {
	ptesting.RunTest(ptesting.Preparation{
		ChainName:    CHAIN_NAME,
		Configs:      CONFIG_PATH,
		Prefetch:     true,
		RawBlockType: reflect.TypeOf(new(types.Block)),
		RawTxType:    reflect.TypeOf(*new(types.UserTransaction)),
		Tokens: []types.TokenInfo{
			{
				Address:  "0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR",
				Amount:   "5922173296656",
				Decimals: 9,
				Symbol:   "STAR",
			},
		},
		Users: map[string]string{
			"0x6280a3f90ac6c223ac75bdb139a9fb4d": "1",
			"0x4d9f5104cd2d3ad1ce4e81d0fcb85cbc": "1",
		},
		IndexBlockNumbers: []uint64{
			8362165,
		},
		Assert: func() {
			record, err := data.StcTransactionRecordRepoClient.FindByTxhash(context.Background(), biz.GetTableName(CHAIN_NAME), "0x16d1bf8d60636a805ff582629a236154076df5241f09cbac3b9176af24a84d91")
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, record.BlockNumber, 8362165)
			assert.Equal(t, record.Nonce, int64(185))
			assert.Equal(t, record.TransactionHash, "0x16d1bf8d60636a805ff582629a236154076df5241f09cbac3b9176af24a84d91")
			assert.Equal(t, record.FromAddress, "0x6280a3f90ac6c223ac75bdb139a9fb4d")
			assert.Equal(t, record.ToAddress, "0x4d9f5104cd2d3ad1ce4e81d0fcb85cbc")
			assert.Equal(t, record.FromUid, "1")
			assert.Equal(t, record.ToUid, "1")
			assert.Equal(t, record.FeeAmount, decimal.NewFromInt(105807))
			assert.Equal(t, record.Amount, decimal.NewFromInt(5922173296656))
			assert.Equal(t, record.Status, biz.SUCCESS)
			assert.Equal(t, record.ContractAddress, "0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR")
			assert.Equal(t, record.TransactionType, biz.TRANSFER)

			// Background jobs, shouldn't compare here.
			// assert.Equal(t, record.ParseData, "{\"stc\":{\"sequence_number\":\"185\"},\"token\":{\"address\":\"0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR\",\"amount\":\"5922173296656\",\"decimals\":9,\"symbol\":\"STAR\"}}")
		},
	})
}

func TestPendingTx(t *testing.T) {
	txHash := "0x16d1bf8d60636a805ff582629a236154076df5241f09cbac3b9176af24a84d91"
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
			"0x6280a3f90ac6c223ac75bdb139a9fb4d": "1",
		},
		PendingTxs: []*v1.TransactionReq{
			{
				Uid:             "1",
				ChainName:       CHAIN_NAME,
				TransactionHash: txHash,
				Status:          biz.PENDING,
				FromAddress:     "0x6280a3f90ac6c223ac75bdb139a9fb4d",
				ToAddress:       "0x4d9f5104cd2d3ad1ce4e81d0fcb85cbc",
				FeeAmount:       "5922173296656",
				Amount:          "105807",
				TransactionType: biz.TRANSFER,
			},
		},
		BeforePrepare: func() {
			_, err := data.StcTransactionRecordRepoClient.Delete(context.TODO(), tableName, req)
			assert.NoError(t, err)
		},
		AfterPrepare: func() {
			record, err := data.StcTransactionRecordRepoClient.FindByTxhash(context.TODO(), tableName, txHash)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, biz.PENDING, record.Status)
		},
		Assert: func() {
			record, err := data.StcTransactionRecordRepoClient.FindByTxhash(context.TODO(), tableName, txHash)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, biz.SUCCESS, record.Status)
		},
	})
}

var amount, _ = decimal.NewFromString("219919129047")
var txRecords = []*data.StcTransactionRecord{{
	BlockNumber:     8359410,
	Nonce:           int64(29),
	TransactionHash: "0xf854e233789ca3994bf0cd6ca85701ea082a27c82fbde71bd82057a8a54eb772",
	FromAddress:     "0xb635233869954d3d1d93e86c3f05e6c7",
	ToAddress:       "0xc031d9eb4efc168342038ea328714080",
	FromUid:         "1",
	ToUid:           "1",
	FeeAmount:       decimal.NewFromInt(105807),
	Amount:          amount,
	Status:          biz.SUCCESS,
	TxTime:          1665724977,
	ContractAddress: "0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR",
	TransactionType: biz.TRANSFER,
	ParseData:       "{\"stc\":{\"sequence_number\":\"29\",\"type\":\"0\"},\"token\":{\"address\":\"0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR\",\"amount\":\"219919129047\",\"decimals\":9,\"symbol\":\"STAR\"}}",
	Data:            "{\"ScriptFunction\":{\"module\":\"0x00000000000000000000000000000001::TransferScripts\",\"function\":\"peer_to_peer_v2\",\"ty_args\":[\"0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR\"],\"args\":[\"0xc031d9eb4efc168342038ea328714080\",219919129047]}}",
	ClientData:      "{\"sendTime\":1677039146259,\"txInput\":\"\"}",
}}

var txNftRecords = []*data.StcTransactionRecord{{
	BlockNumber:     16073725,
	Nonce:           int64(10),
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
	Data:            "{\"ScriptFunction\":{\"module\":\"0x00000000000000000000000000000001::TransferScripts\",\"function\":\"peer_to_peer_v2\",\"ty_args\":[\"0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR\"],\"args\":[\"0xc031d9eb4efc168342038ea328714080\",219919129047]}}",
}}

var txPendingRecords = []*data.StcTransactionRecord{{
	Nonce:           int64(29),
	TransactionHash: "0xf854e233789ca3994bf0cd6ca85701ea082a27c82fbde71bd82057a8a54eb773",
	FromAddress:     "0xb635233869954d3d1d93e86c3f05e6c7",
	ToAddress:       "0xc031d9eb4efc168342038ea328714080",
	FromUid:         "1",
	ToUid:           "1",
	FeeAmount:       decimal.NewFromInt(105807),
	Amount:          amount,
	Status:          biz.PENDING,
	ContractAddress: "0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR",
	TransactionType: biz.TRANSFER,
	ClientData:      "{\"sendTime\":1677039146259,\"txInput\":\"\"}",
}, {
	Nonce:           int64(29),
	TransactionHash: "0xf854e233789ca3994bf0cd6ca85701ea082a27c82fbde71bd82057a8a54eb774",
	FromAddress:     "0xb635233869954d3d1d93e86c3f05e6c7",
	ToAddress:       "0xc031d9eb4efc168342038ea328714080",
	FromUid:         "1",
	ToUid:           "1",
	FeeAmount:       decimal.NewFromInt(105807),
	Amount:          decimal.NewFromInt(0),
	Status:          biz.PENDING,
	ContractAddress: "0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR",
	TransactionType: biz.TRANSFER,
	ClientData:      "{\"sendTime\":1677039146259,\"txInput\":\"\"}",
}}

func TestHandleTokenPush(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "0xc031d9eb4efc168342038ea328714080",
		TokenAddressList: []string{"0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR"},
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
			starcoin.HandleTokenPush(CHAIN_NAME, *client.(*starcoin.Client), txRecords)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.NoError(t, err)
			assert.Equal(t, result, "{\"chainName\":\"STC\",\"uid\":\"1\",\"address\":\"0xc031d9eb4efc168342038ea328714080\",\"tokenAddress\":\"0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR\",\"decimals\":9,\"symbol\":\"STAR\"}")
			assert.Equal(t, strings.Contains(result, CHAIN_NAME), true)
			assert.Equal(t, strings.Contains(result, "0xc031d9eb4efc168342038ea328714080"), true)
			assert.Equal(t, strings.Contains(result, "0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR"), true)
		},
	})
}

func TestHandleTokenPush1(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "0xc031d9eb4efc168342038ea328714080",
		TokenAddressList: []string{"0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR"},
	}
	var txRecordList []*data.StcTransactionRecord
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
			starcoin.HandleTokenPush(CHAIN_NAME, *client.(*starcoin.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleTokenPush2(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "0xc031d9eb4efc168342038ea328714080",
		TokenAddressList: []string{"0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR"},
	}
	var txRecordList []*data.StcTransactionRecord
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
			starcoin.HandleTokenPush(CHAIN_NAME, *client.(*starcoin.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleTokenPush3(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "0xc031d9eb4efc168342038ea328714080",
		TokenAddressList: []string{"0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR"},
	}
	var txRecordList []*data.StcTransactionRecord
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
			starcoin.HandleTokenPush(CHAIN_NAME, *client.(*starcoin.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleTokenPush4(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "0xc031d9eb4efc168342038ea328714080",
		TokenAddressList: []string{"0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR"},
	}
	var txRecordList []*data.StcTransactionRecord
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
			starcoin.HandleTokenPush(CHAIN_NAME, *client.(*starcoin.Client), txRecordList)
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
	var txRecordList []*data.StcTransactionRecord
	utils.CopyProperties(txNftRecords, &txRecordList)
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		Tokens: []types.TokenInfo{
			{
				Address:  "0x495f947276749Ce646f68AC8c248420045cb7b5e",
				Amount:   "1",
				Decimals: 2,
				Symbol:   "Fake",
			},
		},
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
			starcoin.HandleTokenPush(CHAIN_NAME, *client.(*starcoin.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleUserAsset(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"0xb635233869954d3d1d93e86c3f05e6c7", "0xc031d9eb4efc168342038ea328714080"},
		TokenAddressList: []string{"", "0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR"},
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		Tokens: []types.TokenInfo{
			{
				Address:  "0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR",
				Amount:   "1",
				Decimals: 1,
				Symbol:   "Fake",
			},
		},
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[len(plat.Conf.RpcURL)-1])
			c := client.(*starcoin.Client)

			outputs := []gomonkey.OutputCell{
				{
					Values: gomonkey.Params{"5", nil},
				},
				{
					Values: gomonkey.Params{"10", nil},
				},
				{
					Values: gomonkey.Params{"1", nil},
				},
			}
			patches := gomonkey.ApplyMethodSeq(reflect.TypeOf(c), "GetTokenBalance", outputs)
			// 执行完毕后释放桩序列
			defer patches.Reset()

			starcoin.HandleUserAsset(CHAIN_NAME, *c, txRecords)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 3)
			for _, asset := range list {
				if asset.Address == "0xb635233869954d3d1d93e86c3f05e6c7" {
					if asset.TokenAddress == "" {
						assert.Equal(t, asset.Balance, "1")
						assert.Equal(t, asset.Symbol, "STC")
					} else if asset.TokenAddress == "0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR" {
						assert.Equal(t, asset.Balance, "5")
						assert.Equal(t, asset.Symbol, "STAR")
					}
				} else if asset.Address == "0xc031d9eb4efc168342038ea328714080" {
					assert.Equal(t, asset.TokenAddress, "0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR")
					assert.Equal(t, asset.Balance, "10")
					assert.Equal(t, asset.Symbol, "STAR")
				}

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
				assert.Equal(t, asset.Decimals, int32(9))
			}
		},
	})
}

func TestHandleUserAsset1(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"0xb635233869954d3d1d93e86c3f05e6c7", "0xc031d9eb4efc168342038ea328714080"},
		TokenAddressList: []string{"", "0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR"},
	}
	var txRecordList []*data.StcTransactionRecord
	utils.CopyProperties(txRecords, &txRecordList)
	for _, record := range txRecordList {
		record.Status = biz.DROPPED
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		Tokens: []types.TokenInfo{
			{
				Address:  "0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR",
				Amount:   "1",
				Decimals: 1,
				Symbol:   "Fake",
			},
		},
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[len(plat.Conf.RpcURL)-1])
			c := client.(*starcoin.Client)

			starcoin.HandleUserAsset(CHAIN_NAME, *c, txRecordList)
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
		AddressList:      []string{"0xb635233869954d3d1d93e86c3f05e6c7", "0xc031d9eb4efc168342038ea328714080"},
		TokenAddressList: []string{"", "0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR"},
	}
	var txRecordList []*data.StcTransactionRecord
	utils.CopyProperties(txRecords, &txRecordList)
	for _, record := range txRecordList {
		record.TransactionType = biz.CONTRACT
		index := strings.Index(record.ParseData, "\"token\"")
		record.ParseData = record.ParseData[:index] + "\"token\":{\"address\":\"\",\"amount\":\"\",\"decimals\":0,\"symbol\":\"\"}}"
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		Tokens: []types.TokenInfo{
			{
				Address:  "0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR",
				Amount:   "1",
				Decimals: 1,
				Symbol:   "Fake",
			},
		},
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[len(plat.Conf.RpcURL)-1])
			c := client.(*starcoin.Client)

			patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetBalance", func(_ *starcoin.Client, _ string) (string, error) {
				return "1", nil
			})
			// 执行完毕后释放桩序列
			defer patches.Reset()

			starcoin.HandleUserAsset(CHAIN_NAME, *c, txRecordList)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 1)
			for _, asset := range list {
				assert.Equal(t, asset.Address, "0xb635233869954d3d1d93e86c3f05e6c7")
				assert.Equal(t, asset.TokenAddress, "")
				// The balance may be changed, so shouldn't compare here.
				// assert.Equal(t, asset.Balance, "1")
				assert.Equal(t, asset.Symbol, "STC")

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
				assert.Equal(t, asset.Decimals, int32(9))
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
			c := client.(*starcoin.Client)

			patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetBalance", func(_ *starcoin.Client, _ string) (string, error) {
				return "1", nil
			})
			// 执行完毕后释放桩序列
			defer patches.Reset()

			starcoin.HandleUserAsset(CHAIN_NAME, *c, txNftRecords)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 1)
			for _, asset := range list {
				assert.Equal(t, asset.Address, "0x580Bad3785E99E679e8F128Aad8F47DD85b26A0D")
				assert.Equal(t, asset.TokenAddress, "")
				assert.Equal(t, asset.Balance, "1")
				assert.Equal(t, asset.Symbol, "STC")

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
				assert.Equal(t, asset.Decimals, int32(9))
			}
		},
	})
}*/

func TestHandleUserStatistic(t *testing.T) {
	req := &data.StatisticRequest{
		ChainName:         CHAIN_NAME,
		TokenAddressList:  []string{"0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR"},
		FundDirectionList: []int16{3},
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		TokenPrices: []ptesting.TokenPrice{
			{
				TokenAddress: "0x8c109349c6bd91411d6bc962e080c4a3",
				Currency:     "CNY",
				Price:        "1",
			},
			{
				TokenAddress: "0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR",
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
			starcoin.HandleUserStatistic(CHAIN_NAME, *client.(*starcoin.Client), txRecords)
			list, err := data.TransactionStatisticRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list) >= 1, true)
			for _, asset := range list {
				assert.Equal(t, asset.TokenAddress, "0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR")
				assert.Equal(t, asset.FundDirection, int16(3))

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
			}
		},
	})
}

func TestHandleUserStatistic1(t *testing.T) {
	req := &data.StatisticRequest{
		ChainName:         CHAIN_NAME,
		TokenAddressList:  []string{"0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR"},
		FundDirectionList: []int16{3},
	}
	var txRecordList []*data.StcTransactionRecord
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
			starcoin.HandleUserStatistic(CHAIN_NAME, *client.(*starcoin.Client), txRecordList)
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
		TokenAddressList:  []string{"0x8c109349c6bd91411d6bc962e080c4a3::STAR::STAR"},
		FundDirectionList: []int16{3},
	}
	var txRecordList []*data.StcTransactionRecord
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
			starcoin.HandleUserStatistic(CHAIN_NAME, *client.(*starcoin.Client), txRecordList)
			list, err := data.TransactionStatisticRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 0)
		},
	})
}

func TestHandleRecordStatus(t *testing.T) {
	req := &data.TransactionRequest{
		FromAddress:        "0xb635233869954d3d1d93e86c3f05e6c7",
		ToAddress:          "0xc031d9eb4efc168342038ea328714080",
		Nonce:              29,
		ClientDataNotEmpty: true,
		OrderBy:            "id asc",
	}
	var txRecordList []*data.StcTransactionRecord
	var txPendingRecordList []*data.StcTransactionRecord
	utils.CopyProperties(txRecords, &txRecordList)
	utils.CopyProperties(txPendingRecords, &txPendingRecordList)
	txPendingRecordList = append(txPendingRecordList, txRecordList...)
	now := time.Now().Unix() - 100
	for _, record := range txPendingRecordList {
		record.CreatedAt = now
		now++
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			req.ClientDataNotEmpty = false
			_, err := data.StcTransactionRecordRepoClient.Delete(context.Background(), biz.GetTableName(CHAIN_NAME), req)
			assert.NoError(t, err)
			total, err := data.StcTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, biz.GetTableName(CHAIN_NAME), txPendingRecordList)
			assert.NoError(t, err)
			assert.Equal(t, total, int64(3))
			req.ClientDataNotEmpty = true
		},
		Test: func(plat *platform.Bootstrap) {
			starcoin.HandleRecordStatus(CHAIN_NAME, txPendingRecordList)
			list, err := data.StcTransactionRecordRepoClient.List(context.Background(), biz.GetTableName(CHAIN_NAME), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 3)
			for _, asset := range list {
				if asset.TransactionHash == txPendingRecordList[0].TransactionHash {
					assert.Equal(t, asset.Status, biz.DROPPED_REPLACED)
					assert.Equal(t, asset.OperateType, "")
				} else if asset.TransactionHash == txPendingRecordList[1].TransactionHash {
					assert.Equal(t, asset.Status, biz.DROPPED_REPLACED)
					assert.Equal(t, asset.OperateType, biz.CANCEL)
				} else if asset.TransactionHash == txRecordList[0].TransactionHash {
					assert.Equal(t, asset.Status, biz.SUCCESS)
					assert.Equal(t, asset.OperateType, biz.SPEED_UP)
				}

				assert.Equal(t, asset.FromAddress, req.FromAddress)
				assert.Equal(t, asset.ToAddress, req.ToAddress)
				assert.Equal(t, asset.Nonce, req.Nonce)
				assert.Equal(t, asset.TransactionType, biz.TRANSFER)
			}
		},
	})
}

func TestHandleRecordStatus1(t *testing.T) {
	req := &data.TransactionRequest{
		FromAddress:        "0xb635233869954d3d1d93e86c3f05e6c7",
		ToAddress:          "0xc031d9eb4efc168342038ea328714080",
		Nonce:              29,
		ClientDataNotEmpty: true,
		OrderBy:            "id asc",
	}
	var txRecordList []*data.StcTransactionRecord
	var txPendingRecordList []*data.StcTransactionRecord
	utils.CopyProperties(txRecords, &txRecordList)
	utils.CopyProperties(txPendingRecords, &txPendingRecordList)
	txRecordList[0].TransactionType = biz.NATIVE
	txPendingRecordList = append(txPendingRecordList, txRecordList...)
	now := time.Now().Unix() - 100
	for _, record := range txPendingRecordList {
		record.CreatedAt = now
		now++
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			req.ClientDataNotEmpty = false
			_, err := data.StcTransactionRecordRepoClient.Delete(context.Background(), biz.GetTableName(CHAIN_NAME), req)
			assert.NoError(t, err)
			total, err := data.StcTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, biz.GetTableName(CHAIN_NAME), txPendingRecordList)
			assert.NoError(t, err)
			assert.Equal(t, total, int64(3))
			req.ClientDataNotEmpty = true
		},
		Test: func(plat *platform.Bootstrap) {
			starcoin.HandleRecordStatus(CHAIN_NAME, txPendingRecordList)
			list, err := data.StcTransactionRecordRepoClient.List(context.Background(), biz.GetTableName(CHAIN_NAME), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 3)
			for _, asset := range list {
				if asset.TransactionHash == txPendingRecordList[0].TransactionHash {
					assert.Equal(t, asset.Status, biz.DROPPED_REPLACED)
					assert.Equal(t, asset.OperateType, "")
				} else if asset.TransactionHash == txPendingRecordList[1].TransactionHash {
					assert.Equal(t, asset.Status, biz.DROPPED_REPLACED)
					assert.Equal(t, asset.OperateType, biz.CANCEL)
				} else if asset.TransactionHash == txRecordList[0].TransactionHash {
					assert.Equal(t, asset.Status, biz.SUCCESS)
					assert.Equal(t, asset.OperateType, biz.SPEED_UP)
				}

				assert.Equal(t, asset.FromAddress, req.FromAddress)
				assert.Equal(t, asset.ToAddress, req.ToAddress)
				assert.Equal(t, asset.Nonce, req.Nonce)
				if asset.TransactionHash == txRecordList[0].TransactionHash {
					assert.Equal(t, asset.TransactionType, biz.NATIVE)
				} else {
					assert.Equal(t, asset.TransactionType, biz.TRANSFER)
				}
			}
		},
	})
}

func TestHandleRecordStatus2(t *testing.T) {
	req := &data.TransactionRequest{
		FromAddress:        "0xb635233869954d3d1d93e86c3f05e6c7",
		ToAddress:          "0xc031d9eb4efc168342038ea328714080",
		Nonce:              29,
		ClientDataNotEmpty: true,
		OrderBy:            "id asc",
	}
	var txRecordList []*data.StcTransactionRecord
	var txPendingRecordList []*data.StcTransactionRecord
	utils.CopyProperties(txRecords, &txRecordList)
	utils.CopyProperties(txPendingRecords, &txPendingRecordList)
	txPendingRecordList = append(txPendingRecordList, txRecordList...)
	now := time.Now().Unix() - 100
	for _, record := range txPendingRecordList {
		record.CreatedAt = now
		now++
	}
	txPendingRecordList[0].Status = biz.DROPPED
	txPendingRecordList[1].Status = biz.DROPPED
	txPendingRecordList[2].CreatedAt = txPendingRecordList[2].CreatedAt + 21611
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			req.ClientDataNotEmpty = false
			_, err := data.StcTransactionRecordRepoClient.Delete(context.Background(), biz.GetTableName(CHAIN_NAME), req)
			assert.NoError(t, err)
			total, err := data.StcTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, biz.GetTableName(CHAIN_NAME), txPendingRecordList)
			assert.NoError(t, err)
			assert.Equal(t, total, int64(3))
			req.ClientDataNotEmpty = true
		},
		Test: func(plat *platform.Bootstrap) {
			starcoin.HandleRecordStatus(CHAIN_NAME, txPendingRecordList)
			list, err := data.StcTransactionRecordRepoClient.List(context.Background(), biz.GetTableName(CHAIN_NAME), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 3)
			for _, asset := range list {
				if asset.TransactionHash == txPendingRecordList[0].TransactionHash {
					assert.Equal(t, asset.Status, biz.DROPPED_REPLACED)
					assert.Equal(t, asset.OperateType, "")
				} else if asset.TransactionHash == txPendingRecordList[1].TransactionHash {
					assert.Equal(t, asset.Status, biz.DROPPED_REPLACED)
					assert.Equal(t, asset.OperateType, biz.CANCEL)
				} else if asset.TransactionHash == txRecordList[0].TransactionHash {
					assert.Equal(t, asset.Status, biz.SUCCESS)
					assert.Equal(t, asset.OperateType, biz.SPEED_UP)
				}

				assert.Equal(t, asset.FromAddress, req.FromAddress)
				assert.Equal(t, asset.ToAddress, req.ToAddress)
				assert.Equal(t, asset.Nonce, req.Nonce)
				assert.Equal(t, asset.TransactionType, biz.TRANSFER)
			}
		},
	})
}

func TestHandleRecordStatus3(t *testing.T) {
	req := &data.TransactionRequest{
		FromAddress:        "0xb635233869954d3d1d93e86c3f05e6c7",
		ToAddress:          "0xc031d9eb4efc168342038ea328714080",
		Nonce:              0,
		ClientDataNotEmpty: true,
		OrderBy:            "id asc",
	}
	var txRecord1 *data.StcTransactionRecord
	var txRecord2 *data.StcTransactionRecord
	utils.CopyProperties(txRecords[0], &txRecord1)
	utils.CopyProperties(txRecords[0], &txRecord2)
	txRecord1.TransactionHash = txRecord1.TransactionHash[0:len(txRecord1.TransactionHash)-1] + "5"
	txRecord2.TransactionHash = txRecord2.TransactionHash[0:len(txRecord2.TransactionHash)-1] + "6"
	txRecordList := []*data.StcTransactionRecord{txRecord1, txRecord2}
	now := time.Now().Unix() - 100
	for _, record := range txRecordList {
		record.Nonce = req.Nonce
		record.ClientData = ""
		record.CreatedAt = now
		now++
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			req.ClientDataNotEmpty = false
			_, err := data.StcTransactionRecordRepoClient.Delete(context.Background(), biz.GetTableName(CHAIN_NAME), req)
			assert.NoError(t, err)
			total, err := data.StcTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, biz.GetTableName(CHAIN_NAME), txRecordList)
			assert.NoError(t, err)
			assert.Equal(t, total, int64(2))
			req.ClientDataNotEmpty = true
		},
		Test: func(plat *platform.Bootstrap) {
			starcoin.HandleRecordStatus(CHAIN_NAME, txRecordList)
			list, err := data.StcTransactionRecordRepoClient.List(context.Background(), biz.GetTableName(CHAIN_NAME), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 0)
		},
	})
}
