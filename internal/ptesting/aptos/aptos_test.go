package solana

import (
	v1 "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"
	clientv1 "block-crawling/internal/client"
	"block-crawling/internal/data"
	"block-crawling/internal/platform"
	"block-crawling/internal/platform/aptos"
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

const CHAIN_NAME = "Aptos"
const CONFIG_PATH = "../../../configs"

const TIMEOUT = 5 * time.Second

// Unique queue to avoid conflicts as tests in different packages will run in parallel.
const TOKEN_INFO_QUEUE_TOPIC = biz.TOKEN_INFO_QUEUE_TOPIC + ":aptos"

func TestIndexBlock(t *testing.T) {
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		Prefetch:  true,
		Tokens: []types.TokenInfo{
			{
				Address:  "0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC",
				Amount:   "11174278708279",
				Decimals: 8,
				Symbol:   "DLC",
			},
		},
		RawBlockType: reflect.TypeOf(*new(aptos.BlockerInfo)),
		RawTxType:    reflect.TypeOf(new(aptos.TransactionInfo)),
		Users: map[string]string{
			"0x7687dd44a2657b5817002a12252566f3574657ef886f1fed7034b4cccf5a7658": "1",
			"0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993": "1",
		},
		IndexBlockNumbers: []uint64{
			25731630,
		},
		Assert: func() {
			record, err := data.AptTransactionRecordRepoClient.FindByTxhash(context.Background(), biz.GetTableName(CHAIN_NAME), "0x039cb14c4746d2322b15249c994395f81c08895932952ac977a217dd87bfbd22")
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, record.BlockNumber, 25731630)
			assert.Equal(t, record.Nonce, int64(402))
			assert.Equal(t, record.TransactionVersion, 70215949)
			assert.Equal(t, record.TransactionHash, "0x039cb14c4746d2322b15249c994395f81c08895932952ac977a217dd87bfbd22")
			assert.Equal(t, record.FromAddress, "0x7687dd44a2657b5817002a12252566f3574657ef886f1fed7034b4cccf5a7658")
			assert.Equal(t, record.ToAddress, "0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993")
			assert.Equal(t, record.FromUid, "1")
			assert.Equal(t, record.ToUid, "1")
			assert.Equal(t, record.FeeAmount, decimal.NewFromInt(54100))
			assert.Equal(t, record.Amount, decimal.NewFromInt(11174278708279))
			assert.Equal(t, record.Status, biz.SUCCESS)
			assert.Equal(t, record.ContractAddress, "0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC")
			assert.Equal(t, record.TransactionType, biz.TRANSFER)
			// 异步内容，不在测试范围内，不应在这里对比。
			// assert.Equal(t, record.ParseData, "{\"aptos\":{\"sequence_number\":\"402\"},\"token\":{\"address\":\"0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC\",\"amount\":\"11174278708279\",\"decimals\":8,\"symbol\":\"DLC\"}}")
		},
	})
}

func TestPendingTx(t *testing.T) {
	txHash := "0x039cb14c4746d2322b15249c994395f81c08895932952ac977a217dd87bfbd22"
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
			"0x7687dd44a2657b5817002a12252566f3574657ef886f1fed7034b4cccf5a7658": "1",
		},
		PendingTxs: []*v1.TransactionReq{
			{
				Uid:             "1",
				ChainName:       CHAIN_NAME,
				TransactionHash: txHash,
				Status:          biz.PENDING,
				FromAddress:     "0x7687dd44a2657b5817002a12252566f3574657ef886f1fed7034b4cccf5a7658",
				ToAddress:       "0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993",
				FeeAmount:       "54100",
				Amount:          "11174278708279",
				TransactionType: biz.TRANSFER,
			},
		},
		BeforePrepare: func() {
			_, err := data.AptTransactionRecordRepoClient.Delete(context.TODO(), tableName, req)
			assert.NoError(t, err)
		},
		AfterPrepare: func() {
			record, err := data.AptTransactionRecordRepoClient.FindByTxhash(context.TODO(), tableName, txHash)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, biz.PENDING, record.Status)
		},
		Assert: func() {
			record, err := data.AptTransactionRecordRepoClient.FindByTxhash(context.TODO(), tableName, txHash)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, biz.SUCCESS, record.Status)
		},
	})
}

var txRecords = []*data.AptTransactionRecord{{
	BlockNumber:        25731630,
	Nonce:              int64(402),
	TransactionVersion: 70215949,
	TransactionHash:    "0x039cb14c4746d2322b15249c994395f81c08895932952ac977a217dd87bfbd22",
	FromAddress:        "0x7687dd44a2657b5817002a12252566f3574657ef886f1fed7034b4cccf5a7658",
	ToAddress:          "0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993",
	FromUid:            "1",
	ToUid:              "1",
	FeeAmount:          decimal.NewFromInt(54100),
	Amount:             decimal.NewFromInt(11174278708279),
	Status:             biz.SUCCESS,
	TxTime:             1673885415,
	ContractAddress:    "0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC",
	TransactionType:    biz.TRANSFER,
	ParseData:          "{\"aptos\":{\"sequence_number\":\"402\"},\"token\":{\"address\":\"0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC\",\"amount\":\"11174278708279\",\"decimals\":8,\"symbol\":\"DLC\"}}",
	Data:               "{\"changes\":{\"0x7687dd44a2657b5817002a12252566f3574657ef886f1fed7034b4cccf5a7658\":{\"0x1::aptos_coin::AptosCoin\":\"1464103717\",\"0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC\":\"1932269559686844\"},\"0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993\":{\"0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC\":\"151174278708279\"}},\"payload\":{\"type\":\"entry_function_payload\",\"function\":\"0x1::coin::transfer\",\"type_arguments\":[\"0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC\"],\"arguments\":[\"0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993\",\"11174278708279\"]}}",
}}

var txNftRecords = []*data.AptTransactionRecord{{
	BlockNumber:        25732712,
	Nonce:              int64(5),
	TransactionVersion: 70218766,
	TransactionHash:    "0xb7edddf98a655d31d74fabd3c5405c237c42d6081034301a70462d24a8ba9909",
	FromAddress:        "0x40f03195b0af675a4891402528598858fcfcfdd8f0737111f09a0dad9af13bd4",
	ToAddress:          "0xb46fd4e5ba65db9f7ab7761ee581ef47b5fa9b1985c30b4185b1eac48571c771",
	FromUid:            "1",
	ToUid:              "1",
	FeeAmount:          decimal.NewFromInt(158100),
	Amount:             decimal.NewFromInt(1),
	Status:             biz.SUCCESS,
	TxTime:             1673885739,
	ContractAddress:    "0x8deb6b035d92a755e7f6984c7077e01b3074bbe6a77a4900744800280044323f::Degen Queue::0",
	TransactionType:    biz.TRANSFERNFT,
	ParseData:          "{\"aptos\":{\"sequence_number\":\"5\"},\"token\":{\"address\":\"0x8deb6b035d92a755e7f6984c7077e01b3074bbe6a77a4900744800280044323f::Degen Queue::0\",\"amount\":\"1\",\"decimals\":0,\"symbol\":\"\",\"collection_name\":\"Degen Queue\",\"token_type\":\"AptosNFT\",\"token_id\":\"432416b0f895f068fa83bb998e40c0de0c9932180bc4764e7a19151a42940a32\",\"item_name\":\"DegenQ #1057\",\"item_uri\":\"https://arweave.net/23wcK8H3Cv_i4_PF7ND761V9xIy7wjWiquH3uDslZ1w?ext=png\"}}",
	Data:               "{\"changes\":{\"0x40f03195b0af675a4891402528598858fcfcfdd8f0737111f09a0dad9af13bd4\":{\"0x1::aptos_coin::AptosCoin\":\"213200\"}},\"payload\":{\"type\":\"entry_function_payload\",\"function\":\"0x3::token_transfers::cancel_offer_script\",\"type_arguments\":[],\"arguments\":[\"0xb46fd4e5ba65db9f7ab7761ee581ef47b5fa9b1985c30b4185b1eac48571c771\",\"0x8deb6b035d92a755e7f6984c7077e01b3074bbe6a77a4900744800280044323f\",\"Degen Queue\",\"DegenQ #1057\",\"0\"]}}",
}}

func TestHandleTokenPush(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993",
		TokenAddressList: []string{"0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC"},
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
			aptos.HandleTokenPush(CHAIN_NAME, *client.(*aptos.Client), txRecords)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.NoError(t, err)
			assert.Equal(t, result, "{\"chainName\":\"Aptos\",\"uid\":\"1\",\"address\":\"0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993\",\"tokenAddress\":\"0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC\",\"decimals\":8,\"symbol\":\"DLC\"}")
			assert.Equal(t, strings.Contains(result, CHAIN_NAME), true)
			assert.Equal(t, strings.Contains(result, "0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993"), true)
			assert.Equal(t, strings.Contains(result, "0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272"), true)
		},
	})
}

func TestHandleTokenPush1(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993",
		TokenAddressList: []string{"0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC"},
	}
	var txRecordList []*data.AptTransactionRecord
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
			aptos.HandleTokenPush(CHAIN_NAME, *client.(*aptos.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleTokenPush2(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993",
		TokenAddressList: []string{"0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC"},
	}
	var txRecordList []*data.AptTransactionRecord
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
			aptos.HandleTokenPush(CHAIN_NAME, *client.(*aptos.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleTokenPush3(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993",
		TokenAddressList: []string{"0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC"},
	}
	var txRecordList []*data.AptTransactionRecord
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
			aptos.HandleTokenPush(CHAIN_NAME, *client.(*aptos.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleTokenPush4(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993",
		TokenAddressList: []string{"0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC"},
	}
	var txRecordList []*data.AptTransactionRecord
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
			aptos.HandleTokenPush(CHAIN_NAME, *client.(*aptos.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleTokenPush5(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "0xb46fd4e5ba65db9f7ab7761ee581ef47b5fa9b1985c30b4185b1eac48571c771",
		TokenAddressList: []string{"0x8deb6b035d92a755e7f6984c7077e01b3074bbe6a77a4900744800280044323f::Degen Queue::0"},
	}
	var txRecordList []*data.AptTransactionRecord
	utils.CopyProperties(txNftRecords, &txRecordList)
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		Tokens: []types.TokenInfo{
			{
				Address:  "0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC",
				Amount:   "11174278708279",
				Decimals: 8,
				Symbol:   "DLC",
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
			aptos.HandleTokenPush(CHAIN_NAME, *client.(*aptos.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleUserAsset(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"0x7687dd44a2657b5817002a12252566f3574657ef886f1fed7034b4cccf5a7658", "0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993"},
		TokenAddressList: []string{"", "0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC"},
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			c := client.(*aptos.Client)

			outputs := []gomonkey.OutputCell{
				{
					Values: gomonkey.Params{"1", nil},
				},
			}
			patches := gomonkey.ApplyMethodSeq(reflect.TypeOf(c), "GetTokenBalance", outputs)
			// 执行完毕后释放桩序列
			defer patches.Reset()

			aptos.HandleUserAsset(CHAIN_NAME, *c, txRecords)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 3)
			for _, asset := range list {
				if asset.Address == "0x7687dd44a2657b5817002a12252566f3574657ef886f1fed7034b4cccf5a7658" {
					if asset.TokenAddress == "" {
						assert.Equal(t, asset.Balance, "1")
						assert.Equal(t, asset.Symbol, "APT")
					} else if asset.TokenAddress == "0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC" {
						assert.Equal(t, asset.Balance, "19322695.59686844")
						assert.Equal(t, asset.Symbol, "DLC")
					}
				} else if asset.Address == "0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993" {
					assert.Equal(t, asset.TokenAddress, "0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC")
					assert.Equal(t, asset.Balance, "1511742.78708279")
					assert.Equal(t, asset.Symbol, "DLC")
				}

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
				assert.Equal(t, asset.Decimals, int32(8))
			}
		},
	})

	for _, record := range txRecords {
		record.Data = ""
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			c := client.(*aptos.Client)

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

			aptos.HandleUserAsset(CHAIN_NAME, *c, txRecords)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 3)
			for _, asset := range list {
				if asset.Address == "0x7687dd44a2657b5817002a12252566f3574657ef886f1fed7034b4cccf5a7658" {
					if asset.TokenAddress == "" {
						assert.Equal(t, asset.Balance, "1")
						assert.Equal(t, asset.Symbol, "APT")
					} else if asset.TokenAddress == "0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC" {
						assert.Equal(t, asset.Balance, "5")
						assert.Equal(t, asset.Symbol, "DLC")
					}
				} else if asset.Address == "0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993" {
					assert.Equal(t, asset.TokenAddress, "0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC")
					assert.Equal(t, asset.Balance, "10")
					assert.Equal(t, asset.Symbol, "DLC")
				}

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
				assert.Equal(t, asset.Decimals, int32(8))
			}
		},
	})
}

func TestHandleUserAsset1(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"0x7687dd44a2657b5817002a12252566f3574657ef886f1fed7034b4cccf5a7658", "0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993"},
		TokenAddressList: []string{"", "0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC"},
	}
	var txRecordList []*data.AptTransactionRecord
	utils.CopyProperties(txRecords, &txRecordList)
	for _, record := range txRecordList {
		record.Status = biz.DROPPED
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		Tokens: []types.TokenInfo{
			{
				Address:  "0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC",
				Amount:   "11174278708279",
				Decimals: 8,
				Symbol:   "DLC",
			},
		},
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			aptos.HandleUserAsset(CHAIN_NAME, *client.(*aptos.Client), txRecordList)
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
		AddressList:      []string{"0x7687dd44a2657b5817002a12252566f3574657ef886f1fed7034b4cccf5a7658", "0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993"},
		TokenAddressList: []string{"", "0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC"},
	}
	var txRecordList []*data.AptTransactionRecord
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
				Address:  "0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC",
				Amount:   "11174278708279",
				Decimals: 8,
				Symbol:   "DLC",
			},
		},
		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			c := client.(*aptos.Client)

			outputs := []gomonkey.OutputCell{
				{
					Values: gomonkey.Params{"1", nil},
				},
			}
			patches := gomonkey.ApplyMethodSeq(reflect.TypeOf(c), "GetBalance", outputs)
			// 执行完毕后释放桩序列
			defer patches.Reset()

			aptos.HandleUserAsset(CHAIN_NAME, *c, txRecordList)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 1)
			for _, asset := range list {
				assert.Equal(t, asset.Address, "0x7687dd44a2657b5817002a12252566f3574657ef886f1fed7034b4cccf5a7658")
				assert.Equal(t, asset.TokenAddress, "")
				assert.Equal(t, asset.Balance, "1")
				assert.Equal(t, asset.Symbol, "APT")

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
				assert.Equal(t, asset.Decimals, int32(8))
			}
		},
	})
}

func TestHandleUserAsset3(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"0x40f03195b0af675a4891402528598858fcfcfdd8f0737111f09a0dad9af13bd4", "0xb46fd4e5ba65db9f7ab7761ee581ef47b5fa9b1985c30b4185b1eac48571c771"},
		TokenAddressList: []string{"", "0x8deb6b035d92a755e7f6984c7077e01b3074bbe6a77a4900744800280044323f::Degen Queue::0"},
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,

		BeforePrepare: func() {
			_, err := data.UserAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			c := client.(*aptos.Client)

			outputs := []gomonkey.OutputCell{
				{
					Values: gomonkey.Params{"1", nil},
				},
			}
			patches := gomonkey.ApplyMethodSeq(reflect.TypeOf(c), "GetBalance", outputs)
			// 执行完毕后释放桩序列
			defer patches.Reset()

			aptos.HandleUserAsset(CHAIN_NAME, *c, txNftRecords)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 1)
			for _, asset := range list {
				assert.Equal(t, asset.Address, "0x40f03195b0af675a4891402528598858fcfcfdd8f0737111f09a0dad9af13bd4")
				assert.Equal(t, asset.TokenAddress, "")
				assert.Equal(t, asset.Balance, "1")
				assert.Equal(t, asset.Symbol, "APT")

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
				assert.Equal(t, asset.Decimals, int32(8))
			}
		},
	})
}

func TestHandleUserStatistic(t *testing.T) {
	req := &data.StatisticRequest{
		ChainName:         CHAIN_NAME,
		TokenAddressList:  []string{"0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC"},
		FundDirectionList: []int16{3},
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		TokenPrices: []ptesting.TokenPrice{
			{
				TokenAddress: "0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC",
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
			aptos.HandleUserStatistic(CHAIN_NAME, *client.(*aptos.Client), txRecords)
			list, err := data.TransactionStatisticRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list) >= 1, true)
			for _, asset := range list {
				assert.Equal(t, asset.TokenAddress, "0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC")
				assert.Equal(t, asset.FundDirection, int16(3))

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
			}
		},
	})
}

func TestHandleUserStatistic1(t *testing.T) {
	req := &data.StatisticRequest{
		ChainName:         CHAIN_NAME,
		TokenAddressList:  []string{"0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC"},
		FundDirectionList: []int16{3},
	}
	var txRecordList []*data.AptTransactionRecord
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
			aptos.HandleUserStatistic(CHAIN_NAME, *client.(*aptos.Client), txRecordList)
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
		TokenAddressList:  []string{"0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC"},
		FundDirectionList: []int16{3},
	}
	var txRecordList []*data.AptTransactionRecord
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
			aptos.HandleUserStatistic(CHAIN_NAME, *client.(*aptos.Client), txRecordList)
			list, err := data.TransactionStatisticRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 0)
		},
	})
}

func TestHandleUserNftAsset(t *testing.T) {
	req := &data.NftAssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"0x40f03195b0af675a4891402528598858fcfcfdd8f0737111f09a0dad9af13bd4", "0xb46fd4e5ba65db9f7ab7761ee581ef47b5fa9b1985c30b4185b1eac48571c771"},
		TokenAddressList: []string{"0x8deb6b035d92a755e7f6984c7077e01b3074bbe6a77a4900744800280044323f::Degen Queue::0"},
		TokenIdList:      []string{"432416b0f895f068fa83bb998e40c0de0c9932180bc4764e7a19151a42940a32"},
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		NFTs: []*clientv1.GetNftReply_NftInfoResp{
			{
				TokenId:        "DegenQ #1057",
				TokenAddress:   "0x8deb6b035d92a755e7f6984c7077e01b3074bbe6a77a4900744800280044323f::Degen Queue::0",
				Name:           "DegenQ #1057",
				Symbol:         "DegenQ",
				TokenType:      biz.APTOSNFT,
				CollectionName: "Degen Queue",
				ImageURL:       "https://arweave.net/23wcK8H3Cv_i4_PF7ND761V9xIy7wjWiquH3uDslZ1w?ext=png",
				NftName:        "DegenQ #1057",
			},
		},
		BeforePrepare: func() {
			_, err := data.UserNftAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			c := client.(*aptos.Client)

			outputs := []gomonkey.OutputCell{
				{
					Values: gomonkey.Params{"0", nil},
				},
				{
					Values: gomonkey.Params{"1", nil},
				},
			}
			patches := gomonkey.ApplyMethodSeq(reflect.TypeOf(c), "Erc1155BalanceByTokenId", outputs)
			// 执行完毕后释放桩序列
			defer patches.Reset()

			aptos.HandleUserNftAsset(CHAIN_NAME, *c, txNftRecords)
			list, err := data.UserNftAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 2)
			for _, asset := range list {
				// The sequence of balance may be changed during runtime.
				/*
					if asset.Address == "0x40f03195b0af675a4891402528598858fcfcfdd8f0737111f09a0dad9af13bd4" {
						assert.Equal(t, asset.Balance, "0")
					} else if asset.Address == "0xb46fd4e5ba65db9f7ab7761ee581ef47b5fa9b1985c30b4185b1eac48571c771" {
						assert.Equal(t, asset.Balance, "1")
					}
				*/
				assert.Equal(t, asset.ChainName, CHAIN_NAME)
				assert.Equal(t, asset.TokenAddress, "0x8deb6b035d92a755e7f6984c7077e01b3074bbe6a77a4900744800280044323f::Degen Queue::0")
				assert.Equal(t, asset.TokenId, "432416b0f895f068fa83bb998e40c0de0c9932180bc4764e7a19151a42940a32")
				assert.Equal(t, asset.TokenType, biz.APTOSNFT)
				assert.Equal(t, asset.CollectionName, "Degen Queue")
				assert.Equal(t, asset.ItemName, "DegenQ #1057")
				assert.Equal(t, asset.ItemUri, "https://arweave.net/23wcK8H3Cv_i4_PF7ND761V9xIy7wjWiquH3uDslZ1w?ext=png")
			}
		},
	})
}

func TestHandleUserNftAsset1(t *testing.T) {
	req := &data.NftAssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"0x40f03195b0af675a4891402528598858fcfcfdd8f0737111f09a0dad9af13bd4", "0xb46fd4e5ba65db9f7ab7761ee581ef47b5fa9b1985c30b4185b1eac48571c771"},
		TokenAddressList: []string{"0x8deb6b035d92a755e7f6984c7077e01b3074bbe6a77a4900744800280044323f::Degen Queue::0"},
		TokenIdList:      []string{"432416b0f895f068fa83bb998e40c0de0c9932180bc4764e7a19151a42940a32"},
	}
	var txRecordList []*data.AptTransactionRecord
	utils.CopyProperties(txNftRecords, &txRecordList)
	for _, record := range txRecordList {
		record.TransactionType = biz.CONTRACT
		index := strings.Index(record.ParseData, "\"token\"")
		record.ParseData = record.ParseData[:index] + "\"token\":{\"address\":\"\",\"amount\":\"\",\"decimals\":0,\"symbol\":\"\"}}"
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.UserNftAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			c := client.(*aptos.Client)

			aptos.HandleUserNftAsset(CHAIN_NAME, *c, txRecordList)
			list, err := data.UserNftAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 0)
		},
	})
}

func TestHandleUserNftAsset2(t *testing.T) {
	req := &data.NftAssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"0x40f03195b0af675a4891402528598858fcfcfdd8f0737111f09a0dad9af13bd4", "0xb46fd4e5ba65db9f7ab7761ee581ef47b5fa9b1985c30b4185b1eac48571c771"},
		TokenAddressList: []string{"0x8deb6b035d92a755e7f6984c7077e01b3074bbe6a77a4900744800280044323f::Degen Queue::0"},
		TokenIdList:      []string{"432416b0f895f068fa83bb998e40c0de0c9932180bc4764e7a19151a42940a32"},
	}
	var txRecordList []*data.AptTransactionRecord
	utils.CopyProperties(txNftRecords, &txRecordList)
	for _, record := range txRecordList {
		record.Status = biz.FAIL
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.UserNftAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			c := client.(*aptos.Client)

			aptos.HandleUserNftAsset(CHAIN_NAME, *c, txRecordList)
			list, err := data.UserNftAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 0)
		},
	})
}

func TestHandleUserNftAsset3(t *testing.T) {
	req := &data.NftAssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"0x7687dd44a2657b5817002a12252566f3574657ef886f1fed7034b4cccf5a7658", "0xf9d2dd868e0058cdb4e2b95e12adb9530cb3cef4c53eff4ed86c2971c5818993"},
		TokenAddressList: []string{"", "0x84edd115c901709ef28f3cb66a82264ba91bfd24789500b6fd34ab9e8888e272::coin::DLC"},
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.UserNftAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			c := client.(*aptos.Client)

			aptos.HandleUserNftAsset(CHAIN_NAME, *c, txRecords)
			list, err := data.UserNftAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 0)
		},
	})
}
