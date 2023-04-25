package solana

import (
	v1 "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/platform"
	"block-crawling/internal/platform/solana"
	"block-crawling/internal/ptesting"
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

const CHAIN_NAME = "Solana"
const CONFIG_PATH = "../../../configs"

// Unique queue to avoid conflicts as tests in different packages will run in parallel.
const TOKEN_INFO_QUEUE_TOPIC = biz.TOKEN_INFO_QUEUE_TOPIC + ":solana"

const TIMEOUT = 5 * time.Second

func TestIndexBlock(t *testing.T) {
	ptesting.RunTest(ptesting.Preparation{
		ChainName:    CHAIN_NAME,
		Configs:      CONFIG_PATH,
		Prefetch:     true,
		RawBlockType: reflect.TypeOf(1),
		RawTxType:    reflect.TypeOf(new(solana.TransactionInfo)),
		Users: map[string]string{
			"CicfACV8ucYy2nQhHcJn6mW4GjBuhMPDmRkN76G1MeEy": "1",
		},
		IndexBlockNumbers: []uint64{
			175757542,
			175757543,
		},
		Assert: func() {
			record, err := data.SolTransactionRecordRepoClient.FindByTxhash(context.Background(), biz.GetTableName(CHAIN_NAME), "2yXp5Gwdg2xx6Ekev5jjYkcr4nK9T1uhre7ciNvNvBGDYrh4BKLgeS57ECE9CD2PYzJ9EiCKr9awjFm7NhLequGc")
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, record.ToAddress, "CicfACV8ucYy2nQhHcJn6mW4GjBuhMPDmRkN76G1MeEy")
			assert.Equal(t, record.TransactionHash, "2yXp5Gwdg2xx6Ekev5jjYkcr4nK9T1uhre7ciNvNvBGDYrh4BKLgeS57ECE9CD2PYzJ9EiCKr9awjFm7NhLequGc")
			assert.Equal(t, record.ToUid, "1")
			assert.Equal(t, record.TransactionType, biz.TRANSFER)
			assert.Equal(t, record.BlockNumber, 159381368)
		},
	})
}

func TestIndexBlockFloatAmount(t *testing.T) {
	ptesting.RunTest(ptesting.Preparation{
		ChainName:    CHAIN_NAME,
		Configs:      CONFIG_PATH,
		Prefetch:     true,
		RawBlockType: reflect.TypeOf(1),
		RawTxType:    reflect.TypeOf(new(solana.TransactionInfo)),
		Users:        map[string]string{},
		IndexBlockNumbers: []uint64{
			189267116,
		},
		AssertErrs: func(errs []error) {
			// Float amount should not cause panic.
			// See https://gitlab.bixin.com/mili/block-crawling/-/merge_requests/303
			ei := make([]interface{}, 0, len(errs))
			for _, v := range errs {
				ei = append(ei, v)
			}
			assert.Equal(t, 0, len(errs), ei...)
		},
	})
}

func TestPendingTx(t *testing.T) {
	txHash := "645vEGLMcbAdm7YuRpeDRpqYMEGQpZG9BSf3B4vaQLieJpUELQ8XjCHtdBM5yejAr7ejjvS2ktenda77rysBaCJN"
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
			"FSmS8G3UL1XvxCXrVRpnGiUv1xDD8nPfSEGBXnZCsuzL": "1",
		},
		PendingTxs: []*v1.TransactionReq{
			{
				Uid:             "1",
				ChainName:       CHAIN_NAME,
				TransactionHash: txHash,
				Status:          biz.PENDING,
				FromAddress:     "FSmS8G3UL1XvxCXrVRpnGiUv1xDD8nPfSEGBXnZCsuzL",
				ToAddress:       "7E1QHT8haaDRvDF4XxFGWtSJW2i5sDfna8SmmyzQVxv6",
				Amount:          "0.04078303",
				FeeAmount:       "0.00005",
				TransactionType: biz.NATIVE,
			},
		},
		BeforePrepare: func() {
			_, err := data.SolTransactionRecordRepoClient.Delete(context.TODO(), tableName, req)
			assert.NoError(t, err)
		},
		AfterPrepare: func() {
			record, err := data.SolTransactionRecordRepoClient.FindByTxhash(context.TODO(), tableName, txHash)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, biz.PENDING, record.Status)
		},
		Assert: func() {
			record, err := data.SolTransactionRecordRepoClient.FindByTxhash(context.TODO(), tableName, txHash)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, biz.SUCCESS, record.Status)
		},
	})
}

var txRecords = []*data.SolTransactionRecord{{
	SlotNumber:      154662217,
	BlockNumber:     139691367,
	TransactionHash: "3XWLh2wCf2rciZwB8yUbMNAtwuPgzxd99brncyWS87qNwNaLfBDiSDZv3mQy5E1ep2HZB26cpJzUuPehzH5xvMxG",
	FromAddress:     "6ZRCB7AAqGre6c72PRz3MHLC73VMYvJ8bi9KHf1HFpNk",
	ToAddress:       "2z8NEc3jZwpu6CrdBhdRikJYVvPxDKyWop4YUKenbRJy",
	FromUid:         "1",
	ToUid:           "1",
	FeeAmount:       decimal.NewFromInt(2044280),
	Amount:          decimal.NewFromInt(34385157),
	Status:          biz.SUCCESS,
	TxTime:          1665416066,
	ContractAddress: "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt",
	TransactionType: biz.TRANSFER,
	ParseData:       "{\"token\":{\"address\":\"SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt\",\"amount\":\"34385157\",\"decimals\":6,\"symbol\":\"SRM\"}}",
	Data:            "{\"accountKey\":{\"11111111111111111111111111111111\":{\"index\":3,\"pubkey\":\"11111111111111111111111111111111\",\"signer\":false,\"source\":\"transaction\",\"writable\":false,\"amount\":1,\"transferAmount\":0},\"2z8NEc3jZwpu6CrdBhdRikJYVvPxDKyWop4YUKenbRJy\":{\"index\":4,\"pubkey\":\"2z8NEc3jZwpu6CrdBhdRikJYVvPxDKyWop4YUKenbRJy\",\"signer\":false,\"source\":\"transaction\",\"writable\":false,\"amount\":14693173366,\"transferAmount\":0},\"4MNPdKu9wFMvEeZBMt3Eipfs5ovVWTJb31pEXDJAAxX5\":{\"index\":5,\"pubkey\":\"4MNPdKu9wFMvEeZBMt3Eipfs5ovVWTJb31pEXDJAAxX5\",\"signer\":false,\"source\":\"transaction\",\"writable\":false,\"amount\":327064320,\"transferAmount\":0},\"664TAXG6PQZuJKEwfkt8YCP8L1QHJpdsm2qKneVrgjP6\":{\"index\":1,\"pubkey\":\"664TAXG6PQZuJKEwfkt8YCP8L1QHJpdsm2qKneVrgjP6\",\"signer\":false,\"source\":\"transaction\",\"writable\":true,\"amount\":2039280,\"transferAmount\":2039280},\"6ZRCB7AAqGre6c72PRz3MHLC73VMYvJ8bi9KHf1HFpNk\":{\"index\":0,\"pubkey\":\"6ZRCB7AAqGre6c72PRz3MHLC73VMYvJ8bi9KHf1HFpNk\",\"signer\":true,\"source\":\"transaction\",\"writable\":true,\"amount\":1311720856884253,\"transferAmount\":-2044280},\"ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL\":{\"index\":6,\"pubkey\":\"ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL\",\"signer\":false,\"source\":\"transaction\",\"writable\":false,\"amount\":731913600,\"transferAmount\":0},\"B5bUw121JxoFN1svqe8KZSm8S8BDLVgDGFzdGiQue1pa\":{\"index\":2,\"pubkey\":\"B5bUw121JxoFN1svqe8KZSm8S8BDLVgDGFzdGiQue1pa\",\"signer\":false,\"source\":\"transaction\",\"writable\":true,\"amount\":2039280,\"transferAmount\":0},\"SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt\":{\"index\":7,\"pubkey\":\"SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt\",\"signer\":false,\"source\":\"transaction\",\"writable\":false,\"amount\":840614741399,\"transferAmount\":0},\"SysvarRent111111111111111111111111111111111\":{\"index\":8,\"pubkey\":\"SysvarRent111111111111111111111111111111111\",\"signer\":false,\"source\":\"transaction\",\"writable\":false,\"amount\":1009200,\"transferAmount\":0},\"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\":{\"index\":9,\"pubkey\":\"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\",\"signer\":false,\"source\":\"transaction\",\"writable\":false,\"amount\":934087680,\"transferAmount\":0}},\"tokenBalance\":{\"2z8NEc3jZwpu6CrdBhdRikJYVvPxDKyWop4YUKenbRJy\":{\"accountIndex\":1,\"account\":\"664TAXG6PQZuJKEwfkt8YCP8L1QHJpdsm2qKneVrgjP6\",\"mint\":\"SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt\",\"owner\":\"2z8NEc3jZwpu6CrdBhdRikJYVvPxDKyWop4YUKenbRJy\",\"programId\":\"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\",\"uiTokenAmount\":{\"amount\":\"34385157\",\"decimals\":6,\"uiAmount\":34.385157,\"uiAmountString\":\"34.385157\",\"transferAmount\":-325641462057383}},\"6ZRCB7AAqGre6c72PRz3MHLC73VMYvJ8bi9KHf1HFpNk\":{\"accountIndex\":2,\"account\":\"B5bUw121JxoFN1svqe8KZSm8S8BDLVgDGFzdGiQue1pa\",\"mint\":\"SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt\",\"owner\":\"6ZRCB7AAqGre6c72PRz3MHLC73VMYvJ8bi9KHf1HFpNk\",\"programId\":\"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\",\"uiTokenAmount\":{\"amount\":\"325641462057383\",\"decimals\":6,\"uiAmount\":325641462.057383,\"uiAmountString\":\"325641462.057383\",\"transferAmount\":325641462057383}}}}",
}}

var txNftRecords = []*data.SolTransactionRecord{{
	SlotNumber:      178838314,
	BlockNumber:     162183507,
	TransactionHash: "3ScLy5nkjrEEvhgPk6oPNnR9L7zYGY5gX3VMrgK4xhJQvcbhLikSfJfKi41WKsU4fibN8t5ak7JLY4fuF9WHJ8dR",
	FromAddress:     "6bypcprHamxCgmx4DN7RAUDGufTXedQBRfymgMdvjsKZ",
	ToAddress:       "A9A3iqk34KkkpEHmBkyXkxYLcHyGqsua1YiywMjMRzLS",
	FromUid:         "1",
	ToUid:           "1",
	FeeAmount:       decimal.NewFromInt(2044280),
	Amount:          decimal.NewFromInt(1),
	Status:          biz.SUCCESS,
	TxTime:          1676948860,
	ContractAddress: "ECG97JuJph8azFBfVe5VEkQkV3S2nzkB6Z229ATnmJC4",
	TransactionType: biz.TRANSFERNFT,
	ParseData:       "{\"token\":{\"address\":\"ECG97JuJph8azFBfVe5VEkQkV3S2nzkB6Z229ATnmJC4\",\"amount\":\"1\",\"decimals\":0,\"symbol\":\"\",\"collection_name\":\"y00ts\",\"token_type\":\"METAPLEX\",\"token_id\":\"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\",\"item_name\":\"y00t #7209\",\"item_uri\":\"https://i.seadn.io/gae/twerrx0l3-WfxHwPLQxy17G5oOjlznEb4pQf8ksHxV32Mffno_KLLwklOrXU_w2AvtxWHaE7w0womO70ZKq5vVpS8RD1jFTvRltDwPU?w=500&auto=format\"}}",
	Data:            "{\"accountKey\":{\"11111111111111111111111111111111\":{\"index\":3,\"pubkey\":\"11111111111111111111111111111111\",\"signer\":false,\"source\":\"transaction\",\"writable\":false,\"amount\":1,\"transferAmount\":0},\"2XQpKx8M78CSk8RAi4rzwWMXDi2PFD8njXi8Wx4ZvhES\":{\"index\":1,\"pubkey\":\"2XQpKx8M78CSk8RAi4rzwWMXDi2PFD8njXi8Wx4ZvhES\",\"signer\":false,\"source\":\"transaction\",\"writable\":true,\"amount\":2039280,\"transferAmount\":2039280},\"6bypcprHamxCgmx4DN7RAUDGufTXedQBRfymgMdvjsKZ\":{\"index\":0,\"pubkey\":\"6bypcprHamxCgmx4DN7RAUDGufTXedQBRfymgMdvjsKZ\",\"signer\":true,\"source\":\"transaction\",\"writable\":true,\"amount\":1912715640,\"transferAmount\":-2044280},\"A9A3iqk34KkkpEHmBkyXkxYLcHyGqsua1YiywMjMRzLS\":{\"index\":4,\"pubkey\":\"A9A3iqk34KkkpEHmBkyXkxYLcHyGqsua1YiywMjMRzLS\",\"signer\":false,\"source\":\"transaction\",\"writable\":false,\"amount\":2039280,\"transferAmount\":0},\"ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL\":{\"index\":5,\"pubkey\":\"ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL\",\"signer\":false,\"source\":\"transaction\",\"writable\":false,\"amount\":731913600,\"transferAmount\":0},\"DeJBGdMFa1uynnnKiwrVioatTuHmNLpyFKnmB5kaFdzQ\":{\"index\":6,\"pubkey\":\"DeJBGdMFa1uynnnKiwrVioatTuHmNLpyFKnmB5kaFdzQ\",\"signer\":false,\"source\":\"transaction\",\"writable\":false,\"amount\":1141440,\"transferAmount\":0},\"ECG97JuJph8azFBfVe5VEkQkV3S2nzkB6Z229ATnmJC4\":{\"index\":7,\"pubkey\":\"ECG97JuJph8azFBfVe5VEkQkV3S2nzkB6Z229ATnmJC4\",\"signer\":false,\"source\":\"transaction\",\"writable\":false,\"amount\":1461600,\"transferAmount\":0},\"Hc7KNJUXN4zhzLBh3uzGPHzPhVpT2iuiSoPKTwnsMRZR\":{\"index\":2,\"pubkey\":\"Hc7KNJUXN4zhzLBh3uzGPHzPhVpT2iuiSoPKTwnsMRZR\",\"signer\":false,\"source\":\"transaction\",\"writable\":true,\"amount\":2039280,\"transferAmount\":0},\"SysvarRent111111111111111111111111111111111\":{\"index\":8,\"pubkey\":\"SysvarRent111111111111111111111111111111111\",\"signer\":false,\"source\":\"transaction\",\"writable\":false,\"amount\":1009200,\"transferAmount\":0},\"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\":{\"index\":9,\"pubkey\":\"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\",\"signer\":false,\"source\":\"transaction\",\"writable\":false,\"amount\":934087680,\"transferAmount\":0}},\"tokenBalance\":{\"6bypcprHamxCgmx4DN7RAUDGufTXedQBRfymgMdvjsKZ\":{\"accountIndex\":2,\"account\":\"Hc7KNJUXN4zhzLBh3uzGPHzPhVpT2iuiSoPKTwnsMRZR\",\"mint\":\"ECG97JuJph8azFBfVe5VEkQkV3S2nzkB6Z229ATnmJC4\",\"owner\":\"6bypcprHamxCgmx4DN7RAUDGufTXedQBRfymgMdvjsKZ\",\"programId\":\"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\",\"uiTokenAmount\":{\"amount\":\"0\",\"decimals\":0,\"uiAmount\":0,\"uiAmountString\":\"0\",\"transferAmount\":0}},\"A9A3iqk34KkkpEHmBkyXkxYLcHyGqsua1YiywMjMRzLS\":{\"accountIndex\":1,\"account\":\"2XQpKx8M78CSk8RAi4rzwWMXDi2PFD8njXi8Wx4ZvhES\",\"mint\":\"ECG97JuJph8azFBfVe5VEkQkV3S2nzkB6Z229ATnmJC4\",\"owner\":\"A9A3iqk34KkkpEHmBkyXkxYLcHyGqsua1YiywMjMRzLS\",\"programId\":\"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\",\"uiTokenAmount\":{\"amount\":\"1\",\"decimals\":0,\"uiAmount\":1,\"uiAmountString\":\"1\",\"transferAmount\":0}}}}",
}}

func TestHandleTokenPush(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "2z8NEc3jZwpu6CrdBhdRikJYVvPxDKyWop4YUKenbRJy",
		TokenAddressList: []string{"SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt"},
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
			solana.HandleTokenPush(CHAIN_NAME, *client.(*solana.Client), txRecords)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.NoError(t, err)
			assert.Equal(t, result, "{\"chainName\":\"Solana\",\"uid\":\"1\",\"address\":\"2z8NEc3jZwpu6CrdBhdRikJYVvPxDKyWop4YUKenbRJy\",\"tokenAddress\":\"SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt\",\"decimals\":6,\"symbol\":\"SRM\"}")
			assert.Equal(t, strings.Contains(result, CHAIN_NAME), true)
			assert.Equal(t, strings.Contains(result, "2z8NEc3jZwpu6CrdBhdRikJYVvPxDKyWop4YUKenbRJy"), true)
			assert.Equal(t, strings.Contains(result, "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt"), true)
		},
	})
}

func TestHandleTokenPush1(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "2z8NEc3jZwpu6CrdBhdRikJYVvPxDKyWop4YUKenbRJy",
		TokenAddressList: []string{"SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt"},
	}
	var txRecordList []*data.SolTransactionRecord
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
			solana.HandleTokenPush(CHAIN_NAME, *client.(*solana.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleTokenPush2(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "2z8NEc3jZwpu6CrdBhdRikJYVvPxDKyWop4YUKenbRJy",
		TokenAddressList: []string{"SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt"},
	}
	var txRecordList []*data.SolTransactionRecord
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
			solana.HandleTokenPush(CHAIN_NAME, *client.(*solana.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleTokenPush3(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "2z8NEc3jZwpu6CrdBhdRikJYVvPxDKyWop4YUKenbRJy",
		TokenAddressList: []string{"SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt"},
	}
	var txRecordList []*data.SolTransactionRecord
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
			solana.HandleTokenPush(CHAIN_NAME, *client.(*solana.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleTokenPush4(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "2z8NEc3jZwpu6CrdBhdRikJYVvPxDKyWop4YUKenbRJy",
		TokenAddressList: []string{"SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt"},
	}
	var txRecordList []*data.SolTransactionRecord
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
			solana.HandleTokenPush(CHAIN_NAME, *client.(*solana.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleTokenPush5(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "A9A3iqk34KkkpEHmBkyXkxYLcHyGqsua1YiywMjMRzLS",
		TokenAddressList: []string{"ECG97JuJph8azFBfVe5VEkQkV3S2nzkB6Z229ATnmJC4"},
	}
	var txRecordList []*data.SolTransactionRecord
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
			solana.HandleTokenPush(CHAIN_NAME, *client.(*solana.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleUserAsset(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"6ZRCB7AAqGre6c72PRz3MHLC73VMYvJ8bi9KHf1HFpNk", "2z8NEc3jZwpu6CrdBhdRikJYVvPxDKyWop4YUKenbRJy"},
		TokenAddressList: []string{"", "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt"},
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
			solana.HandleUserAsset(false, CHAIN_NAME, *client.(*solana.Client), txRecords)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 3)
			for _, asset := range list {
				if asset.Address == "6ZRCB7AAqGre6c72PRz3MHLC73VMYvJ8bi9KHf1HFpNk" {
					if asset.TokenAddress == "" {
						assert.Equal(t, asset.Balance, "0.316854167")
						assert.Equal(t, asset.Symbol, "SOL")
						assert.Equal(t, asset.Decimals, int32(9))
					} else if asset.TokenAddress == "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt" {
						assert.Equal(t, asset.Balance, "325641462.057383")
						assert.Equal(t, asset.Symbol, "SRM")
						assert.Equal(t, asset.Decimals, int32(6))
					}
				} else if asset.Address == "2z8NEc3jZwpu6CrdBhdRikJYVvPxDKyWop4YUKenbRJy" {
					assert.Equal(t, asset.TokenAddress, "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt")
					assert.Equal(t, asset.Balance, "34.385157")
					assert.Equal(t, asset.Symbol, "SRM")
					assert.Equal(t, asset.Decimals, int32(6))
				}

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
			}
		},
	})

	/*for _, record := range txRecords {
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
			c := client.(*solana.Client)

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

			solana.HandleUserAsset(false, CHAIN_NAME, *c, txRecords)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 3)
			for _, asset := range list {
				if asset.Address == "6ZRCB7AAqGre6c72PRz3MHLC73VMYvJ8bi9KHf1HFpNk" {
					if asset.TokenAddress == "" {
						assert.Equal(t, asset.Balance, "1")
						assert.Equal(t, asset.Symbol, "SOL")
						assert.Equal(t, asset.Decimals, int32(9))
					} else if asset.TokenAddress == "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt" {
						assert.Equal(t, asset.Balance, "5")
						assert.Equal(t, asset.Symbol, "SRM")
						assert.Equal(t, asset.Decimals, int32(6))
					}
				} else if asset.Address == "2z8NEc3jZwpu6CrdBhdRikJYVvPxDKyWop4YUKenbRJy" {
					assert.Equal(t, asset.TokenAddress, "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt")
					assert.Equal(t, asset.Balance, "10")
					assert.Equal(t, asset.Symbol, "SRM")
					assert.Equal(t, asset.Decimals, int32(6))
				}

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
			}
		},
	})*/
}

func TestHandleUserAsset1(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"6ZRCB7AAqGre6c72PRz3MHLC73VMYvJ8bi9KHf1HFpNk", "2z8NEc3jZwpu6CrdBhdRikJYVvPxDKyWop4YUKenbRJy"},
		TokenAddressList: []string{"", "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt"},
	}
	var txRecordList []*data.SolTransactionRecord
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
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			solana.HandleUserAsset(false, CHAIN_NAME, *client.(*solana.Client), txRecordList)
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
		AddressList:      []string{"6ZRCB7AAqGre6c72PRz3MHLC73VMYvJ8bi9KHf1HFpNk", "2z8NEc3jZwpu6CrdBhdRikJYVvPxDKyWop4YUKenbRJy"},
		TokenAddressList: []string{"", "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt"},
	}
	var txRecordList []*data.SolTransactionRecord
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
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			c := client.(*solana.Client)

			outputs := []gomonkey.OutputCell{
				{
					Values: gomonkey.Params{"1", nil},
				},
			}
			patches := gomonkey.ApplyMethodSeq(reflect.TypeOf(c), "GetBalance", outputs)
			// 执行完毕后释放桩序列
			defer patches.Reset()

			solana.HandleUserAsset(false, CHAIN_NAME, *c, txRecordList)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 1)
			for _, asset := range list {
				assert.Equal(t, asset.Address, "6ZRCB7AAqGre6c72PRz3MHLC73VMYvJ8bi9KHf1HFpNk")
				assert.Equal(t, asset.TokenAddress, "")
				assert.Equal(t, asset.Balance, "1")
				assert.Equal(t, asset.Symbol, "SOL")

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
				assert.Equal(t, asset.Decimals, int32(9))
			}
		},
	})
}

/*func TestHandleUserAsset3(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"6bypcprHamxCgmx4DN7RAUDGufTXedQBRfymgMdvjsKZ", "A9A3iqk34KkkpEHmBkyXkxYLcHyGqsua1YiywMjMRzLS"},
		TokenAddressList: []string{"", "ECG97JuJph8azFBfVe5VEkQkV3S2nzkB6Z229ATnmJC4"},
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
			c := client.(*solana.Client)

			outputs := []gomonkey.OutputCell{
				{
					Values: gomonkey.Params{"1", nil},
				},
			}
			patches := gomonkey.ApplyMethodSeq(reflect.TypeOf(c), "GetBalance", outputs)
			// 执行完毕后释放桩序列
			defer patches.Reset()

			solana.HandleUserAsset(false, CHAIN_NAME, *c, txNftRecords)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 1)
			for _, asset := range list {
				assert.Equal(t, asset.Address, "6bypcprHamxCgmx4DN7RAUDGufTXedQBRfymgMdvjsKZ")
				assert.Equal(t, asset.TokenAddress, "")
				assert.Equal(t, asset.Balance, "1")
				assert.Equal(t, asset.Symbol, "SOL")

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
				assert.Equal(t, asset.Decimals, int32(9))
			}
		},
	})
}*/

func TestHandleUserStatistic(t *testing.T) {
	req := &data.StatisticRequest{
		ChainName:         CHAIN_NAME,
		TokenAddressList:  []string{"SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt"},
		FundDirectionList: []int16{3},
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		TokenPrices: []ptesting.TokenPrice{
			{
				TokenAddress: "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt",
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
			solana.HandleUserStatistic(CHAIN_NAME, *client.(*solana.Client), txRecords)
			list, err := data.TransactionStatisticRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list) >= 1, true)
			for _, asset := range list {
				assert.Equal(t, asset.TokenAddress, "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt")
				assert.Equal(t, asset.FundDirection, int16(3))

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
			}
		},
	})
}

func TestHandleUserStatistic1(t *testing.T) {
	req := &data.StatisticRequest{
		ChainName:         CHAIN_NAME,
		TokenAddressList:  []string{"SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt"},
		FundDirectionList: []int16{3},
	}
	var txRecordList []*data.SolTransactionRecord
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
			solana.HandleUserStatistic(CHAIN_NAME, *client.(*solana.Client), txRecordList)
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
		TokenAddressList:  []string{"SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt"},
		FundDirectionList: []int16{3},
	}
	var txRecordList []*data.SolTransactionRecord
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
			solana.HandleUserStatistic(CHAIN_NAME, *client.(*solana.Client), txRecordList)
			list, err := data.TransactionStatisticRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 0)
		},
	})
}
