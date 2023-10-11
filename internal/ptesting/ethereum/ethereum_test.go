package ethereum

import (
	v1 "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"
	clientv1 "block-crawling/internal/client"
	"block-crawling/internal/data"
	"block-crawling/internal/platform"
	"block-crawling/internal/platform/ethereum"
	"block-crawling/internal/ptesting"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/go-redis/redis"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

const CHAIN_NAME = "ETH"
const CONFIG_PATH = "../../../configs"

// Unique queue to avoid conflicts as tests in different packages will run in parallel.
const TOKEN_INFO_QUEUE_TOPIC = biz.TOKEN_INFO_QUEUE_TOPIC + ":ethereum"

const TIMEOUT = 5 * time.Second

func TestIndexBlock(t *testing.T) {
	ptesting.RunTest(ptesting.Preparation{
		ChainName:       CHAIN_NAME,
		Configs:         CONFIG_PATH,
		Prefetch:        true,
		SourceFromChain: true,
		RawTxType:       reflect.TypeOf(new(ethereum.Transaction)),
		Users: map[string]string{
			"0x1EaCa1277BcDFa83E60658D8938B3D63cD3E63C1": "1",
		},
		IndexBlockNumbers: []uint64{
			16625722,
			//16617086,
		},
		Assert: func() {
			record, err := data.EvmTransactionRecordRepoClient.FindByTxHash(context.Background(), biz.GetTableName(CHAIN_NAME), "0x15806fb2a2983f39f898079fc51295bf263a1c0be84e9286eb2d8ab28a46d852")
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, record.BlockNumber, 16625722)
			assert.Equal(t, record.Nonce, int64(1854))
			assert.Equal(t, record.TransactionHash, "0x15806fb2a2983f39f898079fc51295bf263a1c0be84e9286eb2d8ab28a46d852")
			assert.Equal(t, record.FromAddress, "0x1EaCa1277BcDFa83E60658D8938B3D63cD3E63C1")
			assert.Equal(t, record.ToAddress, "0xe0B6e0a0e1ddB6476afd64Da80B990c24DEf31E7")
			assert.Equal(t, record.FromUid, "1")
			//assert.Equal(t, record.ToUid, "")
			assert.Equal(t, record.FeeAmount, decimal.NewFromInt(2526525259639220))
			assert.Equal(t, record.Amount, decimal.NewFromInt(0))
			assert.Equal(t, record.Status, biz.SUCCESS)
			assert.Equal(t, record.ContractAddress, "0xe0B6e0a0e1ddB6476afd64Da80B990c24DEf31E7")
			assert.Equal(t, record.TransactionType, biz.CONTRACT)
			assert.Equal(t, record.BlockNumber, 16625722)
		},
	})
}

func TestPendingTx(t *testing.T) {
	txHash := "0x06fa3ef6bc6e75d0cd7bbe86fe1fe1b66b3b0983d2cdc51da47bda3d96a48af1"
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
			"0xE7e058A59F2Bd68418f355f0C1387f135304C09C": "123",
		},
		PendingTxs: []*v1.TransactionReq{
			{
				Uid:             "123",
				ChainName:       CHAIN_NAME,
				TransactionHash: txHash,
				Status:          "pending",
				FromAddress:     "0xE7e058A59F2Bd68418f355f0C1387f135304C09C",
				ToAddress:       "7E1QHT8haaDRvDF4XxFGWtSJW2i5sDfna8SmmyzQVxv6",
				FeeAmount:       "209870214626176",
				Amount:          "10000000000000000000",
				TransactionType: "native",
			},
		},
		BeforePrepare: func() {
			_, err := data.EvmTransactionRecordRepoClient.Delete(context.TODO(), tableName, req)
			assert.NoError(t, err)
		},
		AfterPrepare: func() {
			record, err := data.EvmTransactionRecordRepoClient.FindByTxHash(context.TODO(), tableName, txHash)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, biz.PENDING, record.Status)
		},
		Assert: func() {
			record, err := data.EvmTransactionRecordRepoClient.FindByTxHash(context.TODO(), tableName, txHash)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, biz.SUCCESS, record.Status)
		},
	})
}

var amount, _ = decimal.NewFromString("49977000000000000000000")
var txRecords = []*data.EvmTransactionRecord{{
	BlockNumber:     15953574,
	Nonce:           int64(2350642),
	TransactionHash: "0x4f0400c14b1f69877a027fa40966dad194b924b24e628fb1de223ca5b4e32f7d",
	FromAddress:     "0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88",
	ToAddress:       "0x451aDe6606AeD956a843235E763208Af0080385E",
	FromUid:         "1",
	ToUid:           "1",
	FeeAmount:       decimal.NewFromInt(1132340000000000),
	Amount:          amount,
	Status:          biz.SUCCESS,
	TxTime:          1668251771,
	ContractAddress: "0xf4d2888d29D722226FafA5d9B24F9164c092421E",
	TransactionType: biz.TRANSFER,
	ParseData:       "{\"evm\":{\"nonce\":\"2350642\",\"type\":\"0\"},\"token\":{\"address\":\"0xf4d2888d29D722226FafA5d9B24F9164c092421E\",\"amount\":\"49977000000000000000000\",\"decimals\":18,\"symbol\":\"LOOKS\"}}",
	Data:            "a9059cbb000000000000000000000000451ade6606aed956a843235e763208af0080385e000000000000000000000000000000000000000000000a954233868171440000",
	ClientData:      "{\"sendTime\":1677039146259,\"txInput\":\"\"}",
}}

var txNftRecords = []*data.EvmTransactionRecord{{
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
	ParseData:       "{\"evm\":{\"nonce\":\"10\",\"type\":\"2\"},\"token\":{\"address\":\"0x495f947276749Ce646f68AC8c248420045cb7b5e\",\"amount\":\"1\",\"decimals\":0,\"symbol\":\"OPENSTORE\",\"collection_name\":\"Red Panda Pals NFT Collection\",\"token_type\":\"ERC1155\",\"token_id\":\"113131626276964395896987417252704130741588147670481760275076171533867519311873\",\"item_name\":\"Red Panda Pal #255\",\"item_uri\":\"https://i.seadn.io/gae/z1cmnY8oAYmUtnbpdC_77YIzdbj4J8t_Sut4WtPrHCrS74D7JVYYNLRN0C4UgIwGM4V-lSiwyaWM2waOv36GDFhxQkxtJ5GnP5OtaZU?w=500&auto=format\"}}",
	Data:            "f242432a000000000000000000000000580bad3785e99e679e8f128aad8f47dd85b26a0d00000000000000000000000011c2254143310e834640f0fdafd6e44516340e40fa1e3b383ce51310492056a038770cac39506124000000000003190000000001000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000360c6ebe",
}}

var txPendingRecords = []*data.EvmTransactionRecord{{
	Nonce:           int64(2350642),
	TransactionHash: "0x4f0400c14b1f69877a027fa40966dad194b924b24e628fb1de223ca5b4e32f71",
	FromAddress:     "0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88",
	ToAddress:       "0x451aDe6606AeD956a843235E763208Af0080385E",
	FromUid:         "1",
	ToUid:           "1",
	FeeAmount:       decimal.NewFromInt(1132340000000000),
	Amount:          amount,
	Status:          biz.PENDING,
	ContractAddress: "0xf4d2888d29D722226FafA5d9B24F9164c092421E",
	TransactionType: biz.TRANSFER,
	ClientData:      "{\"sendTime\":1677039146259,\"txInput\":\"\"}",
}, {
	Nonce:           int64(2350642),
	TransactionHash: "0x4f0400c14b1f69877a027fa40966dad194b924b24e628fb1de223ca5b4e32f72",
	FromAddress:     "0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88",
	ToAddress:       "0x451aDe6606AeD956a843235E763208Af0080385E",
	FromUid:         "1",
	ToUid:           "1",
	FeeAmount:       decimal.NewFromInt(1132340000000000),
	Amount:          decimal.NewFromInt(0),
	Status:          biz.PENDING,
	ContractAddress: "0xf4d2888d29D722226FafA5d9B24F9164c092421E",
	TransactionType: biz.TRANSFER,
	ClientData:      "{\"sendTime\":1677039146259,\"txInput\":\"\"}",
}}

func TestNonce(t *testing.T) {
	fromAddress := "0x000000000000000000000000000000000000000d"
	var noChangeRecord = []*data.EvmTransactionRecord{
		{
			BlockNumber:     16073725,
			Nonce:           int64(111110),
			TransactionHash: "0xcf9db3314b103eb91e49346b6b65dd5594c2cfab8a113fa575d80ab3219a860a",
			FromAddress:     fromAddress,
			ToAddress:       "0x11C2254143310e834640F0FDafd6e44516340E41",
			FromUid:         "1",
			ToUid:           "",
			FeeAmount:       decimal.NewFromInt(551454279931832),
			Amount:          decimal.NewFromInt(1),
			Status:          biz.SUCCESS,
			TxTime:          1669701707,
			ContractAddress: "0x495f947276749Ce646f68AC8c248420045cb7b5e",
			TransactionType: biz.TRANSFERNFT,
		},
	}

	var txNonceRecord = []*data.EvmTransactionRecord{
		{
			BlockNumber:     16073725,
			Nonce:           int64(10),
			TransactionHash: "0xcf9db3314b103eb91e49346b6b65dd5594c2cfab8a113fa575d80ab3219a860a",
			FromAddress:     fromAddress,
			ToAddress:       "0x11C2254143310e834640F0FDafd6e44516340E41",
			FromUid:         "1",
			ToUid:           "",
			FeeAmount:       decimal.NewFromInt(551454279931832),
			Amount:          decimal.NewFromInt(1),
			Status:          biz.SUCCESS,
			TxTime:          1669701707,
			ContractAddress: "0x495f947276749Ce646f68AC8c248420045cb7b5e",
			TransactionType: biz.TRANSFERNFT,
		}, {
			BlockNumber:     16073725,
			Nonce:           int64(11),
			TransactionHash: "0xcf9db3314b103eb91e49346b6b65dd5594c2cfab8a113fa575d80ab3219a860a",
			FromAddress:     fromAddress,
			ToAddress:       "0x11C2254143310e834640F0FDafd6e44516340E42",
			FromUid:         "1",
			ToUid:           "",
			FeeAmount:       decimal.NewFromInt(551454279931832),
			Amount:          decimal.NewFromInt(1),
			Status:          biz.SUCCESS,
			TxTime:          1669701707,
			ContractAddress: "0x495f947276749Ce646f68AC8c248420045cb7b5e",
			TransactionType: biz.TRANSFERNFT,
		},
		//{
		//	BlockNumber:     16073725,
		//	Nonce:           int64(1000),
		//	TransactionHash: "0xcf9db3314b103eb91e49346b6b65dd5594c2cfab8a113fa575d80ab3219a8601",
		//	FromAddress:     fromAddress,
		//	ToAddress:       "0x11C2254143310e834640F0FDafd6e44516340E40",
		//	FromUid:         "1",
		//	ToUid:           "",
		//	FeeAmount:       decimal.NewFromInt(551454279931832),
		//	Amount:          decimal.NewFromInt(1),
		//	Status:          biz.FAIL,
		//	TxTime:          1669701707,
		//	ContractAddress: "0x495f947276749Ce646f68AC8c248420045cb7b5e",
		//	TransactionType: biz.TRANSFERNFT,
		//},
		{
			BlockNumber:     16073725,
			Nonce:           int64(12),
			TransactionHash: "0xcf9db3314b103eb91e49346b6b65dd5594c2cfab8a113fa575d80ab3219a8602",
			FromAddress:     fromAddress,
			ToAddress:       "0x11C2254143310e834640F0FDafd6e44516340E42",
			FromUid:         "1",
			ToUid:           "",
			FeeAmount:       decimal.NewFromInt(551454279931832),
			Amount:          decimal.NewFromInt(1),
			Status:          biz.PENDING,
			TxTime:          1669701707,
			ContractAddress: "0x495f947276749Ce646f68AC8c248420045cb7b5e",
			TransactionType: biz.TRANSFERNFT,
		},
	}
	configs := "configs"
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   configs,
		Test: func(plat *platform.Bootstrap) {
			nonceKey := biz.ADDRESS_DONE_NONCE + CHAIN_NAME + ":" + fromAddress

			data.RedisClient.Set(nonceKey, 9, 0)
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			patches := gomonkey.ApplyFuncReturn(ethereum.ExecuteRetry, uint64(12), nil)
			defer patches.Reset()

			ethereum.HandleUserNonce(CHAIN_NAME, *client.(*ethereum.Client), txNonceRecord)
			nonceStr, _ := data.RedisClient.Get(nonceKey).Result()
			assert.Equal(t, "11", nonceStr)

			ethereum.HandleUserNonce(CHAIN_NAME, *client.(*ethereum.Client), noChangeRecord)
			nonceNoStr, _ := data.RedisClient.Get(nonceKey).Result()
			assert.NotEqual(t, "111110", nonceNoStr)
		},
	})
}

func TestDappApprove(t *testing.T) {
	configs := "configs"
	txHash := "0xf426034aa4366f6667ee81bd447dfffb32c9998c3bf3da10348c6749a3509526"

	ptesting.RunTest(ptesting.Preparation{
		ChainName:       CHAIN_NAME,
		Configs:         configs,
		Prefetch:        true,
		SourceFromChain: true,
		RawTxType:       reflect.TypeOf(new(ethereum.Transaction)),
		Users: map[string]string{
			"0x6834A3781fFc2D3c976038555fa7Cf6bfcbb48aB": "approveTestUserId",
		},
		IndexBlockNumbers: []uint64{
			14984276,
			//16617086,
		},
		Assert: func() {
			record, err := data.EvmTransactionRecordRepoClient.FindByTxHash(context.Background(), biz.GetTableName(CHAIN_NAME), txHash)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.NotNil(t, record.TransactionType, biz.APPROVE)
			var txRecord []*data.EvmTransactionRecord
			txRecord = append(txRecord, record)
			biz.DappApproveFilter(CHAIN_NAME, txRecord)

			dappRecord, _ := data.DappApproveRecordRepoClient.FindByLasTxtHash(context.Background(), txHash)

			assert.Equal(t, dappRecord.Address, record.FromAddress)
			assert.Equal(t, record.TransactionHash, dappRecord.LastTxhash)
			assert.Equal(t, dappRecord.Amount, record.Amount.String())
			assert.Equal(t, record.ContractAddress, dappRecord.Token)
			assert.Equal(t, record.ToAddress, dappRecord.ToAddress)
		},
	})
}

func TestUpdateDappApprove(t *testing.T) {
	//select * from bsc_transaction_record where from_address = '0xB157a7209aAA96B6fCBB71d20154aBAAb76802Ed' and to_address ='0x6A7873e00eB39277dCFe88919481648317832cB0' and transaction_type = 'approve' and contract_address = '0x1CE0c2827e2eF14D5C4f29a091d735A204794041';
	configs := "configs"
	//授权
	txApproveHash := "0xa8fadfd5d1e428899ea432069a7287ee825e40c1399cc8ce9a4f53aa0c779ea7"
	// update 敞口
	txHash := "0x10d638e14d4ce482a48f0a1bd3bc2eb509cda63c83508d6c2d55c16e48ed9c88"

	ptesting.RunTest(ptesting.Preparation{
		ChainName: "BSC",
		Configs:   configs,
		Prefetch:  true,
		Users: map[string]string{
			"0xB157a7209aAA96B6fCBB71d20154aBAAb76802Ed": "1",
			"0x6A7873e00eB39277dCFe88919481648317832cB0": "2",
		},
		IndexBlockNumbers: []uint64{},
		TokenPrices: []ptesting.TokenPrice{
			{
				TokenAddress: "0x3Ac92FF3C5bf439A87b26741ac6Db1793Ba1b025",
				Currency:     "CNY",
				Price:        "1",
			},
			{
				TokenAddress: "0x1CE0c2827e2eF14D5C4f29a091d735A204794041",
				Currency:     "CNY",
				Price:        "1",
			},
		},
		Tokens: []types.TokenInfo{
			{
				Address:  "0x3Ac92FF3C5bf439A87b26741ac6Db1793Ba1b025",
				Amount:   "10",
				Decimals: 6,
				Symbol:   "Fake",
			},
			{
				Address:  "0x1CE0c2827e2eF14D5C4f29a091d735A204794041",
				Amount:   "1",
				Decimals: 6,
				Symbol:   "Fake",
			},
		},
		Test: func(plat *platform.Bootstrap) {
			var ptxhash []*data.EvmTransactionRecord

			ptxhash = append(ptxhash, &data.EvmTransactionRecord{
				TransactionHash: txApproveHash,
				Status:          "pending",
			}, &data.EvmTransactionRecord{
				TransactionHash: txHash,
				Status:          "pending"})

			_, err := data.EvmTransactionRecordRepoClient.BatchSaveOrUpdateSelective(context.TODO(), biz.GetTableName("BSC"), ptxhash)
			patches := gomonkey.ApplyFunc(ethereum.HandlePendingRecord, func(chainName string, client ethereum.Client, txRecords []*data.EvmTransactionRecord) {})
			defer patches.Reset()

			plat.Spider.SealPendingTransactions(plat.Platform.CreateBlockHandler(1000))

			aRecord, aErr := data.EvmTransactionRecordRepoClient.FindByTxHash(context.TODO(), biz.GetTableName("BSC"), txApproveHash)
			records, err := data.EvmTransactionRecordRepoClient.FindByTxHashLike(context.TODO(), biz.GetTableName("BSC"), txHash)

			assert.NoError(t, err)
			assert.NotNil(t, records)
			assert.NoError(t, aErr)
			assert.NotNil(t, aRecord)

			var txRecord []*data.EvmTransactionRecord
			txRecord = append(txRecord, aRecord)

			for _, rdd := range records {
				txRecord = append(txRecord, rdd)
			}

			biz.DappApproveFilter("BSC", txRecord)
			dappRecord, _ := data.DappApproveRecordRepoClient.FindByLasTxtHash(context.Background(), txHash)

			assert.Equal(t, dappRecord.Address, aRecord.FromAddress)
			amount := dappRecord.Amount
			a := ""
			for _, r := range records {
				if r.FromAddress == dappRecord.Address && r.ToAddress == dappRecord.ToAddress && r.ContractAddress == dappRecord.Token {
					ra := r.Amount
					dr, _ := decimal.NewFromString(amount)
					a = ra.Add(dr).String()
				}
			}
			assert.Equal(t, dappRecord.Original, aRecord.Amount.String())
			assert.Equal(t, dappRecord.Original, a)

		},
	})

}

func TestNftApprove(t *testing.T) {
	configs := "configs"
	txHash := "0xf426034aa4366f6667ee81bd447dfffb32c9998c3bf3da10348c6749a3509526"

	ptesting.RunTest(ptesting.Preparation{
		ChainName:       CHAIN_NAME,
		Configs:         configs,
		Prefetch:        true,
		SourceFromChain: true,
		RawTxType:       reflect.TypeOf(new(ethereum.Transaction)),
		Users: map[string]string{
			"0x6834A3781fFc2D3c976038555fa7Cf6bfcbb48aB": "approveTestUserId",
		},
		IndexBlockNumbers: []uint64{
			14984276,
			//16617086,
		},
		Assert: func() {
			record, err := data.EvmTransactionRecordRepoClient.FindByTxHash(context.Background(), biz.GetTableName(CHAIN_NAME), txHash)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.NotNil(t, record.TransactionType, biz.APPROVENFT)
			var txRecord []*data.EvmTransactionRecord
			txRecord = append(txRecord, record)
			biz.DappApproveFilter(CHAIN_NAME, txRecord)

			dappRecord, _ := data.DappApproveRecordRepoClient.FindByLasTxtHash(context.Background(), txHash)

			assert.Equal(t, dappRecord.Address, record.FromAddress)
			assert.Equal(t, record.TransactionHash, dappRecord.LastTxhash)
			assert.Equal(t, dappRecord.Amount, record.Amount.String())
			assert.Equal(t, record.ContractAddress, dappRecord.Token)
			assert.Equal(t, record.ToAddress, dappRecord.ToAddress)
		},
	})
}

func TestHandleTokenPush(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "0x451aDe6606AeD956a843235E763208Af0080385E",
		TokenAddressList: []string{"0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
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
			ethereum.HandleTokenPush(CHAIN_NAME, *client.(*ethereum.Client), txRecords)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.NoError(t, err)
			assert.Equal(t, result, "{\"chainName\":\"ETH\",\"uid\":\"1\",\"address\":\"0x451aDe6606AeD956a843235E763208Af0080385E\",\"tokenAddress\":\"0xf4d2888d29D722226FafA5d9B24F9164c092421E\",\"decimals\":18,\"symbol\":\"LOOKS\"}")
			assert.Equal(t, strings.Contains(result, CHAIN_NAME), true)
			assert.Equal(t, strings.Contains(result, "0x451aDe6606AeD956a843235E763208Af0080385E"), true)
			assert.Equal(t, strings.Contains(result, "0xf4d2888d29D722226FafA5d9B24F9164c092421E"), true)
		},
	})
}

func TestHandleTokenPush1(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "0x451aDe6606AeD956a843235E763208Af0080385E",
		TokenAddressList: []string{"0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
	}
	var txRecordList []*data.EvmTransactionRecord
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
			ethereum.HandleTokenPush(CHAIN_NAME, *client.(*ethereum.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleTokenPush2(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "0x451aDe6606AeD956a843235E763208Af0080385E",
		TokenAddressList: []string{"0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
	}
	var txRecordList []*data.EvmTransactionRecord
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
			ethereum.HandleTokenPush(CHAIN_NAME, *client.(*ethereum.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleTokenPush3(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "0x451aDe6606AeD956a843235E763208Af0080385E",
		TokenAddressList: []string{"0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
	}
	var txRecordList []*data.EvmTransactionRecord
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
			ethereum.HandleTokenPush(CHAIN_NAME, *client.(*ethereum.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleTokenPush4(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		Address:          "0x451aDe6606AeD956a843235E763208Af0080385E",
		TokenAddressList: []string{"0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
	}
	var txRecordList []*data.EvmTransactionRecord
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
			ethereum.HandleTokenPush(CHAIN_NAME, *client.(*ethereum.Client), txRecordList)
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
	var txRecordList []*data.EvmTransactionRecord
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
			ethereum.HandleTokenPush(CHAIN_NAME, *client.(*ethereum.Client), txRecordList)
			result, err := data.RedisQueueManager.QueueGet(TOKEN_INFO_QUEUE_TOPIC, biz.TOKEN_INFO_QUEUE_PARTITION, TIMEOUT)
			assert.Equal(t, err, redis.Nil)
			assert.Equal(t, result, "")
		},
	})
}

func TestHandleUserAsset(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88", "0x451aDe6606AeD956a843235E763208Af0080385E"},
		TokenAddressList: []string{"", "0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
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
			c := client.(*ethereum.Client)

			outputs := []gomonkey.OutputCell{
				{
					Values: gomonkey.Params{map[string]interface{}{"0xf4d2888d29D722226FafA5d9B24F9164c092421E": "5"}, nil},
				},
				{
					Values: gomonkey.Params{map[string]interface{}{"0xf4d2888d29D722226FafA5d9B24F9164c092421E": "10"}, nil},
				},
			}
			patches := gomonkey.ApplyMethodSeq(reflect.TypeOf(c), "BatchTokenBalance", outputs)
			patches.ApplyMethod(reflect.TypeOf(c), "GetBalance", func(_ *ethereum.Client, _ string) (string, error) {
				return "1", nil
			})
			// 执行完毕后释放桩序列
			defer patches.Reset()

			ethereum.HandleUserAsset(CHAIN_NAME, *c, txRecords)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 3)
			for _, asset := range list {
				if asset.Address == "0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88" {
					if asset.TokenAddress == "" {
						assert.Equal(t, asset.Balance, "1")
						assert.Equal(t, asset.Symbol, "ETH")
					} else if asset.TokenAddress == "0xf4d2888d29D722226FafA5d9B24F9164c092421E" {
						assert.Equal(t, asset.Balance, "5")
						assert.Equal(t, asset.Symbol, "LOOKS")
					}
				} else if asset.Address == "0x451aDe6606AeD956a843235E763208Af0080385E" {
					assert.Equal(t, asset.TokenAddress, "0xf4d2888d29D722226FafA5d9B24F9164c092421E")
					assert.Equal(t, asset.Balance, "10")
					assert.Equal(t, asset.Symbol, "LOOKS")
				}

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
				assert.Equal(t, asset.Decimals, int32(18))
			}
		},
	})
}

func TestHandleUserAsset1(t *testing.T) {
	req := &data.AssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88", "0x451aDe6606AeD956a843235E763208Af0080385E"},
		TokenAddressList: []string{"", "0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
	}
	var txRecordList []*data.EvmTransactionRecord
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
			c := client.(*ethereum.Client)

			ethereum.HandleUserAsset(CHAIN_NAME, *c, txRecordList)
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
		AddressList:      []string{"0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88", "0x451aDe6606AeD956a843235E763208Af0080385E"},
		TokenAddressList: []string{"", "0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
	}
	var txRecordList []*data.EvmTransactionRecord
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
			c := client.(*ethereum.Client)

			patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetBalance", func(_ *ethereum.Client, _ string) (string, error) {
				return "1", nil
			})
			// 执行完毕后释放桩序列
			defer patches.Reset()

			ethereum.HandleUserAsset(CHAIN_NAME, *c, txRecordList)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 1)
			for _, asset := range list {
				assert.Equal(t, asset.Address, "0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88")
				assert.Equal(t, asset.TokenAddress, "")
				assert.Equal(t, asset.Balance, "1")
				assert.Equal(t, asset.Symbol, "ETH")

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
				assert.Equal(t, asset.Decimals, int32(18))
			}
		},
	})
}

func TestHandleUserAsset3(t *testing.T) {
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
			c := client.(*ethereum.Client)

			patches := gomonkey.ApplyMethod(reflect.TypeOf(c), "GetBalance", func(_ *ethereum.Client, _ string) (string, error) {
				return "1", nil
			})
			// 执行完毕后释放桩序列
			defer patches.Reset()

			ethereum.HandleUserAsset(CHAIN_NAME, *c, txNftRecords)
			list, err := data.UserAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 1)
			for _, asset := range list {
				assert.Equal(t, asset.Address, "0x580Bad3785E99E679e8F128Aad8F47DD85b26A0D")
				assert.Equal(t, asset.TokenAddress, "")
				assert.Equal(t, asset.Balance, "1")
				assert.Equal(t, asset.Symbol, "ETH")

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
				assert.Equal(t, asset.Decimals, int32(18))
			}
		},
	})
}

func TestHandleUserStatistic(t *testing.T) {
	req := &data.StatisticRequest{
		ChainName:         CHAIN_NAME,
		TokenAddressList:  []string{"0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
		FundDirectionList: []int16{3},
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		TokenPrices: []ptesting.TokenPrice{
			{
				TokenAddress: "0xf4d2888d29D722226FafA5d9B24F9164c092421E",
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
			ethereum.HandleUserStatistic(CHAIN_NAME, *client.(*ethereum.Client), txRecords)
			list, err := data.TransactionStatisticRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list) >= 1, true)
			for _, asset := range list {
				assert.Equal(t, asset.TokenAddress, "0xf4d2888d29D722226FafA5d9B24F9164c092421E")
				assert.Equal(t, asset.FundDirection, int16(3))

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
			}
		},
	})
}

func TestHandleUserStatistic1(t *testing.T) {
	req := &data.StatisticRequest{
		ChainName:         CHAIN_NAME,
		TokenAddressList:  []string{"0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
		FundDirectionList: []int16{3},
	}
	var txRecordList []*data.EvmTransactionRecord
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
			ethereum.HandleUserStatistic(CHAIN_NAME, *client.(*ethereum.Client), txRecordList)
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
		TokenAddressList:  []string{"0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
		FundDirectionList: []int16{3},
	}
	var txRecordList []*data.EvmTransactionRecord
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
			ethereum.HandleUserStatistic(CHAIN_NAME, *client.(*ethereum.Client), txRecordList)
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
		AddressList:      []string{"0x580Bad3785E99E679e8F128Aad8F47DD85b26A0D", "0x11C2254143310e834640F0FDafd6e44516340E40"},
		TokenAddressList: []string{"0x495f947276749Ce646f68AC8c248420045cb7b5e"},
		TokenIdList:      []string{"113131626276964395896987417252704130741588147670481760275076171533867519311873"},
	}
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		NFTs: []*clientv1.GetNftReply_NftInfoResp{
			{
				TokenId:        "113131626276964395896987417252704130741588147670481760275076171533867519311873",
				TokenAddress:   "0x495f947276749Ce646f68AC8c248420045cb7b5e",
				Name:           "Red Panda Pal #255",
				Symbol:         "Red Panda Pal #255",
				TokenType:      biz.ERC1155,
				ImageURL:       "https://i.seadn.io/gae/z1cmnY8oAYmUtnbpdC_77YIzdbj4J8t_Sut4WtPrHCrS74D7JVYYNLRN0C4UgIwGM4V-lSiwyaWM2waOv36GDFhxQkxtJ5GnP5OtaZU?w=500&auto=format",
				Chain:          CHAIN_NAME,
				CollectionName: "Red Panda Pals NFT Collection",
				NftName:        "Red Panda Pal #255",
			},
		},
		BeforePrepare: func() {
			_, err := data.UserNftAssetRepoClient.Delete(context.Background(), req)
			assert.NoError(t, err)
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			c := client.(*ethereum.Client)

			outputs := []gomonkey.OutputCell{
				{
					Values: gomonkey.Params{"0", nil},
				},
				{
					Values: gomonkey.Params{"1", nil},
				},
			}
			patches := gomonkey.ApplyMethodSeq(reflect.TypeOf(c), "Erc1155Balance", outputs)
			// 执行完毕后释放桩序列
			defer patches.Reset()

			ethereum.HandleUserNftAsset(false, CHAIN_NAME, *c, txNftRecords)
			list, err := data.UserNftAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 2)
			for _, asset := range list {
				if asset.Address == "0x580Bad3785E99E679e8F128Aad8F47DD85b26A0D" {
					assert.Equal(t, asset.Balance, "0")
				} else if asset.Address == "0x11C2254143310e834640F0FDafd6e44516340E40" {
					assert.Equal(t, asset.Balance, "1")
				}

				assert.Equal(t, asset.ChainName, CHAIN_NAME)
				assert.Equal(t, asset.TokenAddress, "0x495f947276749Ce646f68AC8c248420045cb7b5e")
				assert.Equal(t, asset.TokenId, "113131626276964395896987417252704130741588147670481760275076171533867519311873")
				assert.Equal(t, asset.TokenType, biz.ERC1155)
				assert.Equal(t, asset.CollectionName, "Red Panda Pals NFT Collection")
				assert.Equal(t, asset.ItemName, "Red Panda Pal #255")
				assert.Equal(t, asset.ItemUri, "https://i.seadn.io/gae/z1cmnY8oAYmUtnbpdC_77YIzdbj4J8t_Sut4WtPrHCrS74D7JVYYNLRN0C4UgIwGM4V-lSiwyaWM2waOv36GDFhxQkxtJ5GnP5OtaZU?w=500&auto=format")
			}
		},
	})
}

func TestHandleUserNftAsset1(t *testing.T) {
	req := &data.NftAssetRequest{
		ChainName:        CHAIN_NAME,
		AddressList:      []string{"0x580Bad3785E99E679e8F128Aad8F47DD85b26A0D", "0x11C2254143310e834640F0FDafd6e44516340E40"},
		TokenAddressList: []string{"0x495f947276749Ce646f68AC8c248420045cb7b5e"},
		TokenIdList:      []string{"113131626276964395896987417252704130741588147670481760275076171533867519311873"},
	}
	var txRecordList []*data.EvmTransactionRecord
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
			c := client.(*ethereum.Client)

			ethereum.HandleUserNftAsset(false, CHAIN_NAME, *c, txRecordList)
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
		AddressList:      []string{"0x580Bad3785E99E679e8F128Aad8F47DD85b26A0D", "0x11C2254143310e834640F0FDafd6e44516340E40"},
		TokenAddressList: []string{"0x495f947276749Ce646f68AC8c248420045cb7b5e"},
		TokenIdList:      []string{"113131626276964395896987417252704130741588147670481760275076171533867519311873"},
	}
	var txRecordList []*data.EvmTransactionRecord
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
			c := client.(*ethereum.Client)

			ethereum.HandleUserNftAsset(false, CHAIN_NAME, *c, txRecordList)
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
		AddressList:      []string{"0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88", "0x451aDe6606AeD956a843235E763208Af0080385E"},
		TokenAddressList: []string{"", "0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
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
			c := client.(*ethereum.Client)

			ethereum.HandleUserNftAsset(false, CHAIN_NAME, *c, txRecords)
			list, err := data.UserNftAssetRepoClient.List(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, list)
			assert.Equal(t, len(list), 0)
		},
	})
}

func TestCreatePendingTx(t *testing.T) {
	configs := "configs"
	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   configs,
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()
			p := v1.NewTransactionClient(conn)
			req := new(v1.TransactionReq)
			req.ChainName = CHAIN_NAME
			req.Uid = "yanding"
			req.TransactionHash = "woshijiahash"
			req.Status = "pending"
			req.FromAddress = "0xE2101f6B5FE0855F7E5809766fCc2E6A985C2F00"
			req.ToAddress = "0x1b02da8cb0d097eb8d57a175b88c7d8b47997506"
			req.Amount = "10000000000000"
			req.FeeAmount = "8414891296532942"
			req.TransactionType = "contrac"
			req.DappData = "{\"origin\":\"https://sushiv2.openblock.vip\",\"icon\":\"https://obapps.243096.com/images/dapp/ab75120c50cdd0fbba12761a7ca4007a.png\",\"dappName\":\"Sushi v2\",\"supportChains\":[\"eth\",\"arbitrum\",\"bsc\",\"avalanche\",\"fantom\",\"arbitrumnova\",\"polygon\",\"optimism\"],\"channel\":\"inner\",\"href\":\"\"}"
			req.ParseData = "{\"evm\":{\"nonce\":\"40\",\"type\":0}}"
			req.ClientData = "{\"sendTime\":1677060993671,\"txInput\":\"\"}"
			req.FeeData = "{\"max_fee_per_gas\":\"0\",\"max_priority_fee_per_gas\":\"0\",\"gas_limit\":\"163969\",\"gas_price\":\"51320013518\"}"
			response, err := p.CreateRecordFromWallet(context.Background(), req)

			assert.NoError(t, err)
			assert.NotNil(t, response)
			assert.Equal(t, response.Status, true)
			assert.Equal(t, response.Code, uint64(200))
			assert.Equal(t, response.Mes, "")
		},
	})
}

func TestGetNonce(t *testing.T) {
	t.Skip("Nonce updated in background and the background tasks had been replaced by stub")
	configs := "configs"
	//跟上面test 的 nonce保持一致
	address := "0x000000000000000000000000000000000000000d"

	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   configs,
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()
			p := v1.NewTransactionClient(conn)

			req := new(v1.NonceReq)
			req.ChainName = CHAIN_NAME
			req.Address = address
			response, err := p.GetNonce(context.Background(), req)

			assert.NoError(t, err)
			assert.NotNil(t, response)
			assert.Equal(t, response.Ok, true)
			assert.Equal(t, response.Nonce, int64(12))
		},
	})
}
