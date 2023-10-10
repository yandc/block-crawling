package utxo

import (
	v1 "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/platform"
	"block-crawling/internal/platform/bitcoin"
	"block-crawling/internal/ptesting"
	"block-crawling/internal/types"
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

const CHAIN_NAME = "BTC"
const CONFIG_PATH = "configs"
const TIMEOUT = 5 * time.Second

func TestIndexBlock(t *testing.T) {
	ptesting.RunTest(ptesting.Preparation{
		ChainName:       CHAIN_NAME,
		Configs:         CONFIG_PATH,
		Prefetch:        true,
		SourceFromChain: true,
		RawTxType:       reflect.TypeOf(new(types.Tx)),
		Users: map[string]string{
			"1LuHpBdxUd6c1zPKzRsqkKFQTibirTsJWY": "btctest",
			"16cLsRM3iMhBqwMtXrc6GC2NdutbABbkAq": "btctest2",
		},
		IndexBlockNumbers: []uint64{
			778843,
		},
		Assert: func() {
			record, err := data.BtcTransactionRecordRepoClient.FindByTxHash(context.Background(), biz.GetTableName(CHAIN_NAME), "61b89eaa060ba90cdd06c318327d50d9a2c041d1b05f94a65b8bb6f62baa50c2")
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, record.ToAddress, "16cLsRM3iMhBqwMtXrc6GC2NdutbABbkAq")
			assert.Equal(t, record.FromAddress, "1LuHpBdxUd6c1zPKzRsqkKFQTibirTsJWY")
			assert.Equal(t, record.TransactionHash, "61b89eaa060ba90cdd06c318327d50d9a2c041d1b05f94a65b8bb6f62baa50c2")
			assert.Equal(t, record.BlockNumber, 778843)
		},
	})
}

func TestPendingTx(t *testing.T) {
	txHash := "21a82e82ccb2aae1b8eb51ea00dddc1994c06ee90ecb6ff4c6de78196231248c"
	ptesting.RunTest(ptesting.Preparation{
		ChainName:         CHAIN_NAME,
		Configs:           CONFIG_PATH,
		IndexBlockNumbers: []uint64{},
		Users: map[string]string{
			"bc1qy0zve0pk98yluy4cx2xk9m6g95fw66u553te97": "123",
		},
		PendingTxs: []*v1.TransactionReq{
			{
				Uid:             "123",
				ChainName:       CHAIN_NAME,
				TransactionHash: txHash,
				Status:          "pending",
				FromAddress:     "bc1qy0zve0pk98yluy4cx2xk9m6g95fw66u553te97",
				ToAddress:       "bc1q3m5d7e7l7ekgfx9v5z7mw02t9vul9upvqhkhlr",
				Amount:          "0.04078303",
				FeeAmount:       "0.00005",
				TransactionType: "native",
			},
		},

		BeforePrepare: func() {
			_, err := data.BtcTransactionRecordRepoClient.DeleteByTxHash(context.TODO(), biz.GetTableName(CHAIN_NAME), txHash)
			assert.NoError(t, err)
		},
		AfterPrepare: func() {
			record, err := data.BtcTransactionRecordRepoClient.FindByTxHash(context.TODO(), biz.GetTableName(CHAIN_NAME), txHash)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, "pending", record.Status)
		},
		Assert: func() {
			record, err := data.BtcTransactionRecordRepoClient.FindByTxHash(context.TODO(), biz.GetTableName(CHAIN_NAME), txHash)
			assert.NoError(t, err)
			assert.NotNil(t, record)
			assert.Equal(t, "success", record.Status)
		},
	})
}

/*
*******************************************************************************************************************************

		0.mock 更新对应地址的utxo
		1.插入pending 更新本地utxo 此处用的是 mock 所以 这步默认成功了
		2.获取本utxo
	    3.获取 节点utox
	 ********************************************************************************************************************************
*/
func TestGetUnspentTx(t *testing.T) {
	from := "bc1qqfjl06tqpvquxcp4anjtd24rpqhn59zrelj2vf"

	var txRecords = []*data.BtcTransactionRecord{{
		BlockNumber:     15953574,
		TransactionHash: "0x4f0400c14b1f69877a027fa40966dad194b924b24e628fb1de223ca5b4e32f7d",
		FromAddress:     from,
		ToAddress:       "0x451aDe6606AeD956a843235E763208Af0080385E",
		FromUid:         "pendingUid",
		ToUid:           "",
		FeeAmount:       decimal.NewFromInt(1132340000000000),
		Amount:          decimal.NewFromInt(100),
		Status:          biz.SUCCESS,
		TxTime:          1668251771,
		ClientData:      "{\"sendTime\":1677039146259,\"txInput\":\"\"}",
	}}

	ptesting.RunTest(ptesting.Preparation{
		ChainName: CHAIN_NAME,
		Configs:   CONFIG_PATH,
		Users: map[string]string{
			"bc1qqfjl06tqpvquxcp4anjtd24rpqhn59zrelj2vf": "pendingUid",
		},
		Test: func(plat *platform.Bootstrap) {
			client := plat.Platform.CreateClient(plat.Conf.RpcURL[0])
			bitcoin.UnspentTx(CHAIN_NAME, *client.(*bitcoin.Client), txRecords)

			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()
			p := v1.NewTransactionClient(conn)

			req := new(v1.TransactionReq)
			req.ChainName = CHAIN_NAME
			req.Uid = "yanding"
			req.TransactionHash = "jjqqkk7788"
			req.Status = "pending"
			req.FromAddress = from
			req.ToAddress = "0x1b02da8cb0d097eb8d57a175b88c7d8b47997506"
			req.Amount = "10000000000000"
			req.FeeAmount = "8414891296532942"
			req.TransactionType = "native"
			_, err1 := data.BtcTransactionRecordRepoClient.DeleteByTxHash(context.TODO(), biz.GetTableName(CHAIN_NAME), req.TransactionHash)
			assert.NoError(t, err1)
			response, err := p.CreateRecordFromWallet(context.Background(), req)
			assert.NoError(t, err)
			assert.NotNil(t, response)
			assert.Equal(t, response.Status, true)

			//unspentReq := &v1.UnspentReq{
			//	ChainName: CHAIN_NAME,
			//	IsUnspent: "1",
			//	Address:   from,
			//}
			//unspent, err1 := p.GetUnspentTx(context.Background(), unspentReq)
			//len := len(unspent.UtxoList)
			//
			//var flag string
			//if CHAIN_NAME == "BTC" {
			//	flag = "/bitcoin/mainnet/"
			//} else if CHAIN_NAME == "LTC" {
			//	flag = "/litecoin/mainnet/"
			//} else if CHAIN_NAME == "DOGE" {
			//	flag = "/dogecoin/mainnet/"
			//} else {
			//	flag = ""
			//}
			//url := "https://Bearer:bd1bd2JBVNTa8XTPQOI7ytO8mK5AZpSpQ14sOwZn2CqD0Cd@ubiquity.api.blockdaemon.com/v1"
			//
			//list, err := btc.GetUnspentUtxo(url+flag, from)

			assert.NoError(t, err)
			assert.NoError(t, err1)
			//assert.NotNil(t, unspent)
			//assert.Equal(t, len, list.Total)

		},
	})

}
