package main

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"context"
	"flag"
	"fmt"
	//"github.com/shopspring/decimal"
	"google.golang.org/grpc"
	"os"
)

// go build -ldflags "-X main.Version=x.y.z"
var (
	// Name is the name of the compiled software.
	Name string
	// Version is the version of the compiled software.
	Version string
	// flagconf is the config flag.
	flagconf string
	testFunc string
	id, _    = os.Hostname()
)

func init() {
	flag.StringVar(&flagconf, "conf", "../../configs", "config path, eg: -conf config.yaml")
	flag.StringVar(&testFunc, "name", "price", "test func name")
}

func main() {
	flag.Parse()
	fmt.Println("func name", testFunc)
	//testGetBalance()
	fmt.Println("test main end")
	//testInsertDappRecord()
	testDappPrice()
	//testDapp()

	getDappListPageList()


}

func getDappListPageList()  {
	conn, err := grpc.Dial("127.0.0.1:8999", grpc.WithInsecure())
	if err != nil {
		fmt.Println("error:", err)
	}
	defer conn.Close()
	p := pb.NewTransactionClient(conn)

	req := new(pb.DappPageListReq)
	req.ChainName="ETH"

	p.GetDappListPageList(context.Background(), req)
}


func testDappPrice(){

	conn, err := grpc.Dial("127.0.0.1:8999", grpc.WithInsecure())
	if err != nil {
		fmt.Println("error:", err)
	}
	defer conn.Close()
	p := pb.NewTransactionClient(conn)

	req := new(pb.OpenAmountReq)
	req.Uid = "3"
	req.Currency = "USD"
	req.ChainName = "ETH"
	resp, err := p.GetAllOpenAmount(context.Background(), req)
	if err != nil {
		fmt.Println("get balacne error", err)
	}
	fmt.Println(resp)

}

func testDapp()  {

	conn, err := grpc.Dial("127.0.0.1:8999", grpc.WithInsecure())
	if err != nil {
		fmt.Println("error:", err)
	}
	defer conn.Close()
	p := pb.NewTransactionClient(conn)
	req := new(pb.DappListReq)
	req.Uid = "3"
	ads := []string{0:"0x2B23e3B6671CDa35202339FA12599E7267B7Ed7f",1:"0x1C6808A5051A27D01D676DC66Bb234e81732141a"}
	req.Addresses = ads
	//req.ContractAddress = "1"
	req.IsCancel = true

	resp, err := p.GetDappList(context.Background(), req)
	if err != nil {
		fmt.Println("get balacne error", err)
	}
	fmt.Println(resp)

}



func testInsertDappRecord()  {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("GetTransactions error, chainName:{}", e)
			} else {
				log.Errore("GetTransactions panic, chainName:{}",e)
			}


			return
		}
	}()
	ret , err :=data.DappApproveRecordRepoClient.SaveOrUpdate(nil,&data.DappApproveRecord{

		Uid      : "4488377Uid",
		LastTxhash : "sdjfkdjtxhash",
		Decimals   : 18,
		ChainName  : "ETC",
		Address    : "yonghudizhi",
		Token      :"代币地址",
		ToAddress  :"dappdizhi",
		Amount     : "2",
		Symbol     :"EtC",

	})

	if err != nil {
		fmt.Println("get insert error", err)
	}
	fmt.Println("result:", ret)

}


func testGetBalance() {
	conn, err := grpc.Dial("127.0.0.1:8999", grpc.WithInsecure())
	if err != nil {
		fmt.Println("error:", err)
	}
	defer conn.Close()
	p := pb.NewTransactionClient(conn)
	req := new(pb.TransactionReq)
	//id   |               uuid               |          created_at           |          updated_at           | block_hash | block_number |                          transaction_hash                          | status  |  tx_time   |  chain  | chain_name | transaction_type | transaction_source |                  from_obj                  |                   to_obj                   | data |       amount       | fee_amount |                     fee_data                     | contract_address |           parse_data           | dapp_data | event_log |        client_data         | approve_logic_data
	//32435 | 6e078f0f37b94869a44998d067fb5129 | 2022-07-15 07:22:17.739039+00 | 2022-07-15 07:22:17.739055+00 |            |              | 1c9329db210069df550f184b2fa60d4305eb842a8a8002cb4fbe7e1074c197b4   | pending |          0 |         | BTCTEST    | native           |                    | mhtPsoPhNHVLUuDgdYt26dVyGMYmypgbWx         | n2xbzJ88pv2MBG8X4L9MT3EV6aMbLzKVqR         |      | 0.0001             |            | {}                                               |                  | {}                             |           |           | {"sendTime":1657869735643} |
	req.Uid = "6e078f0f37b94869a44998d067fb5129"
	req.CreatedAt = 1660615627
	req.UpdatedAt = 1660615627
	req.BlockHash = ""
	//req.BlockNumber = 0
	req.TransactionHash = "1c9329db210069df550f184b2fa60d4305eb842a8a8002cb4fbe7e1074c197b4"
	req.Status = "pending"
	req.TxTime = 0
	req.ChainName = "BTCTEST"
	req.TransactionType = "native"
	req.FromAddress ="mhtPsoPhNHVLUuDgdYt26dVyGMYmypgbWx"
	req.ToAddress = "n2xbzJ88pv2MBG8X4L9MT3EV6aMbLzKVqR"
	req.Amount = "0.0001"
	req.ClientData ="{\"sendTime\":1657869735643}"




	//15888 | 781323461749462297f85cbf5fa03813 | 2022-06-14 12:07:56.504716+00 | 2022-06-14 12:07:56.504736+00 |            |              | 0xc1fab01ab0e54a6f4d780de4d9e41308e1a95f93203d1e74bd0664ab5d2fe64d | pending |          0 |         | Oasis      | native           |                    | 0xFf0dE40a64848f84F4A113bdff5cABfAd1CED1fb | 0x65B29150f3b110A708BfbF7A4c6735080b453a69 |      | 500000000000000000 |            | {"gas_price":"100000000000","gas_limit":"21000"} |                  | {"evm":{"nonce":"0","type":0}} |           |           | {"sendTime":1655208475824} |
	req1 := new(pb.TransactionReq)


	req1.Uid = "6e078f0f37b94869a44998d067fb5129"
	req1.CreatedAt = 1660615627
	req1.UpdatedAt = 1660615627
	req1.BlockHash = ""
	//req.BlockNumber = 0
	req1.TransactionHash = "0xc1fab01ab0e54a6f4d780de4d9e41308e1a95f93003d1e74bd0664ab5d2fe64d"
	req1.Status = biz.NO_STATUS
	req1.TxTime = 0
	req1.ChainName = "ETH"//不支持的链
	req1.TransactionType = "native"
	req1.FromAddress ="0xFf0dE40a64848f84F4A113bdff5cABfAd1CED1fb"
	req1.ToAddress = "0x65B29150f3b110A708BfbF7A4c6735080b453a69"
	req1.Amount = "500000000000000000"
	req1.GasLimit = "21000"
	req1.GasPrice = "100000000000"
	req1.ParseData ="{\"evm\":{\"nonce\":\"0\",\"type\":0}}"
	req1.ClientData ="{\"sendTime\":1657869735643}"



	resp, err := p.CreateRecordFromWallet(context.Background(), req1)
	if err != nil {
		fmt.Println("get balacne error", err)
	}
	fmt.Println("result:", resp)





}

