package main

import (
	pb "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/shopspring/decimal"
	"strconv"
	"strings"

	"time"

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

	testJsonrpcCreateRecordWallet()
	str := "[{\"from\":\"0x0b068018dF753618d87e05862d2a05303efbb519\",\"to\":\"0xBA12222222228d8Ba445958a75a0704d566BF2C8\",\"amount\":146896760000000000,\"token\":{\"address\":\"\",\"amount\":\"\",\"decimals\":0,\"symbol\":\"\"}},{\"from\":\"0x0b068018dF753618d87e05862d2a05303efbb519\",\"to\":\"0xDEF171Fe48CF0115B1d80b88dc8eAB59176FEe57\",\"amount\":7197941240000000000,\"token\":{\"address\":\"\",\"amount\":\"\",\"decimals\":0,\"symbol\":\"\"}}]"
	var resp []types.EventLog
	json.Unmarshal([]byte(str),&resp)
	fmt.Println(resp)
	//格式化dt
	tm := time.Unix(1687310927,0)
	var dt = utils.GetDayTime(&tm)
	fmt.Println(dt)
	fmt.Println(uint32(dt))



	gas := []int{1,2,3,4,5,6,7,8,9,10,11,12,13}
	result := utils.GetMinHeap(gas, len(gas))//由高到低
	fmt.Println(result)
	s := "10"
	xx, _ := decimal.NewFromString(s)
	xxy, _ := decimal.NewFromString("3")

	fs := utils.StringDecimals(xx.Neg().String(), 3)
	yy, _ := decimal.NewFromString(fs)
	fmt.Println(fs)
	fmt.Println(yy)

	fmt.Println(xx.DivRound(xxy, 4))

	//go func() {
		for true{
			fmt.Println(utils.ZeroPoint())
			customChainPlan := time.NewTicker(utils.ZeroPoint())
			fmt.Println("wojinlail")
			select {
			case <-customChainPlan.C:
				fmt.Println("mmmt-------")
			}
		}

	//}()

}

func main2() {
	endTime := time.Now()
	m, _ := time.ParseDuration("-1h")
	startTime := endTime.Add(m)
	fmt.Println(endTime.Unix())
	fmt.Println(startTime.Unix())

	dds := "[{\"origin\":\"https://sushiv2.openblock.vip\",\"icon\":\"https://obapps.243096.com/images/dapp/ab75120c50cdd0fbba12761a7ca4007a.png\",\"dappName\":\"Sushi v2\",\"supportChains\":[\"eth\",\"arbitrum\",\"bsc\",\"avalanche\",\"fantom\",\"arbitrumnova\",\"polygon\",\"optimism\"],\"channel\":\"inner\",\"href\":\"\"}]"
	var vs []biz.DappInfo
	json.Unmarshal([]byte(dds), &vs)
	fmt.Println(vs)

	tokenIdsMap := make(map[string]string)
	tokenIdsMap["1"] = "11"
	tokenIdsMap["2"] = "21"
	tokenIdsMap["1"] = "31"
	tokenIdsMap["4"] = "41"

	tokenIds := make([]string, len(tokenIdsMap))
	for k, v := range tokenIdsMap {
		tokenIds = append(tokenIds, k)
		fmt.Println(k)
		fmt.Println(v)
		fmt.Println("============")
	}

	cnyFloat := strconv.FormatFloat(7.017010327577591, 'f', 2, 64)
	fmt.Println(cnyFloat)
	s := "10"
	xx, _ := decimal.NewFromString(s)
	xxy, _ := decimal.NewFromString("3")

	fs := utils.StringDecimals(xx.Neg().String(), 3)
	yy, _ := decimal.NewFromString(fs)
	fmt.Println(fs)
	fmt.Println(yy)

	fmt.Println(xx.DivRound(xxy, 4))

}
func main1() {
	flag.Parse()
	fmt.Println("func name", testFunc)
	//testGetBalance()
	aax := decimal.NewFromInt(-10).String() // output: "-10"
	fmt.Println(aax)
	fmt.Println("test main end")

	//txTime := 1664883095000 /1000
	//t := time.Unix(int64(txTime),0).Format("2006-01-02 15:04:05")

	s := "2022-03-05T01:38:32" // opensea
	ss := "2022-11-29T23:26:22.720Z"

	ft, _ := time.Parse("2006-01-02T15:04:05", s)
	fmt.Println(ft.Unix() * 1000)
	ft1, _ := time.Parse("2006-01-02T15:04:05.000Z", ss)
	fmt.Println(ft1.Unix() * 1000)

	doneNonce := make(map[string]int)

	doneNonce["key"] = 1
	doneNonce["key1"] = 11
	doneNonce["key2"] = 12
	doneNonce["key3"] = 13

	for k, v := range doneNonce {
		fmt.Println(k)
		fmt.Println(v)
	}

	x, _ := utils.HexStringToBigInt("0xcd3096")
	fmt.Println(x)
	l := "Error: Block not found: 13481229"
	yy := strings.Contains(l, "Block not found")
	fmt.Println(yy)

	sy := "00000000000000000000000000000000000000000000000000e0fc3d0e3ac218"
	y, _ := strconv.ParseUint(sy, 16, 0)
	//y := strings.Replace(s,"T"," ",1)
	fmt.Println(y)
	//s1 :="2022-03-05 01:38:32"
	//y1 := strings.Replace(s1,"T"," ",1)
	//fmt.Println(y1)
	//
	//fmt.Println(t)
	//testInsertDappRecord()
	//testDappPrice()
	//testDapp()

	//getDappListPageList()

	yyx := "095ea7b300000000000000000000000043e6777d20e73ccaea86922c20664072d667e3dc0000000000000000000000000000000000000000000000000000000038cda040"
	toAddress := utils.TronHexToBase58("41" + yyx[32:72])
	fmt.Println(toAddress)

	dongdongdong := "a614f803b6fd780986a42c78ec9c7f77e6ded13c"
	ddddd := utils.TronHexToBase58("41" + dongdongdong)
	fmt.Println(ddddd)

	var TokenInfoMap = make(map[string]types.TokenInfo)
	ddongq := types.TokenInfo{
		Address:  "TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8",
		Amount:   "10000",
		Decimals: 6,
		Symbol:   "USDC",
	}

	TokenInfoMap["token"] = ddongq

	eventLogJson, _ := json.Marshal(TokenInfoMap)

	fmt.Println(string(eventLogJson))

	fmt.Println("================")
	k := "done:zkSyncTEST:0xbB5117144fA27417DAa43C50513967bDba4145DA"
	rets := strings.Split(k, ":")

	fmt.Println(rets[0])
	fmt.Println(rets[1])
	fmt.Println(rets[2])
	fmt.Println(len(rets) >= 3)

	gl := "150878"
	gu := "149039"
	gasLimit, _ := strconv.ParseFloat(gl, 64)
	gasUsed, _ := strconv.ParseFloat(gu, 64)

	f := gasUsed / gasLimit
	if f > 0.9 {
		fmt.Println(f)
		fmt.Println(f > 0.9)
	}

}

func Dec2HexStr(v []byte) []string {
	//v = {0x19,0xa3}
	var data = make([]string, 0)
	if len(v)%2 != 0 {
		return data
	}
	k := ""
	for i, b := range v {
		i1 := int64(b)
		k += strconv.FormatInt(i1, 16)
		if (i+1)%2 == 0 {
			data = append(data, "0x"+k)
			k = ""
		}
	}
	return data
}
func Hex2Dec(v []string) []int64 {
	//v = ["0800","0800"]   result =  16*16*8
	var data = make([]int64, 0)
	for _, i2 := range v {
		i, err := strconv.ParseUint(i2, 0, 16) // 第二个参数为0时会自动判断字符类型
		if err != nil {
			return data
		}
		data = append(data, int64(i))
	}
	return data
}

func getDappListPageList() {
	conn, err := grpc.Dial("127.0.0.1:8999", grpc.WithInsecure())
	if err != nil {
		fmt.Println("error:", err)
	}
	defer conn.Close()
	p := pb.NewTransactionClient(conn)

	req := new(pb.DappPageListReq)
	req.ChainName = "ETH"

	p.GetDappListPageList(context.Background(), req)
}

func testDappPrice() {

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

func testDapp() {

	conn, err := grpc.Dial("127.0.0.1:8999", grpc.WithInsecure())
	if err != nil {
		fmt.Println("error:", err)
	}
	defer conn.Close()
	p := pb.NewTransactionClient(conn)
	req := new(pb.DappListReq)
	req.Uid = "3"
	ads := []string{0: "0x2B23e3B6671CDa35202339FA12599E7267B7Ed7f", 1: "0x1C6808A5051A27D01D676DC66Bb234e81732141a"}
	req.Addresses = ads
	//req.ContractAddress = "1"
	req.IsCancel = true

	resp, err := p.GetDappList(context.Background(), req)
	if err != nil {
		fmt.Println("get balacne error", err)
	}
	fmt.Println(resp)

}

func testInsertDappRecord() {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("GetTransactions error, chainName:{}", e)
			} else {
				log.Errore("GetTransactions panic, chainName:{}", e)
			}

			return
		}
	}()
	ret, err := data.DappApproveRecordRepoClient.SaveOrUpdate(nil, &data.DappApproveRecord{

		Uid:        "4488377Uid",
		LastTxhash: "sdjfkdjtxhash",
		Decimals:   18,
		ChainName:  "ETC",
		Address:    "yonghudizhi",
		Token:      "代币地址",
		ToAddress:  "dappdizhi",
		Amount:     "2",
		Symbol:     "EtC",
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
	req.FromAddress = "mhtPsoPhNHVLUuDgdYt26dVyGMYmypgbWx"
	req.ToAddress = "n2xbzJ88pv2MBG8X4L9MT3EV6aMbLzKVqR"
	req.Amount = "0.0001"
	req.ClientData = "{\"sendTime\":1657869735643}"

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
	req1.ChainName = "ETH" //不支持的链
	req1.TransactionType = "native"
	req1.FromAddress = "0xFf0dE40a64848f84F4A113bdff5cABfAd1CED1fb"
	req1.ToAddress = "0x65B29150f3b110A708BfbF7A4c6735080b453a69"
	req1.Amount = "500000000000000000"
	req1.GasLimit = "21000"
	req1.GasPrice = "100000000000"
	//req1.ParseData = "{\"evm\":{\"nonce\":\"0\",\"type\":0}}"
	req1.ClientData = "{\"sendTime\":1657869735643}"

	resp, err := p.CreateRecordFromWallet(context.Background(), req1)
	if err != nil {
		fmt.Println("get balacne error", err)
	}
	fmt.Println("result:", resp)

}
func testJsonrpcCreateRecordWallet() {

	conn, err := grpc.Dial("127.0.0.1:8999", grpc.WithInsecure())
	if err != nil {
		fmt.Println("error:", err)
	}
	defer conn.Close()
	p := pb.NewTransactionClient(conn)



	req1 := new(pb.TransactionReq)

	req1.Uid = "6e078f0f37b94869a44998d067fb5129"
	req1.CreatedAt = 1660615627
	req1.UpdatedAt = 1660615627
	req1.BlockHash = ""
	//req.BlockNumber = 0
	req1.TransactionHash = "0x063eaa36c294dba06a3fbeec2d2a42b5fed08ad5eff09c06e369676a7a0112e9"
	req1.Status = "pending"
	req1.TxTime = 0
	req1.ChainName = "Polygon"
	req1.TransactionType = "contract"
	req1.FromAddress = "0x6D7439A78c648402b30Fc3AB7d89E35F94FcD76a"
	req1.ToAddress = "0xf0511f123164602042ab2bCF02111fA5D3Fe97CD"
	req1.Amount = "500000000000000000"
	req1.GasLimit = "21000"
	req1.GasPrice = "100000000000"
	req1.ParseData = "{\"evm\":{\"nonce\":\"0\",\"type\":0}}"
	req1.ClientData = "{\"sendTime\":1657869735643}"
	req1.EventLog = "[{\"from\":\"0xEB02b2417fc0eb83aeD10D30DE6d3d77A4bab810\",\"to\":\"0x57F35E3d9A29cc62dc2129Abdca035c235DFa4B7\",\"amount\":1000000000000000000,\"token\":{\"address\":\"0x7CdC0421469398e0F3aA8890693d86c840Ac8931\",\"amount\":\"1000000000000000000\",\"decimals\":18,\"symbol\":\"AZUKI\",\"token_uri\":\"https://obstatic.243096.com/download/token/images/polygon-pos/polygon-pos_0x7cdc0421469398e0f3aa8890693d86c840ac8931.png\"}}]"

	resp, err := p.CreateRecordFromWallet(context.Background(), req1)
	if err != nil {
		fmt.Println("get balacne error", err)
	}
	fmt.Println(resp)

}