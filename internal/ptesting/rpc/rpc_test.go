package rpc

import (
	v1 "block-crawling/api/transaction/v1"
	"block-crawling/internal/biz"
	clientv1 "block-crawling/internal/client"
	"block-crawling/internal/data"
	"block-crawling/internal/platform"
	"block-crawling/internal/ptesting"
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

const CONFIG_PATH = "../../../configs"

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

func TestPageList(t *testing.T) {
	chainName := "ETH"
	req := &v1.PageListRequest{
		ChainName: chainName,
		Address:   "0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88",
		OrderBy:   "tx_time desc",
		PageSize:  20,
		Platform:  biz.WEB,
		Total:     true,
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			data.EvmTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, biz.GetTableName(chainName), txRecords)
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.PageList(context.Background(), req)

			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.Equal(t, len(res.List) >= 1, true)
			assert.Equal(t, res.Total >= 1, true)
			for _, record := range res.List {
				assert.Equal(t, record.ChainName, chainName)
				if record.TransactionHash == "0x4f0400c14b1f69877a027fa40966dad194b924b24e628fb1de223ca5b4e32f7d" {
					assert.Equal(t, record.FromAddress, "0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88")
					assert.Equal(t, record.ToAddress, "0x451aDe6606AeD956a843235E763208Af0080385E")
					assert.Equal(t, record.TransactionType, biz.TRANSFER)
					assert.Equal(t, record.ContractAddress, "0xf4d2888d29D722226FafA5d9B24F9164c092421E")
				}
			}
		},
	})
}

func TestPageList1(t *testing.T) {
	chainName := "ETH"
	req := &v1.PageListRequest{
		Address:  "0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88",
		OrderBy:  "tx_time desc",
		PageSize: 20,
		Platform: biz.WEB,
		Total:    true,
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			data.EvmTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, biz.GetTableName(chainName), txRecords)
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.PageList(context.Background(), req)

			assert.Error(t, err)
			assert.Equal(t, res == nil, true)
			assert.Equal(t, err.Error(), "rpc error: code = InvalidArgument desc = invalid PageListRequest.ChainName: value length must be at least 1 runes")
		},
	})
}

var assetRecords = []*data.UserAsset{{
	ChainName:    "ETH",
	Uid:          "1",
	Address:      "0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88",
	TokenAddress: "",
	Balance:      "1",
	Decimals:     18,
	Symbol:       "ETH",
}, {
	ChainName:    "ETH",
	Uid:          "1",
	Address:      "0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88",
	TokenAddress: "0xf4d2888d29D722226FafA5d9B24F9164c092421E",
	Balance:      "5",
	Decimals:     18,
	Symbol:       "LOOKS",
}, {
	ChainName:    "ETH",
	Uid:          "1",
	Address:      "0x451aDe6606AeD956a843235E763208Af0080385E",
	TokenAddress: "0xf4d2888d29D722226FafA5d9B24F9164c092421E",
	Balance:      "10",
	Decimals:     18,
	Symbol:       "LOOKS",
}}

var nftAssetRecords = []*data.UserNftAsset{{
	ChainName:      "ETH",
	Uid:            "1",
	Address:        "0x580Bad3785E99E679e8F128Aad8F47DD85b26A0D",
	TokenAddress:   "0x495f947276749Ce646f68AC8c248420045cb7b5e",
	TokenId:        "113131626276964395896987417252704130741588147670481760275076171533867519311873",
	Balance:        "5",
	TokenType:      biz.ERC1155,
	CollectionName: "Red Panda Pals NFT Collection",
	Symbol:         "OPENSTORE",
	Name:           "OpenSea Collection",
	ItemName:       "Red Panda Pal #255",
	ItemUri:        "https://i.seadn.io/gae/z1cmnY8oAYmUtnbpdC_77YIzdbj4J8t_Sut4WtPrHCrS74D7JVYYNLRN0C4UgIwGM4V-lSiwyaWM2waOv36GDFhxQkxtJ5GnP5OtaZU?w=500&auto=format",
}, {
	ChainName:      "ETH",
	Uid:            "1",
	Address:        "0x11C2254143310e834640F0FDafd6e44516340E40",
	TokenAddress:   "0x495f947276749Ce646f68AC8c248420045cb7b5e",
	TokenId:        "113131626276964395896987417252704130741588147670481760275076171533867519311873",
	Balance:        "10",
	TokenType:      biz.ERC1155,
	CollectionName: "Red Panda Pals NFT Collection",
	Symbol:         "OPENSTORE",
	Name:           "OpenSea Collection",
	ItemName:       "Red Panda Pal #255",
	ItemUri:        "https://i.seadn.io/gae/z1cmnY8oAYmUtnbpdC_77YIzdbj4J8t_Sut4WtPrHCrS74D7JVYYNLRN0C4UgIwGM4V-lSiwyaWM2waOv36GDFhxQkxtJ5GnP5OtaZU?w=500&auto=format",
}}

var statisticRecords = []*data.TransactionStatistic{{
	ChainName:           "ETH",
	TokenAddress:        "",
	FundDirection:       3,
	FundType:            1,
	TransactionQuantity: 1,
	Amount:              decimal.NewFromInt(5),
	CnyAmount:           decimal.NewFromInt(50000),
	Dt:                  1662048000,
	CreatedAt:           1662048001,
}, {
	ChainName:           "ETH",
	TokenAddress:        "0xf4d2888d29D722226FafA5d9B24F9164c092421E",
	FundDirection:       3,
	FundType:            1,
	TransactionQuantity: 2,
	Amount:              decimal.NewFromInt(10),
	CnyAmount:           decimal.NewFromInt(100000),
	Dt:                  1662048000,
	CreatedAt:           1662048001,
}}

func TestPageListAsset(t *testing.T) {
	chainName := "ETH"
	req := &v1.PageListAssetRequest{
		ChainName:        chainName,
		AddressList:      []string{"0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88", "0x451aDe6606AeD956a843235E763208Af0080385E"},
		TokenAddressList: []string{"", "0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
		Currency:         biz.CNY,
		OrderBy:          "id desc",
		PageSize:         20,
		Total:            true,
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			data.UserAssetRepoClient.PageBatchSaveOrUpdate(context.Background(), assetRecords, biz.PAGE_SIZE)
		},
		TokenPrices: []ptesting.TokenPrice{
			{
				TokenAddress: "0xf4d2888d29D722226FafA5d9B24F9164c092421E",
				Currency:     biz.CNY,
				Price:        "1",
			},
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.PageListAsset(context.Background(), req)

			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.Equal(t, len(res.List) >= 3, true)
			assert.Equal(t, res.Total >= 3, true)
			for _, asset := range res.List {
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

				assert.Equal(t, asset.ChainName, chainName)
				assert.Equal(t, asset.Decimals, int32(18))
			}
		},
	})
}

func TestPageListAsset1(t *testing.T) {
	chainName := "ETH"
	req := &v1.PageListAssetRequest{
		ChainName:        chainName,
		AddressList:      []string{"0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88", "0x451aDe6606AeD956a843235E763208Af0080385E"},
		TokenAddressList: []string{"", "0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
		OrderBy:          "id desc",
		PageSize:         20,
		Total:            true,
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			data.UserAssetRepoClient.PageBatchSaveOrUpdate(context.Background(), assetRecords, biz.PAGE_SIZE)
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.PageListAsset(context.Background(), req)

			assert.Error(t, err)
			assert.Equal(t, res == nil, true)
			assert.Equal(t, err.Error(), "rpc error: code = Unknown desc = currency must be CNY or USD")
		},
	})
}

func TestClientPageListAsset(t *testing.T) {
	chainName := "ETH"
	req := &v1.PageListAssetRequest{
		ChainName:        chainName,
		AddressList:      []string{"0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88", "0x451aDe6606AeD956a843235E763208Af0080385E"},
		TokenAddressList: []string{"", "0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
		Currency:         biz.CNY,
		OrderBy:          "id desc",
		PageSize:         20,
		Total:            true,
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		TokenPrices: []ptesting.TokenPrice{
			{
				TokenAddress: "0xf4d2888d29D722226FafA5d9B24F9164c092421E",
				Currency:     biz.CNY,
				Price:        "1",
			},
		},
		BeforePrepare: func() {
			data.UserAssetRepoClient.PageBatchSaveOrUpdate(context.Background(), assetRecords, biz.PAGE_SIZE)
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.ClientPageListAsset(context.Background(), req)

			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.Equal(t, len(res.List) >= 3, true)
			assert.Equal(t, res.Total >= 3, true)
			for _, asset := range res.List {
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

				assert.Equal(t, asset.ChainName, chainName)
				assert.Equal(t, asset.Decimals, int32(18))
			}
		},
	})
}

func TestClientPageListAsset1(t *testing.T) {
	chainName := "ETH"
	req := &v1.PageListAssetRequest{
		ChainName:        chainName,
		AddressList:      []string{"0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88", "0x451aDe6606AeD956a843235E763208Af0080385E"},
		TokenAddressList: []string{"", "0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
		OrderBy:          "id desc",
		PageSize:         20,
		Total:            true,
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			data.UserAssetRepoClient.PageBatchSaveOrUpdate(context.Background(), assetRecords, biz.PAGE_SIZE)
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.ClientPageListAsset(context.Background(), req)

			assert.Error(t, err)
			assert.Equal(t, res == nil, true)
			assert.Equal(t, err.Error(), "rpc error: code = Unknown desc = currency must be CNY or USD")
		},
	})
}

func TestGetBalance(t *testing.T) {
	chainName := "ETH"
	req := &v1.AssetRequest{
		ChainName:        chainName,
		Address:          "0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88",
		TokenAddressList: []string{"", "0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			data.UserAssetRepoClient.PageBatchSaveOrUpdate(context.Background(), assetRecords, biz.PAGE_SIZE)
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.GetBalance(context.Background(), req)

			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.Equal(t, len(res.List) >= 2, true)
			for _, asset := range res.List {
				if asset.TokenAddress == "" {
					assert.Equal(t, asset.Balance, "1")
				} else if asset.TokenAddress == "0xf4d2888d29D722226FafA5d9B24F9164c092421E" {
					assert.Equal(t, asset.Balance, "5")
				}
			}
		},
	})
}

func TestGetBalance1(t *testing.T) {
	req := &v1.AssetRequest{
		Address:          "0x75e89d5979E4f6Fba9F97c104c2F0AFB3F1dcB88",
		TokenAddressList: []string{"", "0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			data.UserAssetRepoClient.PageBatchSaveOrUpdate(context.Background(), assetRecords, biz.PAGE_SIZE)
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.GetBalance(context.Background(), req)

			assert.Error(t, err)
			assert.Equal(t, res == nil, true)
			assert.Equal(t, err.Error(), "rpc error: code = InvalidArgument desc = invalid AssetRequest.ChainName: value length must be at least 1 runes")
		},
	})
}

func getTokensPrice() []ptesting.TokenPrice {
	r := make([]ptesting.TokenPrice, 0, len(assetRecords))

	for _, a := range assetRecords {
		r = append(r, ptesting.TokenPrice{
			TokenAddress: a.TokenAddress,
			Currency:     biz.CNY,
			Price:        "1",
		})
	}
	return r
}

func TestListAmountUidDimension(t *testing.T) {
	req := &v1.ListAmountUidDimensionRequest{
		UidList:  []string{"1"},
		Currency: biz.CNY,
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			data.UserAssetRepoClient.PageBatchSaveOrUpdate(context.Background(), assetRecords, biz.PAGE_SIZE)
		},
		TokenPrices: getTokensPrice(),
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.ListAmountUidDimension(context.Background(), req)

			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.Equal(t, len(res.List) >= 1, true)
			for _, asset := range res.List {
				assert.Equal(t, asset.Uid, "1")
			}
		},
	})
}

func TestListAmountUidDimension1(t *testing.T) {
	req := &v1.ListAmountUidDimensionRequest{
		UidList: []string{"1"},
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			data.UserAssetRepoClient.PageBatchSaveOrUpdate(context.Background(), assetRecords, biz.PAGE_SIZE)
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.ListAmountUidDimension(context.Background(), req)

			assert.Error(t, err)
			assert.Equal(t, res == nil, true)
			assert.Equal(t, err.Error(), "rpc error: code = Unknown desc = currency must be CNY or USD")
		},
	})
}

func TestListHasBalanceUidDimension(t *testing.T) {
	req := &v1.ListHasBalanceUidDimensionRequest{
		UidList: []string{"1"},
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			data.UserAssetRepoClient.PageBatchSaveOrUpdate(context.Background(), assetRecords, biz.PAGE_SIZE)
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.ListHasBalanceUidDimension(context.Background(), req)

			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.Equal(t, len(res.List) >= 1, true)
			for _, asset := range res.List {
				assert.Equal(t, asset.Uid, "1")
				assert.Equal(t, asset.HasBalance, true)
			}
		},
	})
}

func TestListHasBalanceUidDimension1(t *testing.T) {
	req := &v1.ListHasBalanceUidDimensionRequest{}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			data.UserAssetRepoClient.PageBatchSaveOrUpdate(context.Background(), assetRecords, biz.PAGE_SIZE)
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.ListHasBalanceUidDimension(context.Background(), req)

			assert.Error(t, err)
			assert.Equal(t, res == nil, true)
			assert.Equal(t, err.Error(), "rpc error: code = InvalidArgument desc = invalid ListHasBalanceUidDimensionRequest.UidList: value must contain at least 1 item(s)")
		},
	})
}

func getNFTs() []*clientv1.GetNftReply_NftInfoResp {
	r := make([]*clientv1.GetNftReply_NftInfoResp, 0, len(nftAssetRecords))
	for _, v := range nftAssetRecords {
		r = append(r, &clientv1.GetNftReply_NftInfoResp{
			TokenId:      v.TokenId,
			TokenAddress: v.TokenAddress,
			Name:         v.Name,
			Chain:        v.ChainName,
			Symbol:       v.Symbol,
		})
	}
	return r
}

func TestClientPageListNftAssetGroup(t *testing.T) {
	chainName := "ETH"
	req := &v1.PageListNftAssetRequest{
		ChainName:        chainName,
		AddressList:      []string{"0x580Bad3785E99E679e8F128Aad8F47DD85b26A0D", "0x11C2254143310e834640F0FDafd6e44516340E40"},
		TokenAddressList: []string{"0x495f947276749Ce646f68AC8c248420045cb7b5e"},
		OrderBy:          "balance desc",
		PageSize:         20,
		Total:            true,
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			data.UserNftAssetRepoClient.PageBatchSaveOrUpdate(context.Background(), nftAssetRecords, biz.PAGE_SIZE)
		},
		NFTs: getNFTs(),
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.ClientPageListNftAssetGroup(context.Background(), req)

			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.Equal(t, len(res.List) >= 1, true)
			for _, asset := range res.List {
				balance, _ := strconv.Atoi(asset.Balance)
				if asset.Address == "0x580Bad3785E99E679e8F128Aad8F47DD85b26A0D" {
					assert.Equal(t, balance >= 5, true)
				} else if asset.Address == "0x11C2254143310e834640F0FDafd6e44516340E40" {
					assert.Equal(t, balance >= 10, true)
				}

				assert.Equal(t, asset.TokenAddress, "0x495f947276749Ce646f68AC8c248420045cb7b5e")
				assert.Equal(t, asset.ChainName, chainName)
				assert.Equal(t, asset.TokenType, biz.ERC1155)
			}
		},
	})
}

func TestClientPageListNftAssetGroup1(t *testing.T) {
	chainName := "ETH"
	req := &v1.PageListNftAssetRequest{
		ChainName:        chainName,
		TokenAddressList: []string{"0x495f947276749Ce646f68AC8c248420045cb7b5e"},
		OrderBy:          "balance desc",
		PageSize:         20,
		Total:            true,
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			data.UserNftAssetRepoClient.PageBatchSaveOrUpdate(context.Background(), nftAssetRecords, biz.PAGE_SIZE)
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.ClientPageListNftAssetGroup(context.Background(), req)

			assert.Error(t, err)
			assert.Equal(t, res == nil, true)
			assert.Equal(t, err.Error(), "rpc error: code = Unknown desc = uid or addressList is required")
		},
	})
}

func TestClientPageListNftAsset(t *testing.T) {
	chainName := "ETH"
	req := &v1.PageListNftAssetRequest{
		ChainName:        chainName,
		AddressList:      []string{"0x580Bad3785E99E679e8F128Aad8F47DD85b26A0D", "0x11C2254143310e834640F0FDafd6e44516340E40"},
		TokenAddressList: []string{"0x495f947276749Ce646f68AC8c248420045cb7b5e"},
		OrderBy:          "id desc",
		PageSize:         20,
		Total:            true,
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			data.UserNftAssetRepoClient.PageBatchSaveOrUpdate(context.Background(), nftAssetRecords, biz.PAGE_SIZE)
		},
		NFTs: getNFTs(),
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.ClientPageListNftAsset(context.Background(), req)

			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.Equal(t, len(res.List) >= 1, true)
			for _, asset := range res.List {
				if asset.TokenId == "113131626276964395896987417252704130741588147670481760275076171533867519311873" {
					if asset.Address == "0x580Bad3785E99E679e8F128Aad8F47DD85b26A0D" {
						assert.Equal(t, asset.Balance, "5")
					} else if asset.Address == "0x11C2254143310e834640F0FDafd6e44516340E40" {
						assert.Equal(t, asset.Balance, "10")
					}
				}

				assert.Equal(t, asset.TokenAddress, "0x495f947276749Ce646f68AC8c248420045cb7b5e")
				assert.Equal(t, asset.ChainName, chainName)
				assert.Equal(t, asset.TokenType, biz.ERC1155)
			}
		},
	})
}

func TestClientPageListNftAsset1(t *testing.T) {
	chainName := "ETH"
	req := &v1.PageListNftAssetRequest{
		ChainName:        chainName,
		TokenAddressList: []string{"0x495f947276749Ce646f68AC8c248420045cb7b5e"},
		OrderBy:          "balance desc",
		PageSize:         20,
		Total:            true,
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		NFTs:    getNFTs(),
		BeforePrepare: func() {
			data.UserNftAssetRepoClient.PageBatchSaveOrUpdate(context.Background(), nftAssetRecords, biz.PAGE_SIZE)
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.ClientPageListNftAsset(context.Background(), req)

			assert.Error(t, err)
			assert.Equal(t, res == nil, true)
			assert.Equal(t, err.Error(), "rpc error: code = Unknown desc = uid or addressList is required")
		},
	})
}

func TestGetNftBalance(t *testing.T) {
	chainName := "ETH"
	req := &v1.NftAssetRequest{
		ChainName:    chainName,
		Address:      "0x580Bad3785E99E679e8F128Aad8F47DD85b26A0D",
		TokenAddress: "0x495f947276749Ce646f68AC8c248420045cb7b5e",
		TokenId:      "113131626276964395896987417252704130741588147670481760275076171533867519311873",
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			data.UserNftAssetRepoClient.PageBatchSaveOrUpdate(context.Background(), nftAssetRecords, biz.PAGE_SIZE)
		},
		NFTs: getNFTs(),
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.GetNftBalance(context.Background(), req)

			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.Equal(t, res.Balance, "5")
		},
	})
}

func TestGetNftBalance1(t *testing.T) {
	req := &v1.NftAssetRequest{
		Address:      "0x580Bad3785E99E679e8F128Aad8F47DD85b26A0D",
		TokenAddress: "0x495f947276749Ce646f68AC8c248420045cb7b5e",
		TokenId:      "113131626276964395896987417252704130741588147670481760275076171533867519311873",
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			data.UserNftAssetRepoClient.PageBatchSaveOrUpdate(context.Background(), nftAssetRecords, biz.PAGE_SIZE)
		},
		NFTs: getNFTs(),
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.GetNftBalance(context.Background(), req)

			assert.Error(t, err)
			assert.Equal(t, res == nil, true)
			assert.Equal(t, err.Error(), "rpc error: code = InvalidArgument desc = invalid NftAssetRequest.ChainName: value length must be at least 1 runes")
		},
	})
}

func TestPageListStatistic(t *testing.T) {
	chainName := "ETH"
	dreq := &data.StatisticRequest{
		ChainName:         chainName,
		TokenAddressList:  []string{"0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
		FundDirectionList: []int16{3},
	}
	req := &v1.PageListStatisticRequest{
		ChainName:         chainName,
		TokenAddress:      "0xf4d2888d29D722226FafA5d9B24F9164c092421E",
		FundDirectionList: []int32{3},
		StartTime:         1662047000,
		StopTime:          1662048100,
		OrderBy:           "id desc",
		PageSize:          20,
		Total:             true,
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.TransactionStatisticRepoClient.Delete(context.Background(), dreq)
			assert.NoError(t, err)
			data.TransactionStatisticRepoClient.PageBatchSaveOrUpdate(context.Background(), statisticRecords, biz.PAGE_SIZE)
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.PageListStatistic(context.Background(), req)

			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.Equal(t, len(res.List) >= 1, true)
			assert.Equal(t, res.Total >= 1, true)
			for _, asset := range res.List {
				assert.Equal(t, asset.TokenAddress, "0xf4d2888d29D722226FafA5d9B24F9164c092421E")
				assert.Equal(t, asset.FundDirection, int32(3))

				assert.Equal(t, asset.ChainName, chainName)
			}
		},
	})
}

func TestPageListStatistic1(t *testing.T) {
	chainName := "ETH"
	dreq := &data.StatisticRequest{
		ChainName:         chainName,
		TokenAddressList:  []string{"0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
		FundDirectionList: []int16{3},
	}
	req := &v1.PageListStatisticRequest{
		ChainName:         chainName,
		TokenAddress:      "0xf4d2888d29D722226FafA5d9B24F9164c092421E",
		FundDirectionList: []int32{3},
		OrderBy:           "id desc",
		PageSize:          20,
		Total:             true,
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.TransactionStatisticRepoClient.Delete(context.Background(), dreq)
			assert.NoError(t, err)
			data.TransactionStatisticRepoClient.PageBatchSaveOrUpdate(context.Background(), statisticRecords, biz.PAGE_SIZE)
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.PageListStatistic(context.Background(), req)

			assert.Error(t, err)
			assert.Equal(t, res == nil, true)
			assert.Equal(t, err.Error(), "rpc error: code = Unknown desc = startTime is greater than stopTime")
		},
	})
}

func TestStatisticFundAmount(t *testing.T) {
	chainName := "ETH"
	dreq := &data.StatisticRequest{
		ChainName:         chainName,
		TokenAddressList:  []string{"0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
		FundDirectionList: []int16{3},
	}
	req := &v1.StatisticFundRequest{
		ChainName:         chainName,
		FundDirectionList: []int32{3},
		StartTime:         1662047000,
		StopTime:          1662048100,
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.TransactionStatisticRepoClient.Delete(context.Background(), dreq)
			assert.NoError(t, err)
			data.TransactionStatisticRepoClient.PageBatchSaveOrUpdate(context.Background(), statisticRecords, biz.PAGE_SIZE)
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.StatisticFundAmount(context.Background(), req)

			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.Equal(t, len(res.List) >= 1, true)
			for _, asset := range res.List {
				assert.Equal(t, asset.FundDirection, int32(3))
				assert.Equal(t, asset.TransactionQuantity, "3")
			}
		},
	})
}

func TestStatisticFundAmount1(t *testing.T) {
	chainName := "ETH"
	dreq := &data.StatisticRequest{
		ChainName:         chainName,
		TokenAddressList:  []string{"0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
		FundDirectionList: []int16{3},
	}
	req := &v1.StatisticFundRequest{
		ChainName:         chainName,
		FundDirectionList: []int32{3},
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.TransactionStatisticRepoClient.Delete(context.Background(), dreq)
			assert.NoError(t, err)
			data.TransactionStatisticRepoClient.PageBatchSaveOrUpdate(context.Background(), statisticRecords, biz.PAGE_SIZE)
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.StatisticFundAmount(context.Background(), req)

			assert.Error(t, err)
			assert.Equal(t, res == nil, true)
			assert.Equal(t, err.Error(), "rpc error: code = InvalidArgument desc = invalid StatisticFundRequest.StartTime: value must be greater than 0")
		},
	})
}

func TestStatisticFundRate(t *testing.T) {
	chainName := "ETH"
	dreq := &data.StatisticRequest{
		ChainName:         chainName,
		TokenAddressList:  []string{"0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
		FundDirectionList: []int16{3},
	}
	req := &v1.StatisticFundRequest{
		ChainName:         chainName,
		FundDirectionList: []int32{3},
		StartTime:         1662047000,
		StopTime:          1662048100,
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.TransactionStatisticRepoClient.Delete(context.Background(), dreq)
			assert.NoError(t, err)
			data.TransactionStatisticRepoClient.PageBatchSaveOrUpdate(context.Background(), statisticRecords, biz.PAGE_SIZE)
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.StatisticFundRate(context.Background(), req)

			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.Equal(t, len(res.List) >= 1, true)
			for _, asset := range res.List {
				assert.Equal(t, asset.FundDirection, int32(3))
				assert.Equal(t, asset.TransactionQuantity, "3")
			}
		},
	})
}

func TestStatisticFundRate1(t *testing.T) {
	chainName := "ETH"
	dreq := &data.StatisticRequest{
		ChainName:         chainName,
		TokenAddressList:  []string{"0xf4d2888d29D722226FafA5d9B24F9164c092421E"},
		FundDirectionList: []int16{3},
	}
	req := &v1.StatisticFundRequest{
		ChainName:         chainName,
		FundDirectionList: []int32{3},
	}
	ptesting.RunTest(ptesting.Preparation{
		Configs: CONFIG_PATH,
		BeforePrepare: func() {
			_, err := data.TransactionStatisticRepoClient.Delete(context.Background(), dreq)
			assert.NoError(t, err)
			data.TransactionStatisticRepoClient.PageBatchSaveOrUpdate(context.Background(), statisticRecords, biz.PAGE_SIZE)
		},
		Test: func(plat *platform.Bootstrap) {
			conn, err := ptesting.DialGRPC()
			if err != nil {
				fmt.Println("error:", err)
			}
			defer conn.Close()

			p := v1.NewTransactionClient(conn)
			res, err := p.StatisticFundRate(context.Background(), req)

			assert.Error(t, err)
			assert.Equal(t, res == nil, true)
			assert.Equal(t, err.Error(), "rpc error: code = InvalidArgument desc = invalid StatisticFundRequest.StartTime: value must be greater than 0")
		},
	})
}
