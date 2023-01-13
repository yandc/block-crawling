package ethereum

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/common"
	"block-crawling/internal/subhandle"
	"errors"
	"fmt"
	"strings"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"go.uber.org/zap"
)

// ERC20 or ERC721
const APPROVAL_TOPIC = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"

// ERC721 or ERC1155
const APPROVALFORALL_TOPIC = "0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31"

// ERC20 or ERC721
const TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

// ERC1155
const TRANSFERSINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"

// ERC1155
const TRANSFERBATCH_TOPIC = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"

const WITHDRAWAL_TOPIC = "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"
const DEPOSIT_TOPIC = "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"
const BLOCK_NO_TRANSCATION = "server returned empty transaction list but block header indicates transactions"
const BLOCK_NONAL_TRANSCATION = "server returned non-empty transaction list but block header indicates no transactions"
const TOO_MANY_REQUESTS = "429 Too Many Requests"
const POLYGON_CODE = "0x0000000000000000000000000000000000001010"
const BRIDGE_TRANSFERNATIVE = "0xb4a87134099d10c48345145381989042ab07dc53e6e62a6511fca55438562e26"
const FANTOM_SWAPED = "0x76af224a143865a50b41496e1a73622698692c565c1214bc862f18e22d829c5e"
const FANTOM_SWAPED_V1 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
const ARBITRUM_TRANSFERNATIVE = "0x79fa08de5149d912dce8e5e8da7a7c17ccdf23dd5d3bfe196802e6eb86347c7c"
const OPTIMISM_WITHDRAWETH = "0x94effa14ea3a1ef396fa2fd829336d1597f1d76b548c26bfa2332869706638af"
const OPTIMISM_FANTOM_LOGANYSWAPIN = "0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55"
const OPTIMISM_NONE = "0x4f56ec39e98539920503fd54ee56ae0cbebe9eb15aa778f18de67701eeae7c65"
const ARBITRUM_INTXAMOUNT = "0xc6c1e0630dbe9130cc068028486c0d118ddcea348550819defd5cb8c257f8a38"
const ARBITRUM_UNLOCKEVENT = "0xd90288730b87c2b8e0c45bd82260fd22478aba30ae1c4d578b8daba9261604df"
const KLAYTN_EXCHANGEPOS = "0x022d176d604c15661a2acf52f28fd69bdd2c755884c08a67132ffeb8098330e0"

//dapp 白名单 chainName + contractAddress + methodId
var BridgeWhiteMethodIdList = map[string][]string{
	//dapp: https://stargate.finance/ 代币，主币 兑换
	"Optimism_MethodId": {"Optimism_0x81E792e5a9003CC1C8BF5569A00f34b65d75b017_252f7b01", "Optimism_0x81E792e5a9003CC1C8BF5569A00f34b65d75b017_0508941e",
		"Optimism_0x8F957Ed3F969D7b6e5d6dF81e61A5Ff45F594dD1_4782f779", "Optimism_0xDC42728B0eA910349ed3c6e1c9Dc06b5FB591f98_0175b1c4",
		"Optimism_0xa81D244A1814468C734E5b4101F7b9c0c577a8fC_3d12a85a", "Optimism_0xAf41a65F786339e7911F4acDAD6BD49426F2Dc6b_17357892"},
	//dapp: https://cbridge.celer.network/10/42161/ETH?ref=bitkeep2022
	"Arbitrum_MethodId": {"Arbitrum_0x1619DE6B6B20eD217a58d00f37B9d47C7663feca_cdd1b25d", "Arbitrum_0x7ceA671DABFBa880aF6723bDdd6B9f4caA15C87B_d450e04c"},
	"Fantom_MethodId":   {"Fantom_0xf3Ce95Ec61114a4b1bFC615C16E6726015913CCC_0175b1c4"},
	"Polygon_MethodId":   {"Polygon_0x2eF4A574b72E1f555185AfA8A09c6d1A8AC4025C_0175b1c4"},
}

//dapp 白名单 chainName + contractAddress + topic
var BridgeWhiteTopicList = map[string][]string{
	//swap：对应 function topic0
	"Optimism_Topic": {"Optimism_0x81e792e5a9003cc1c8bf5569a00f34b65d75b017_0xb4a87134099d10c48345145381989042ab07dc53e6e62a6511fca55438562e26",
		"Optimism_0x8f957ed3f969d7b6e5d6df81e61a5ff45f594dd1_0x94effa14ea3a1ef396fa2fd829336d1597f1d76b548c26bfa2332869706638af",
		"Optimism_0xdc42728b0ea910349ed3c6e1c9dc06b5fb591f98_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55",
		//https://synapseprotocol.com/?inputCurrency=USDC&outputCurrency=USDC&outputChain=42161
		"Optimism_0xaf41a65f786339e7911f4acdad6bd49426f2dc6b_0x4f56ec39e98539920503fd54ee56ae0cbebe9eb15aa778f18de67701eeae7c65"},
	//OpenOceanExchange.V2 https://openocean.finance/
	"Fantom_Topic": {"Fantom_0x6352a56caadc4f1e25cd6c75970fa768a3304e64_0x76af224a143865a50b41496e1a73622698692c565c1214bc862f18e22d829c5e",
		"Fantom_0x31f63a33141ffee63d4b26755430a390acdd8a4d_0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
		"Fantom_0xf3ce95ec61114a4b1bfc615c16e6726015913ccc_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55"},
	"Arbitrum_Topic": {"Arbitrum_0x1619de6b6b20ed217a58d00f37b9d47c7663feca_0x79fa08de5149d912dce8e5e8da7a7c17ccdf23dd5d3bfe196802e6eb86347c7c",
		//合约内部 调用amount
		"Arbitrum_0x7cea671dabfba880af6723bddd6b9f4caa15c87b_0xc6c1e0630dbe9130cc068028486c0d118ddcea348550819defd5cb8c257f8a38",
		"Arbitrum_0x7cea671dabfba880af6723bddd6b9f4caa15c87b_0xd90288730b87c2b8e0c45bd82260fd22478aba30ae1c4d578b8daba9261604df"},
	"Klaytn_Topic": {"Klaytn_0xc6a2ad8cc6e4a7e08fc37cc5954be07d499e7654_0x022d176d604c15661a2acf52f28fd69bdd2c755884c08a67132ffeb8098330e0"},
	"Polygon_Topic": {"Polygon_0x2ef4a574b72e1f555185afa8a09c6d1a8ac4025c_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55"},
}

type Platform struct {
	subhandle.CommPlatform
	NodeURL   string
	CoinIndex uint
	UrlList   []string
	spider    *chain.BlockSpider
	conf      *conf.PlatInfo
}

type Config struct {
	ProjectId []string
}

type KVPair struct {
	Key string
	Val int
}

func Init(handler string, c *conf.PlatInfo, nodeURL []string, height int) *Platform {
	chainType := c.Handler // ethereum
	chainName := c.Chain   // ETH

	clients := make([]chain.Clienter, 0, len(nodeURL))
	for _, url := range nodeURL {
		c, err := NewClient(url, chainName)
		if err != nil {
			panic(err)
		}
		clients = append(clients, c)
	}
	spider := chain.NewBlockSpider(NewStateStore(chainName), clients...)
	if len(c.StandbyRPCURL) > 0 {
		standby := make([]chain.Clienter, 0, len(c.StandbyRPCURL))
		for _, url := range c.StandbyRPCURL {
			c, err := NewClient(url, chainName)
			if err != nil {
				panic(err)
			}
			standby = append(standby, c)
		}
		spider.AddStandby(standby...)
	}
	spider.Watch(common.NewDectorZapWatcher(chainName))

	return &Platform{
		CoinIndex: coins.HandleMap[handler],
		NodeURL:   nodeURL[0],
		CommPlatform: subhandle.CommPlatform{
			Height:         height,
			Chain:          chainType,
			ChainName:      chainName,
			HeightAlarmThr: int(c.GetMonitorHeightAlarmThr()),
		},
		UrlList: nodeURL,
		spider:  spider,
		conf:    c,
	}
}

func (p *Platform) SetNodeURL(nodeURL string) {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	p.NodeURL = nodeURL
}

func (p *Platform) Coin() coins.Coin {
	return coins.Coins[p.CoinIndex]
}

func (p *Platform) GetTransactions() {
	log.Info(
		"GetTransactions starting, chainName:"+p.ChainName,
		zap.Bool("roundRobinConcurrent", p.conf.GetRoundRobinConcurrent()),
	)

	if p.conf.GetRoundRobinConcurrent() {
		p.spider.EnableRoundRobin()
	}

	liveInterval := time.Duration(p.Coin().LiveInterval) * time.Millisecond

	p.spider.StartIndexBlock(
		newHandler(p.ChainName, liveInterval),
		int(p.conf.GetSafelyConcurrentBlockDelta()),
		int(p.conf.GetMaxConcurrency()),
	)
}

func (p *Platform) GetTransactionResultByTxhash() {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("GetTransactionsResult error, chainName:"+p.ChainName, e)
			} else {
				log.Errore("GetTransactionsResult panic, chainName:"+p.ChainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链处理交易结果失败, error：%s", p.ChainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	liveInterval := time.Duration(p.Coin().LiveInterval) * time.Millisecond
	p.spider.SealPendingTransactions(newHandler(p.ChainName, liveInterval))
}

func BatchSaveOrUpdate(txRecords []*data.EvmTransactionRecord, tableName string) error {
	total := len(txRecords)
	pageSize := biz.PAGE_SIZE
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	for start < stop {
		subTxRecords := txRecords[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}

		_, err := data.EvmTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = data.EvmTransactionRecordRepoClient.BatchSaveOrUpdateSelective(nil, tableName, subTxRecords)
		}
		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY) {
			return err
		}
	}
	return nil
}

// nonstandardEVM returns true if the block hash should be retrieved from tx receipt.
func isNonstandardEVM(chainName string) bool {
	return chainName == "OEC" || chainName == "Optimism" || chainName == "Cronos" || chainName == "Polygon" ||
		chainName == "Fantom" || chainName == "Avalanche" || chainName == "Klaytn" || chainName == "xDai" ||
		chainName == "OECTEST" || chainName == "OptimismTEST" || chainName == "CronosTEST" || chainName == "PolygonTEST" ||
		chainName == "FantomTEST" || chainName == "AvalancheTEST" || chainName == "KlaytnTEST" || chainName == "xDaiTEST"
}
