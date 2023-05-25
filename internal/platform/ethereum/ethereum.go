package ethereum

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	"block-crawling/internal/data/kanban"
	"fmt"
	"strings"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
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
const FILE_BLOCK_NULL = "requested epoch was a null round"
const BLOCK_NONAL_TRANSCATION = "server returned non-empty transaction list but block header indicates no transactions"
const TOO_MANY_REQUESTS = "429 Too Many Requests"
const POLYGON_CODE = "0x0000000000000000000000000000000000001010"
const ZKSYNC_ADDRESS = "0x0000000000000000000000000000000000008001"
const BRIDGE_TRANSFERNATIVE = "0xb4a87134099d10c48345145381989042ab07dc53e6e62a6511fca55438562e26"
const FANTOM_SWAPED = "0x76af224a143865a50b41496e1a73622698692c565c1214bc862f18e22d829c5e"
const FANTOM_SWAPED_V1 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
const FANTOM_NEWLIQUIDITYORDER = "0x66340b656a5a83a68f9bf9206f8b20299c11d9c3c7c5ae3bc6ab4ba12f82927e"
const ARBITRUM_TRANSFERNATIVE = "0x79fa08de5149d912dce8e5e8da7a7c17ccdf23dd5d3bfe196802e6eb86347c7c"
const OPTIMISM_WITHDRAWETH = "0x94effa14ea3a1ef396fa2fd829336d1597f1d76b548c26bfa2332869706638af"
const OPTIMISM_FANTOM_LOGANYSWAPIN = "0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55"
const OPTIMISM_NONE = "0x4f56ec39e98539920503fd54ee56ae0cbebe9eb15aa778f18de67701eeae7c65"
const ARBITRUM_INTXAMOUNT = "0xc6c1e0630dbe9130cc068028486c0d118ddcea348550819defd5cb8c257f8a38"
const ARBITRUM_UNLOCKEVENT = "0xd90288730b87c2b8e0c45bd82260fd22478aba30ae1c4d578b8daba9261604df"
const KLAYTN_EXCHANGEPOS = "0x022d176d604c15661a2acf52f28fd69bdd2c755884c08a67132ffeb8098330e0"

const ARBITRUM_GMX_SWAP = "0x0874b2d545cb271cdbda4e093020c452328b24af12382ed62c4d00f5c26709db"
const ARBITRUM_GMX_EXECUTEDECREASEPOSITION = "0x21435c5b618d77ff3657140cd3318e2cffaebc5e0e1b7318f56a9ba4044c3ed2"
const ETH_BRIDGECALLTRIGGERED = "0x2d9d115ef3e4a606d698913b1eae831a3cdfe20d9a83d48007b0526749c3d466"
const ARBITRUM_GMX_SWAP_V2 = "0xcd3829a3813dc3cdd188fd3d01dcf3268c16be2fdd2dd21d0665418816e46062"
const MATIC_BRIDGE = "0xe6497e3ee548a3372136af2fcb0696db31fc6cf20260707645068bd3fe97f3c4"

// dapp 白名单 chainName + contractAddress + methodId
var BridgeWhiteMethodIdList = map[string][]string{
	//dapp: https://stargate.finance/ 代币，主币 兑换
	"Optimism_MethodId": {"Optimism_0x81E792e5a9003CC1C8BF5569A00f34b65d75b017_252f7b01", "Optimism_0x81E792e5a9003CC1C8BF5569A00f34b65d75b017_0508941e",
		"Optimism_0x8F957Ed3F969D7b6e5d6dF81e61A5Ff45F594dD1_4782f779", "Optimism_0xDC42728B0eA910349ed3c6e1c9Dc06b5FB591f98_0175b1c4",
		"Optimism_0xa81D244A1814468C734E5b4101F7b9c0c577a8fC_3d12a85a", "Optimism_0xAf41a65F786339e7911F4acDAD6BD49426F2Dc6b_17357892",
		"Optimism_0x83f6244Bd87662118d96D9a6D44f09dffF14b30E_3d12a85a", "Optimism_0x9D39Fc627A6d9d9F8C831c16995b209548cc3401_cdd1b25d",
		"Optimism_0xF480f38C366dAaC4305dC484b2Ad7a496FF00CeA_0175b1c4"},

	//dapp: https://cbridge.celer.network/10/42161/ETH?ref=bitkeep2022
	"Arbitrum_MethodId": {"Arbitrum_0x1619DE6B6B20eD217a58d00f37B9d47C7663feca_cdd1b25d", "Arbitrum_0x7ceA671DABFBa880aF6723bDdd6B9f4caA15C87B_d450e04c",
		"Arbitrum_0x11D62807dAE812a0F1571243460Bf94325F43BB7_574ec1be", "Arbitrum_0x650Af55D5877F289837c30b94af91538a7504b76_0175b1c4",
		"Arbitrum_0xaBBc5F99639c9B6bCb58544ddf04EFA6802F4064_2d4ba6a7", "Arbitrum_0x8F957Ed3F969D7b6e5d6dF81e61A5Ff45F594dD1_d9caed12",
		"Arbitrum_0x0e0E3d2C5c292161999474247956EF542caBF8dd_3d12a85a", "Arbitrum_0x72209Fe68386b37A40d6bCA04f78356fd342491f_3d12a85a",
		"Arbitrum_0x7aC115536FE3A185100B2c4DE4cb328bf3A58Ba6_3d12a85a", "Arbitrum_0xa71353Bb71DdA105D383B02fc2dD172C4D39eF8B_0175b1c4"},

	"Fantom_MethodId": {"Fantom_0xf3Ce95Ec61114a4b1bFC615C16E6726015913CCC_0175b1c4", "Fantom_0x0c30b10462CdED51C3CA31e7C51019b7d25a965B_d0ee758c", "Fantom_0x0c30b10462CdED51C3CA31e7C51019b7d25a965B_4af3e017",
		"Fantom_0x374B8a9f3eC5eB2D97ECA84Ea27aCa45aa1C57EF_cdd1b25d","Fantom_0x8F957Ed3F969D7b6e5d6dF81e61A5Ff45F594dD1_d9caed12"},
	"Polygon_MethodId": {"Polygon_0x2eF4A574b72E1f555185AfA8A09c6d1A8AC4025C_0175b1c4", "Polygon_0x242Ea2A8C4a3377A738ed8a0d8cC0Fe8B4D6C36E_d9caed12", "Polygon_0xa9a6A3626993D487d2Dbda3173cf58cA1a9D9e9f_1bf7e13e",
		"Polygon_0x25D8039bB044dC227f741a9e381CA4cEAE2E6aE8_3d12a85a", "Polygon_0x553bC791D746767166fA3888432038193cEED5E2_3d12a85a", "Polygon_0x88DCDC47D2f83a99CF0000FDF667A468bB958a78_cdd1b25d",
		"Polygon_0xf0511f123164602042ab2bCF02111fA5D3Fe97CD_41706c4e"},
	"ArbitrumNova_MethodId": {"ArbitrumNova_0x28e0f3ebab59a998C4f1019358388B5E2ca92cfA_18cbafe5", "ArbitrumNova_0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506_18cbafe5",
		"ArbitrumNova_0xEe01c0CD76354C383B8c7B4e65EA88D00B06f36f_18cbafe5", "ArbitrumNova_0x639A647fbe20b6c8ac19E48E2de44ea792c62c5C_0175b1c4"},
	"SmartBCH_MethodId": {"SmartBCH_0x639A647fbe20b6c8ac19E48E2de44ea792c62c5C_825bb13c"},
	"ETC_MethodId":      {"ETC_0x5D9ab5522c64E1F6ef5e3627ECCc093f56167818_0175b1c4"},
	"ETH_MethodId": {"ETH_0xBa8Da9dcF11B50B03fd5284f164Ef5cdEF910705_0175b1c4", "ETH_0x00000000006c3852cbEf3e08E8dF289169EdE581_ed98a574", "ETH_0x92e929d8B2c8430BcAF4cD87654789578BB2b786_d9caed12",
		"ETH_0xe0B6e0a0e1ddB6476afd64Da80B990c24DEf31E7_a8739642", "ETH_0x00000000006c3852cbEf3e08E8dF289169EdE581_87201b41", "ETH_0x178A86D36D89c7FDeBeA90b739605da7B131ff6A_760f2a0b",
		"ETH_0x22B1Cbb8D98a01a3B71D034BB899775A76Eb1cc2_23c452cd", "ETH_0x84a0856b038eaAd1cC7E297cF34A7e72685A8693_41706c4e"},
	"BSC_MethodId": {"BSC_0x1eD5685F345b2fa564Ea4a670dE1Fde39e484751_d9caed12", "BSC_0x749Cf83EBD327A71bfbD16515Ff39f6c0610EAEA_a8739642", "BSC_0xdd90E5E87A2081Dcf0391920868eBc2FFB81a1aF_cdd1b25d",
		"BSC_0x86C80a8aa58e0A4fa09A69624c31Ab2a6CAD56b8_41706c4e"},
	"OEC_MethodId":     {"OEC_0x37809F06F0Daf8f1614e8a31076C9bbEF4992Ff9_d9caed12"},
	"Conflux_MethodId": {"Conflux_0x0dCb0CB0120d355CdE1ce56040be57Add0185BAa_0175b1c4"},
	"xDai_MethodId": {"xDai_0x25D8039bB044dC227f741a9e381CA4cEAE2E6aE8_3d12a85a", "xDai_0xFD5a186A7e8453Eb867A360526c5d987A00ACaC2_3d12a85a", "xDai_0x0460352b91D7CF42B0E1C1c30f06B602D9ef2238_3d12a85a",
		"xDai_0x75Df5AF045d91108662D8080fD1FEFAd6aA0bb59_e7a2c01f"},
	"Avalanche_MethodId":   {"Avalanche_0xef3c714c9425a8F3697A9C969Dc1af30ba82e5d4_cdd1b25d", "Avalanche_0xef3c714c9425a8F3697A9C969Dc1af30ba82e5d4_cdd1b25d"},
	"PolygonTEST_MethodId": {"PolygonTEST_0x69015912AA33720b842dCD6aC059Ed623F28d9f7_41706c4e"},
	"BSCTEST_MethodId":     {"BSCTEST_0x61456BF1715C1415730076BB79ae118E806E74d2_41706c4e"},
}

// dapp 白名单 chainName + contractAddress + topic
var BridgeWhiteTopicList = map[string][]string{
	//swap：对应 function topic0
	"Optimism_Topic": {"Optimism_0x81e792e5a9003cc1c8bf5569a00f34b65d75b017_0xb4a87134099d10c48345145381989042ab07dc53e6e62a6511fca55438562e26",
		"Optimism_0x8f957ed3f969d7b6e5d6df81e61a5ff45f594dd1_0x94effa14ea3a1ef396fa2fd829336d1597f1d76b548c26bfa2332869706638af",
		"Optimism_0xdc42728b0ea910349ed3c6e1c9dc06b5fb591f98_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55",
		//https://synapseprotocol.com/?inputCurrency=USDC&outputCurrency=USDC&outputChain=42161
		"Optimism_0xaf41a65f786339e7911f4acdad6bd49426f2dc6b_0x4f56ec39e98539920503fd54ee56ae0cbebe9eb15aa778f18de67701eeae7c65",
		"Optimism_0xf480f38c366daac4305dc484b2ad7a496ff00cea_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55"},
	//OpenOceanExchange.V2 https://openocean.finance/
	"Fantom_Topic": {"Fantom_0x6352a56caadc4f1e25cd6c75970fa768a3304e64_0x76af224a143865a50b41496e1a73622698692c565c1214bc862f18e22d829c5e",
		"Fantom_0x31f63a33141ffee63d4b26755430a390acdd8a4d_0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
		"Fantom_0xf3ce95ec61114a4b1bfc615c16e6726015913ccc_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55",
		"Fantom_0x0c30b10462cded51c3ca31e7c51019b7d25a965b_0x66340b656a5a83a68f9bf9206f8b20299c11d9c3c7c5ae3bc6ab4ba12f82927e"},
	"Arbitrum_Topic": {"Arbitrum_0x1619de6b6b20ed217a58d00f37b9d47c7663feca_0x79fa08de5149d912dce8e5e8da7a7c17ccdf23dd5d3bfe196802e6eb86347c7c",
		//合约内部 调用amount
		"Arbitrum_0x7cea671dabfba880af6723bddd6b9f4caa15c87b_0xc6c1e0630dbe9130cc068028486c0d118ddcea348550819defd5cb8c257f8a38",
		"Arbitrum_0x7cea671dabfba880af6723bddd6b9f4caa15c87b_0xd90288730b87c2b8e0c45bd82260fd22478aba30ae1c4d578b8daba9261604df",
		"Arbitrum_0x650af55d5877f289837c30b94af91538a7504b76_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55",
		"Arbitrum_0x11d62807dae812a0f1571243460bf94325f43bb7_0x0874b2d545cb271cdbda4e093020c452328b24af12382ed62c4d00f5c26709db",
		"Arbitrum_0x11d62807dae812a0f1571243460bf94325f43bb7_0x21435c5b618d77ff3657140cd3318e2cffaebc5e0e1b7318f56a9ba4044c3ed2",
		"Arbitrum_0xabbc5f99639c9b6bcb58544ddf04efa6802f4064_0xcd3829a3813dc3cdd188fd3d01dcf3268c16be2fdd2dd21d0665418816e46062",
		"Arbitrum_0xa71353bb71dda105d383b02fc2dd172c4d39ef8b_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55"},
	"Klaytn_Topic":  {"Klaytn_0xc6a2ad8cc6e4a7e08fc37cc5954be07d499e7654_0x022d176d604c15661a2acf52f28fd69bdd2c755884c08a67132ffeb8098330e0"},
	"Polygon_Topic": {"Polygon_0x2ef4a574b72e1f555185afa8a09c6d1a8ac4025c_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55", "Polygon_0x553bc791d746767166fa3888432038193ceed5e2_0xe6497e3ee548a3372136af2fcb0696db31fc6cf20260707645068bd3fe97f3c4"},
	"ETH_Topic": {
		"ETH_0x0b9857ae2d4a3dbe74ffe1d7df045bb7f96e4840_0x2d9d115ef3e4a606d698913b1eae831a3cdfe20d9a83d48007b0526749c3d466",
		"ETH_0xba8da9dcf11b50b03fd5284f164ef5cdef910705_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55"},
	"ArbitrumNova_Topic": {"ArbitrumNova_0x28e0f3ebab59a998c4f1019358388b5e2ca92cfa_0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
		"ArbitrumNova_0x1b02da8cb0d097eb8d57a175b88c7d8b47997506_0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
		"ArbitrumNova_0xee01c0cd76354c383b8c7b4e65ea88d00b06f36f_0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
		"ArbitrumNova_0x639a647fbe20b6c8ac19e48e2de44ea792c62c5c_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55"},
	"ETC_Topic":     {"ETC_0x5d9ab5522c64e1f6ef5e3627eccc093f56167818_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55"},
	"Conflux_Topic": {"Conflux_0x0dcb0cb0120d355cde1ce56040be57add0185baa_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55"},
}

type Platform struct {
	biz.CommPlatform
	CoinIndex uint
	spider    *chain.BlockSpider

	kanbanEnabled bool
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

	return &Platform{
		CoinIndex:     coins.HandleMap[handler],
		kanbanEnabled: c.GetEnableKanban(),
		CommPlatform: biz.CommPlatform{
			Height:         height,
			Chain:          chainType,
			ChainName:      chainName,
			Source:         c.Source,
			HeightAlarmThr: int(c.GetMonitorHeightAlarmThr()),
		},
	}
}

func (p *Platform) Coin() coins.Coin {
	return coins.Coins[p.CoinIndex]
}

func (p *Platform) CreateStateStore() chain.StateStore {
	return NewStateStore(p.ChainName)
}

func (p *Platform) CreateClient(url string) chain.Clienter {
	c, err := NewClient(url, p.ChainName)
	if err != nil {
		panic(err)
	}
	return c
}

func (p *Platform) CreateBlockHandler(liveInterval time.Duration) chain.BlockHandler {
	return newHandler(p.ChainName, liveInterval, p.kanbanEnabled)
}

func (p *Platform) GetBlockSpider() *chain.BlockSpider {
	return p.spider
}

func (p *Platform) SetBlockSpider(blockSpider *chain.BlockSpider) {
	p.spider = blockSpider
}

func BatchSaveOrUpdate(txRecords []*data.EvmTransactionRecord, tableName string, saveKanban bool) error {
	total := len(txRecords)
	pageSize := biz.PAGE_SIZE
	start := 0
	stop := pageSize
	if stop > total {
		stop = total
	}
	repoMethod := data.EvmTransactionRecordRepoClient.BatchSaveOrUpdateSelective
	if saveKanban {
		repoMethod = kanban.EvmTransactionRecordRepoClient.BatchSaveOrUpdateSelective
	}
	for start < stop {
		subTxRecords := txRecords[start:stop]
		start = stop
		stop += pageSize
		if stop > total {
			stop = total
		}

		_, err := repoMethod(nil, tableName, subTxRecords)
		for i := 0; i < 3 && err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY); i++ {
			time.Sleep(time.Duration(i*1) * time.Second)
			_, err = repoMethod(nil, tableName, subTxRecords)
		}
		if err != nil && !strings.Contains(fmt.Sprintf("%s", err), data.POSTGRES_DUPLICATE_KEY) {
			return err
		}
	}
	return nil
}
