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
const ZKSYNC_CODE = "0x000000000000000000000000000000000000800A"
const ZKSYNC_ADDRESS = "0x0000000000000000000000000000000000008001"
const BRIDGE_TRANSFERNATIVE = "0xb4a87134099d10c48345145381989042ab07dc53e6e62a6511fca55438562e26"
const FANTOM_SWAPED = "0x76af224a143865a50b41496e1a73622698692c565c1214bc862f18e22d829c5e"
const FANTOM_SWAPED_V1 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
const FANTOM_NEWLIQUIDITYORDER = "0x66340b656a5a83a68f9bf9206f8b20299c11d9c3c7c5ae3bc6ab4ba12f82927e"
const ARBITRUM_TRANSFERNATIVE = "0x79fa08de5149d912dce8e5e8da7a7c17ccdf23dd5d3bfe196802e6eb86347c7c"
const WITHDRAWETH_TOPIC = "0x94effa14ea3a1ef396fa2fd829336d1597f1d76b548c26bfa2332869706638af"
const OPTIMISM_FANTOM_LOGANYSWAPIN = "0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55"
const OPTIMISM_NONE = "0x4f56ec39e98539920503fd54ee56ae0cbebe9eb15aa778f18de67701eeae7c65"
const TOKENSWAP_TOPIC = "0xc6c1e0630dbe9130cc068028486c0d118ddcea348550819defd5cb8c257f8a38"
const ARBITRUM_UNLOCKEVENT = "0xd90288730b87c2b8e0c45bd82260fd22478aba30ae1c4d578b8daba9261604df"
const KLAYTN_EXCHANGEPOS = "0x022d176d604c15661a2acf52f28fd69bdd2c755884c08a67132ffeb8098330e0"

const ARBITRUM_GMX_SWAP = "0x0874b2d545cb271cdbda4e093020c452328b24af12382ed62c4d00f5c26709db"
const ARBITRUM_GMX_EXECUTEDECREASEPOSITION = "0x21435c5b618d77ff3657140cd3318e2cffaebc5e0e1b7318f56a9ba4044c3ed2"
const ETH_BRIDGECALLTRIGGERED = "0x2d9d115ef3e4a606d698913b1eae831a3cdfe20d9a83d48007b0526749c3d466"
const ARBITRUM_GMX_SWAP_V2 = "0xcd3829a3813dc3cdd188fd3d01dcf3268c16be2fdd2dd21d0665418816e46062"
const MATIC_BRIDGE = "0xe6497e3ee548a3372136af2fcb0696db31fc6cf20260707645068bd3fe97f3c4"
const WITHDRAWALBONDED_TOPIC = "0x0c3d250c7831051e78aa6a56679e590374c7c424415ffe4aa474491def2fe705"
const REDEEM_TOPIC = "0xe02f6383e19e87c24e0c03e2cd5dbd05156cb29a1b0f3dbca1fa3430e444f63d"
const NEWPOSITIONORDER_TOPIC = "0x00da46badc3d23e4ffbd09bb00128a2274573502fdbb15d233ec3441c01b6af3"
const CLAIMED_TOPIC = "0x987d620f307ff6b94d58743cb7a7509f24071586a77759b77c2d4e29f75a2f9a"
const RUN_METHOD_TOPIC = "0x5beea7b3b87c573953fec05007114d17712e5775d364acc106d8da9e74849033"
const SOLDLISTING_TOPIC = "0xa803ea994bad68b4565b18fffd9978cdc51c754097460d94eb39a3412eb31a3b"
const FILL_TOPIC = "0x35d3c32d322e234e99b7f37b3ff4cb69c8a4027a04bee33d3a716da66ee1e6a1"
const SEND_TOPIC = "0x2f824f69f211e444df15d741157e83cdf23c50f39399b9523853a84b91379ca6"
const BASE_TOPIC = "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31"

// openBlock跨链Swap
const OPENBLOCK_SWAP_TOPIC = "0x5faac1ba95d8a22d60a2290d300369a4a7b8334e60291f5f6c83ee09f03cab38"
const OPENBLOCK_SWAP_ADDRESS_TOPIC = "0x893b740df66b6918f171be0e008e349151ce603b7b3dd31603afe3e635db87a6"

// 用户将代币授权给手续费代付合约操作无主币付手续费时，平台会给用户转一些主币垫资手续费
const GAS_PAY_TOPIC = "0x4e5abb321ba55df7284bf55a0bca37c5c3ce2044bb229403828ac6d1911cdc62"

// 手续费代付
const FEE_TOPIC = "0x496c2c66bc44ed4dbeaf44a930a49ceaf848c046517dbec592243394b4e871d6"
const TRANSFER_FROM_L1_COMPLETED_TOPIC = "0x320958176930804eb66c2343c7343fc0367dc16249590c0f195783bee199d094"
const FINALIZEDEPOSITETH_TOPIC = "0x9e86c356e14e24e26e3ce769bf8b87de38e0faa0ed0ca946fa09659aa606bd2d"

// dapp 白名单 chainName + contractAddress + methodId
var BridgeWhiteMethodIdList = map[string][]string{
	//dapp: https://stargate.finance/ 代币，主币 兑换
	"Optimism_MethodId": {"Optimism_0x81E792e5a9003CC1C8BF5569A00f34b65d75b017_252f7b01", "Optimism_0x81E792e5a9003CC1C8BF5569A00f34b65d75b017_0508941e",
		"Optimism_0x8F957Ed3F969D7b6e5d6dF81e61A5Ff45F594dD1_4782f779", "Optimism_0xDC42728B0eA910349ed3c6e1c9Dc06b5FB591f98_0175b1c4",
		"Optimism_0xa81D244A1814468C734E5b4101F7b9c0c577a8fC_3d12a85a", "Optimism_0xAf41a65F786339e7911F4acDAD6BD49426F2Dc6b_17357892",
		"Optimism_0x83f6244Bd87662118d96D9a6D44f09dffF14b30E_3d12a85a", "Optimism_0x9D39Fc627A6d9d9F8C831c16995b209548cc3401_cdd1b25d",
		"Optimism_0xF480f38C366dAaC4305dC484b2Ad7a496FF00CeA_0175b1c4", "Optimism_0x8F957Ed3F969D7b6e5d6dF81e61A5Ff45F594dD1_d9caed12"},

	//dapp: https://cbridge.celer.network/10/42161/ETH?ref=bitkeep2022
	"Arbitrum_MethodId": {"Arbitrum_0x1619DE6B6B20eD217a58d00f37B9d47C7663feca_cdd1b25d", "Arbitrum_0x7ceA671DABFBa880aF6723bDdd6B9f4caA15C87B_d450e04c",
		"Arbitrum_0x11D62807dAE812a0F1571243460Bf94325F43BB7_574ec1be", "Arbitrum_0x650Af55D5877F289837c30b94af91538a7504b76_0175b1c4",
		"Arbitrum_0xaBBc5F99639c9B6bCb58544ddf04EFA6802F4064_2d4ba6a7", "Arbitrum_0x8F957Ed3F969D7b6e5d6dF81e61A5Ff45F594dD1_d9caed12",
		"Arbitrum_0x0e0E3d2C5c292161999474247956EF542caBF8dd_3d12a85a", "Arbitrum_0x72209Fe68386b37A40d6bCA04f78356fd342491f_3d12a85a",
		"Arbitrum_0x7aC115536FE3A185100B2c4DE4cb328bf3A58Ba6_3d12a85a", "Arbitrum_0xa71353Bb71DdA105D383B02fc2dD172C4D39eF8B_0175b1c4",
		"Arbitrum_0x8F957Ed3F969D7b6e5d6dF81e61A5Ff45F594dD1_4782f779"},

	"Fantom_MethodId": {"Fantom_0xf3Ce95Ec61114a4b1bFC615C16E6726015913CCC_0175b1c4", "Fantom_0x0c30b10462CdED51C3CA31e7C51019b7d25a965B_d0ee758c", "Fantom_0x0c30b10462CdED51C3CA31e7C51019b7d25a965B_4af3e017",
		"Fantom_0x374B8a9f3eC5eB2D97ECA84Ea27aCa45aa1C57EF_cdd1b25d", "Fantom_0x8F957Ed3F969D7b6e5d6dF81e61A5Ff45F594dD1_d9caed12"},
	"Polygon_MethodId": {"Polygon_0x2eF4A574b72E1f555185AfA8A09c6d1A8AC4025C_0175b1c4", "Polygon_0x242Ea2A8C4a3377A738ed8a0d8cC0Fe8B4D6C36E_d9caed12", "Polygon_0xa9a6A3626993D487d2Dbda3173cf58cA1a9D9e9f_1bf7e13e",
		"Polygon_0x25D8039bB044dC227f741a9e381CA4cEAE2E6aE8_3d12a85a", "Polygon_0x553bC791D746767166fA3888432038193cEED5E2_3d12a85a", "Polygon_0x88DCDC47D2f83a99CF0000FDF667A468bB958a78_cdd1b25d",
		"Polygon_0xf0511f123164602042ab2bCF02111fA5D3Fe97CD_41706c4e", "Polygon_0xad3b67BCA8935Cb510C8D18bD45F0b94F54A968f_06bb5402", "Polygon_0x6c9a1ACF73bd85463A46B0AFc076FBdf602b690B_3d12a85a",
		"Polygon_0x0000000000000000000000000000000000000000_"},
	"ArbitrumNova_MethodId": {"ArbitrumNova_0x28e0f3ebab59a998C4f1019358388B5E2ca92cfA_18cbafe5", "ArbitrumNova_0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506_18cbafe5",
		"ArbitrumNova_0xEe01c0CD76354C383B8c7B4e65EA88D00B06f36f_18cbafe5", "ArbitrumNova_0x639A647fbe20b6c8ac19E48E2de44ea792c62c5C_0175b1c4"},
	"SmartBCH_MethodId": {"SmartBCH_0x639A647fbe20b6c8ac19E48E2de44ea792c62c5C_825bb13c"},
	"ETC_MethodId":      {"ETC_0x5D9ab5522c64E1F6ef5e3627ECCc093f56167818_0175b1c4"},
	"ETH_MethodId": {"ETH_0xBa8Da9dcF11B50B03fd5284f164Ef5cdEF910705_0175b1c4", "ETH_0x00000000006c3852cbEf3e08E8dF289169EdE581_ed98a574", "ETH_0x92e929d8B2c8430BcAF4cD87654789578BB2b786_d9caed12",
		"ETH_0xe0B6e0a0e1ddB6476afd64Da80B990c24DEf31E7_a8739642", "ETH_0x00000000006c3852cbEf3e08E8dF289169EdE581_87201b41", "ETH_0x178A86D36D89c7FDeBeA90b739605da7B131ff6A_760f2a0b",
		"ETH_0x22B1Cbb8D98a01a3B71D034BB899775A76Eb1cc2_23c452cd", "ETH_0x84a0856b038eaAd1cC7E297cF34A7e72685A8693_41706c4e", "ETH_0x6d4506A451f5ec7475582Bae1d1cd47D0aA89ECA_04cdf566"},
	"BSC_MethodId": {"BSC_0x1eD5685F345b2fa564Ea4a670dE1Fde39e484751_d9caed12", "BSC_0x749Cf83EBD327A71bfbD16515Ff39f6c0610EAEA_a8739642", "BSC_0xdd90E5E87A2081Dcf0391920868eBc2FFB81a1aF_cdd1b25d",
		"BSC_0x86C80a8aa58e0A4fa09A69624c31Ab2a6CAD56b8_41706c4e"},
	"OEC_MethodId":     {"OEC_0x37809F06F0Daf8f1614e8a31076C9bbEF4992Ff9_d9caed12"},
	"Conflux_MethodId": {"Conflux_0x0dCb0CB0120d355CdE1ce56040be57Add0185BAa_0175b1c4"},
	"xDai_MethodId": {"xDai_0x25D8039bB044dC227f741a9e381CA4cEAE2E6aE8_3d12a85a", "xDai_0xFD5a186A7e8453Eb867A360526c5d987A00ACaC2_3d12a85a", "xDai_0x0460352b91D7CF42B0E1C1c30f06B602D9ef2238_3d12a85a",
		"xDai_0x75Df5AF045d91108662D8080fD1FEFAd6aA0bb59_e7a2c01f"},
	"Avalanche_MethodId":     {"Avalanche_0xef3c714c9425a8F3697A9C969Dc1af30ba82e5d4_cdd1b25d", "Avalanche_0xef3c714c9425a8F3697A9C969Dc1af30ba82e5d4_cdd1b25d"},
	"PolygonTEST_MethodId":   {"PolygonTEST_0x69015912AA33720b842dCD6aC059Ed623F28d9f7_41706c4e"},
	"BSCTEST_MethodId":       {"BSCTEST_0x61456BF1715C1415730076BB79ae118E806E74d2_41706c4e"},
	"ETHGoerliTEST_MethodId": {"ETHGoerliTEST_0xE041608922d06a4F26C0d4c27d8bCD01daf1f792_41706c4e"},
	"Ronin_MethodId":         {"Ronin_0xffF9Ce5f71ca6178D3BEEcEDB61e7Eff1602950E_95a4ec00"},
}

//TODO, 后面弃用上面的方式，改用下面的方式

// method白名单列表 chainName + contractAddress + method
var WhiteListMethodMap = map[string][]string{
	//支持特定链特定合约
	"ETH_Contract_Method":     {"0x10fC9e6Ed49E648bde6b6214f07d8e63E12349E8_11290d59"},
	"BSC_Contract_Method":     {"0xa2d57fa083aD42Fe7d042FE0794cFeF13bd2603c_11290d59"},
	"Polygon_Contract_Method": {"0xD0Db2F29056E0226168c6b32363A339Fe8fD46b5_11290d59"},
	//支持所有链特定合约
	//"Contract_Method": {"0xB7FDda5330DaEA72514Db2b84211afEBD19277Ca_4630a0d8"},
	//支持所有链所有合约
	//"Method": {"11290d59"},
}

// dapp 白名单 chainName + contractAddress + topic
var BridgeWhiteTopicList = map[string][]string{
	//swap：对应 function topic0
	"Optimism_Topic": {"Optimism_0x81e792e5a9003cc1c8bf5569a00f34b65d75b017_0xb4a87134099d10c48345145381989042ab07dc53e6e62a6511fca55438562e26",
		"Optimism_0xdc42728b0ea910349ed3c6e1c9dc06b5fb591f98_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55",
		//https://synapseprotocol.com/?inputCurrency=USDC&outputCurrency=USDC&outputChain=42161
		"Optimism_0xaf41a65f786339e7911f4acdad6bd49426f2dc6b_0x4f56ec39e98539920503fd54ee56ae0cbebe9eb15aa778f18de67701eeae7c65",
		"Optimism_0xf480f38c366daac4305dc484b2ad7a496ff00cea_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55",
		"Optimism_0x9d39fc627a6d9d9f8c831c16995b209548cc3401_0x79fa08de5149d912dce8e5e8da7a7c17ccdf23dd5d3bfe196802e6eb86347c7c"},
	//OpenOceanExchange.V2 https://openocean.finance/
	"Fantom_Topic": {"Fantom_0x6352a56caadc4f1e25cd6c75970fa768a3304e64_0x76af224a143865a50b41496e1a73622698692c565c1214bc862f18e22d829c5e",
		"Fantom_0x31f63a33141ffee63d4b26755430a390acdd8a4d_0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
		"Fantom_0xf3ce95ec61114a4b1bfc615c16e6726015913ccc_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55",
		"Fantom_0x0c30b10462cded51c3ca31e7c51019b7d25a965b_0x66340b656a5a83a68f9bf9206f8b20299c11d9c3c7c5ae3bc6ab4ba12f82927e"},
	"Arbitrum_Topic": {"Arbitrum_0x1619de6b6b20ed217a58d00f37b9d47c7663feca_0x79fa08de5149d912dce8e5e8da7a7c17ccdf23dd5d3bfe196802e6eb86347c7c",
		//合约内部 调用amount
		"Arbitrum_0x7cea671dabfba880af6723bddd6b9f4caa15c87b_0xc6c1e0630dbe9130cc068028486c0d118ddcea348550819defd5cb8c257f8a38",
		"Arbitrum_0x3749c4f034022c39ecaffaba182555d4508caccc_0xc6c1e0630dbe9130cc068028486c0d118ddcea348550819defd5cb8c257f8a38",
		"Arbitrum_0x7cea671dabfba880af6723bddd6b9f4caa15c87b_0xd90288730b87c2b8e0c45bd82260fd22478aba30ae1c4d578b8daba9261604df",
		"Arbitrum_0x650af55d5877f289837c30b94af91538a7504b76_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55",
		"Arbitrum_0x11d62807dae812a0f1571243460bf94325f43bb7_0x0874b2d545cb271cdbda4e093020c452328b24af12382ed62c4d00f5c26709db",
		"Arbitrum_0x11d62807dae812a0f1571243460bf94325f43bb7_0x21435c5b618d77ff3657140cd3318e2cffaebc5e0e1b7318f56a9ba4044c3ed2",
		"Arbitrum_0xabbc5f99639c9b6bcb58544ddf04efa6802f4064_0xcd3829a3813dc3cdd188fd3d01dcf3268c16be2fdd2dd21d0665418816e46062",
		"Arbitrum_0xa71353bb71dda105d383b02fc2dd172c4d39ef8b_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55"},
	"Klaytn_Topic": {"Klaytn_0xc6a2ad8cc6e4a7e08fc37cc5954be07d499e7654_0x022d176d604c15661a2acf52f28fd69bdd2c755884c08a67132ffeb8098330e0"},
	"Polygon_Topic": {"Polygon_0x2ef4a574b72e1f555185afa8a09c6d1a8ac4025c_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55",
		"Polygon_0x553bc791d746767166fa3888432038193ceed5e2_0xe6497e3ee548a3372136af2fcb0696db31fc6cf20260707645068bd3fe97f3c4",
		"Polygon_0xd0db2f29056e0226168c6b32363a339fe8fd46b5_0xe6497e3ee548a3372136af2fcb0696db31fc6cf20260707645068bd3fe97f3c4",
		"Polygon_0xb7fdda5330daea72514db2b84211afebd19277ca_0xe6497e3ee548a3372136af2fcb0696db31fc6cf20260707645068bd3fe97f3c4"},
	"ETH_Topic": {
		"ETH_0x0b9857ae2d4a3dbe74ffe1d7df045bb7f96e4840_0x2d9d115ef3e4a606d698913b1eae831a3cdfe20d9a83d48007b0526749c3d466",
		"ETH_0xba8da9dcf11b50b03fd5284f164ef5cdef910705_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55",
		"ETH_0xb8901acb165ed027e32754e0ffe830802919727f_0x0c3d250c7831051e78aa6a56679e590374c7c424415ffe4aa474491def2fe705"},
	"ArbitrumNova_Topic": {"ArbitrumNova_0x28e0f3ebab59a998c4f1019358388b5e2ca92cfa_0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
		"ArbitrumNova_0x1b02da8cb0d097eb8d57a175b88c7d8b47997506_0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
		"ArbitrumNova_0xee01c0cd76354c383b8c7b4e65ea88d00b06f36f_0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
		"ArbitrumNova_0x639a647fbe20b6c8ac19e48e2de44ea792c62c5c_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55"},
	"ETC_Topic":     {"ETC_0x5d9ab5522c64e1f6ef5e3627eccc093f56167818_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55"},
	"Conflux_Topic": {"Conflux_0x0dcb0cb0120d355cde1ce56040be57add0185baa_0xaac9ce45fe3adf5143598c4f18a369591a20a3384aedaf1b525d29127e1fcd55"},
	"BSC_Topic": {"BSC_0x33a32f0ad4aa704e28c93ed8ffa61d50d51622a7_0xe02f6383e19e87c24e0c03e2cd5dbd05156cb29a1b0f3dbca1fa3430e444f63d",
		"BSC_0xa67aa293642c4e02d1b9f360b007c0dbdc451a08_0x00da46badc3d23e4ffbd09bb00128a2274573502fdbb15d233ec3441c01b6af3"},
}

//TODO, 后面弃用上面的方式，改用下面的方式

// topic白名单列表 chainName + contractAddress + method + topic
var WhiteListTopicMap = map[string][]string{
	//支持特定链特定合约
	"ETH_Contract_Method_Topic": {"0x10fc9e6ed49e648bde6b6214f07d8e63e12349e8_11290d59_" + OPENBLOCK_SWAP_TOPIC},
	"BSC_Contract_Method_Topic": {"0xa2d57fa083ad42fe7d042fe0794cfef13bd2603c_11290d59_" + OPENBLOCK_SWAP_TOPIC},
	"Polygon_Contract_Method_Topic": {"0x0000000000000000000000000000000000000000__" + TRANSFER_FROM_L1_COMPLETED_TOPIC,
		"0xd0db2f29056e0226168c6b32363a339fe8fd46b5_11290d59_" + OPENBLOCK_SWAP_TOPIC},
	"Scroll_Contract_Method_Topic": {"0x781e90f1c8fc4611c9b7497c3b47f99ef6969cbc_8ef1332e_" + FINALIZEDEPOSITETH_TOPIC},
	//支持所有链特定合约
	"Contract_Method_Topic": {"0xe2b90003d4ab5a1c6885262865a38c074c5f3e2d_5b4363bf_" + CLAIMED_TOPIC,
		"0x86232f68b5bf2a3a03851d98556352512a3b12b9_ba847759_" + RUN_METHOD_TOPIC,
		"0x39523401c0fc321660dc8fb37f285cdc141faa41_0d7bb214_" + SOLDLISTING_TOPIC,
		"0xbf6bfe5d6b86308cf3b7f147dd03ef11f80bfde3_e98b3b7e_" + FILL_TOPIC,
		"0x00000000000000adc04c56bf30ac9d3c0aaf14dc_00000000_" + BASE_TOPIC,
		"0xb7fdda5330daea72514db2b84211afebd19277ca_4630a0d8_" + OPENBLOCK_SWAP_TOPIC},
	//支持所有链所有合约
	"Method_Topic": {"4782f779_" + WITHDRAWETH_TOPIC, "0ddedd84_" + SEND_TOPIC, "1e9d2490_" + GAS_PAY_TOPIC, "11290d59_" + FEE_TOPIC},
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

func Init(handler string, c *conf.PlatInfo, nodeURL []string) *Platform {
	chainType := c.Handler // ethereum
	chainName := c.Chain   // ETH

	return &Platform{
		CoinIndex:     coins.HandleMap[handler],
		kanbanEnabled: c.GetEnableKanban(),
		CommPlatform: biz.CommPlatform{
			Chain:          chainType,
			ChainName:      chainName,
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
	return NewClient(url, p.ChainName)
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
	var ssr []biz.SignStatusRequest
	for _, r := range txRecords {
		ssr = append(ssr, biz.SignStatusRequest{
			TransactionHash: r.TransactionHash,
			Status:          r.Status,
			TransactionType: r.TransactionType,
			Nonce:           r.Nonce,
			TxTime:          r.TxTime,
		})
	}
	go biz.SyncStatus(ssr)
	return nil
}
