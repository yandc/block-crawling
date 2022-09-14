package common

import (
	"fmt"
)

// Coin is the native currency of a blockchain
type Coin struct {
	ID           uint
	Handle       string
	Symbol       string
	Name         string
	Decimals     uint
	LiveInterval int // Millisecond
}

func (c *Coin) String() string {
	return fmt.Sprintf("[%s] %s (#%d)", c.Symbol, c.Name, c.ID)
}

const (
	ETHEREUM     = 60
	CLASSIC      = 61
	ICON         = 74
	COSMOS       = 118
	RIPPLE       = 144
	STELLAR      = 148
	POA          = 178
	TRON         = 195
	FIO          = 235
	NIMIQ        = 242
	IOTEX        = 304
	ZILLIQA      = 313
	AION         = 425
	AETERNITY    = 457
	KAVA         = 459
	THETA        = 500
	BINANCE      = 714
	VECHAIN      = 818
	CALLISTO     = 820
	TOMOCHAIN    = 889
	THUNDERTOKEN = 1001
	ONTOLOGY     = 1024
	TEZOS        = 1729
	KIN          = 2017
	NEBULAS      = 2718
	GOCHAIN      = 6060
	WANCHAIN     = 5718350
	WAVES        = 5741564
	BITCOIN      = 0
	LITECOIN     = 2
	DOGE         = 3
	DASH         = 5
	VIACOIN      = 14
	GROESTLCOIN  = 17
	ZCASH        = 133
	ZCOIN        = 136
	BITCOINCASH  = 145
	RAVENCOIN    = 175
	QTUM         = 2301
	ZELCASH      = 19167
	DECRED       = 42
	ALGORAND     = 283
	NANO         = 165
	DIGIBYTE     = 20
	HARMONY      = 1023
	KUSAMA       = 434
	POLKADOT     = 354
	SOLANA       = 501
	NEAR         = 397
	ELROND       = 508
	SMARTCHAIN   = 20000714
	FILECOIN     = 461
	OASIS        = 474
	STARCOIN     = 101010
	APTOS        = 101011
)

var HandleMap = map[string]uint{
	"ethereum": ETHEREUM,
	"starcoin": STARCOIN,
	"bitcoin":  BITCOIN,
	"tron":     TRON,
	"aptos":    APTOS,
}

var Coins = map[uint]Coin{
	ETHEREUM: {
		ID:           60,
		Handle:       "ethereum",
		Symbol:       "ETH",
		Name:         "Ethereum",
		Decimals:     18,
		LiveInterval: 1000,
	},
	CLASSIC: {
		ID:           61,
		Handle:       "classic",
		Symbol:       "ETC",
		Name:         "Ethereum Classic",
		Decimals:     18,
		LiveInterval: 1000,
	},
	ICON: {
		ID:           74,
		Handle:       "icon",
		Symbol:       "ICX",
		Name:         "ICON",
		Decimals:     18,
		LiveInterval: 1000,
	},
	COSMOS: {
		ID:           118,
		Handle:       "cosmos",
		Symbol:       "ATOM",
		Name:         "Cosmos",
		Decimals:     6,
		LiveInterval: 1000,
	},
	RIPPLE: {
		ID:           144,
		Handle:       "ripple",
		Symbol:       "XRP",
		Name:         "Ripple",
		Decimals:     6,
		LiveInterval: 1000,
	},
	STELLAR: {
		ID:           148,
		Handle:       "stellar",
		Symbol:       "XLM",
		Name:         "Stellar",
		Decimals:     7,
		LiveInterval: 1000,
	},
	POA: {
		ID:           178,
		Handle:       "poa",
		Symbol:       "POA",
		Name:         "Poa",
		Decimals:     18,
		LiveInterval: 1000,
	},
	TRON: {
		ID:           195,
		Handle:       "tron",
		Symbol:       "TRX",
		Name:         "Tron",
		Decimals:     6,
		LiveInterval: 1000,
	},
	FIO: {
		ID:           235,
		Handle:       "fio",
		Symbol:       "FIO",
		Name:         "FIO",
		Decimals:     9,
		LiveInterval: 1000,
	},
	NIMIQ: {
		ID:           242,
		Handle:       "nimiq",
		Symbol:       "NIM",
		Name:         "Nimiq",
		Decimals:     5,
		LiveInterval: 1000,
	},
	IOTEX: {
		ID:           304,
		Handle:       "iotex",
		Symbol:       "IOTX",
		Name:         "IoTeX",
		Decimals:     18,
		LiveInterval: 1000,
	},
	ZILLIQA: {
		ID:           313,
		Handle:       "zilliqa",
		Symbol:       "ZIL",
		Name:         "Zilliqa",
		Decimals:     12,
		LiveInterval: 1000,
	},
	AION: {
		ID:           425,
		Handle:       "aion",
		Symbol:       "AION",
		Name:         "Aion",
		Decimals:     18,
		LiveInterval: 1000,
	},
	AETERNITY: {
		ID:           457,
		Handle:       "aeternity",
		Symbol:       "AE",
		Name:         "Aeternity",
		Decimals:     18,
		LiveInterval: 1000,
	},
	KAVA: {
		ID:           459,
		Handle:       "kava",
		Symbol:       "KAVA",
		Name:         "Kava",
		Decimals:     6,
		LiveInterval: 1000,
	},
	THETA: {
		ID:           500,
		Handle:       "theta",
		Symbol:       "THETA",
		Name:         "Theta",
		Decimals:     18,
		LiveInterval: 1000,
	},
	BINANCE: {
		ID:           714,
		Handle:       "binance",
		Symbol:       "BNB",
		Name:         "BNB",
		Decimals:     8,
		LiveInterval: 1000,
	},
	VECHAIN: {
		ID:           818,
		Handle:       "vechain",
		Symbol:       "VET",
		Name:         "VeChain Token",
		Decimals:     18,
		LiveInterval: 1000,
	},
	CALLISTO: {
		ID:           820,
		Handle:       "callisto",
		Symbol:       "CLO",
		Name:         "Callisto",
		LiveInterval: 1000,
	},
	TOMOCHAIN: {
		ID:           889,
		Handle:       "tomochain",
		Symbol:       "TOMO",
		Name:         "TOMO",
		Decimals:     18,
		LiveInterval: 1000,
	},
	THUNDERTOKEN: {
		ID:           1001,
		Handle:       "thundertoken",
		Symbol:       "TT",
		Name:         "ThunderCore",
		Decimals:     18,
		LiveInterval: 1000,
	},
	ONTOLOGY: {
		ID:           1024,
		Handle:       "ontology",
		Symbol:       "ONT",
		Name:         "Ontology",
		Decimals:     0,
		LiveInterval: 1000,
	},
	TEZOS: {
		ID:           1729,
		Handle:       "tezos",
		Symbol:       "XTZ",
		Name:         "Tezos",
		Decimals:     6,
		LiveInterval: 1000,
	},
	KIN: {
		ID:           2017,
		Handle:       "kin",
		Symbol:       "KIN",
		Name:         "Kin",
		Decimals:     5,
		LiveInterval: 1000,
	},
	NEBULAS: {
		ID:           2718,
		Handle:       "nebulas",
		Symbol:       "NAS",
		Name:         "Nebulas",
		Decimals:     18,
		LiveInterval: 1000,
	},
	GOCHAIN: {
		ID:           6060,
		Handle:       "gochain",
		Symbol:       "GO",
		Name:         "GoChain GO",
		Decimals:     18,
		LiveInterval: 1000,
	},
	WANCHAIN: {
		ID:           5718350,
		Handle:       "wanchain",
		Symbol:       "WAN",
		Name:         "Wanchain",
		Decimals:     18,
		LiveInterval: 1000,
	},
	WAVES: {
		ID:           5741564,
		Handle:       "waves",
		Symbol:       "WAVES",
		Name:         "WAVES",
		Decimals:     8,
		LiveInterval: 1000,
	},
	BITCOIN: {
		ID:           0,
		Handle:       "bitcoin",
		Symbol:       "BTC",
		Name:         "Bitcoin",
		Decimals:     8,
		LiveInterval: 300000, // 5m
	},
	LITECOIN: {
		ID:           2,
		Handle:       "litecoin",
		Symbol:       "LTC",
		Name:         "Litecoin",
		Decimals:     8,
		LiveInterval: 1000,
	},
	DOGE: {
		ID:           3,
		Handle:       "doge",
		Symbol:       "DOGE",
		Name:         "Dogecoin",
		Decimals:     8,
		LiveInterval: 1000,
	},
	DASH: {
		ID:           5,
		Handle:       "dash",
		Symbol:       "DASH",
		Name:         "Dash",
		Decimals:     8,
		LiveInterval: 1000,
	},
	VIACOIN: {
		ID:           14,
		Handle:       "viacoin",
		Symbol:       "VIA",
		Name:         "Viacoin",
		LiveInterval: 1000,
	},
	GROESTLCOIN: {
		ID:           17,
		Handle:       "groestlcoin",
		Symbol:       "GRS",
		Name:         "Groestlcoin",
		Decimals:     8,
		LiveInterval: 1000,
	},
	ZCASH: {
		ID:           133,
		Handle:       "zcash",
		Symbol:       "ZEC",
		Name:         "Zcash",
		Decimals:     8,
		LiveInterval: 1000,
	},
	ZCOIN: {
		ID:           136,
		Handle:       "zcoin",
		Symbol:       "FIRO",
		Name:         "Firo",
		Decimals:     8,
		LiveInterval: 1000,
	},
	BITCOINCASH: {
		ID:           145,
		Handle:       "bitcoincash",
		Symbol:       "BCH",
		Name:         "Bitcoin Cash",
		Decimals:     8,
		LiveInterval: 1000,
	},
	RAVENCOIN: {
		ID:           175,
		Handle:       "ravencoin",
		Symbol:       "RVN",
		Name:         "Raven",
		Decimals:     8,
		LiveInterval: 1000,
	},
	QTUM: {
		ID:           2301,
		Handle:       "qtum",
		Symbol:       "QTUM",
		Name:         "Qtum",
		Decimals:     8,
		LiveInterval: 1000,
	},
	ZELCASH: {
		ID:           19167,
		Handle:       "zelcash",
		Symbol:       "ZEL",
		Name:         "Zelcash",
		Decimals:     8,
		LiveInterval: 1000,
	},
	DECRED: {
		ID:           42,
		Handle:       "decred",
		Symbol:       "DCR",
		Name:         "Decred",
		Decimals:     8,
		LiveInterval: 1000,
	},
	ALGORAND: {
		ID:           283,
		Handle:       "algorand",
		Symbol:       "ALGO",
		Name:         "Algorand",
		Decimals:     6,
		LiveInterval: 1000,
	},
	NANO: {
		ID:           165,
		Handle:       "nano",
		Symbol:       "NANO",
		Name:         "Nano",
		Decimals:     30,
		LiveInterval: 1000,
	},
	DIGIBYTE: {
		ID:           20,
		Handle:       "digibyte",
		Symbol:       "DGB",
		Name:         "DigiByte",
		Decimals:     8,
		LiveInterval: 1000,
	},
	HARMONY: {
		ID:           1023,
		Handle:       "harmony",
		Symbol:       "ONE",
		Name:         "Harmony",
		Decimals:     18,
		LiveInterval: 1000,
	},
	KUSAMA: {
		ID:           434,
		Handle:       "kusama",
		Symbol:       "KSM",
		Name:         "Kusama",
		Decimals:     12,
		LiveInterval: 1000,
	},
	POLKADOT: {
		ID:           354,
		Handle:       "polkadot",
		Symbol:       "DOT",
		Name:         "Polkadot",
		Decimals:     10,
		LiveInterval: 1000,
	},
	SOLANA: {
		ID:           501,
		Handle:       "solana",
		Symbol:       "SOL",
		Name:         "Solana",
		Decimals:     9,
		LiveInterval: 1000,
	},
	NEAR: {
		ID:           397,
		Handle:       "near",
		Symbol:       "NEAR",
		Name:         "NEAR",
		Decimals:     18,
		LiveInterval: 1000,
	},
	ELROND: {
		ID:           508,
		Handle:       "elrond",
		Symbol:       "eGLD",
		Name:         "Elrond",
		Decimals:     18,
		LiveInterval: 1000,
	},
	SMARTCHAIN: {
		ID:           20000714,
		Handle:       "smartchain",
		Symbol:       "BNB",
		Name:         "Smart Chain",
		Decimals:     18,
		LiveInterval: 1000,
	},
	FILECOIN: {
		ID:           461,
		Handle:       "filecoin",
		Symbol:       "FIL",
		Name:         "Filecoin",
		Decimals:     18,
		LiveInterval: 1000,
	},
	OASIS: {
		ID:           474,
		Handle:       "oasis",
		Symbol:       "ROSE",
		Name:         "Oasis",
		Decimals:     9,
		LiveInterval: 1000,
	},
	STARCOIN: {
		ID:           101010,
		Handle:       "starcoin",
		Symbol:       "STC",
		Name:         "Starcoin",
		Decimals:     9,
		LiveInterval: 3000,
	},
	APTOS: {
		ID:           101011,
		Handle:       "aptos",
		Symbol:       "APT",
		Name:         "Aptos",
		Decimals:     0,
		LiveInterval: 1000,
	},
}

func CoinByHandler(handler string) Coin {
	return Coins[HandleMap[handler]]
}

func Ethereum() Coin {
	return Coins[ETHEREUM]
}
func Classic() Coin {
	return Coins[CLASSIC]
}
func Icon() Coin {
	return Coins[ICON]
}
func Cosmos() Coin {
	return Coins[COSMOS]
}
func Ripple() Coin {
	return Coins[RIPPLE]
}
func Stellar() Coin {
	return Coins[STELLAR]
}
func Poa() Coin {
	return Coins[POA]
}
func Tron() Coin {
	return Coins[TRON]
}
func Fio() Coin {
	return Coins[FIO]
}
func Nimiq() Coin {
	return Coins[NIMIQ]
}
func Iotex() Coin {
	return Coins[IOTEX]
}
func Zilliqa() Coin {
	return Coins[ZILLIQA]
}
func Aion() Coin {
	return Coins[AION]
}
func Aeternity() Coin {
	return Coins[AETERNITY]
}
func Kava() Coin {
	return Coins[KAVA]
}
func Theta() Coin {
	return Coins[THETA]
}
func Binance() Coin {
	return Coins[BINANCE]
}
func Vechain() Coin {
	return Coins[VECHAIN]
}
func Callisto() Coin {
	return Coins[CALLISTO]
}
func Tomochain() Coin {
	return Coins[TOMOCHAIN]
}
func Thundertoken() Coin {
	return Coins[THUNDERTOKEN]
}
func Ontology() Coin {
	return Coins[ONTOLOGY]
}
func Tezos() Coin {
	return Coins[TEZOS]
}
func Kin() Coin {
	return Coins[KIN]
}
func Nebulas() Coin {
	return Coins[NEBULAS]
}
func Gochain() Coin {
	return Coins[GOCHAIN]
}
func Wanchain() Coin {
	return Coins[WANCHAIN]
}
func Waves() Coin {
	return Coins[WAVES]
}
func Bitcoin() Coin {
	return Coins[BITCOIN]
}
func Litecoin() Coin {
	return Coins[LITECOIN]
}
func Doge() Coin {
	return Coins[DOGE]
}
func Dash() Coin {
	return Coins[DASH]
}
func Viacoin() Coin {
	return Coins[VIACOIN]
}
func Groestlcoin() Coin {
	return Coins[GROESTLCOIN]
}
func Zcash() Coin {
	return Coins[ZCASH]
}
func Zcoin() Coin {
	return Coins[ZCOIN]
}
func Bitcoincash() Coin {
	return Coins[BITCOINCASH]
}
func Ravencoin() Coin {
	return Coins[RAVENCOIN]
}
func Qtum() Coin {
	return Coins[QTUM]
}
func Zelcash() Coin {
	return Coins[ZELCASH]
}
func Decred() Coin {
	return Coins[DECRED]
}
func Algorand() Coin {
	return Coins[ALGORAND]
}
func Nano() Coin {
	return Coins[NANO]
}
func Digibyte() Coin {
	return Coins[DIGIBYTE]
}
func Harmony() Coin {
	return Coins[HARMONY]
}
func Kusama() Coin {
	return Coins[KUSAMA]
}
func Polkadot() Coin {
	return Coins[POLKADOT]
}
func Solana() Coin {
	return Coins[SOLANA]
}
func Near() Coin {
	return Coins[NEAR]
}
func Elrond() Coin {
	return Coins[ELROND]
}
func Smartchain() Coin {
	return Coins[SMARTCHAIN]
}
func Filecoin() Coin {
	return Coins[FILECOIN]
}
func Oasis() Coin {
	return Coins[OASIS]
}
func Starcoin() Coin {
	return Coins[STARCOIN]
}
func Aptos() Coin {
	return Coins[APTOS]
}
