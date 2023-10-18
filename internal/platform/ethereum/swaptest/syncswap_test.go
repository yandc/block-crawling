package swap

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSyncswap(t *testing.T) {
	// https://explorer.zksync.io/tx/0x0ab1fec89c6cbb251761789316f43cb8cc434c12350eddfe176ae5dd0397d638#eventlog
	pairs := doExtractPairs(t, "0x0ab1fec89c6cbb251761789316f43cb8cc434c12350eddfe176ae5dd0397d638", "https://mainnet.era.zksync.io", "zkSync")

	assert.Len(t, pairs, 1, "no extracted")

	lowerCaseEqual(t, "0x80115c708E12eDd42E504c1cD52Aea96C547c05c", pairs[0].PairContract, "pair")
	lowerCaseEqual(t, "0x2da10A1e27bF85cEdD8FFb1AbBe97e53391C0295", pairs[0].DexContract, "dex")
	lowerCaseEqual(t, "0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4", pairs[0].Input.Address, "output token")
	lowerCaseEqual(t, "0x5aea5775959fbc2557cc8789bc1bf90a239d9a91", pairs[0].Output.Address, "output token")
	assert.Equal(t, "62495571", pairs[0].Input.Amount, "input amount")
	assert.Equal(t, "38112556721290734", pairs[0].Output.Amount, "output amount")

	// https://explorer.zksync.io/tx/0x50d724dd38f0e564fe31ad364ac5ee796a079ceb6a2122e59c7e3110a147b187#eventlog
	pairs = doExtractPairs(t, "0x50d724dd38f0e564fe31ad364ac5ee796a079ceb6a2122e59c7e3110a147b187", "https://mainnet.era.zksync.io", "zkSync")

	assert.Len(t, pairs, 1, "no extracted")

	lowerCaseEqual(t, "0x0E595bfcAfb552F83E25d24e8a383F88c1Ab48A4", pairs[0].PairContract, "pair")
	lowerCaseEqual(t, "0x2da10A1e27bF85cEdD8FFb1AbBe97e53391C0295", pairs[0].DexContract, "dex")
	lowerCaseEqual(t, "0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4", pairs[0].Input.Address, "output token")
	lowerCaseEqual(t, "0x493257fD37EDB34451f62EDf8D2a0C418852bA4C", pairs[0].Output.Address, "output token")
	assert.Equal(t, "71360788", pairs[0].Input.Amount, "input amount")
	assert.Equal(t, "71287226", pairs[0].Output.Amount, "output amount")

}

func lowerCaseEqual(t *testing.T, a string, b string, msgs ...interface{}) {
	assert.Equal(t, strings.ToLower(a), strings.ToLower(b), msgs...)
}

func TestSyncswapMismatch(t *testing.T) {
	// https://explorer.zksync.io/tx/0xcfe47dc8bff4b6dfca55937704853370c1e41d935ec4616100ac6cbdf66fd55d#eventlog
	pairs := doExtractPairs(t, "0xcfe47dc8bff4b6dfca55937704853370c1e41d935ec4616100ac6cbdf66fd55d", "https://mainnet.era.zksync.io", "zkSync")

	assert.Len(t, pairs, 1, "no extracted")

	lowerCaseEqual(t, "0x9D1Dc34c8146C5fC84ddb9FB724dB1F11B0F693c", pairs[0].PairContract, "pair")
	lowerCaseEqual(t, "0x2da10A1e27bF85cEdD8FFb1AbBe97e53391C0295", pairs[0].DexContract, "dex")
	lowerCaseEqual(t, "0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4", pairs[0].Input.Address, "output token")
	lowerCaseEqual(t, "0x787c09494Ec8Bcb24DcAf8659E7d5D69979eE508", pairs[0].Output.Address, "output token")
	assert.Equal(t, "3000000", pairs[0].Input.Amount, "input amount")
	assert.Equal(t, "13927928853267069904", pairs[0].Output.Amount, "output amount")
}
