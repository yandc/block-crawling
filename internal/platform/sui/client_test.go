package sui

import (
	"block-crawling/internal/conf"
	"block-crawling/internal/log"
	"block-crawling/internal/platform/sui/stypes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSuiSimpleStructTag(t *testing.T) {
	s := "0xa5e3d1ae22b6fa1b238a8552752eb6a6d39835889fc4abed82cc287f55b84a88::pool::Pool"
	actual, err := ParseSuiSimpleStructTag("BenfenTEST", s)
	assert.NoError(t, err)
	assert.Equal(t, "BFCa5e3d1ae22b6fa1b238a8552752eb6a6d39835889fc4abed82cc287f55b84a8885c7", actual.Address, "addr")
	assert.Equal(t, "pool", actual.Module, "mod")
	assert.Equal(t, "Pool", actual.Name, "name")

	s = "0xa5e3d1ae22b6fa1b238a8552752eb6a6d39835889fc4abed82cc287f55b84a88::pool::Pool<0x2::bfc::BFC, 0x00000000000000000000000000000000000000000000000000000000000000c8::busd::BUSD>"
	actual, err = ParseSuiSimpleStructTag("BenfenTEST", s)
	assert.NoError(t, err)
	assert.Equal(t, "BFCa5e3d1ae22b6fa1b238a8552752eb6a6d39835889fc4abed82cc287f55b84a8885c7", actual.Address, "addr")
	assert.Equal(t, "pool", actual.Module, "mod")
	assert.Equal(t, "Pool", actual.Name, "name")
}

func TestSuiStringTx(t *testing.T) {
	log.BootstrapLogger(&conf.Logger{
		DEBUG:      true,
		FileName:   "stdout",
		Level:      "info",
		ArchiveDir: "",
		MaxSize:    0,
		MaxAge:     0,
		MaxBackups: 0,
	})
	client := NewClient("https://fullnode.mainnet.sui.io", "SUI")
	blk, err := client.GetBlock(25199973)
	assert.NoError(t, err)
	for _, v := range blk.Transactions {
		for _, t := range v.Raw.(*stypes.TransactionInfo).Transaction.Data.Transaction.Transactions() {
			_ = t
		}
	}
}
