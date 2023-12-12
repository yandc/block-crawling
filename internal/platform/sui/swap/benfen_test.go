package swap

import (
	"testing"

	"github.com/go-playground/assert/v2"
)

func TestNormalizeCoinType(t *testing.T) {
	assert.Equal(t, "BFC0000000000000000000000000000000000000000000000000000000000000268e4::bfc::BFC", NormalizeBenfenCoinType("BenfenTEST", "0x2::bfc::BFC"))
	assert.Equal(t, "0x2::bfc::BFC", NormalizeBenfenCoinType("SUITEST", "0x2::bfc::BFC"))
	assert.Equal(t, "", NormalizeBenfenCoinType("SUITEST", ""))
	assert.Equal(t, "", NormalizeBenfenCoinType("BenfenTEST", ""))

	assert.Equal(t, "BFC00000000000000000000000000000000000000000000000000000000000000c8e30a::busd::BUSD", NormalizeBenfenCoinType("BenfenTEST", "0xc8::busd::BUSD"))
	assert.Equal(t, "BFC00000000000000000000000000000000000000000000000000000000000000c8e30a::busd::BUSD", NormalizeBenfenCoinType("BenfenTEST", "0x00000000000000000000000000000000000000000000000000000000000000c8::busd::BUSD"))
}

func TestDenormalizeCoinType(t *testing.T) {
	assert.Equal(t, "0x2::bfc::BFC", DenormalizeBenfenCoinType("BenfenTEST", "BFC00000000000000000000000000000000000000000000000000000000000002e7e9::bfc::BFC"))
	assert.Equal(t, "0x2::bfc::BFC", DenormalizeBenfenCoinType("SUITEST", "0x2::bfc::BFC"))
	assert.Equal(t, "", DenormalizeBenfenCoinType("SUITEST", ""))
	assert.Equal(t, "", DenormalizeBenfenCoinType("BenfenTEST", ""))
}
