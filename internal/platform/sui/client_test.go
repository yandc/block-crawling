package sui

import (
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
