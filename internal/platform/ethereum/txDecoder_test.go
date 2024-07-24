package ethereum

import (
	"encoding/hex"
	"testing"
)

func TestExtractMesonDirectRelease(t *testing.T) {
	data := `ab115fd8010000000302d800000000002119a059000000001400669e39406868f3003cf274ceadf9b728561ef51cce8b0380bdfd5e44bd91e8b04ce8de77cef00a0b80a8552231931783d8c54282d6e40431db7262964914deaa0f98fffa7de149c8f55c000000000000000000000000cee4ddc6384a6b75fd2a40f4bd00d1df4a035e190000000000000000000000001dcd520ef3306f47a98e4025fc5f973469cadc9c`

	encodedAmountByts, _ := hex.DecodeString(data[8 : 8+64])
	amount, ok := extractMesonFiDirectReleaseAmount(data[0:8], encodedAmountByts)
	if !ok {
		t.Fail()
	}
	if amount.String() != "750000000000000" {
		t.Fail()
	}
}
