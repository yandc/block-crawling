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

func TestExtractMesonDirectRelease2(t *testing.T) {
	data := `ab115fd8010000000baf980000000000c8e287ce00000002080066a70e512105ff003cffdc7327cff777338b2104ac93008c2126fc51f4c9d9f0876720bc67839f563624bed6cdb8f0d52aa02c791878e1c3e3e3ec46a8a1442f1f58a6c886112bc0935e00000000000000000000000069c53a067422ec5e839fb015c549ede997b474d70000000000000000000000004763adfb2544f349336e27e6211ec8900f293789`
	encodedAmountByts, _ := hex.DecodeString(data[8 : 8+64])
	amount, ok := extractMesonFiDirectReleaseAmount(data[0:8], encodedAmountByts)
	if !ok {
		t.Fail()
	}
	if amount.String() != "1971000000000000" {
		t.Fatal(amount.String())
	}
}
