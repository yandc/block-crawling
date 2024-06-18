package tonutils

import (
	"encoding/base64"
	"encoding/hex"
)

func Base64ToHex(hash string) string {
	byts, _ := base64.StdEncoding.DecodeString(hash)
	return hex.EncodeToString(byts)
}

func UnifyAddressToHuman(address string) string {
	aa, err := ParseAccountID(address)
	if err != nil {
		return address
	}
	return aa.ToHuman(true, false)
}
