package tron

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/btcsuite/btcutil/base58"
	"strings"
)

func Base58ToHex(address string) string {
	decodeCheck := base58.Decode(address)
	if len(decodeCheck) < 4 {
		return "0"
	}
	decodeData := decodeCheck[:len(decodeCheck)-4]
	result := hex.EncodeToString(decodeData)
	return result
}

func HexToBase58(address string) string {
	addr41, _ := hex.DecodeString(address)
	hash2561 := sha256.Sum256(addr41)
	hash2562 := sha256.Sum256(hash2561[:])
	tronAddr := base58.Encode(append(addr41, hash2562[:4]...))
	return tronAddr
}

func HexToAddress(address string) string {
	if strings.HasPrefix(address, "0x") {
		address = address[2:]
	}
	return "0x" + address
}
