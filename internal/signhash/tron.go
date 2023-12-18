package signhash

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"strings"

	"golang.org/x/crypto/sha3"
)

type tvmSignHash struct{}

func (s *tvmSignHash) Hash(req *SignMessageRequest) (string, error) {
	var msg string
	if err := json.Unmarshal(req.Message, &msg); err != nil {
		return "", err
	}
	if strings.HasPrefix(msg, "0x") {
		msg = msg[2:]
	}

	data, err := hex.DecodeString(msg)
	if err != nil {
		return "", err
	}
	var hasher hash.Hash
	var message []byte
	if req.Method == "signRawTxWithSha256" {
		hasher = sha256.New()
		message = data
	} else {
		message = []byte(fmt.Sprintf("\x19TRON Signed Message:\n%d%s", len(data), string(data)))
		hasher = sha3.NewLegacyKeccak256()
	}

	hasher.Write(message)
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func init() {
	chainTypeHasher["TVM"] = &tvmSignHash{}
}
