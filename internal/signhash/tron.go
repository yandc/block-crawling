package signhash

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	message := fmt.Sprintf("\x19TRON Signed Message:\n%d%s", len(data), string(data))

	hasher := sha3.NewLegacyKeccak256()
	hasher.Write([]byte(message))
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func init() {
	chainTypeHasher["TVM"] = &tvmSignHash{}
}
