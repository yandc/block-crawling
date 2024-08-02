package signhash

import (
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/fardream/go-bcs/bcs"
	"github.com/minio/blake2b-simd"
)

type eddsaSignHash struct{}

func (s *eddsaSignHash) Hash(req *SignMessageRequest) (string, error) {

	var msg string
	if err := json.Unmarshal(req.Message, &msg); err != nil {
		return msg, err
	}
	if strings.Contains(req.ChainName, "Benfen") {
		s1, err := hex.DecodeString(msg)
		if err != nil {
			return "", err
		}
		// 对 s1 使用 bcs 进行序列化
		b2, err := bcs.Marshal(s1)
		if err != nil {
			return "", err
		}
		var b3 = append([]byte{3, 0, 0}, b2...)
		var b4 = blake2b.Sum256(b3)
		return hex.EncodeToString(b4[:]), nil
	}
	return msg, nil
}

func init() {
	chainTypeHasher["SOL"] = &eddsaSignHash{}
	chainTypeHasher["SUI"] = &eddsaSignHash{}
}
