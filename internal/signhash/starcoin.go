package signhash

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/novifinancial/serde-reflection/serde-generate/runtime/golang/bcs"
	"golang.org/x/crypto/sha3"
)

type stcSignHash struct{}

const STARCOIN_HASH_PREFIX = "STARCOIN::"
const STARCOIN_HASH_TYPE = "SigningMessage"

func (s *stcSignHash) Hash(req *SignMessageRequest) (string, error) {
	var msg string
	err := json.Unmarshal(req.Message, &msg)
	if err != nil {
		return "", err
	}
	msgBytes, err := hexutil.Decode(msg)
	if err != nil {
		return "", err
	}

	// https://github.com/starcoinorg/starcoin.js/blob/a9b21262f9550cb98aaf7bf9f7ef2d5d2967fc7a/src/crypto_hash/index.ts#L14-L17
	hasher := sha3.New256()
	hasher.Write([]byte(fmt.Sprint(STARCOIN_HASH_PREFIX, STARCOIN_HASH_TYPE)))
	hashSalt := hasher.Sum(nil)

	// https://github.com/starcoinorg/starcoin.js/blob/a9b21262f9550cb98aaf7bf9f7ef2d5d2967fc7a/src/utils/signed-message.ts#L27-L31
	ser := bcs.NewSerializer()
	if err := ser.SerializeBytes(msgBytes); err != nil {
		return "", err
	}
	msgBcsBytes := ser.GetBytes()

	// https://github.com/starcoinorg/starcoin.js/blob/a9b21262f9550cb98aaf7bf9f7ef2d5d2967fc7a/src/utils/signed-message.ts#L33-L38
	hashSalt = append(hashSalt, msgBcsBytes...)
	return hex.EncodeToString(hashSalt), nil
}

func init() {
	chainTypeHasher["STC"] = &stcSignHash{}
}
