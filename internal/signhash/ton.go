package signhash

import (
	"block-crawling/internal/platform/ton/tonutils"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net/url"
)

type tonSignHash struct{}

type tonMessage struct {
	Items     []tonMessageItem `json:"items"`
	Timestamp int              `json:"timestamp"`
}

// [{"name":"ton_addr"},{"name":"ton_proof","payload":"gems"}]
type tonMessageItem struct {
	Name    string `json:"name"`
	Payload string `json:"payload"`
}

func (s *tonSignHash) Hash(req *SignMessageRequest) (string, error) {
	var msgs tonMessage
	if err := json.Unmarshal(req.Message, &msgs); err != nil {
		return "", err
	}

	buf := bytes.NewBuffer([]byte("ton-proof-item-v2/"))
	tonAddr, err := tonutils.ParseAccountID(req.Address)
	if err != nil {
		return "", err
	}
	binary.Write(buf, binary.LittleEndian, tonAddr.Workchain)
	buf.Write(tonAddr.Address[:])

	parsed, err := url.Parse(req.Application)
	if err != nil {
		return "", err
	}

	binary.Write(buf, binary.LittleEndian, int32(len(parsed.Host)))
	buf.Write([]byte(parsed.Host))

	binary.Write(buf, binary.LittleEndian, int64(msgs.Timestamp))
	for _, item := range msgs.Items {
		if item.Name == "ton_addr" {
			continue
		}
		buf.Write([]byte(item.Payload))
	}
	bufferToSign := bytes.NewBuffer([]byte{255, 255}) // ffff
	bufferToSign.Write([]byte("ton-connect"))

	bufferToSign.Write(sha256Bytes(buf.Bytes()))
	return sha256hex(bufferToSign.Bytes()), nil
}

func sha256hex(byts []byte) string {
	return hex.EncodeToString(sha256Bytes(byts))
}

func sha256Bytes(byts []byte) []byte {
	hasher := sha256.New()
	hasher.Write(byts)
	return hasher.Sum(nil)
}

func init() {
	chainTypeHasher["TON"] = &tonSignHash{}
}
