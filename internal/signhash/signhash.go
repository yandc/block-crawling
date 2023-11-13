package signhash

import (
	"encoding/json"
	"errors"
)

const (
	TRXMessagePrefix = "\x19TRON Signed Message:\n%d"
	// it should be: '\x15TRON Signed Message:\n32';
	ETHMessagePrefix = "\x19Ethereum Signed Message:\n%d"
)

var chainTypeHasher = make(map[string]SignHasher)

// SignHasher to generate hash of message(include typed data, aka struct).
type SignHasher interface {
	Hash(payload *SignMessageRequest) (string, error)
}

func Hash(chainType string, payload *SignMessageRequest) (string, error) {
	if h, ok := chainTypeHasher[chainType]; ok {
		return h.Hash(payload)
	}
	return "", errors.New("not supported chain")
}

type SignMessageRequest struct {
	SessionId   string          `json:"sessionId"`
	Address     string          `json:"address"`
	ChainName   string          `json:"chainName"`
	RawChainId  json.RawMessage `json:"chainId"`
	Application string          `json:"application"`

	Message json.RawMessage `json:"message"`
}

func (r *SignMessageRequest) ChainId() (int, error) {
	var chainId int
	if err := json.Unmarshal(r.RawChainId, &chainId); err != nil {
		return 0, err
	}
	return chainId, nil
}
