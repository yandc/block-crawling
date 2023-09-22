package signhash

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
)

type aptSignHash struct{}

// aptMessagePayload https://docs.openblock.com/OpenBlock/Iframe/aptos/#signmessage
type aptMessagePayload struct {
	Address     bool            `json:"address"`     // Should we include the address of the account in the message
	Application bool            `json:"application"` // Should we include the domain of the dApp
	ChainId     bool            `json:"chainId"`     // Should we include the current chain id the wallet is connected to
	Message     string          `json:"message"`
	RawNonce    json.RawMessage `json:"nonce"` // A nonce the dApp should generate
}

func (p *aptMessagePayload) Nonce() (int, error) {
	var r int
	err := json.Unmarshal(p.RawNonce, &r)
	if err == nil {
		return r, nil
	}
	var s string
	if err := json.Unmarshal(p.RawNonce, &s); err == nil {
		return strconv.Atoi(s)
	}
	return 0, err
}

func (s *aptSignHash) Hash(req *SignMessageRequest) (string, error) {
	{
		var msg string
		if err := json.Unmarshal(req.Message, &msg); err == nil {
			return msg, nil
		}
	}
	var params *aptMessagePayload
	if err := json.Unmarshal(req.Message, &params); err != nil {
		return "", err
	}

	fullMessages := &bytes.Buffer{}
	fullMessages.WriteString("APTOS\n")
	if params.Address {
		fullMessages.WriteString(fmt.Sprintf("address: %s\n", req.Address))
	}
	if params.ChainId {
		fullMessages.WriteString(fmt.Sprintf("chain_id: %d\n", req.ChainId))
	}
	if params.Application {
		fullMessages.WriteString(fmt.Sprintf("application: %s\n", req.Application))
	}
	nonce, err := params.Nonce()
	if err != nil {
		return "", err
	}
	fullMessages.WriteString(fmt.Sprintf("nonce: %d\n", nonce))
	fullMessages.WriteString(fmt.Sprintf("message: %s", params.Message))
	return hex.EncodeToString(fullMessages.Bytes()), nil
}

func init() {
	chainTypeHasher["APTOS"] = &aptSignHash{}
}
