package signhash

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

var stcMsg = `{
  "address": "0xb7dddf9f82312a2a4dcc0d8c22b9ed1c",
  "application": "https://movetool.app",
  "chainId": 1,
  "chainName": "STC",
  "message": "0x4578616d706c652060706572736f6e616c5f7369676e60206d657373616765",
  "method": "personal_sign",
  "sessionId": "a973910d0ece451893054052a4a1fc0e"
}`

func TestStarcoin(t *testing.T) {
	var req *SignMessageRequest
	err := json.Unmarshal([]byte(stcMsg), &req)
	assert.NoError(t, err)
	h, err := Hash("STC", req)
	assert.NoError(t, err)
	assert.Equal(t, "1e350a8f0e461f0f6d89beaabf501711583b40deaeb045b0ccb44dd1e071733e1f4578616d706c652060706572736f6e616c5f7369676e60206d657373616765", h)
}
