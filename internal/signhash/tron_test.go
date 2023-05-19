package signhash

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

const tvmMsg = `{
  "address": "TVi9wgZGUc5QM13EvyTDbfuLDJjcVHti7V",
  "application": "https://dappsdk.openblock.vip",
  "chainId": 1,
  "chainName": "TRX",
  "message": "20f32d5c2b612fa2dd50f27dec6b3461fc58096b59f4cae45a31c74b067fb2fc",
  "method": "tron_signMessage",
  "sessionId": "95766d09a6f44b1e96177529e4059ca1"
}`

func TestTVM(t *testing.T) {
	var req *SignMessageRequest
	err := json.Unmarshal([]byte(tvmMsg), &req)
	assert.NoError(t, err)
	h, err := Hash("TVM", req)
	assert.NoError(t, err)
	assert.Equal(t, "99628124bf1ad09e40fa0a010aa29e74c03821a43ec78685ed86dc1c87f699ec", h)
}

const tvmMsgWith0x = `{
  "address": "TVi9wgZGUc5QM13EvyTDbfuLDJjcVHti7V",
  "application": "https://dappsdk.openblock.vip",
  "chainId": 1,
  "chainName": "TRX",
  "message": "0x5a398f5979d5681bae3f09e7fedc255ce0b524cc80b12e3632eb54d4acccc280",
  "method": "tron_signMessage",
  "sessionId": "95766d09a6f44b1e96177529e4059ca1"
}`

func TestTVMWith0x(t *testing.T) {
	var req *SignMessageRequest
	err := json.Unmarshal([]byte(tvmMsgWith0x), &req)
	assert.NoError(t, err)
	h, err := Hash("TVM", req)
	assert.NoError(t, err)
	assert.Equal(t, "5531733245bf91d9dedc30c091ea556b3695e31f933213fb289fb85259ee35d9", h)
}
