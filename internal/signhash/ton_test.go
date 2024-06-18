package signhash

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTonMessage(t *testing.T) {
	rawReq := `{
  "sessionId": "340cd647d2994909aae2ce040d0b1a72",
  "address": "EQACcSWU92sTeKZ9OgBMP_P9KvW-MPvZDLw0QFQTQC0Dlucl",
  "chainName": "Ton",
  "chainId": "",
  "application": "https://getgems.io",
  "method": "ton_signMessage",
  "message": {
		 "timestamp": 1717745977,
		 "items": [
			 {
				 "name": "ton_addr"
			 },
			 {
				 "name": "ton_proof",
				 "payload": "gems"
			 }
		 ]
	}
}`
	var r *SignMessageRequest
	_ = json.Unmarshal([]byte(rawReq), &r)
	s := &tonSignHash{}
	h, err := s.Hash(r)
	assert.NoError(t, err)
	t.Log(h)
}
