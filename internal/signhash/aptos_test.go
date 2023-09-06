package signhash

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

var aptMsg = `{
  "sessionId": "1312ea82f17d4120a87a5dc509eddd15",
  "address": "0xa947accd7fec4ccd974af49ccd196b1cb7dddf9f82312a2a4dcc0d8c22b9ed1c",
  "chainName": "Aptos",
  "chainId": 1,
  "application": "https://dappsdk.openblock.vip",
  "method": "aptos_sign",
  "message": {
    "address": true,
    "application": true,
    "chainId": true,
    "message": "hello world!",
    "nonce": 888
  }
}`

var aptStrNonce = `{
  "sessionId": "bf76c153484e4bdd9f9a2bcda4def30d",
  "address": "0x9d12b7f3fc6276c69ff91986076d5993c294b2260f2fd446919efa74d8d0205d",
  "chainName": "Aptos",
  "chainId": 1,
  "application": "https://bluemove.net/connect-wallet",
  "message": {
    "message": "Click to sign in and accept the BlueMove Terms of Service. This request will not cost any gas fees. Nonce: 1373",
    "nonce": "1373"
  }
}`

func TestAptos(t *testing.T) {
	var req *SignMessageRequest
	err := json.Unmarshal([]byte(aptMsg), &req)
	assert.NoError(t, err)
	h, err := Hash("APTOS", req)
	assert.NoError(t, err)
	assert.Equal(t, "4150544f530a616464726573733a203078613934376163636437666563346363643937346166343963636431393662316362376464646639663832333132613261346463633064386332326239656431630a636861696e5f69643a20310a6170706c69636174696f6e3a2068747470733a2f2f6461707073646b2e6f70656e626c6f636b2e7669700a6e6f6e63653a203838380a6d6573736167653a2068656c6c6f20776f726c6421", h)
}

func TestAptosStrNonce(t *testing.T) {
	var req *SignMessageRequest
	err := json.Unmarshal([]byte(aptStrNonce), &req)
	assert.NoError(t, err)
	h, err := Hash("APTOS", req)
	assert.NoError(t, err)
	assert.Equal(t, "4150544f530a6e6f6e63653a20313337330a6d6573736167653a20436c69636b20746f207369676e20696e20616e64206163636570742074686520426c75654d6f7665205465726d73206f6620536572766963652e205468697320726571756573742077696c6c206e6f7420636f737420616e792067617320666565732e204e6f6e63653a2031333733", h)
}
