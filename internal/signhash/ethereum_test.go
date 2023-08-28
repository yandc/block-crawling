package signhash

import (
	"encoding/hex"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/stretchr/testify/assert"
)

const chainId = 0x5

var (
	// https://github.com/MetaMask/test-dapp/blob/fb848448bd58d4ef725d6817ad2f99bc86827a27/src/index.js#L1327
	signTypedData = []map[string]interface{}{
		{
			"type":  "string",
			"name":  "message",
			"value": "Hi, Alice!",
		},
		{
			"type":  "uint8",
			"name":  "value",
			"value": 10,
		},
	}

	// https://github.com/MetaMask/test-dapp/blob/fb848448bd58d4ef725d6817ad2f99bc86827a27/src/index.js#L1368
	signTypedDataV3 = map[string]interface{}{
		"types": map[string]interface{}{
			"EIP712Domain": []map[string]interface{}{
				{"name": "name", "type": "string"},
				{"name": "version", "type": "string"},
				{"name": "chainId", "type": "uint256"},
				{"name": "verifyingContract", "type": "address"},
			},
			"Person": []map[string]interface{}{
				{"name": "name", "type": "string"},
				{"name": "wallet", "type": "address"},
			},
			"Mail": []map[string]interface{}{
				{"name": "from", "type": "Person"},
				{"name": "to", "type": "Person"},
				{"name": "contents", "type": "string"},
			},
		},
		"primaryType": "Mail",
		"domain": map[string]interface{}{
			"name":              "Ether Mail",
			"version":           "1",
			"chainId":           "0x05",
			"verifyingContract": "0xCcCCccccCCCCcCCCCCCcCcCccCcCCCcCcccccccC",
		},
		"message": map[string]interface{}{
			"from": map[string]interface{}{
				"name":   "Cow",
				"wallet": "0xCD2a3d9F938E13CD947Ec05AbC7FE734Df8DD826",
			},
			"to": map[string]interface{}{
				"name":   "Bob",
				"wallet": "0xbBbBBBBbbBBBbbbBbbBbbbbBBbBbbbbBbBbbBBbB",
			},
			"contents": "Hello, Bob!",
		},
	}

	// https://github.com/MetaMask/test-dapp/blob/fb848448bd58d4ef725d6817ad2f99bc86827a27/src/index.js#L1490
	signTypedDataV4 = map[string]interface{}{
		"domain": map[string]interface{}{
			"chainId":           "0x5",
			"name":              "Ether Mail",
			"verifyingContract": "0xCcCCccccCCCCcCCCCCCcCcCccCcCCCcCcccccccC",
			"version":           "1",
		},
		"message": map[string]interface{}{
			"contents": "Hello, Bob!",
			"from": map[string]interface{}{
				"name": "Cow",
				"wallets": []string{
					"0xCD2a3d9F938E13CD947Ec05AbC7FE734Df8DD826",
					"0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF",
				},
			},
			"to": []map[string]interface{}{
				{
					"name": "Bob",
					"wallets": []string{
						"0xbBbBBBBbbBBBbbbBbbBbbbbBBbBbbbbBbBbbBBbB",
						"0xB0BdaBea57B0BDABeA57b0bdABEA57b0BDabEa57",
						"0xB0B0b0b0b0b0B000000000000000000000000000",
					},
				},
			},
		},
		"primaryType": "Mail",
		"types": map[string]interface{}{
			"EIP712Domain": []map[string]interface{}{
				{"name": "name", "type": "string"},
				{"name": "version", "type": "string"},
				{"name": "chainId", "type": "uint256"},
				{"name": "verifyingContract", "type": "address"},
			},
			"Group": []map[string]interface{}{
				{"name": "name", "type": "string"},
				{"name": "members", "type": "Person[]"},
			},
			"Mail": []map[string]interface{}{
				{"name": "from", "type": "Person"},
				{"name": "to", "type": "Person[]"},
				{"name": "contents", "type": "string"},
			},
			"Person": []map[string]interface{}{
				{"name": "name", "type": "string"},
				{"name": "wallets", "type": "address[]"},
			},
		},
	}

	signTypedDataV4IntChainId = map[string]interface{}{
		"domain": map[string]interface{}{
			"chainId":           5,
			"name":              "Ether Mail",
			"verifyingContract": "0xCcCCccccCCCCcCCCCCCcCcCccCcCCCcCcccccccC",
			"version":           "1",
		},
		"message": map[string]interface{}{
			"contents": "Hello, Bob!",
			"from": map[string]interface{}{
				"name": "Cow",
				"wallets": []string{
					"0xCD2a3d9F938E13CD947Ec05AbC7FE734Df8DD826",
					"0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF",
				},
			},
			"to": []map[string]interface{}{
				{
					"name": "Bob",
					"wallets": []string{
						"0xbBbBBBBbbBBBbbbBbbBbbbbBBbBbbbbBbBbbBBbB",
						"0xB0BdaBea57B0BDABeA57b0bdABEA57b0BDabEa57",
						"0xB0B0b0b0b0b0B000000000000000000000000000",
					},
				},
			},
		},
		"primaryType": "Mail",
		"types": map[string]interface{}{
			"EIP712Domain": []map[string]interface{}{
				{"name": "name", "type": "string"},
				{"name": "version", "type": "string"},
				{"name": "chainId", "type": "uint256"},
				{"name": "verifyingContract", "type": "address"},
			},
			"Group": []map[string]interface{}{
				{"name": "name", "type": "string"},
				{"name": "members", "type": "Person[]"},
			},
			"Mail": []map[string]interface{}{
				{"name": "from", "type": "Person"},
				{"name": "to", "type": "Person[]"},
				{"name": "contents", "type": "string"},
			},
			"Person": []map[string]interface{}{
				{"name": "name", "type": "string"},
				{"name": "wallets", "type": "address[]"},
			},
		},
	}
)

func TestEvmV1NonHexMessage(t *testing.T) {
	rawReq := `{
  "sessionId": "661c9b61e22f4739ad262e7799ab8426",
  "address": "0xE8e3d1031b6316136df77B01BcF01C151C14A53A",
  "chainName": "ETH",
  "chainId": 1,
  "application": "https://unibot.app/dashboard",
  "method": "personal_sign",
  "message": "Verify wallet ownership to view and claim Unibot revenue share rewards."
}`
	var r *SignMessageRequest
	_ = json.Unmarshal([]byte(rawReq), &r)
	s := &evmSignHash{}
	h, err := s.Hash(r)
	assert.NoError(t, err)
	t.Log(h)
}

func TestEvmV1(t *testing.T) {
	byts, err := json.Marshal(signTypedData)
	assert.NoError(t, err)
	s := &evmSignHash{}
	h, err := s.Hash(&SignMessageRequest{
		SessionId:   "",
		Address:     "",
		ChainName:   "",
		ChainId:     chainId,
		Application: "",
		Message:     byts,
	})
	assert.NoError(t, err)
	assert.Equal(t, "0xf7ad23226db5c1c00ca0ca1468fd49c8f8bbc1489bc1c382de5adc557a69c229", h)
}

func TestEvmV3(t *testing.T) {
	byts, err := json.Marshal(signTypedDataV3)
	assert.NoError(t, err)
	sbyts, _ := json.Marshal(string(byts))
	s := &evmSignHash{}
	h, err := s.Hash(&SignMessageRequest{
		SessionId:   "",
		Address:     "",
		ChainName:   "",
		ChainId:     chainId,
		Application: "",
		Message:     sbyts,
	})
	assert.NoError(t, err)
	t.Log(h)
}

func TestEvmV4(t *testing.T) {
	byts, err := json.Marshal(signTypedDataV4)
	assert.NoError(t, err)
	sbyts, _ := json.Marshal(string(byts))
	s := &evmSignHash{}
	h, err := s.Hash(&SignMessageRequest{
		SessionId:   "",
		Address:     "",
		ChainName:   "",
		ChainId:     chainId,
		Application: "",
		Message:     sbyts,
	})
	assert.NoError(t, err)
	t.Log(h)
}

func TestEvmV4IntChainId(t *testing.T) {
	byts, err := json.Marshal(signTypedDataV4IntChainId)
	assert.NoError(t, err)
	sbyts, _ := json.Marshal(string(byts))
	s := &evmSignHash{}
	h, err := s.Hash(&SignMessageRequest{
		SessionId:   "",
		Address:     "",
		ChainName:   "",
		ChainId:     chainId,
		Application: "",
		Message:     sbyts,
	})
	assert.NoError(t, err)
	t.Log(h)
}

func TestEvmPackTightBool(t *testing.T) {
	s := &evmSignHash{}

	byts, err := s.solidityPack([]string{"bool"}, true)
	assert.NoError(t, err)
	assert.Equal(t, "01", hex.EncodeToString(byts), "bool true")

	byts, err = s.solidityPack([]string{"bool"}, false)
	assert.NoError(t, err)
	assert.Equal(t, "00", hex.EncodeToString(byts), "bool false")

}

func TestEvmPackTightString(t *testing.T) {
	s := &evmSignHash{}

	byts, err := s.solidityPack([]string{"string"}, "test")
	assert.NoError(t, err)
	assert.Equal(t, "74657374", hex.EncodeToString(byts), "string")
}

func TestEvmPackTightAddr(t *testing.T) {
	s := &evmSignHash{}

	addr := common.HexToAddress("0x43989fb883ba8111221e89123897538475893837")
	byts, err := s.solidityPack([]string{"address"}, addr)
	assert.NoError(t, err, "address")
	assert.Equal(t, "43989fb883ba8111221e89123897538475893837", hex.EncodeToString(byts), "address")

}

func TestEvmPackTightBytes(t *testing.T) {
	s := &evmSignHash{}

	input, err := hexutil.Decode("0x123456")
	assert.NoError(t, err)
	byts, err := s.solidityPack([]string{"bytes"}, input)
	assert.NoError(t, err)
	assert.Equal(t, "123456", hex.EncodeToString(byts), "bytes")

}

func TestEvmPackTightBytes8(t *testing.T) {
	s := &evmSignHash{}

	input, err := hexutil.Decode("0x123456")

	assert.NoError(t, err)
	byts, err := s.solidityPack([]string{"bytes8"}, (*[8]byte)(common.RightPadBytes(input, 8)))
	assert.NoError(t, err, "bytes8")
	assert.Equal(t, "1234560000000000", hex.EncodeToString(byts), "bytes8")

	byts, err = s.solidityPack([]string{"int"}, big.NewInt(42))
	assert.NoError(t, err)
	assert.Equal(t, "000000000000000000000000000000000000000000000000000000000000002a", hex.EncodeToString(byts), "int")
}

func TestEvmPackTightUint(t *testing.T) {
	s := &evmSignHash{}

	byts, err := s.solidityPack([]string{"uint"}, big.NewInt(42))
	assert.NoError(t, err)
	assert.Equal(t, "000000000000000000000000000000000000000000000000000000000000002a", hex.EncodeToString(byts), "uint")
}

func TestEvmPackTightUint16(t *testing.T) {
	s := &evmSignHash{}
	byts, err := s.solidityPack([]string{"uint16"}, uint16(42))
	assert.NoError(t, err)
	assert.Equal(t, "002a", hex.EncodeToString(byts), "uint16")
}

func TestEvmPackTightInt(t *testing.T) {
	s := &evmSignHash{}

	byts, err := s.solidityPack([]string{"int"}, big.NewInt(-42))
	assert.NoError(t, err)
	assert.Equal(t, "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffd6", hex.EncodeToString(byts), "int")
}

func TestEvmPackTightInt16(t *testing.T) {
	s := &evmSignHash{}

	byts, err := s.solidityPack([]string{"int16"}, int16(-42))
	assert.NoError(t, err)
	assert.Equal(t, "ffd6", hex.EncodeToString(byts), "uint16")
}

func TestEvmPackTightMultiple(t *testing.T) {
	s := &evmSignHash{}

	input, err := hexutil.Decode("0x123456")
	assert.NoError(t, err)
	byts, err := s.solidityPack(
		[]string{"bytes32", "uint32", "uint32", "uint32", "uint32"},
		(*[32]byte)(common.RightPadBytes(input, 32)), uint32(6), uint32(7), uint32(8), uint32(9),
	)
	assert.NoError(t, err)
	assert.Equal(t, "123456000000000000000000000000000000000000000000000000000000000000000006000000070000000800000009", hex.EncodeToString(byts))
}

func TestEvmPackTightUint32Array(t *testing.T) {
	s := &evmSignHash{}

	byts, err := s.solidityPack([]string{"uint32[]"}, []uint32{8, 9})
	assert.NoError(t, err)
	assert.Equal(t, "00000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000000000000000000000000000000000009", hex.EncodeToString(byts))
}

func TestEvmPackTightBoolArrayArray(t *testing.T) {
	s := &evmSignHash{}
	byts, err := s.solidityPack([]string{"bool[][]"}, [][]bool{
		{true, false},
		{false, true},
	})
	assert.NoError(t, err)
	assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001", hex.EncodeToString(byts), "bool[][]")

}

func TestEvmPackTightAddressArray(t *testing.T) {
	s := &evmSignHash{}
	addr := common.HexToAddress("0x43989fb883ba8111221e89123897538475893837")
	byts, err := s.solidityPack([]string{"address[]"}, []common.Address{addr})
	assert.NoError(t, err, "address[]")
	assert.Equal(t, "00000000000000000000000043989fb883ba8111221e89123897538475893837", hex.EncodeToString(byts), "address[]")

}

func TestEvmPackTightFixedUint32Array(t *testing.T) {
	s := &evmSignHash{}
	byts, err := s.solidityPack([]string{"uint32[2]"}, []uint32{11, 12})
	assert.NoError(t, err, "uint32[2]")
	assert.Equal(
		t,
		"000000000000000000000000000000000000000000000000000000000000000b000000000000000000000000000000000000000000000000000000000000000c",
		hex.EncodeToString(byts),
		"uint32[2]",
	)
}
