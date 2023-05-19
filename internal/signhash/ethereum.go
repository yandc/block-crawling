package signhash

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
)

const (
	ETHMethodETHSign         = "eth_sign"
	ETHMethodPersonalSign    = "personal_sign"
	ETHMethodSignTypedDataV1 = "eth_signTypedData"
	ETHMethodSignTypedDataV3 = "eth_signTypedData_v3"
	ETHMethodSignTypedDataV4 = "eth_signTypedData_v4"
)

// https://github.com/MetaMask/eth-sig-util/blob/c3da17cf7e9d428e4a70d15370f188a36a15a794/src/sign-typed-data.ts#L19-L35
type TypedDataV1 = []TypedDataV1Field

type TypedDataV1Field struct {
	Name  string          `json:"name"`
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

func (f *TypedDataV1Field) UnmarshalValue(t reflect.Type) (interface{}, error) {
	v := reflect.New(t)
	err := json.Unmarshal(f.Value, v.Interface())
	return v.Elem().Interface(), err
}

type TypedData = apitypes.TypedData

type evmSignHash struct {
}

func (s *evmSignHash) Hash(req *SignMessageRequest) (string, error) {

	var msg string
	if err := json.Unmarshal(req.Message, &msg); err != nil {
		return "", err
	}
	if strings.HasPrefix(msg, "0x") {
		message, err := hexutil.Decode(msg)
		if err != nil {
			return "", err
		}
		return hex.EncodeToString(accounts.TextHash(message)), nil
	}

	if r, err := s.signTypedDataV1(json.RawMessage(msg)); err == nil {
		return r, nil
	}
	return s.signTypedDataV3V4(json.RawMessage(msg))
}

// https://github.com/MetaMask/eth-sig-util/blob/c3da17cf7e9d428e4a70d15370f188a36a15a794/src/sign-typed-data.ts#L457-L506
func (s *evmSignHash) signTypedDataV1(v json.RawMessage) (string, error) {
	var typedData TypedDataV1
	if err := json.Unmarshal(v, &typedData); err != nil {
		return "", err
	}
	types := make([]string, 0, len(typedData))
	data := make([]interface{}, 0, len(typedData))
	schemaTypes := make([]string, 0, len(typedData))
	schema := make([]interface{}, 0, len(typedData))
	for _, item := range typedData {
		ty, err := s.parseType(item.Type)
		if err != nil {
			return "", err
		}
		v, err := item.UnmarshalValue(ty.GetType())
		if err != nil {
			return "", err
		}
		data = append(data, v)
		types = append(types, item.Type)
		schema = append(schema, fmt.Sprintf("%s %s", item.Type, item.Name))
		schemaTypes = append(schemaTypes, "string")
	}
	packedSchema, err := s.solidityPack(schemaTypes, schema...)
	if err != nil {
		return "", err
	}
	packedData, err := s.solidityPack(types, data...)
	if err != nil {
		return "", err
	}
	packedTogether, err := s.solidityPack(
		[]string{"bytes32", "bytes32"},
		(*[32]byte)(crypto.Keccak256(packedSchema)),
		(*[32]byte)(crypto.Keccak256(packedData)),
	)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(crypto.Keccak256(packedTogether)), nil
}

func (s *evmSignHash) parseType(ty string) (abi.Type, error) {
	n := s.elementaryName(ty)
	return abi.NewType(n, "", nil)
}

func (s *evmSignHash) solidityPack(types []string, params ...interface{}) ([]byte, error) {
	buf := &bytes.Buffer{}

	args := make(abi.Arguments, 0, len(types))
	for i, ty := range types {
		t, err := s.parseType(ty)
		if err != nil {
			return nil, err
		}

		args = []abi.Argument{
			{
				Name:    "_",
				Type:    t,
				Indexed: false,
			},
		}
		byts, err := args.Pack(params[i])
		if err != nil {
			return nil, err
		}

		buf.Write(s.tightenPackedBytes(t, byts))
	}

	return buf.Bytes(), nil
}

func (s *evmSignHash) tightenPackedBytes(t abi.Type, byts []byte) []byte {
	switch t.T {
	case abi.BoolTy:
		return byts[31:]
	case abi.StringTy, abi.BytesTy:
		return s.unpckBytesSlice(byts)
	case abi.AddressTy:
		return common.TrimLeftZeroes(byts)
	case abi.IntTy, abi.UintTy:
		return byts[len(byts)-t.Size/8:]
	case abi.SliceTy:
		padding := 64
		// TOTO matrix
		return byts[padding:]
	case abi.FixedBytesTy:
		return byts[:t.Size]
	}
	return byts
}

func (s *evmSignHash) unpckBytesSlice(byts []byte) []byte {
	sl := hexutil.Encode(common.TrimLeftZeroes(byts[32:64]))
	sl = strings.Replace(sl, "0x0", "0x", 1)
	length, err := hexutil.DecodeBig(sl)
	if err != nil {
		panic(err)
	}
	return byts[64 : 64+length.Uint64()]
}

func (s *evmSignHash) elementaryName(name string) string {
	if strings.HasPrefix(name, "int[") {
		return fmt.Sprintf("int256%s", name[3:])
	} else if name == "int" {
		return "int256"
	} else if strings.HasPrefix(name, "uint[") {
		return fmt.Sprintf("uint256%s", name[4:])
	} else if name == "uint" {
		return "uint256"
	} else if strings.HasPrefix(name, "fixed[") {
		return fmt.Sprintf("fixed128x128%s", name[5:])
	} else if name == "fixed" {
		return "fixed128x128"
	} else if strings.HasPrefix(name, "ufixed[") {
		return fmt.Sprintf("ufixed128x128%s", name[6:])
	} else if name == "ufixed" {
		return "ufixed128x128"
	}
	return name
}

func (s *evmSignHash) signTypedDataV3V4(v json.RawMessage) (string, error) {
	var typedData TypedData
	if err := json.Unmarshal(v, &typedData); err != nil {
		return "", err
	}
	hash, _, err := apitypes.TypedDataAndHash(typedData)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(hash), nil
}

func init() {
	chainTypeHasher["EVM"] = &evmSignHash{}
}
