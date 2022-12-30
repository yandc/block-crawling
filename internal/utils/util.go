package utils

import (
	"block-crawling/internal/model"
	"block-crawling/internal/types"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/btcsuite/btcutil/base58"
	types2 "github.com/ethereum/go-ethereum/common"
)

var recordRPCUrl sync.Map
var dayFormat = "2006-01-02"
var local, _ = time.LoadLocation("Asia/Shanghai")

func BigIntString(balance *big.Int, decimals int) string {
	amount := balance.String()
	return StringDecimals(amount, decimals)
}

func StringDecimals(amount string, decimals int) string {
	var result string
	if len(amount) > decimals {
		result = fmt.Sprintf("%s.%s", amount[0:len(amount)-decimals], amount[len(amount)-decimals:])
	} else {
		sub := decimals - len(amount)
		var zero string
		for i := 0; i < sub; i++ {
			zero += "0"
		}
		result = "0." + zero + amount
	}
	return clean(strings.TrimRight(result, "0"))
}

func bigIntFloat(balance *big.Int, decimals int64) *big.Float {
	if balance.Sign() == 0 {
		return big.NewFloat(0)
	}
	bal := big.NewFloat(0)
	bal.SetInt(balance)
	pow := bigPow(10, decimals)
	p := big.NewFloat(0)
	p.SetInt(pow)
	bal.Quo(bal, p)
	return bal
}

func bigPow(a, b int64) *big.Int {
	r := big.NewInt(a)
	return r.Exp(r, big.NewInt(b), nil)
}

func clean(newNum string) string {
	stringBytes := bytes.TrimRight([]byte(newNum), "0")
	newNum = string(stringBytes)
	if stringBytes[len(stringBytes)-1] == 46 {
		newNum = newNum[:len(stringBytes)-1]
	}
	if stringBytes[0] == 46 {
		newNum = "0" + newNum
	}
	return newNum
}

func NewTxResponse(blockHash, blockNumber, txHash, status, exTime, from, to, amount, nonce, gasUsed, gasPrice, gasLimit,
	sequenceNumber, baseFeePerGas string, token types.TokenInfo) types.TransactionResponse {
	return types.TransactionResponse{
		BlockHash:               blockHash,
		BlockNumber:             blockNumber,
		TransactionHash:         txHash,
		Status:                  status,
		ExpirationTimestampSecs: exTime,
		From:                    from,
		To:                      to,
		Amount:                  amount,
		Nonce:                   nonce,
		GasUsed:                 gasUsed,
		GasPrice:                gasPrice,
		GasLimit:                gasLimit,
		SequenceNumber:          sequenceNumber,
		BaseFeePerGas:           baseFeePerGas,
		Token:                   token,
	}
}

func CreateTxRecords(blockHash, blockNumber, txHash, status, exTime, from, to, amount, chain, chainName, txType, data,
	contractAddress, parseData, txSource, feeData, feeAmount, eventLog string) model.TransactionRecords {
	return model.TransactionRecords{
		BlockHash: blockHash, BlockNumber: blockNumber, TransactionHash: txHash, Status: status, TxTime: exTime,
		Chain: chain, TransactionType: txType, From: from, To: to, Data: data, Amount: amount, ChainName: chainName,
		ContractAddress: contractAddress, ParseData: parseData, TransactionSource: txSource, FeeAmount: feeAmount,
		FeeData: feeData, EventLog: eventLog,
	}

}

func GetSessions(addressInfo map[string]struct{}, addressList []string) bool {
	for _, address := range addressList {
		if _, ok := addressInfo[address]; ok {
			return true
		}
	}
	return false
}

func RemoveDuplicate(arr []string) []string {
	resArr := make([]string, 0, len(arr))
	tmpMap := make(map[string]interface{})
	for _, val := range arr {
		if _, ok := tmpMap[val]; !ok {
			resArr = append(resArr, val)
			tmpMap[val] = nil
		}
	}
	return resArr
}

func TronHexToBase58(address string) string {
	addr41, _ := hex.DecodeString(address)
	hash2561 := sha256.Sum256(addr41)
	hash2562 := sha256.Sum256(hash2561[:])
	tronAddr := base58.Encode(append(addr41, hash2562[:4]...))
	return tronAddr
}

func UpdateRecordRPCURL(rpc string, success bool) {
	tmp := types.RecordRPCURLInfo{SumCount: 1}
	if success {
		tmp.SuccessCount = 1
	}
	value, ok := recordRPCUrl.LoadOrStore(rpc, tmp)
	if ok {
		r := value.(types.RecordRPCURLInfo)
		r.SumCount++
		if success {
			r.SuccessCount++
		}
		recordRPCUrl.Store(rpc, r)
	}
}

func GetRPCFailureRate(rpc string) int {
	value, ok := recordRPCUrl.Load(rpc)
	if !ok {
		return 0
	}
	r := value.(types.RecordRPCURLInfo)
	fail := r.SumCount - r.SuccessCount
	failRate := int((fail * 100) / r.SumCount)
	return failRate
}

func JsonEncode(source interface{}) (string, error) {
	bytesBuffer := &bytes.Buffer{}
	encoder := json.NewEncoder(bytesBuffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(source)
	if err != nil {
		return "", err
	}

	jsons := string(bytesBuffer.Bytes())
	tsjsons := strings.TrimSuffix(jsons, "\n")
	return tsjsons, nil
}

func CopyProperties(source interface{}, target interface{}) error {
	bytesBuffer := &bytes.Buffer{}
	encoder := json.NewEncoder(bytesBuffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(source)
	if err != nil {
		return err
	}

	decoder := json.NewDecoder(bytesBuffer)
	err = decoder.Decode(&target)
	return err
}

func ListToString(list interface{}) string {
	tsjsons, err := JsonEncode(list)
	if err != nil {
		return ""
	}
	tsjsons = tsjsons[1 : len(tsjsons)-1]
	return tsjsons
}

func HexToAddress(hexList []string) []string {
	var addressList []string
	if len(hexList) > 0 {
		for _, addr := range hexList {
			if addr == "" {
				addressList = append(addressList, "")
			} else {
				addressList = append(addressList, types2.HexToAddress(addr).Hex())
			}
		}
	}
	return addressList
}

func GetDayTime(t *time.Time) int64 {
	var tm time.Time
	if t == nil {
		tm = time.Now()
	} else {
		tm = *t
	}
	day := tm.Format(dayFormat)
	dayTime, _ := time.ParseInLocation(dayFormat, day, local)
	return dayTime.Unix()
}

func HexStringToInt(hetStr string) (*big.Int, error) {
	if strings.HasPrefix(hetStr, "0x") {
		hetStr = hetStr[2:]
	}
	if len(hetStr)&1 == 1 {
		hetStr = "0" + hetStr
	}
	byteValue, err := hex.DecodeString(hetStr)
	if err != nil {
		return nil, err
	}
	intValue := new(big.Int).SetBytes(byteValue)
	return intValue, nil
}
