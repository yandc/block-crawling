package common

import (
	"block-crawling/internal/model"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/btcsuite/btcutil/base58"
	"math/big"
	"sync"
)

var recordRPCUrl sync.Map

func BigIntString(balance *big.Int, decimals int64) string {
	amount := bigIntFloat(balance, decimals)
	deci := fmt.Sprintf("%%0.%vf", decimals)
	return clean(fmt.Sprintf(deci, amount))
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
	sequenceNumber, baseFeePerGas string, token model.TokenInfo) model.TransactionResponse {
	return model.TransactionResponse{
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
	tmp := model.RecordRPCURLInfo{SumCount: 1}
	if success {
		tmp.SuccessCount = 1
	}
	value, ok := recordRPCUrl.LoadOrStore(rpc, tmp)
	if ok {
		r := value.(model.RecordRPCURLInfo)
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
	r := value.(model.RecordRPCURLInfo)
	fail := r.SumCount - r.SuccessCount
	failRate := int((fail * 100) / r.SumCount)
	return failRate
}
