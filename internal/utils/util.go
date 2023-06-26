package utils

import (
	"block-crawling/internal/types"
	"bytes"
	"container/heap"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/btcsuite/btcutil/base58"
	types2 "github.com/ethereum/go-ethereum/common"
)

var recordRPCUrl sync.Map
var dayFormat = "2006-01-02"
var local, _ = time.LoadLocation("Asia/Shanghai")

type minHeap []int

func (h minHeap) Len() int {
	return len(h)
}

//小根堆
func (h minHeap) Less(i, j int) bool {
	return h[i] < h[j]
}

func (h *minHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *minHeap) Push(x interface{}) {
	*h = append(*h, x.(int))
}
func (h *minHeap) Pop() interface{} {
	res := (*h)[len(*h)-1]
	*h = (*h)[:len(*h)-1]
	return res
}
func GetMaxHeap(gas []int, i int) []int {
	var result []int
	h := minHeap{}
	heap.Init(&h)
	for index, v := range gas {
		if index  < i {
			heap.Push(&h, v)
		}else {
			heap.Push(&h, v)
			heap.Pop(&h)
		}
	}
	if len(h) > i {
		for n := 1; n <= i; n++ {
			result = append(result, heap.Pop(&h).(int))
		}
	} else {
		for len(h) != 0 {
			result = append(result, heap.Pop(&h).(int))
		}
	}

	return result
}

func GetMinHeap(gas []int, i int) []int {
	var result []int
	h := maxHeap{}
	heap.Init(&h)
	for index, v := range gas {
		if index  < i {
			heap.Push(&h, v)
		}else {
			heap.Push(&h, v)
			heap.Pop(&h)
		}
	}
	if len(h) > i {
		for n := 1; n <= i; n++ {
			result = append(result, heap.Pop(&h).(int))
		}
	} else {
		for len(h) != 0 {
			result = append(result, heap.Pop(&h).(int))
		}
	}

	return result
}


type maxHeap []int

func (h maxHeap) Len() int {
	return len(h)
}

//大根堆
func (h maxHeap) Less(i, j int) bool {
	return h[i] > h[j]
}

func (h *maxHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *maxHeap) Push(x interface{}) {
	*h = append(*h, x.(int))
}
func (h *maxHeap) Pop() interface{} {
	res := (*h)[len(*h)-1]
	*h = (*h)[:len(*h)-1]
	return res
}

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
func StringDecimalsValue(amount string, decimals int) string {
	flag := amount[0:1] == "-"
	if flag {
		amount = amount[1:]
	}
	s := StringDecimals(amount, decimals)
	if flag {
		s = "-" + s
	}
	return s
}

func StringSpiltByIndex(evmStr string, index int) string {
	rawStrSlice := []byte(evmStr)
	return string(rawStrSlice[:index])
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
	tmp := types.NewRecordRPCURLInfo()

	value, ok := recordRPCUrl.LoadOrStore(rpc, tmp)
	if ok {
		r := value.(*types.RecordRPCURLInfo)
		r.Incr(success)
		recordRPCUrl.Store(rpc, r)
	}
}

func GetRPCFailureRate(rpc string) int {
	value, ok := recordRPCUrl.Load(rpc)
	if !ok {
		return 0
	}
	r := value.(*types.RecordRPCURLInfo)
	return r.FailRate()
}

func JsonEncode(source interface{}) (string, error) {
	if source == nil {
		return "", nil
	}

	bytesBuffer := &bytes.Buffer{}
	encoder := json.NewEncoder(bytesBuffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(source)
	if err != nil {
		return "", err
	}

	jsons := string(bytesBuffer.Bytes())
	if jsons == "null\n" {
		return "", nil
	}
	tsjsons := strings.TrimSuffix(jsons, "\n")
	return tsjsons, nil
}

func CopyProperties(source interface{}, target interface{}) error {
	if source == nil {
		return nil
	}

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
	if list == nil {
		return ""
	}

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

func AddressAdd0(address string) string {
	if address == "" {
		return ""
	}

	addressLen := len(address)
	if addressLen < 66 {
		lackNum := 66 - addressLen
		address = "0x" + strings.Repeat("0", lackNum) + address[2:]
	}
	return address
}

func AddressRemove0(address string) string {
	if address == "" {
		return ""
	}

	if len(address) == 66 && strings.HasPrefix(address, "0x0") {
		address = "0x" + strings.TrimLeft(address[2:], "0")
	}
	return address
}

func AddressListRemove0(list []string) []string {
	var addressList []string
	if len(list) > 0 {
		for _, addr := range list {
			addr = AddressRemove0(addr)
			addressList = append(addressList, addr)
		}
	}
	return addressList
}

func AddressIbcToLower(address string) string {
	if len(address) > 4 {
		ibc := address[:4]
		if libc := strings.ToLower(ibc); libc == "ibc/" {
			address = libc + address[4:]
		}
	}
	return address
}

func AddressListIbcToLower(list []string) []string {
	var addressList []string
	if len(list) > 0 {
		for _, addr := range list {
			addr = AddressIbcToLower(addr)
			addressList = append(addressList, addr)
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

func HexStringToBigInt(hetStr string) (*big.Int, error) {
	if strings.HasPrefix(hetStr, "0x") {
		hetStr = hetStr[2:]
	}
	bigInt, ok := new(big.Int).SetString(hetStr, 16)
	if !ok {
		return bigInt, errors.New("invalid hex string " + hetStr)
	}
	return bigInt, nil
}

func HexStringToInt64(hetStr string) (int64, error) {
	bigInt, err := HexStringToBigInt(hetStr)
	if err != nil {
		return 0, err
	}
	intValue := bigInt.Int64()
	return intValue, nil
}

func HexStringToUint64(hetStr string) (uint64, error) {
	bigInt, err := HexStringToBigInt(hetStr)
	if err != nil {
		return 0, err
	}
	intValue := bigInt.Uint64()
	return intValue, nil
}

func HexStringToString(hetStr string) (string, error) {
	bigInt, err := HexStringToBigInt(hetStr)
	if err != nil {
		return "0", err
	}
	stringValue := bigInt.String()
	return stringValue, nil
}

func StringToHexString(str string) (string, error) {
	intValue, err := strconv.Atoi(str)
	if err != nil {
		return "", err
	}
	hexValue := fmt.Sprintf("%x", intValue)
	hexValue += "0x"
	return hexValue, nil
}

func SubError(err error) error {
	if err == nil {
		return err
	}
	errStr := err.Error()
	errList := strings.Split(errStr, "\n")
	if len(errStr) > 10240 {
		nerrStr := errList[0] + errStr[0:10240] + errList[len(errList)-1]
		err = errors.New(nerrStr)
	}
	return err
}

func GetInt(value interface{}) (int, error) {
	var result int
	if intValue, ok := value.(int); ok {
		result = intValue
	} else if int32Value, ok := value.(int32); ok {
		result = int(int32Value)
	} else if int64Value, ok := value.(int64); ok {
		result = int(int64Value)
	} else if float32Value, ok := value.(float32); ok {
		result = int(float32Value)
	} else if float64Value, ok := value.(float64); ok {
		result = int(float64Value)
	} else {
		intValue, err := strconv.Atoi(GetString(value))
		if err != nil {
			return 0, err
		}
		result = intValue
	}
	return result, nil
}

func GetString(value interface{}) string {
	var result string
	if stringValue, ok := value.(string); ok {
		result = stringValue
	} else if floatValue, ok := value.(float64); ok {
		result = fmt.Sprintf("%.f", floatValue)
	} else {
		result = fmt.Sprintf("%v", value)
	}
	return result
}

func GetHexString(value interface{}) string {
	var result string
	if stringValue, ok := value.(string); ok {
		result = stringValue
	} else {
		result = fmt.Sprintf("%x", value)
	}
	return result
}

func RandInt32(min, max int32) int32 {
	//创建随机种子
	rand.Seed(time.Now().Unix())

	//生成min到max之间的int32类型随机数
	//参数max减min保证函数生成的随机数在0到min区间，
	//生成的随机数再加min则落在min到max区间
	rd := rand.Int31n(max-min) + min
	return rd
}
