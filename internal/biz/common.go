package biz

import (
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/signhash"
	"block-crawling/internal/types"
	"block-crawling/internal/utils"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"sync"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	ncommon "gitlab.bixin.com/mili/node-driver/common"

	"github.com/go-redis/redis"

	"go.uber.org/zap"
)

const (
	DEFAULT_MSG_LEVEL      = "FATAL"
	DEFAULT_ALARM_LEVEL    = 1
	DEFAULT_ALARM_CYCLE    = true
	DEFAULT_ALARM_INTERVAL = 3600
	LARK_MSG_KEY           = "lark:msg:%s"

	PAGE_SIZE = 200

	REDIS_NIL_KEY                   = "redis: nil"
	BLOCK_HEIGHT_KEY                = "block:height:"
	TX_FEE_GAS_PRICE                = "tx:fee:gasprice:"
	TX_FEE_GAS_LIMIT                = "tx:fee:gaslimit:"
	TX_FEE_MAX_FEE_PER_GAS          = "tx:fee:maxfeepergas:"
	TX_FEE_MAX_PRIORITY_FEE_PER_GAS = "tx:fee:maxpriorityfeepergas:"

	BLOCK_NODE_HEIGHT_KEY     = "block:height:node:"
	BLOCK_HASH_KEY            = "block:hash:"
	USER_ADDRESS_KEY          = "usercore:"
	BLOCK_HASH_EXPIRATION_KEY = 2 * time.Hour
	TABLE_POSTFIX             = data.TABLE_POSTFIX
	ADDRESS_DONE_NONCE        = "done:"    // + chainanme:address value-->nonce
	ADDRESS_PENDING_NONCE     = "pending:" // + chainname:address:nonce --->过期时间六个小时
	API_SUCCEESS              = "1"
	API_FAIL                  = "0"
	SOURCE_LOCAL              = "local"
	SOURCE_REMOTE             = "remote"
)

const (
	STC      = "STC"
	BTC      = "BTC"
	EVM      = "EVM"
	TVM      = "TVM"
	APTOS    = "APTOS"
	SUI      = "SUI"
	SOLANA   = "SOL"
	NERVOS   = "CKB"
	CASPER   = "CSPR"
	COSMOS   = "COSMOS"
	POLKADOT = "POLKADOT"
	KASPA    = "KASPA"
)

const (
	WEB     = "web"
	ANDROID = "android"
	IOS     = "ios"
)

const (
	TRON_TRANSFER_TRC10 = "TransferAssetContract"
	TRON_DAPP           = "TriggerSmartContract"
)

const (
	SUCCESS          = "success"
	FAIL             = "fail"
	PENDING          = "pending"          //-- 中间状态
	NO_STATUS        = "no_status"        //-- 中间状态
	DROPPED_REPLACED = "dropped_replaced" //--被丢弃或被置换 -- fail
	DROPPED          = "dropped"          //--被丢弃 -- fail
)

const (
	CANCEL        = "cancel"   //中心化操作 --- value CANCEL --success
	SPEED_UP      = "speed_up" //success
	GAS_FEE_LOW   = "gasFeeLow"
	NONCE_QUEUE   = "nonceQueue"
	NONCE_BREAK   = "nonceBreak"
	GAS_LIMIT_LOW = "gasLimitLow"
)

const (
	NATIVE                  = "native"
	TRANSFERNFT             = "transferNFT"
	APPROVENFT              = "approveNFT"
	CONTRACT                = "contract"
	CREATECONTRACT          = "createContract"
	EVENTLOG                = "eventLog"
	CREATEACCOUNT           = "createAccount"
	CLOSEACCOUNT            = "closeAccount"
	REGISTERTOKEN           = "registerToken"
	DIRECTTRANSFERNFTSWITCH = "directTransferNFTSwitch"
	MINT                    = "mint"
	SWAP                    = "swap"
	OTHER                   = "other"            //其他:只用于查询
	CANCELAPPROVE           = "cancelApprove"    //取消普通代币授权:只用于返回查询结果
	CANCELAPPROVENFT        = "cancelApproveNFT" //取消NFT授权:只用于返回查询结果
)

const (
	APPROVE               = "approve"
	SETAPPROVALFORALL     = "setApprovalForAll"
	TRANSFER              = "transfer"
	TRANSFERFROM          = "transferFrom"
	SAFETRANSFERFROM      = "safeTransferFrom"
	SAFEBATCHTRANSFERFROM = "safeBatchTransferFrom"
)

// 币种
const (
	CNY = "CNY" //人民币
	USD = "USD" //美元
)

const TOKEN_INFO_QUEUE_TOPIC = "token:info:queue:topic"
const TOKEN_INFO_QUEUE_PARTITION = "partition1"

// token类型
const (
	ERC20     = "ERC20"
	ERC721    = "ERC721"
	ERC1155   = "ERC1155"
	APTOSNFT  = "AptosNFT"
	SUINFT    = "SuiNFT"
	SOLANANFT = "SolanaNFT"
)

const STC_CODE = "0x00000000000000000000000000000001::STC::STC"

var rocketMsgLevels = map[string]int{
	"DEBUG":   0,
	"NOTICE":  1,
	"WARNING": 2,
	"FATAL":   3,
}

type SignRecordReq struct {
	Address         string   `json:"address"`
	ChainName       string   `json:"chainName"`
	SignType        string   `json:"signType"`
	SignStatus      []string `json:"signStatus"`
	TradeTime       int      `json:"tradeTime"`
	TransactionType string   `json:"transactionType"`
	Page            int      `json:"page"`
	Limit           int      `json:"limit"`
}

type SignRecordResponse struct {
	Ok           bool       `json:"ok,omitempty"`
	ErrorMessage string     `json:"errorMessage,omitempty"`
	Total        int        `json:"total"`
	Page         int        `json:"page"`
	Limit        int        `json:"limit"`
	SignInfos    []SignInfo `json:"signInfos,omitempty"`
}

type SignInfo struct {
	Address         string `json:"address"`
	ChainName       string `json:"chainName"`
	SignType        string `json:"signType"`
	SignStatus      string `json:"signStatus"`
	SignTxInput     string `json:"signTxInput"`
	SignUser        string `json:"signUser"`
	SignTime        int    `json:"signTime"`
	ConfirmTime     int    `json:"confirmTime"`
	TransactionType string `json:"transactionType"`
	TransactionHash string `json:"transactionHash"`
}

type BatchRpcParams struct {
	BatchReq []BatchRpcRequest `json:"batchReq,omitempty"`
}

type BatchRpcRequest struct {
	MethodName string `json:"methodName"`
	//Params    interface{} ` json:"params"`
	Params string ` json:"params"`
}
type BatchRpcResponse struct {
	RpcResponse []RpcResponse `json:"rpcResponse,omitempty"`
}

type RpcResponse struct {
	MethodName string `json:"methodName"`
	Result     string ` json:"result"`
}
type ClearNonceRequest struct {
	Address   string `json:"address"`
	ChainName string `json:"chainName"`
}

type ClearNonceResponse struct {
	Ok      bool   `json:"ok,omitempty"`
	Message string `json:"message,omitempty"`
}

type BroadcastRequest struct {
	SessionId           string   `json:"sessionId"`
	Address             string   `json:"address"`
	UserName            string   `json:"userName"`
	TxInput             string   `json:"txInput"`
	TxInputList         []string `json:"txInputList"`
	ChainName           string   `json:"chainName"`
	ErrMsg              string   `json:"errMsg"`
	Stage               string   `json:"stage"` // txParams：获取交易参数, broadcast：交易广播
	NodeURL             string   `json:"nodeUrl"`
	TransactionHashList []string `json:"transactionHashList"`
	TransactionHash     string   `json:"transactionHash"`
	SignStatus          string   `json:"signStatus"`
	SignType            string   `json:"signType"`
	TransactionType     string   `json:"transactionType"`
}

type BroadcastResponse struct {
	Ok      bool   `json:"ok,omitempty"`
	Message string `json:"message,omitempty"`
}

type DataDictionary struct {
	Ok                     bool     `json:"ok,omitempty"`
	ServiceTransactionType []string `json:"serviceTransactionType,omitempty"`
	ServiceStatus          []string `json:"serviceStatus,omitempty"`
}

type AddressPendingAmountRequest struct {
	ChainAndAddressList []ChainAndAddress `json:"chainAndAddressList,omitempty"`
}

type ChainAndAddress struct {
	ChainName string `json:"chainName,omitempty"`
	Address   string `json:"address,omitempty"`
}

type AddressPendingAmountResponse struct {
	Result map[string]PendingInfo `json:"result,omitempty"`
}

type PendingInfo struct {
	Amount        string                      `json:"amount,omitempty"`        //不带小数点的
	DeciamlAmount string                      `json:"deciamlAmount,omitempty"` //带小数点的
	IsPositive    string                      `json:"isPositive,omitempty"`    // 0 否 1 是
	Token         map[string]PendingTokenInfo `json:"token,omitempty"`
}

type PendingTokenInfo struct {
	Amount        string `json:"amount,omitempty"`        //不带小数点的
	DeciamlAmount string `json:"deciamlAmount,omitempty"` //带小数点的
	IsPositive    string `json:"isPositive,omitempty"`    //是负号 否 true 正 ， false 负
}

type AddressPendingAmount struct {
	ChainName              string               `json:"chainName,omitempty"`
	Address                string               `json:"address,omitempty"`
	Amount                 string               `json:"amount,omitempty"`        //带小数点的
	DeciamlAmount          string               `json:"deciamlAmount,omitempty"` //不带小数点的
	TokenPendingAmountList []TokenPendingAmount `json:"tokenPendingAmountList,omitempty"`
}

type TokenPendingAmount struct {
	TokenAddress       string `json:"tokenAddress,omitempty"`
	TokenAmount        string `json:"tokenAmount,omitempty"`        //带小数点的
	DeciamlTokenAmount string `json:"deciamlTokenAmount,omitempty"` //不带小数点的

}

type ChainFeeInfoReq struct {
	ChainName string `json:"chainName,omitempty"`
}

type ChainFeeInfoResp struct {
	ChainName            string `json:"chainName,omitempty"`
	GasPrice             string `json:"gasPrice,omitempty"`
	MaxFeePerGas         string `json:"maxFeePerGas,omitempty"`
	MaxPriorityFeePerGas string `json:"maxPriorityFeePerGas,omitempty"`
}

type CountOutTxRequest struct {
	ChainName string `json:"chainName"`
	SessionId string `json:"sessionId"`
	Address   string `json:"address"`
	ToAddress string `json:"toAddress"`
}

type CountOutTxResponse struct {
	Count int64 `json:"count"`
}

func CreatePendingInfo(amount, deciamlAmount string, isPositive string, token map[string]PendingTokenInfo) PendingInfo {
	return PendingInfo{
		Amount:        amount,
		DeciamlAmount: deciamlAmount,
		IsPositive:    isPositive,
		Token:         token,
	}
}

func CreatePendingTokenInfo(amount, deciamlAmount string, isPositive string) PendingTokenInfo {
	return PendingTokenInfo{
		Amount:        amount,
		DeciamlAmount: deciamlAmount,
		IsPositive:    isPositive,
	}
}

func CreateAddressPendingTokenAmount(tokenAddress, tokenAmount, deciamlTokenAmount string) TokenPendingAmount {
	return TokenPendingAmount{
		TokenAddress:       tokenAddress,
		TokenAmount:        tokenAmount,
		DeciamlTokenAmount: deciamlTokenAmount,
	}
}

func CreateAddressPendingAmount(chainName, address, amount, DeciamlAmount string, tokenPendingAmountList []TokenPendingAmount) AddressPendingAmount {
	return AddressPendingAmount{
		ChainName:              chainName,
		Address:                address,
		Amount:                 amount,
		DeciamlAmount:          DeciamlAmount,
		TokenPendingAmountList: tokenPendingAmountList,
	}
}

type UserAssetUpdateRequest struct {
	ChainName string `json:"chainName"`
	Address   string `json:"address"`

	Assets []UserAsset `json:"assets"`
}

type UserAsset struct {
	TokenAddress string `json:"tokenAddress"`
	Balance      string `json:"balance"`
	Decimals     int    `json:"decimals"`
}

// alarmOptions 报警相关的一些可自定义参数
type alarmOptions struct {
	channel       string
	level         string
	alarmLevel    int
	alarmCycle    bool
	alarmInterval int
	alarmAtUids   []string
	chainName     string // to reflect chainType as channel
}

var DefaultAlarmOptions = alarmOptions{
	level:         DEFAULT_MSG_LEVEL,
	alarmLevel:    DEFAULT_ALARM_LEVEL,
	alarmCycle:    DEFAULT_ALARM_CYCLE,
	alarmInterval: DEFAULT_ALARM_INTERVAL,
}

func (o *alarmOptions) getChannel() string {
	if o.channel != "" {
		return o.channel
	}
	if o.chainName != "" {
		if c, ok := PlatInfoMap[o.chainName]; ok {
			return c.Type
		}
	}

	return o.channel
}

// AlarmOption 报警自定义参数接口
type AlarmOption interface {
	apply(*alarmOptions)
}

type tempFunc func(*alarmOptions)

type funcAlarmOption struct {
	f tempFunc
}

func (fdo *funcAlarmOption) apply(e *alarmOptions) {
	fdo.f(e)
}

func newfuncAlarmOption(f tempFunc) *funcAlarmOption {
	return &funcAlarmOption{f: f}
}

func WithMsgLevel(level string) AlarmOption {
	return newfuncAlarmOption(func(e *alarmOptions) {
		e.level = level
	})
}

func WithAlarmLevel(alarmLevel int) AlarmOption {
	return newfuncAlarmOption(func(e *alarmOptions) {
		e.alarmLevel = alarmLevel
	})
}

func WithAlarmCycle(alarmCycle bool) AlarmOption {
	return newfuncAlarmOption(func(e *alarmOptions) {
		e.alarmCycle = alarmCycle
	})
}

func WithAlarmInterval(alarmInterval int) AlarmOption {
	return newfuncAlarmOption(func(e *alarmOptions) {
		e.alarmInterval = alarmInterval
	})
}

func WithAlarmAtList(uids ...string) AlarmOption {
	return newfuncAlarmOption(func(e *alarmOptions) {
		e.alarmAtUids = append(e.alarmAtUids, uids...)
	})
}

func WithAlarmChannel(channel string) AlarmOption {
	return newfuncAlarmOption(func(e *alarmOptions) {
		e.channel = channel
	})
}

func WithAlarmChainName(chainName string) AlarmOption {
	return newfuncAlarmOption(func(e *alarmOptions) {
		e.chainName = chainName
	})
}

func GetAlarmTimestamp(key string) (int64, error) {
	val, err := data.RedisClient.Get(key).Result()
	if err != nil {
		return int64(0), err
	}
	timestamp, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		log.Error("GetAlarmTimestamp error: %s", zap.Error(err))
		return int64(0), err
	}

	return timestamp, nil
}

func UserAddressSwitchRetryAlert(chainName, address string) (bool, string, error) {
	if address == "" {
		return false, "", nil
	}

	enable, uid, err := UserAddressSwitch(address)
	if err != nil {
		// redis出错 接入lark报警
		alarmMsg := fmt.Sprintf("请注意：%s链查询redis中用户地址失败，address:%s", chainName, address)
		alarmOpts := WithMsgLevel("FATAL")
		alarmOpts = WithAlarmChainName(chainName)
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	}
	return enable, uid, err
}

func UserAddressSwitch(address string) (bool, string, error) {
	if address == "" {
		return false, "", nil
	}
	enable := true
	uid, err := data.RedisClient.Get(USER_ADDRESS_KEY + address).Result()
	for i := 0; i < 3 && err != nil && err != redis.Nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		uid, err = data.RedisClient.Get(USER_ADDRESS_KEY + address).Result()
	}
	if !AppConfig.ScanAll {
		if err != nil && err != redis.Nil {
			return false, "", err
		}
		if uid == "" {
			enable = false
		}
	}
	return enable, uid, nil
}

var chainGasPriceMap = &sync.Map{}
var chainMaxFeePerGasMap = &sync.Map{}
var chainMaxPriority = &sync.Map{}
var chainGasPriceLock sync.RWMutex

var chainBlockNumberPrice = &sync.Map{}
var chainBlockNumberMaxFee = &sync.Map{}
var chainBlockNumberPriceMaxPriority = &sync.Map{}

var priceSell = &sync.Map{}
var MaxFeeSell = &sync.Map{}
var MaxPrioritySell = &sync.Map{}

var chainBlockGasPrice = &sync.Map{}
var chainBlockMaxFee = &sync.Map{}
var chainBlockMaxPriority = &sync.Map{}

func ChainFeeSwitchRetryAlert(chainName, maxFeePerGasNode, maxPriorityFeePerGasNode, gasPriceNode string, blockNumber uint64, txhash string) {
	//日志打印 详情 chainBlockTxInfoMap
	//key := chainName + "_" + strconv.Itoa(int(blockNumber)) + "_" + txhash
	//value := gasPriceNode

	//log.Info("记录每笔交易", zap.Any(key, value))

	//tx:fee:gasprice:ETH
	gpk := TX_FEE_GAS_PRICE + chainName
	UpdateMap(chainName, gasPriceNode, blockNumber, chainGasPriceMap, gpk, chainBlockNumberPrice, priceSell, chainBlockGasPrice)

	mfpgk := TX_FEE_MAX_FEE_PER_GAS + chainName
	UpdateMap(chainName, maxFeePerGasNode, blockNumber, chainMaxFeePerGasMap, mfpgk, chainBlockNumberMaxFee, MaxFeeSell, chainBlockMaxFee)
	//
	mpfpgk := TX_FEE_MAX_PRIORITY_FEE_PER_GAS + chainName
	UpdateMap(chainName, maxPriorityFeePerGasNode, blockNumber, chainMaxPriority, mpfpgk, chainBlockNumberPriceMaxPriority, MaxPrioritySell, chainBlockMaxPriority)
}

func UpdateMap(chainName string, value string, blockNumber uint64, businessMap *sync.Map, bizRedisKey string, chainBlockNumberMap *sync.Map, sell *sync.Map, chainBlock *sync.Map) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("UpdateMap error, chainName:"+chainName, e)
			} else {
				log.Errore("UpdateMap panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链处理fee失败, error：%s", chainName, fmt.Sprintf("%s", err))
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()

	if value == "" {
		return
	}
	var err error
	_, def4442 := chainBlockNumberMap.LoadOrStore(chainName+strconv.Itoa(int(blockNumber)), blockNumber)
	sl, ok := businessMap.LoadOrStore(chainName, &sync.Map{})

	if !def4442 {
		preBlockNumber := blockNumber - 100
		//清楚本地缓存
		if ok {
			sccm := sl.(*sync.Map)
			sccm.Delete(preBlockNumber)
			var priceValues []int

			//计算一次
			_, def := chainBlockNumberMap.LoadOrStore(chainName+strconv.Itoa(int(blockNumber))+"calculate", blockNumber)
			if !def {
				chainBlockNumberMap.Delete(chainName + strconv.Itoa(int(blockNumber)-10) + "calculate")
				for n := blockNumber - 10; n < blockNumber; n++ {
					//log.Info(chainName+"1", zap.Any("块高333", n), zap.Any("value", dd), zap.Any("有无", f))
					//先获取 块高缓存里面的
					if vb, okb := chainBlock.Load(chainName + strconv.Itoa(int(n)) + "price"); okb {
						priceValues = append(priceValues, vb.(int))
					} else {
						sn := sl.(*sync.Map)
						dd, f := sn.Load(n)
						if f && len(dd.([]int)) > 0 {
							dr := utils.GetMinHeap(dd.([]int), 10)
							sum := 0
							//求平均值
							for _, val := range dr {
								sum += val
							}
							//8 9 10
							//log.Info(chainName+"2", zap.Any("最小十个值", dr), zap.Any("sum", sum), zap.Any("块高333", n))

							avg := math.Ceil(float64(sum) / float64(len(dr)))
							//log.Info(chainName+"3", zap.Any("avg", int(math.Ceil(avg))), zap.Any("块高333", n))
							//这个是一个块里面最小十笔 平均值
							priceValues = append(priceValues, int(math.Ceil(avg)))
							chainBlock.LoadOrStore(chainName+strconv.Itoa(int(blockNumber))+"price", int(math.Ceil(avg)))

						}
					}
					chainBlock.Delete(chainName + strconv.Itoa(int(n)-50) + "price")
				}
				//log.Info(chainName + "4", zap.Any("priceValues", priceValues))

				if len(priceValues) >= 1 {
					dr := utils.GetMaxHeap(priceValues, 1)
					sell.LoadOrStore(chainName, dr[0])
					val := strconv.Itoa(dr[0])
					//log.Info(chainName + "5", zap.Any("最终推荐值", val))
					//sn := sl.(*sync.Map)
					//ddd, ff := sn.LoadOrStore(blockNumber-1, make([]int, 0))
					//if ff && len(ddd.([]int)) > 0 {
					//drr := utils.GetMinHeap(ddd.([]int), 1)
					//log.Info("最近一个块的最小值", zap.Any("集合", ddd.([]int)), zap.Any("min", drr), zap.Any("块高", blockNumber-1))
					//res, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(dr[0])/float64(drr[0])), 64)
					//if drr[0] > dr[0] {
					//	log.Info("最新推荐price低于正常上链price", zap.Any("推荐", dr[0]), zap.Any("上链最小值", drr[0]), zap.Any(chainName, blockNumber), zap.Any("res", res))
					//} else {
					//	log.Info("最新推荐priceheihei", zap.Any("推荐", dr[0]), zap.Any("上链最小值", drr[0]), zap.Any(chainName, blockNumber), zap.Any("res", res))
					//	if res >= 1 && res <= 1.2 {
					//		log.Info("最新推荐price高于正常上链price 20%", zap.Any("推荐", dr[0]), zap.Any("上链最小值", drr[0]), zap.Any(chainName, blockNumber), zap.Any("res", res))
					//	} else if res > 1.2 && res <= 1.3 {
					//		log.Info("最新推荐price高于正常上链price 30%", zap.Any("推荐", dr[0]), zap.Any("上链最小值", drr[0]), zap.Any(chainName, blockNumber), zap.Any("res", res))
					//	} else if res > 1.3 && res <= 1.4 {
					//		log.Info("最新推荐price高于正常上链price 40%", zap.Any("推荐", dr[0]), zap.Any("上链最小值", drr[0]), zap.Any(chainName, blockNumber), zap.Any("res", res))
					//	} else if res > 1.4 && res <= 1.5 {
					//		log.Info("最新推荐price高于正常上链price 50%", zap.Any("推荐", dr[0]), zap.Any("上链最小值", drr[0]), zap.Any(chainName, blockNumber), zap.Any("res", res))
					//	} else if res > 1.5 && res <= 1.6 {
					//		log.Info("最新推荐price高于正常上链price 60%", zap.Any("推荐", dr[0]), zap.Any("上链最小值", drr[0]), zap.Any(chainName, blockNumber), zap.Any("res", res))
					//	} else if res > 1.6 && res <= 1.7 {
					//		log.Info("最新推荐price高于正常上链price 70%", zap.Any("推荐", dr[0]), zap.Any("上链最小值", drr[0]), zap.Any(chainName, blockNumber), zap.Any("res", res))
					//	} else if res > 1.7 && res <= 1.8 {
					//		log.Info("最新推荐price高于正常上链price 80%", zap.Any("推荐", dr[0]), zap.Any("上链最小值", drr[0]), zap.Any(chainName, blockNumber), zap.Any("res", res))
					//	} else if res > 1.8 && res <= 1.9 {
					//		log.Info("最新推荐price高于正常上链price 90%", zap.Any("推荐", dr[0]), zap.Any("上链最小值", drr[0]), zap.Any(chainName, blockNumber), zap.Any("res", res))
					//	} else {
					//		log.Info("最新推荐price高于正常上链price 特高", zap.Any("推荐", dr[0]), zap.Any("上链最小值", drr[0]), zap.Any(chainName, blockNumber), zap.Any("res", res))
					//
					//	}
					//}

					//}
					err = data.RedisClient.Set(bizRedisKey, val, 0).Err()
					log.Info(bizRedisKey+"推荐值", zap.Any("推荐", val), zap.Any(chainName, blockNumber))
				}
			}
		}
	}

	// 最小一笔 剔除 高于上一个推荐的2倍以上交易
	//sn := sl.(*sync.Map)
	//ddd1, _ := sn.Load(blockNumber - 1)
	gpn, _ := strconv.Atoi(value)

	//if ddd1 != nil && len(ddd1.([]int)) > 0 {
	//	drr := utils.GetMinHeap(ddd1.([]int), 1)
	//	fps, _ := gasPriceSell.Load(chainName)
	//	fpsi := fps.(int)
	//	res, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(drr[0])/float64(fpsi)), 64)
	//	log.Info("看看最新块和上一个块的最小", zap.Any("推荐", fpsi), zap.Any("上链最小值", drr[0]), zap.Any(chainName, blockNumber), zap.Any("res", res))
	//
	//	if res > 5  {
	//		return
	//	}
	//}

	fps, _ := sell.Load(chainName)
	if fps != nil {
		fpsi := fps.(int)
		res, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", float64(gpn)/float64(fpsi)), 64)
		//log.Info("看看最新块和上一个块的最小", zap.Any("推荐", fpsi), zap.Any("上链最小值", gpn), zap.Any(chainName, blockNumber), zap.Any("res", res))

		if res > 3 {
			return
		}
	}
	busMap := sl.(*sync.Map)
	chainGasPriceLock.Lock()
	vv, _ := busMap.LoadOrStore(blockNumber, make([]int, 0))

	v := vv.([]int)
	v = append(v, gpn)
	//log.Info("=================", zap.Any("未转化", value), zap.Any("转化后", gpn), zap.Any("", strconv.Itoa(int(blockNumber))+"kkk"),zap.Any("v",v))

	busMap.Store(blockNumber, v)
	businessMap.Store(chainName, busMap)

	//sl2, _ := businessMap.LoadOrStore(chainName, &sync.Map{})
	//busMap2 := sl2.(*sync.Map)
	//vv2, _ := busMap2.LoadOrStore(blockNumber, make([]int,0))
	//
	//log.Info(chainName + "6", zap.Any("vv", vv2),zap.Any("vvo",blockNumber))
	chainGasPriceLock.Unlock()

	if err != nil {
		alarmMsg := fmt.Sprintf("请注意：手续费相关 %s链,key: %s 插入redis失败", chainName, bizRedisKey)
		alarmOpts := WithMsgLevel("FATAL")
		alarmOpts = WithAlarmChannel("fee")
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	}

}

// MD5 对字符串做 MD5
func MD5(v string) string {
	d := []byte(v)
	m := md5.New()
	m.Write(d)
	return hex.EncodeToString(m.Sum(nil))
}

func BjNow() string {
	var cstSh, _ = time.LoadLocation("Asia/Shanghai")
	now := time.Now().In(cstSh).Format("2006-01-02 15:04:05")

	return now
}

func GetTableName(chainName string) string {
	return data.GetTableName(chainName)
}

func GetDecimalsSymbol(chainName, parseData string) (int32, string, error) {
	parseDataJson := make(map[string]interface{})
	err := json.Unmarshal([]byte(parseData), &parseDataJson)
	if err != nil {
		return 0, "", err
	}
	tokenInfoMap := parseDataJson["token"]
	if tokenInfoMap == nil {
		return 0, "", errors.New("token is null")
	}
	tokenInfo := tokenInfoMap.(map[string]interface{})
	var address string
	if tokenInfo["address"] != nil {
		address = tokenInfo["address"].(string)
	}
	var decimals int32
	if tokenInfo["decimals"] != nil {
		decimals32, err := utils.GetInt(tokenInfo["decimals"])
		if err != nil {
			return 0, "", err
		}
		decimals = int32(decimals32)
	}
	var symbol string
	if tokenInfo["symbol"] != nil {
		symbol = tokenInfo["symbol"].(string)
	}
	if address == "" {
		if platInfo, ok := PlatInfoMap[chainName]; ok {
			decimals = platInfo.Decimal
			symbol = platInfo.NativeCurrency
		} else {
			return 0, "", errors.New("chain " + chainName + " is not support")
		}
	}
	return decimals, symbol, nil
}

func ParseGetTokenInfo(chainName, parseData string) (*types.TokenInfo, error) {
	tokenInfo, err := ParseTokenInfo(parseData)
	if err == nil && tokenInfo.Address == "" {
		if platInfo, ok := PlatInfoMap[chainName]; ok {
			tokenInfo.Decimals = int64(platInfo.Decimal)
			tokenInfo.Symbol = platInfo.NativeCurrency
		} else {
			return nil, errors.New("chain " + chainName + " is not support")
		}
	}
	return tokenInfo, err
}

func ParseTokenInfo(parseData string) (*types.TokenInfo, error) {
	parseDataJson := make(map[string]interface{})
	err := json.Unmarshal([]byte(parseData), &parseDataJson)
	if err != nil {
		return nil, err
	}
	tokenInfoMap := parseDataJson["token"]
	if tokenInfoMap == nil {
		return nil, errors.New("token is null")
	}

	tokenInfoByte, err := json.Marshal(tokenInfoMap)
	if err != nil {
		return nil, err
	}
	tokenInfo := &types.TokenInfo{}

	err = json.Unmarshal(tokenInfoByte, tokenInfo)
	if err != nil {
		tokenInfoMapString := make(map[string]interface{})
		err = json.Unmarshal(tokenInfoByte, &tokenInfoMapString)
		if err != nil {
			return nil, err
		}

		if tokenInfoMapString["address"] != nil {
			tokenInfo.Address = tokenInfoMapString["address"].(string)
		}
		if tokenInfoMapString["amount"] != nil {
			if a, ok := tokenInfoMapString["amount"].(string); ok {
				tokenInfo.Amount = a
			}
		}
		if tokenInfoMapString["symbol"] != nil {
			tokenInfo.Symbol = tokenInfoMapString["symbol"].(string)
		}
		if tokenInfoMapString["token_uri"] != nil {
			tokenInfo.TokenUri = tokenInfoMapString["token_uri"].(string)
		}
		if tokenInfoMapString["collection_name"] != nil {
			tokenInfo.CollectionName = tokenInfoMapString["collection_name"].(string)
		}
		if tokenInfoMapString["token_type"] != nil {
			tokenInfo.TokenType = tokenInfoMapString["token_type"].(string)
		}
		if tokenInfoMapString["token_id"] != nil {
			tokenInfo.TokenId = tokenInfoMapString["token_id"].(string)
		}
		if tokenInfoMapString["item_name"] != nil {
			tokenInfo.ItemName = tokenInfoMapString["item_name"].(string)
		}
		if tokenInfoMapString["item_uri"] != nil {
			tokenInfo.ItemUri = tokenInfoMapString["item_uri"].(string)
		}

		if tokenInfoMapString["decimals"] != nil {
			if d, ok := tokenInfoMapString["decimals"].(string); ok {
				ds, _ := strconv.Atoi(d)
				tokenInfo.Decimals = int64(ds)
			} else if di, ok := tokenInfoMapString["decimals"].(int); ok {
				tokenInfo.Decimals = int64(di)
			}

		}

	}
	return tokenInfo, nil
}

func NotifyBroadcastTxFailed(ctx context.Context, req *BroadcastRequest, device Device) {
	var msg string
	alarmOpts := WithMsgLevel("FATAL")

	deviceId := device.Id
	userAgent := device.UserAgent

	if req.Stage == "" || req.Stage == "broadcast" {
		sessionID := req.SessionId
		errMsg := req.ErrMsg
		info, err := data.UserSendRawHistoryRepoInst.GetLatestOneBySessionId(ctx, sessionID)
		if err != nil {
			log.Error(
				"SEND ALARM OF BROADCATING TX FAILED",
				zap.String("sessionId", sessionID),
				zap.String("errMsg", errMsg),
				zap.Error(err),
			)
			msg = fmt.Sprintf(
				"交易广播失败。\nsessionId：%s\n钱包地址：%s\nUser-Agent：%s\n错误消息：%s\n用户名: %s\nDevice-Id: %s",
				sessionID,
				userAgent,
				"Unknown",
				errMsg,
				"Unknown",
				deviceId,
			)
		} else {
			msg = fmt.Sprintf(
				"%s 链交易广播失败。\nsessionId：%s\n钱包地址：%s\nUser-Agent：%s\n错误消息：%s\ntxInput: %s\n用户名: %s\nDevice-Id: %s",
				info.ChainName,
				sessionID,
				info.Address,
				userAgent,
				errMsg,
				info.BaseTxInput,
				info.UserName,
				deviceId,
			)
		}
		alarmOpts = WithAlarmChannel("txinput")
	} else {
		msg = fmt.Sprintf(
			"%s 链获取交易参数失败。\n节点：%s\n钱包地址：%s\nUser-Agent：%s\n错误消息：%s\ntxInput: %s\n用户名: %s\nDevice-Id: %s",
			req.ChainName,
			req.NodeURL,
			req.Address,
			userAgent,
			req.ErrMsg,
			req.TxInput,
			deviceId,
			req.UserName,
		)
		alarmOpts = WithAlarmChannel("node-proxy")
	}
	LarkClient.NotifyLark(msg, nil, nil, alarmOpts)
}

func ExecuteRetry(chainName string, fc func(client chain.Clienter) (interface{}, error)) (interface{}, error) {
	var result interface{}
	var err error

	spider := PlatformMap[chainName].GetBlockSpider()
	err = spider.WithRetry(func(client chain.Clienter) error {
		result, err = fc(client)
		if err != nil {
			return ncommon.Retry(err)
		}
		return nil
	})
	return result, err
}

func ExecuteRetrys(chainName string, chainStateStore chain.StateStore, cfc func(url string) (chain.Clienter, error), fc func(client chain.Clienter) (interface{}, error)) (interface{}, error) {
	var result interface{}
	var err error

	var nodeURL []string
	if platInfo, ok := PlatInfoMap[chainName]; ok {
		nodeURL = platInfo.RpcURL
	} else {
		return nil, errors.New("chain " + chainName + " is not support")
	}
	clients := make([]chain.Clienter, 0, len(nodeURL))
	for _, url := range nodeURL {
		c, err := cfc(url)
		if err != nil {
			log.Warn("调用ExecuteRetry方法，创建Client异常", zap.Any("chainName", len(chainName)), zap.Any("error", err))
		}
		clients = append(clients, c)
	}
	spider := chain.NewBlockSpider(chainStateStore, clients...)
	err = spider.WithRetry(func(client chain.Clienter) error {
		result, err = fc(client)
		if err != nil {
			return ncommon.Retry(err)
		}
		return nil
	})

	return result, err
}

func HashSignMessage(chainName string, req *signhash.SignMessageRequest) (string, error) {
	if v, ok := PlatInfoMap[chainName]; ok {
		rawMsg, err := signhash.Hash(v.Type, req)
		if err != nil {
			return "", err
		}
		ret := data.RedisClient.Set(fmt.Sprintf("signing-message:%s", req.SessionId), rawMsg, time.Hour*24*7)
		if ret.Err() != nil {
			log.Error(
				"SAVE SIGNING MESSAGE TO REDIS FAILED",
				zap.Error(ret.Err()),
				zap.String("chainName", req.ChainName),
				zap.String("sessionId", req.SessionId),
			)
		}
		return rawMsg, nil
	}
	return "", errors.New("unknown chain")
}

type RPCNodeBalancer interface {
	GetBalance(address string) (string, error)
}
type RPCAccountHash interface {
	GetAccountHashByAddress(address string) (string, error)
}

func IsNative(chainName string, tokenAddress string) bool {
	if v, ok := PlatInfoMap[chainName]; ok {
		if v.Type == BTC {
			return true
		}
	}

	if tokenAddress == "" {
		return true
	}

	if chainName == STC && tokenAddress == STC_CODE {
		return true
	}

	return false
}

type Device struct {
	Id        string `json:"id"`
	UserAgent string `json:"userAgent"`
}

type JsonRpcContext struct {
	context.Context

	Device string
}

func (ctx *JsonRpcContext) ParseDevice() Device {
	return ParseDevice(ctx.Device)
}

var userAgentRegex = regexp.MustCompile(
	`openblock[-_](ios|android|linux|mac|windows)/([0-9]*) \(Channel/(.*)[; ] UUID/(.*)[; ] Version/(.*)[; ] Device/(.*)[; ] System/(.*)[;]\)`,
)

func ParseDevice(s string) Device {
	submatches := userAgentRegex.FindStringSubmatch(s)
	var deviceId string
	if len(submatches) > 4 {
		deviceId = submatches[4]
	}
	return Device{
		Id:        deviceId,
		UserAgent: s,
	}
}
