package biz

import (
	v1 "block-crawling/internal/client"
	"block-crawling/internal/data"
	"block-crawling/internal/httpclient"
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
	"math/big"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/exp/slices"

	"gorm.io/datatypes"

	"github.com/shopspring/decimal"

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

	PAGE_SIZE  = 200
	DAY        = time.Hour * 24
	DAY_SECOND = 3600 * 24

	REDIS_NIL_KEY                   = "redis: nil"
	BLOCK_HEIGHT_KEY                = "block:height:"
	TX_FEE_GAS_PRICE                = "tx:fee:gasprice:"
	TX_FEE_GAS_LIMIT                = "tx:fee:gaslimit:"
	TX_FEE_MAX_FEE_PER_GAS          = "tx:fee:maxfeepergas:"
	TX_FEE_MAX_PRIORITY_FEE_PER_GAS = "tx:fee:maxpriorityfeepergas:"

	BLOCK_NODE_HEIGHT_KEY     = "block:height:node:"
	BLOCK_HASH_KEY            = "block:hash:"
	USER_ADDRESS_KEY          = "usercore:"
	USERCENTER_ADDRESS_KEY    = "usercenter:"
	BLOCK_HASH_EXPIRATION_KEY = 2 * time.Hour
	TABLE_POSTFIX             = data.TABLE_POSTFIX
	ADDRESS_DONE_NONCE        = "done:"    // + chainanme:address value-->nonce
	ADDRESS_PENDING_NONCE     = "pending:" // + chainname:address:nonce --->过期时间六个小时
	API_SUCCEESS              = "1"
	API_FAIL                  = "0"
	SOURCE_LOCAL              = "local"
	SOURCE_REMOTE             = "remote"

	BLOCK_HASH_CUSTOM_EXPIRATION_KEY = 15 * time.Minute

	OKLINK              = "oklink"
	GAS_COEFFICIENT_KEY = "gasCoefficient:"
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
	TON      = "TON"
)

const (
	MAIN_NET_TYPE = "main"
	TEST_NET_TYPE = "test"
)

const (
	WEB     = "web"
	ANDROID = "android"
	IOS     = "ios"
)

const (
	BFStationStable  = "bfstation-stable"
	BFStationDexSwap = "bfstation-dex-swap"
	BFStationDexLiq  = "bfstation-dex-liq"
)

const (
	TRON_TRANSFER_TRC10 = "TransferAssetContract"
	TRON_DAPP           = "TriggerSmartContract"
)

const (
	SUCCESS                     = "success"
	FAIL                        = "fail"
	PENDING                     = "pending"          //-- 中间状态
	NO_STATUS                   = "no_status"        //-- 中间状态
	DROPPED_REPLACED            = "dropped_replaced" //--被丢弃或被置换 -- fail
	DROPPED                     = "dropped"          //--被丢弃 -- fail
	SIGNRECORD_CONFIRM          = "2"
	SIGNRECORD_BROADCASTED      = "1"
	SIGNRECORD_DROPPED_REPLACED = "4"
	SIGNRECORD_DROPPED          = "3"
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
	ADDLIQUIDITY            = "addLiquidity"
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

// 钱包类型
const (
	PERSON  = "PERSON"  //个人钱包
	COMPANY = "COMPANY" //企业钱包
	COADMIN = "COADMIN" //共管钱包
)

const TOKEN_INFO_QUEUE_TOPIC = "token:info:queue:topic"
const TOKEN_INFO_QUEUE_PARTITION = "partition1"

// token类型
const (
	ERC20     = types.ERC20
	ERC721    = types.ERC721
	ERC1155   = types.ERC1155
	APTOSNFT  = types.APTOSNFT
	SUINFT    = types.SUINFT
	TONNFT    = types.TONNFT
	BENFENNFT = types.BENFENNFT
	SOLANANFT = types.SOLANANFT
	COSMOSNFT = types.COSMOSNFT
)

const STC_CODE = "0x00000000000000000000000000000001::STC::STC"

const PriceKeyBTC = "bitcoin"
const ETH_USDT_ADDRESS = "0xdAC17F958D2ee523a2206206994597C13D831ec7"

var rocketMsgLevels = map[string]int{
	"DEBUG":   0,
	"NOTICE":  1,
	"WARNING": 2,
	"FATAL":   3,
}

type EventLog struct {
	From   string         `json:"from"`
	To     string         `json:"to"`
	Amount *big.Int       `json:"amount"`
	Token  EventTokenInfo `json:"token"`
}

type EventTokenInfo struct {
	Address  string `json:"address"`
	Amount   string `json:"amount"`
	Decimals int64  `json:"decimals"`
	Symbol   string `json:"symbol"`
}

type AssetDistributionReq struct {
	ChainAndAddress
	TimeRange     string `json:"timeRange,omitempty"`
	DataDirection string `json:"dataDirection,omitempty"`
	OrderField    string `json:"orderField,omitempty"`
}
type AssetDistributionResponse struct {
	Ok               bool           `json:"ok,omitempty"`
	ErrorMessage     string         `json:"errorMessage,omitempty"`
	AssetBalanceList []AssetBalance `json:"assetBalanceList,omitempty"`
}
type AssetBalance struct {
	CnyAmount        string `json:"cnyAmount,omitempty"`        //带小数点的 法币
	UsdAmount        string `json:"usdAmount,omitempty"`        //带小数点的 法币
	NewCnyAmount     string `json:"newCnyAmount,omitempty"`     //带小数点的 法币
	NewUsdAmount     string `json:"newUsdAmount,omitempty"`     //带小数点的 法币
	CnyBalanceAmount string `json:"cnyBalanceAmount,omitempty"` //带小数点的 法币
	UsdBalanceAmount string `json:"usdBalanceAmount,omitempty"` //带小数点的 法币
	Symbol           string `json:"symbol"`
	HoldCnyAmount    string `json:"holdCnyAmount,omitempty"` //带小数点的 法币
	HoldUsdAmount    string `json:"holdUsdAmount,omitempty"` //带小数点的 法币
	Proportion       string `json:"proportion,omitempty"`
	Negative         string `json:"negative,omitempty"`
	Icon             string `json:"icon,omitempty"`
	TokenAddress     string `json:"tokenAddress"`
}
type AddressAssetResponse struct {
	Ok                   bool               `json:"ok,omitempty"`
	ErrorMessage         string             `json:"errorMessage,omitempty"`
	Proportion           string             `json:"proportion,omitempty"`
	Negative             string             `json:"negative,omitempty"`
	ProceedsUsd          string             `json:"proceedsUsd,omitempty"`
	ProceedsCny          string             `json:"proceedsCny,omitempty"`
	AddressAssetTypeList []AddressAssetType `json:"addressAssetTypeList,omitempty"`
	StartTime            int                `json:"startTime,omitempty"`
	EndTime              int                `json:"endTime,omitempty"`
}
type AddressAssetType struct {
	CnyAmount        string          `json:"cnyAmount,omitempty"` //带小数点的 法币
	UsdAmount        string          `json:"usdAmount,omitempty"` //带小数点的 法币
	Dt               int             `json:"dt,omitempty"`
	CnyAmountDecimal decimal.Decimal `json:"cnyAmountDecimal,omitempty"` //带小数点的 法币
	UsdAmountDecimal decimal.Decimal `json:"usdAmountDecimal,omitempty"` //带小数点的 法币
}

type TransactionTopResponse struct {
	Ok                 bool             `json:"ok,omitempty"`
	ErrorMessage       string           `json:"errorMessage,omitempty"`
	TransactionTopList []TransactionTop `json:"transactionTopList,omitempty"`
}

type TransactionTop struct {
	Count     int64  `json:"count"`
	Address   string `json:"address"`
	CnyAmount string `json:"cnyAmount,omitempty"` //带小数点的 法币
	UsdAmount string `json:"usdAmount,omitempty"` //带小数点的 法币
}

type TopDappResponse struct {
	Ok           bool       `json:"ok,omitempty"`
	ErrorMessage string     `json:"errorMessage,omitempty"`
	DappInfos    []DappInfo `json:"dappInfos,omitempty"`
}
type DappInfo struct {
	//Dapp 的 LOGO、名称、交互次数
	Origin     string `json:"origin,omitempty"`
	Icon       string `json:"icon,omitempty"`
	DappName   string `json:"dappName,omitempty"`
	CheckCount int    `json:"checkCount,omitempty"`
}

type TransactionTypeDistributionResponse struct {
	Ok                  bool                          `json:"ok,omitempty"`
	ErrorMessage        string                        `json:"errorMessage,omitempty"`
	Total               int                           `json:"total,omitempty"`
	TransactionTypeList []TransactionTypeDistribution `json:"transactionTypeList,omitempty"`
}

type TokenAssetAndCountResponse struct {
	Ok                        bool                       `json:"ok,omitempty"`
	ErrorMessage              string                     `json:"errorMessage,omitempty"`
	TotalCnyAmount            string                     `json:"totalCnyAmount,omitempty"`
	TotalUsdAmount            string                     `json:"totalUsdAmount,omitempty"`
	TotalTransactionQuantity  int                        `json:"totalTransactionQuantity"`
	ChainTokenNumberAndAssets []ChainTokenNumberAndAsset `json:"chainTokenNumberAndAssets"`
}

type ChainTokenNumberAndAsset struct {
	TokenAddress        string `json:"tokenAddress"`
	Address             string `json:"address"`
	Symbol              string `json:"symbol"`
	CnyAmount           string `json:"cnyAmount,omitempty"` //带小数点的 法币
	UsdAmount           string `json:"usdAmount,omitempty"` //带小数点的 法币
	TransactionQuantity int    `json:"transactionQuantity"`
}

type TransactionTypeDistribution struct {
	TransactionType string `json:"transactionType"`
	Count           int64  `json:"count"`
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
	Stage               string   `json:"stage"` // txParams：获取交易参数, broadcast：交易广播,txHash:校验txHash,urlCheck:校验URL
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

type SignTypeMessageRequest struct {
	SessionId  string `json:"sessionId"`
	SignStatus string `json:"signStatus"`
}
type SignTxRequest struct {
	SessionIds []string `json:"sessionIds"`
}
type SignTxResponse struct {
	Ok                    bool                `json:"ok,omitempty"`
	Message               string              `json:"message,omitempty"`
	SessionTxhashInfoList []SessionTxhashInfo `json:"sessionTxhashInfoList,omitempty"`
}
type SessionTxhashInfo struct {
	SessionId       string `json:"sessionId"`
	TransactionHash string `json:"transactionHash"`
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

type TimeCondition struct {
	StartUnix int `json:"startUnix,omitempty"`
	EndUnix   int `json:"endUnix,omitempty"`
}

type TransactionTypeReq struct {
	TimeRange  string `json:"timeRange,omitempty"`
	OrderField string `json:"orderField,omitempty"`
	ChainAndAddress
	TimeCondition
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
	GasUsed              string `json:"gasUsed,omitempty"`
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

type CreateUtxoPendingReq struct {
	TxHash                string              `json:"tx_hash"`
	CreateUtxoPendingList []CreateUtxoPending `json:"createUtxoPendingList,omitempty"`
}

type CreateUtxoPending struct {
	Uid       string `json:"uid,omitempty"`
	ChainName string `json:"chainName"`
	Address   string `json:"address"`
	N         int    `json:"n"`
	Hash      string `json:"hash"`
}

type GasCoefficientReq struct {
	ChainName string `json:"chainName,omitempty"`
}

type GasCoefficientResp struct {
	GasCoefficient float32 `json:"gas_coefficient"`
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

	Assets []UserAsset     `json:"assets"`
	Extra  json.RawMessage `json:"extra"`
}

type UserAssetExtra struct {
	AllTokens  []UserAsset `json:"allTokens"`
	RecentTxns []string    `json:"recentTxns"`
	TonNFTs    []string    `json:"nft"`
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
	alarmBot      string
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
		if c, ok := GetChainPlatInfo(o.chainName); ok {
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

func WithCollectBot() AlarmOption {
	return WithBot("collect")
}

func WithStationBot() AlarmOption {
	return WithBot("bfstation")
}

func WithBot(bot string) AlarmOption {
	return newfuncAlarmOption(func(e *alarmOptions) {
		e.alarmBot = bot
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

func IsTestNet(chainName string) bool {
	if strings.Contains(strings.ToLower(chainName), TEST_NET_TYPE) {
		return true
	}

	if platInfo, ok := GetChainPlatInfo(chainName); ok {
		return platInfo.NetType == TEST_NET_TYPE
	}

	return false
}

func UserAddressSwitchRetryAlert(chainName, address string) (bool, string, error) {
	if address == "" {
		return false, "", nil
	}

	enable, uid, err := UserAddressSwitchNew(address)
	/*if err != nil || !enable {
		enable, uid, err = UserAddressSwitch(address)
	}*/
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

var UserInfoMap = &sync.Map{}

type UserInfo struct {
	Uid     string `json:"uid"`
	UidType string `json:"uid_type"`
}

func GetUserInfo(address string) (*UserInfo, error) {
	if address == "" {
		return nil, nil
	}
	userInfoStr, err := data.RedisClient.Get(USERCENTER_ADDRESS_KEY + address).Result()
	for i := 0; i < 3 && err != nil && err != redis.Nil; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		userInfoStr, err = data.RedisClient.Get(USERCENTER_ADDRESS_KEY + address).Result()
	}
	if err != nil {
		return nil, err
	}
	if userInfoStr == "" {
		return nil, nil
	}

	var userInfo *UserInfo
	err = json.Unmarshal([]byte(userInfoStr), &userInfo)
	if err != nil {
		return nil, err
	}
	if userInfo != nil {
		UserInfoMap.Store(address, userInfo)
	}
	return userInfo, nil
}

func UserAddressSwitchNew(address string) (bool, string, error) {
	if address == "" {
		return false, "", nil
	}
	enable := true
	var uid string
	userInfo, err := GetUserInfo(address)
	if !AppConfig.ScanAll {
		if err != nil && err != redis.Nil {
			return false, "", err
		}
		if userInfo == nil {
			enable = false
		}
	}
	if userInfo != nil {
		uid = userInfo.Uid
	}

	return enable, uid, nil
}

func GetUidType(address string) (string, error) {
	if address == "" {
		return "", nil
	}
	var uidType string
	userInfo, err := GetUserInfo(address)
	if err != nil && err != redis.Nil {
		return "", err
	}
	if userInfo != nil {
		uidType = userInfo.UidType
	}
	return uidType, nil
}
func ChainTypeAdd(chainName string) map[string]string {
	chainType, _ := GetChainNameType(chainName)
	if chainType == "" && strings.Contains(chainName, "evm") {
		chainType = EVM
		SetChainNameType(chainName, EVM)
	}
	return GetChainNameTypeMap()
}
func GetUidTypeCode(address string) (int8, error) {
	if address == "" {
		return 0, nil
	}
	var uidTypeCode int8
	userInfo, err := GetUserInfo(address)
	if err != nil && err != redis.Nil {
		return 0, err
	}
	if userInfo != nil {
		uidType := userInfo.UidType
		if uidType == PERSON {
			uidTypeCode = 1
		} else if uidType == COMPANY {
			uidTypeCode = 2
		} else if uidType == COADMIN {
			uidTypeCode = 3
		}
	}
	return uidTypeCode, nil
}

func UidCodeToUidType(uidType int8) string {
	switch uidType {
	case 1:
		return PERSON
	case 2:
		return COMPANY
	case 3:
		return COADMIN
	default:
		return ""
	}
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

	if value == "" || value == "0" {
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
					if dr[0] > 0 {
						err = data.RedisClient.Set(bizRedisKey, val, 0).Err()
						log.InfoS(bizRedisKey+"推荐值", zap.Any("推荐", val), zap.Any(chainName, blockNumber))
					}
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
	tableName := data.GetTableName(chainName)
	if strings.Contains(tableName, "-") {
		return `"` + tableName + `"`
	}
	return data.GetTableName(chainName)
}

func GetDecimalsSymbolFromTokenInfo(chainName, tokenInfoJson string) (int32, string, error) {
	var tokenInfo *types.TokenInfo
	if jsonErr := json.Unmarshal([]byte(tokenInfoJson), &tokenInfo); jsonErr != nil {
		return 0, "", jsonErr
	}
	decimals := int32(tokenInfo.Decimals)
	symbol := tokenInfo.Symbol
	address := tokenInfo.Address
	if address == "" {
		if platInfo, ok := GetChainPlatInfo(chainName); ok {
			decimals = platInfo.Decimal
			symbol = platInfo.NativeCurrency
		} else {
			return 0, "", errors.New("chain " + chainName + " is not support")
		}
	}
	return decimals, symbol, nil
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
		if platInfo, ok := GetChainPlatInfo(chainName); ok {
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
		if platInfo, ok := GetChainPlatInfo(chainName); ok {
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

func ConvertGetTokenInfo(chainName, tokenInfoStr string) (*types.TokenInfo, error) {
	var tokenInfo *types.TokenInfo
	err := json.Unmarshal([]byte(tokenInfoStr), &tokenInfo)
	if err == nil && tokenInfo.Address == "" {
		if platInfo, ok := GetChainPlatInfo(chainName); ok {
			tokenInfo.Decimals = int64(platInfo.Decimal)
			tokenInfo.Symbol = platInfo.NativeCurrency
		} else {
			return nil, errors.New("chain " + chainName + " is not support")
		}
	}
	return tokenInfo, err
}

func NotifyBroadcastTxFailed(ctx *JsonRpcContext, req *BroadcastRequest) {
	var msg string
	alarmOpts := WithMsgLevel("FATAL")

	device := ctx.ParseDevice()
	deviceId := device.Id
	userAgent := device.UserAgent

	user := "Unknown"
	userRecord, _ := data.UserRecordRepoInst.FindOne(ctx, ctx.Uid)
	if userRecord != nil {
		user = userRecord.String()
	}
	address := req.Address
	if address == "" {
		address, _ = data.UserRecordRepoInst.FindAddress(ctx, ctx.Uid, ctx.ChainName)
	}

	if req.Stage == "" || req.Stage == "broadcast" {
		sessionID := req.SessionId
		errMsg := req.ErrMsg
		info, err := data.UserSendRawHistoryRepoInst.GetLatestOneBySessionId(ctx, sessionID)
		txInput := req.TxInput
		if err != nil {
			log.Error(
				"SEND ALARM OF BROADCATING TX FAILED",
				zap.String("sessionId", sessionID),
				zap.String("errMsg", errMsg),
				zap.Error(err),
			)
		} else {
			txInput = info.BaseTxInput
			if info.Address != "" {
				address = info.Address
			}
		}
		msg = fmt.Sprintf(
			"%s 链交易广播失败。\nsessionId：%s\n钱包地址：%s\nUser-Agent：%s\n错误消息：%s\ntxInput: %s\n用户: %s\nDevice-Id: %s",
			ctx.ChainName,
			sessionID,
			address,
			userAgent,
			errMsg,
			txInput,
			user,
			deviceId,
		)
		alarmOpts = WithAlarmChannel("txinput")
		//处理gas price too low
		if strings.Contains(errMsg, "gas price too low") {
			HandlerGasCoefficient(ctx.ChainName)
		}
	} else {
		//txHash:校验txHash,urlCheck，txParams：获取交易参数
		alarmOpts = WithCollectBot()
		if req.Stage == "urlCheck" {
			msg = "chainData校验URL失败。"
		} else if req.Stage == "txHash" {
			msg = fmt.Sprintf(
				"%s 链校验上链失败。\n节点：%s\n钱包地址：%s\nUser-Agent：%s\n错误消息：%s\ntxInput: %s\n用户: %s\nDevice-Id: %s",
				req.ChainName,
				req.NodeURL,
				req.Address,
				userAgent,
				req.ErrMsg,
				req.TxInput,
				user,
				deviceId,
			)
			alarmOpts = WithAlarmChannel("node-proxy")
		} else {
			msg = fmt.Sprintf(
				"%s 链获取交易参数失败。\n节点：%s\n钱包地址：%s\nUser-Agent：%s\n错误消息：%s\ntxInput: %s\n用户: %s\nDevice-Id: %s",
				req.ChainName,
				req.NodeURL,
				req.Address,
				userAgent,
				req.ErrMsg,
				req.TxInput,
				user,
				deviceId,
			)
			if req.Stage == "txParamsLark" {
				alarmOpts = WithAlarmChannel("node-proxy")
			}

		}
		//alarmOpts = WithCollectBot()
		//alarmOpts = WithAlarmChannel("node-proxy")
	}
	LarkClient.NotifyLark(msg, nil, nil, alarmOpts)
}

func HandlerGasCoefficient(chainName string) {
	//从redis获取该链的系数
	redisKey := GAS_COEFFICIENT_KEY + chainName
	gasCoefficient := GetGasCoefficient(chainName)
	gasCoefficient = gasCoefficient + gasCoefficient*0.5
	if gasCoefficient > 3 {
		gasCoefficient = 3
	}
	//存到redis
	data.RedisClient.Set(redisKey, gasCoefficient, 1*time.Hour)
}

func GetGasCoefficient(chainName string) float32 {
	redisKey := GAS_COEFFICIENT_KEY + chainName
	gasCoefficient, err := data.RedisClient.Get(redisKey).Float32()
	if gasCoefficient == 0 || err == redis.Nil {
		//redis中没有数据，从配置文件中获取
		if value, ok := AppConfig.ChainData.GasCoefficient[chainName]; ok {
			gasCoefficient = value
		} else {
			gasCoefficient = AppConfig.ChainData.GasCoefficient["default"]
		}
	}
	return gasCoefficient
}

func ExecuteRetry(chainName string, fc func(client chain.Clienter) (interface{}, error)) (interface{}, error) {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("ExecuteRetry error, chainName:"+chainName, e)
			} else {
				log.Errore("ExecuteRetry panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}
			return
		}
	}()
	var result interface{}
	var err error

	plat, ok := GetChainPlatform(chainName)
	if !ok {
		return nil, errors.New("no platform initialized")
	}
	spider := plat.GetBlockSpider()
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
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("ExecuteRetrys error, chainName:"+chainName, e)
			} else {
				log.Errore("ExecuteRetrys panic, chainName:"+chainName, errors.New(fmt.Sprintf("%s", err)))
			}
			return
		}
	}()
	var result interface{}
	var err error

	var nodeURL []string
	if platInfo, ok := GetChainPlatInfo(chainName); ok {
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
	if v, ok := GetChainPlatInfo(chainName); ok {
		rawMsg, err := signhash.Hash(v.Type, req)
		if err != nil {
			reqStr, _ := json.Marshal(req)
			msg := fmt.Sprintf(
				"%s 链生成签名哈希失败。\nRequest: %s\n错误消息：%s",
				chainName, reqStr, err.Error(),
			)
			alarmOpts := WithAlarmChannel("kanban")
			LarkClient.NotifyLark(msg, nil, nil, alarmOpts)
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
	if v, ok := GetChainPlatInfo(chainName); ok {
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

	Device    string
	Uid       string
	ChainName string
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

func GetLogAddressFromEventLogUid(eventLogs []*types.EventLogUid) datatypes.JSON {
	var logAddress datatypes.JSON
	logFromAddressMap := make(map[string]string)
	logAddressMap := make(map[string]string)
	var oneLogFromAddress []string
	var logFromAddress []string
	var logToAddress []string
	for _, log := range eventLogs {
		_, fromOk := logFromAddressMap[log.From]
		if !fromOk {
			logFromAddressMap[log.From] = ""
			oneLogFromAddress = append(oneLogFromAddress, log.From)
		}

		key := log.From + log.To
		_, ok := logAddressMap[key]
		if !ok {
			logAddressMap[key] = ""
			logFromAddress = append(logFromAddress, log.From)
			logToAddress = append(logToAddress, log.To)
		}
	}
	if len(oneLogFromAddress) == 1 {
		logFromAddress = oneLogFromAddress
	}
	logAddressList := [][]string{logFromAddress, logToAddress}
	logAddress, _ = json.Marshal(logAddressList)

	return logAddress
}

func HandleEventLogUid(chainName, recordFromAddress, recordToAddress, recordAmount string, eventLogs []*types.EventLogUid) []*types.EventLogUid {
	if recordAmount == "" || recordAmount == "0" {
		return eventLogs
	}

	if len(eventLogs) > 0 {
		var hasMain bool
		var mainTotal int
		//https://polygonscan.com/tx/0x8b455005112a9e744ec143ccfa81d4185fbc936162367eb33d8e9f6f704a6ec2
		mainAmount := new(big.Int)
		for _, eventLog := range eventLogs {
			if recordFromAddress == eventLog.From {
				if eventLog.Token.Address == "" {
					mainTotal++
					mainAmount = mainAmount.Add(mainAmount, eventLog.Amount)
					if recordToAddress == eventLog.To || recordAmount == eventLog.Amount.String() {
						hasMain = true
						break
					}
				} else {
					var mainSymbol string
					if platInfo, ok := GetChainPlatInfo(chainName); ok {
						mainSymbol = platInfo.NativeCurrency
					}
					if recordToAddress == eventLog.To && recordAmount == eventLog.Amount.String() && eventLog.Token.Symbol == mainSymbol {
						hasMain = true
						break
					}
				}
			}
		}
		if !hasMain && (mainTotal == 1 || recordAmount == mainAmount.String()) {
			hasMain = true
		}
		if !hasMain {
			amount, _ := new(big.Int).SetString(recordAmount, 0)
			eventLog := &types.EventLogUid{
				EventLog: types.EventLog{
					From:   recordFromAddress,
					To:     recordToAddress,
					Amount: amount,
				},
			}
			eventLogs = append(eventLogs, eventLog)
		}
	} else {
		amount, _ := new(big.Int).SetString(recordAmount, 0)
		eventLog := &types.EventLogUid{
			EventLog: types.EventLog{
				From:   recordFromAddress,
				To:     recordToAddress,
				Amount: amount,
			},
		}
		eventLogs = append(eventLogs, eventLog)
	}
	return eventLogs
}

var accBuckets sync.Map

type accItem struct {
	total   uint64
	success uint64
}

func (ai *accItem) Incr(success bool) {
	atomic.AddUint64(&ai.total, 1)
	if success {
		atomic.AddUint64(&ai.success, 1)
	}
}

func (ai *accItem) Total() uint64 {
	return atomic.LoadUint64(&ai.total)
}

func (ai *accItem) SuccessRate() int {
	success := atomic.LoadUint64(&ai.success)
	return int(success * 100 / ai.Total())
}

func AccumulateAlarmFactor(channel interface{}, success bool) (int, bool) {
	raw, _ := accBuckets.LoadOrStore(channel, new(accItem))
	item := raw.(*accItem)
	item.Incr(success)
	return item.SuccessRate(), item.Total() > 100
}

type MarketPrice struct {
	Price    float64
	Delta24H float64
}

func GetAssetsPrice(assets []*data.UserAsset) (map[string]MarketPrice, error) {
	if len(assets) == 0 {
		return make(map[string]MarketPrice), nil
	}

	coinIdSet := map[string]struct{}{}
	tokenSet := map[string]*v1.Tokens{}

	//去重
	for _, userAsset := range assets {
		platInfo, _ := GetChainPlatInfo(userAsset.ChainName)
		if platInfo == nil {
			continue
		}

		//coin 去重
		if userAsset.TokenAddress == "" {
			coinIdSet[platInfo.GetPriceKey] = struct{}{}
		} else {
			//token 去重
			key := fmt.Sprintf("%s_%s", userAsset.ChainName, strings.ToLower(userAsset.TokenAddress))
			tokenSet[key] = &v1.Tokens{
				Chain:   userAsset.ChainName,
				Address: userAsset.TokenAddress,
			}
		}
	}

	var coinIds []string
	for k, _ := range coinIdSet {
		coinIds = append(coinIds, k)
	}
	//默认添加 BTC 价格
	if !slices.Contains(coinIds, PriceKeyBTC) {
		coinIds = append(coinIds, PriceKeyBTC)
	}

	tokens := make([]*v1.Tokens, 0)
	for _, v := range tokenSet {
		tokens = append(tokens, v)
	}
	//默认添加 USDT 价格
	tokens = append(tokens, &v1.Tokens{
		Chain:   "ETH",
		Address: ETH_USDT_ADDRESS,
	})

	tokenPrices, err := GetPriceFromMarket(tokens, coinIds)
	if err != nil {
		return nil, err
	}

	tokenPriceMap := map[string]MarketPrice{}
	for _, coin := range tokenPrices.Coins {
		tokenPriceMap[coin.CoinID] = MarketPrice{
			Price:    coin.Price.Usd,
			Delta24H: coin.Delta24H,
		}
	}

	for _, token := range tokenPrices.Tokens {
		key := fmt.Sprintf("%s_%s", token.Chain, strings.ToLower(token.Address))
		tokenPriceMap[key] = MarketPrice{
			Price:    token.Price.Usd,
			Delta24H: token.Delta24H,
		}
	}

	return tokenPriceMap, err
}

func GetAssetPriceKey(chainName, tokenAddress string) string {
	platInfo, _ := GetChainPlatInfo(chainName)
	if platInfo == nil {
		return ""
	}

	//过滤测试网
	if platInfo.NetType != MAIN_NET_TYPE {
		return ""
	}

	var key string
	if tokenAddress == "" {
		key = platInfo.GetPriceKey
	} else {
		key = fmt.Sprintf("%s_%s", chainName, strings.ToLower(tokenAddress))
	}
	return key
}

// UpdateAssetCostPrice 查出数据库中原始 balance 和 costPrice，与即将入库的资产运算出新的 costPrice
// （最近一次转入前的成本*数量+最近一次转入时的价格*数量）/最近一次转入后的持仓量」
func UpdateAssetCostPrice(ctx context.Context, assets []*data.UserAsset) error {
	tokenPriceMap, err := GetAssetsPrice(assets)
	if err != nil {
		return err
	}

	for _, asset := range assets {

		platInfo, _ := GetChainPlatInfo(asset.ChainName)
		if platInfo == nil {
			continue
		}

		//过滤测试网
		if platInfo.NetType != MAIN_NET_TYPE {
			continue
		}

		var key string
		if asset.TokenAddress == "" {
			key = platInfo.GetPriceKey
		} else {
			key = fmt.Sprintf("%s_%s", asset.ChainName, strings.ToLower(asset.TokenAddress))
		}

		latestPrice := decimal.NewFromFloat(tokenPriceMap[key].Price)

		//如果数据库中没有该资产，或资产为 0，则成本价为最新价
		dbAsset, _ := data.UserAssetRepoClient.GetByChainNameAndAddress(ctx, asset.ChainName, asset.Address, asset.TokenAddress)
		if dbAsset == nil || dbAsset.Balance == "0" {
			asset.CostPrice = latestPrice.String()
			continue
		}

		dbBalance, err := decimal.NewFromString(dbAsset.Balance)
		if err != nil {
			asset.CostPrice = dbAsset.CostPrice
			continue
		}
		balance, err := decimal.NewFromString(asset.Balance)
		if err != nil {
			asset.CostPrice = dbAsset.CostPrice
			continue
		}

		//如果是转出或者金额不变，则成本价不变
		if balance.LessThanOrEqual(dbBalance) {
			asset.CostPrice = dbAsset.CostPrice
			continue
		}

		dbCostPrice, err := decimal.NewFromString(dbAsset.CostPrice)
		if err != nil {
			dbCostPrice = decimal.NewFromFloat(0)
		}

		//新成本价 = ( 旧余额*旧成本价 + (新余额-旧余额) * 最新价 ) / 新余额
		newCostPrice := dbBalance.Mul(dbCostPrice).Add(balance.Sub(dbBalance).Mul(latestPrice)).Div(balance)
		asset.CostPrice = newCostPrice.String()
	}

	return nil

}

// Pow10 return x * 10^y
func Pow10(x decimal.Decimal, y int) decimal.Decimal {
	if y == 0 {
		return x
	} else if y > 0 {
		for i := 0; i < y; i++ {
			x = x.Mul(decimal.NewFromInt(10))
		}
	} else {
		for i := 0; i < -y; i++ {
			x = x.Div(decimal.NewFromInt(10))
		}

	}
	return x
}

type CosmosBlockchain struct {
	Height string `json:"height"`
	Result struct {
		NotBondedTokens string `json:"not_bonded_tokens"`
		BondedTokens    string `json:"bonded_tokens"`
	} `json:"result"`
	CosmosBadResp
}

type CosmosBadResp struct {
	Code    int           `json:"code"`
	Message string        `json:"message"`
	Details []interface{} `json:"details"`
	Error   interface{}   `json:"error"`
}

type CosmosLatestBlock struct {
	Block struct {
		Header struct {
			Height string `json:"height"`
		} `json:"header"`
	} `json:"block"`
	CosmosBadResp
}

type CosmosClient struct {
	url       string
	legacy    int32
	chainName string
}

var errNotImplemented = errors.New("Not Implemented")

func (c *CosmosClient) GetBlockNumber() (int, error) {
	if c.isLegacy() {
		r, err := c.getLegacyBlockNumber()
		if err != errNotImplemented {
			return r, err
		}
		c.setNotLegacy()
	}

	return c.getBlockNumber()
}

func (c *CosmosClient) isLegacy() bool {
	return atomic.LoadInt32(&c.legacy) == 1
}

func (c *CosmosClient) getLegacyBlockNumber() (int, error) {
	u, err := c.buildURL("/staking/pool", nil)
	if err != nil {
		return 0, err
	}
	var chain CosmosBlockchain
	err = c.getResponse(u, &chain)
	if err != nil {
		return 0, err
	}
	if chain.Message != "" {
		if chain.Message == errNotImplemented.Error() {
			return 0, errNotImplemented
		}
		return 0, errors.New(chain.Message)
	}
	if chain.Error != nil {
		return 0, errors.New(utils.GetString(chain.Error))
	}
	height, err := strconv.Atoi(chain.Height)
	if err != nil {
		return 0, err
	}
	return height, err
}

func (c *CosmosClient) setNotLegacy() {
	atomic.CompareAndSwapInt32(&c.legacy, 1, 0)
}

func (c *CosmosClient) buildURL(u string, params map[string]string) (target *url.URL, err error) {
	target, err = url.Parse(c.url + u)
	if err != nil {
		return
	}
	values := target.Query()
	//Set parameters
	for k, v := range params {
		values.Set(k, v)
	}
	//add token to url, if present

	target.RawQuery = values.Encode()
	return
}

func (c *CosmosClient) getBlockNumber() (int, error) {
	u, err := c.buildURL("/cosmos/base/tendermint/v1beta1/blocks/latest", nil)
	if err != nil {
		return 0, err
	}

	var chain CosmosLatestBlock
	err = c.getResponse(u, &chain)
	if err != nil {
		return 0, err
	}
	if chain.Message != "" {
		return 0, errors.New(chain.Message)
	}
	if chain.Error != nil {
		return 0, errors.New(utils.GetString(chain.Error))
	}
	height, err := strconv.Atoi(chain.Block.Header.Height)
	if err != nil {
		return 0, err
	}
	return height, err
}

// getResponse is a boilerplate for HTTP GET responses.
func (c *CosmosClient) getResponse(target *url.URL, decTarget interface{}) (err error) {
	timeoutMS := 10_000 * time.Millisecond
	if c.chainName == "Osmosis" {
		err = httpclient.GetUseCloudscraper(target.String(), &decTarget, &timeoutMS)
	} else {
		_, err = httpclient.GetStatusCode(target.String(), nil, &decTarget, &timeoutMS, nil)
	}
	return
}

func IsDeFiTxType(txType string) bool {
	return txType == ADDLIQUIDITY || txType == CONTRACT || txType == SWAP || txType == MINT
}
