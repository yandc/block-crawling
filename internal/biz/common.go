package biz

import (
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/types"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

const (
	DEFAULT_MSG_LEVEL      = "FATAL"
	DEFAULT_ALARM_LEVEL    = 1
	DEFAULT_ALARM_CYCLE    = true
	DEFAULT_ALARM_INTERVAL = 3600
	LARK_MSG_KEY           = "lark:msg:%s"

	PAGE_SIZE = 200

	REDIS_NIL_KEY             = "redis: nil"
	BLOCK_HEIGHT_KEY          = "block:height:"
	BLOCK_NODE_HEIGHT_KEY     = "block:height:node:"
	BLOCK_HASH_KEY            = "block:hash:"
	USER_ADDRESS_KEY          = "usercore:"
	BLOCK_HASH_EXPIRATION_KEY = 2 * time.Hour
	TABLE_POSTFIX             = "_transaction_record"
	ADDRESS_DONE_NONCE        = "done:"    // + chainanme:address value-->nonce
	ADDRESS_PENDING_NONCE     = "pending:" // + chainname:address:nonce --->过期时间六个小时
)

const (
	STC    = "STC"
	BTC    = "BTC"
	EVM    = "EVM"
	TVM    = "TVM"
	APTOS  = "APTOS"
	SUI    = "SUI"
	SOLANA = "SOL"
	NERVOS = "CKB"
)

const (
	WEB     = "web"
	ANDROID = "android"
	IOS     = "ios"
)

const (
	SUCCESS          = "success"
	FAIL             = "fail"
	PENDING          = "pending"          //-- 中间状态
	NO_STATUS        = "no_status"        //-- 中间状态
	DROPPED_REPLACED = "dropped_replaced" //--被丢弃或被置换 -- fail
	DROPPED          = "dropped"          //--被丢弃 -- fail
	CANCEL           = "cancel"           //中心化操作 --- value CANCEL --success
	SPEED_UP         = "speed_up"         //success
)

const (
	NATIVE                = "native"
	TRANSFER              = "transfer"
	TRANSFERFROM          = "transferFrom"
	SAFETRANSFERFROM      = "safeTransferFrom"
	SAFEBATCHTRANSFERFROM = "safeBatchTransferFrom"
	APPROVE               = "approve"
	CONTRACT              = "contract"
	EVENTLOG              = "eventLog"
	CREATEACCOUNT         = "createAccount"
	REGISTERTOKEN         = "registerToken"
)

// 币种
const (
	CNY = "CNY" //人民币
	USD = "USD" //美元
)

const TOKEN_INFO_QUEUE_TOPIC = "token:info:queue:topic"
const TOKEN_INFO_QUEUE_PARTITION = "partition1"

//token类型
const (
	ERC20   = "ERC20"
	ERC721  = "ERC721"
	ERC1155 = "ERC1155"
)

var rocketMsgLevels = map[string]int{
	"DEBUG":   0,
	"NOTICE":  1,
	"WARNING": 2,
	"FATAL":   3,
}

// alarmOptions 报警相关的一些可自定义参数
type alarmOptions struct {
	level         string
	alarmLevel    int
	alarmCycle    bool
	alarmInterval int
}

var DefaultAlarmOptions = alarmOptions{
	level:         DEFAULT_MSG_LEVEL,
	alarmLevel:    DEFAULT_ALARM_LEVEL,
	alarmCycle:    DEFAULT_ALARM_CYCLE,
	alarmInterval: DEFAULT_ALARM_INTERVAL,
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

func UserAddressSwitch(address string) (bool, string, error) {
	if address == "" {
		return false, "", nil
	}
	enable := true
	uid, err := data.RedisClient.Get(USER_ADDRESS_KEY + address).Result()
	for i := 0; i < 3 && err != nil && fmt.Sprintf("%s", err) != REDIS_NIL_KEY; i++ {
		time.Sleep(time.Duration(i*1) * time.Second)
		uid, err = data.RedisClient.Get(USER_ADDRESS_KEY + address).Result()
	}
	if !AppConfig.ScanAll {
		if err != nil && fmt.Sprintf("%s", err) != REDIS_NIL_KEY {
			return false, "", err
		}
		if uid == "" {
			enable = false
		}
	}
	return enable, uid, nil
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

func GetTalbeName(chainName string) string {
	return strings.ToLower(chainName) + TABLE_POSTFIX
}

func GetDecimalsSymbol(chainName, parseData string) (int32, string, error) {
	paseDataJson := make(map[string]interface{})
	err := json.Unmarshal([]byte(parseData), &paseDataJson)
	if err != nil {
		return 0, "", err
	}
	tokenInfoMap := paseDataJson["token"]
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
		if decimalsInt32, ok := tokenInfo["decimals"].(int32); ok {
			decimals = decimalsInt32
		} else if decimalsFloat, ok := tokenInfo["decimals"].(float64); ok {
			decimals = int32(decimalsFloat)
		} else {
			decimalsInt, err := strconv.Atoi(fmt.Sprintf("%v", tokenInfo["decimals"]))
			if err != nil {
				return 0, "", nil
			}
			decimals = int32(decimalsInt)
		}
	}
	var symbol string
	if tokenInfo["symbol"] != nil {
		symbol = tokenInfo["symbol"].(string)
	}
	if address == "" {
		if platInfo, ok := PlatInfoMap[chainName]; ok {
			decimals = platInfo.Decimal
			symbol = platInfo.NativeCurrency
		}
	}
	return decimals, symbol, nil
}

func PaseGetTokenInfo(chainName, parseData string) (*types.TokenInfo, error) {
	tokenInfo, err := PaseTokenInfo(parseData)
	if err == nil && tokenInfo.Address == "" {
		if platInfo, ok := PlatInfoMap[chainName]; ok {
			tokenInfo.Decimals = int64(platInfo.Decimal)
			tokenInfo.Symbol = platInfo.NativeCurrency
		}
	}
	return tokenInfo, err
}

func PaseTokenInfo(parseData string) (*types.TokenInfo, error) {
	paseDataJson := make(map[string]interface{})
	err := json.Unmarshal([]byte(parseData), &paseDataJson)
	if err != nil {
		return nil, err
	}
	tokenInfoMap := paseDataJson["token"]
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
		return nil, err
	}
	return tokenInfo, nil
}
