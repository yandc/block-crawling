package biz

import (
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"
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
)

const (
	WEB     = "web"
	ANDROID = "android"
	IOS     = "ios"
)

const (
	SUCCESS          = "success"
	FAIL             = "fail"
	PENDING          = "pending"
	NO_STATUS        = "no_status"
	DROPPED_REPLACED = "dropped_replaced"
)

const (
	NATIVE        = "native"
	TRANSFER      = "transfer"
	TRANSFERFROM  = "transferfrom"
	APPROVE       = "approve"
	CONTRACT      = "contract"
	EVENTLOG      = "eventLog"
	CREATEACCOUNT = "createAccount"
	REGISTERTOKEN = "registerToken"
)

//币种
const (
	CNY = "CNY" //人民币
	USD = "USD" //美元
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
	tokenInfo := tokenInfoMap.(map[string]interface{})
	address := tokenInfo["address"].(string)
	decimals := int32(tokenInfo["decimals"].(float64))
	symbol := tokenInfo["symbol"].(string)
	if address == "" {
		if platInfo, ok := PlatInfoMap[chainName]; ok {
			decimals = platInfo.Decimal
			symbol = platInfo.Symbol
		}
	}
	return decimals, symbol, nil
}
