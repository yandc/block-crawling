package biz

import (
	"block-crawling/internal/common"
	"block-crawling/internal/conf"
	internalData "block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Lark .
type Lark struct {
	conf *conf.Lark
	lock common.Syncronized
}

var LarkClient *Lark

// NewLark new a lark.
func NewLark(c *conf.Lark) *Lark {
	LarkClient = &Lark{
		conf: c,
		lock: common.NewSyncronized(c.LockNum),
	}

	return LarkClient
}

type LarkResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

type Content struct {
	Tag      string `json:"tag"`
	UserID   string `json:"user_id,omitempty"`
	UserName string `json:"user_name,omitempty"`
	Text     string `json:"text,omitempty"`
}

func (lark *Lark) MonitorLark(msg string, opts ...AlarmOption) {
	// 默认报警参数
	alarmOpts := DefaultAlarmOptions
	// 根据用户自定义信息更新报警参数
	for _, opt := range opts {
		opt.apply(&alarmOpts)
	}
	// 小于默认报警 Level，不发 lark 信息
	msgLevel, ok := rocketMsgLevels[alarmOpts.level]
	if !ok {
		msgLevel = DEFAULT_ALARM_LEVEL
	}
	if msgLevel < alarmOpts.alarmLevel {
		return
	}

	key := fmt.Sprintf(LARK_MSG_KEY, MD5(msg))
	msgTimestamp, _ := GetAlarmTimestamp(key)

	// 如果有缓存，说明之前发过相同的内容
	if int64(msgTimestamp) > 0 {
		// 如果设置 alarmCycle = False，不再重新发送
		if !alarmOpts.alarmCycle {
			return
		}
		// 如果 alarmCycle = true，并且上次发送时间距离现在小于 alarmInterval，不再重新发送
		if time.Now().Unix()-int64(msgTimestamp) < int64(alarmOpts.alarmInterval) {
			return
		}
	}

	c := make([]Content, 0, 4)

	if lark.conf.GetLarkAtList() != "" {
		atList := strings.Split(lark.conf.GetLarkAtList(), ",")
		for _, v := range atList {
			c = append(c, Content{Tag: "at", UserID: v, UserName: v})
		}
	} else {
		c = append(c, Content{Tag: "at", UserID: "all", UserName: "所有人"})
	}
	c = append(c, Content{Tag: "text", Text: msg + "\n"})
	c = append(c, Content{Tag: "text", Text: "开始时间:\n"}, Content{Tag: "text", Text: BjNow()})
	t := time.Now().Unix()
	sign, _ := GenSign(lark.conf.LarkSecret, t)
	content := make([][]Content, 1)
	content[0] = c
	b, _ := json.Marshal(content)
	data := `{
    "msg_type": "post",
	"timestamp":"` + fmt.Sprintf("%v", t) + `",
	"sign": "` + sign + `",
    "content": {
        "post": {
            "zh_cn": {
                "title": block-crawling块高监测",
                "content":
                    ` + string(b) + `
            	}
        	}
    	}
	}`

	req, err := http.NewRequest(http.MethodPost, lark.conf.LarkHost, strings.NewReader(data))
	if err != nil {
		log.Error("lark http.NewRequest error:", zap.Error(err))
		return
	}

	req.Header.Set("Content-Type", "application/json")
	var client = http.DefaultClient
	response, err := client.Do(req)
	if err != nil {
		log.Error("lark request error: ", zap.Error(err))
		return
	}

	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)
	var resp LarkResponse
	json.Unmarshal(body, &resp)
	if resp.Code > 0 {
		log.Error("send lark error:", zap.Error(errors.New(resp.Msg)))
	}
	// 更新缓存时间戳，失效时间1小时
	if response.StatusCode == 200 {
		internalData.RedisClient.Set(key, time.Now().Unix(), 60*60*time.Second)
	}

}

func (lark *Lark) NotifyLark(msg string, usableRPC, disabledRPC []string, opts ...AlarmOption) {
	lark.lock.Lock(msg)
	defer func() {
		lark.lock.Unlock(msg)
		if err := recover(); err != nil {
			log.Error("NotifyLark panic:", zap.Any("", err))
		}
	}()
	// 默认报警参数
	alarmOpts := DefaultAlarmOptions
	// 根据用户自定义信息更新报警参数
	for _, opt := range opts {
		opt.apply(&alarmOpts)
	}
	// 小于默认报警 Level，不发 lark 信息
	msgLevel, ok := rocketMsgLevels[alarmOpts.level]
	if !ok {
		msgLevel = DEFAULT_ALARM_LEVEL
	}
	if msgLevel < alarmOpts.alarmLevel {
		return
	}

	key := fmt.Sprintf(LARK_MSG_KEY, MD5(msg))
	msgTimestamp, _ := GetAlarmTimestamp(key)

	// 如果有缓存，说明之前发过相同的内容
	if int64(msgTimestamp) > 0 {
		// 如果设置 alarmCycle = False，不再重新发送
		if !alarmOpts.alarmCycle {
			return
		}
		// 如果 alarmCycle = true，并且上次发送时间距离现在小于 alarmInterval，不再重新发送
		if time.Now().Unix()-int64(msgTimestamp) < int64(alarmOpts.alarmInterval) {
			return
		}
	}

	c := make([]Content, 0, 6)

	//atStrList := ""
	//if lark.conf.GetLarkAtList() != "" {
	//	atList := strings.Split(lark.conf.GetLarkAtList(), ",")
	//	tmpList := make([]string, len(atList))
	//
	//	for i, v := range atList {
	//		s := `<at user_id = "%s">%s</at>`
	//		ss := fmt.Sprintf(s, v, v)
	//		tmpList[i] = ss
	//		c = append(c, Content{Tag: "at", UserID: v, UserName: v})
	//
	//	}
	//
	//	//atStrList = strings.Join(tmpList, " ")
	//}
	if lark.conf.GetLarkAtList() != "" {
		atList := strings.Split(lark.conf.GetLarkAtList(), ",")
		for _, v := range atList {
			c = append(c, Content{Tag: "at", UserID: v, UserName: v})
		}
	} else {
		c = append(c, Content{Tag: "at", UserID: "all", UserName: "所有人"})
	}
	//c = append(c, Content{Tag: "at", UserID: "all", UserName: "所有人"})
	c = append(c, Content{Tag: "text", Text: msg + "\n"})

	if len(usableRPC) > 0 {
		c = append(c, Content{Tag: "text", Text: "可用rpc：\n"})
		var rpcs string
		for i := 0; i < len(usableRPC); i++ {
			rpcs += "[" + usableRPC[i] + "]\n"
		}
		c = append(c, Content{Tag: "text", Text: rpcs})
	}

	if len(disabledRPC) > 0 {
		c = append(c, Content{Tag: "text", Text: "不可用rpc：\n"})
		var rpcs string
		rpcStat := make(map[string]int)
		oneNodeStillAvail := false
		for i := 0; i < len(disabledRPC); i++ {
			failRate := utils.GetRPCFailureRate(disabledRPC[i])
			oneNodeStillAvail = oneNodeStillAvail || failRate < 20
			rpcs += "[" + disabledRPC[i] + "]半小时内失败率:" + fmt.Sprintf("%v", failRate) + "%\n"
			rpcStat[disabledRPC[i]] = failRate
		}
		// At least one node is available(fail rate is below 20%).
		// Skip this alarm.
		if oneNodeStillAvail {
			return
		}
		log.Info("NO AVAILABLE RPC NODE", zap.String("msg", msg), zap.Any("nodeFailRates", rpcStat))
		c = append(c, Content{Tag: "text", Text: rpcs})
		// Stop alarming about no available rpc node. Updated @ 2023-01-13
		return
	}
	c = append(c, Content{Tag: "text", Text: "开始时间:\n"}, Content{Tag: "text", Text: BjNow()})
	t := time.Now().Unix()
	sign, _ := GenSign(lark.conf.LarkSecret, t)
	content := make([][]Content, 1)
	content[0] = c
	b, _ := json.Marshal(content)
	data := `{
    "msg_type": "post",
	"timestamp":"` + fmt.Sprintf("%v", t) + `",
	"sign": "` + sign + `",
    "content": {
        "post": {
            "zh_cn": {
                "title": "` + lark.conf.LarkAlarmTitle + `",
                "content":
                    ` + string(b) + `
            	}
        	}
    	}
	}`

	req, err := http.NewRequest(http.MethodPost, lark.conf.LarkHost, strings.NewReader(data))
	if err != nil {
		log.Error("lark http.NewRequest error:", zap.Error(err))
		return
	}

	req.Header.Set("Content-Type", "application/json")
	var client = http.DefaultClient
	response, err := client.Do(req)
	if err != nil {
		log.Error("lark request error: ", zap.Error(err))
		return
	}

	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)
	var resp LarkResponse
	json.Unmarshal(body, &resp)
	if resp.Code > 0 {
		log.Error("send lark error:", zap.Error(errors.New(resp.Msg)))
	}
	// 更新缓存时间戳，失效时间1小时
	if response.StatusCode == 200 {
		internalData.RedisClient.Set(key, time.Now().Unix(), 60*60*time.Second)
	}
}

func GenSign(secret string, timestamp int64) (string, error) {
	//timestamp + key 做sha256, 再进行base64 encode
	stringToSign := fmt.Sprintf("%v", timestamp) + "\n" + secret
	var data []byte
	h := hmac.New(sha256.New, []byte(stringToSign))
	_, err := h.Write(data)
	if err != nil {
		return "", err
	}
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))
	return signature, nil
}
