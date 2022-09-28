package subhandle

import (
	"block-crawling/internal/biz"
	coins "block-crawling/internal/common"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type Platform interface {
	Coin() coins.Coin
	GetTransactions()
	SetRedisHeight()
	GetTransactionResultByTxhash()
	MonitorHeight()
}

type Platforms map[string]map[string]Platform

type CommPlatform struct {
	Height    int
	Chain     string
	ChainName string
	Lock      sync.RWMutex

	HeightAlarmThr int
}

func (p *CommPlatform) SetRedisHeight() {
	data.RedisClient.Set(data.CHAINNAME+p.ChainName, p.Height, 0)
}

func (p *CommPlatform) MonitorHeight() {
	defer func() {
		if err := recover(); err != nil {
			if e, ok := err.(error); ok {
				log.Errore("MonitorHeight error, chainName:"+p.ChainName, e)
			} else {
				log.Errore("MonitorHeight panic, chainName:"+p.ChainName, errors.New(fmt.Sprintf("%s", err)))
			}

			// 程序出错 接入lark报警
			alarmMsg := fmt.Sprintf("请注意：%s链处理pending状态失败, error：%s", p.ChainName, fmt.Sprintf("%s", err))
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()
	//节点 块高
	nodeRedisHeight, _ := data.RedisClient.Get(biz.BLOCK_NODE_HEIGHT_KEY + p.ChainName).Result()
	redisHeight, _ := data.RedisClient.Get(biz.BLOCK_HEIGHT_KEY + p.ChainName).Result()

	oldHeight, _ := strconv.Atoi(redisHeight)
	height, _ := strconv.Atoi(nodeRedisHeight)

	thr := 30
	if p.HeightAlarmThr > 0 {
		thr = p.HeightAlarmThr
	}

	ret := height - oldHeight
	if ret > thr {
		if !strings.Contains(p.ChainName, "TEST") {
			alarmMsg := fmt.Sprintf("请注意：%s链块高相差大于%d,相差%d，链上块高：%d,业务块高：%d", p.ChainName, thr, ret, height, oldHeight)
			alarmOpts := biz.WithMsgLevel("FATAL")
			biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		}

	}

}

func (p *CommPlatform) GetHeight() int {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	return p.Height
}

func (p *CommPlatform) HandlerHeight(height int) int {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	oldHeight := p.Height
	p.Height = height
	data.RedisClient.Set(data.CHAINNAME+p.ChainName, height, 0)
	return oldHeight
}
