package biz

import (
	coins "block-crawling/internal/common"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"errors"
	"fmt"
	"gitlab.bixin.com/mili/node-driver/chain"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Platform interface {
	Coin() coins.Coin
	SetRedisHeight()
	MonitorHeight()

	CreateStateStore() chain.StateStore
	CreateClient(url string) chain.Clienter
	CreateBlockHandler(liveInterval time.Duration) chain.BlockHandler
	GetBlockSpider() *chain.BlockSpider
	SetBlockSpider(spider *chain.BlockSpider)
}

type CommPlatform struct {
	Height    int
	Chain     string
	ChainName string
	Lock      sync.RWMutex

	HeightAlarmThr int

	heightAlarmSeq uint64
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
			alarmOpts := WithMsgLevel("FATAL")
			alarmOpts = WithAlarmChainName(p.ChainName)
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
			return
		}
	}()
	//节点 块高
	nodeRedisHeight, _ := data.RedisClient.Get(BLOCK_NODE_HEIGHT_KEY + p.ChainName).Result()
	redisHeight, _ := data.RedisClient.Get(BLOCK_HEIGHT_KEY + p.ChainName).Result()

	oldHeight, _ := strconv.Atoi(redisHeight)
	height, _ := strconv.Atoi(nodeRedisHeight)

	if strings.Contains(p.ChainName, "TEST") {
		if p.HeightAlarmThr <= 0 {
			// Ignore for TEST chain when its threshold set to 0.
			return
		}

		// 测试环境每 1 小时监控一次，生产环境每 6 小时监控一次。
		seq := atomic.AddUint64(&p.heightAlarmSeq, 1)
		if seq < 60 {
			return
		}
		atomic.StoreUint64(&p.heightAlarmSeq, 0)
	}

	thr := 30

	if p.HeightAlarmThr > 0 {
		thr = p.HeightAlarmThr
	}

	ret := height - oldHeight
	if ret > thr {
		alarmMsg := fmt.Sprintf("请注意：%s链块高相差大于%d,相差%d，链上块高：%d,业务块高：%d", p.ChainName, thr, ret, height, oldHeight)
		alarmOpts := WithMsgLevel("FATAL")
		alarmOpts = WithAlarmChainName(p.ChainName)
		LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
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
