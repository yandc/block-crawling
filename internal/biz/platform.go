package biz

import (
	coins "block-crawling/internal/common"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
)

type Platform interface {
	Coin() coins.Coin
	MonitorHeight(onAvailablityChanged func(bool), liveInterval time.Duration)

	CreateStateStore() chain.StateStore
	CreateClient(url string) chain.Clienter
	CreateBlockHandler(liveInterval time.Duration) chain.BlockHandler
	GetBlockSpider() *chain.BlockSpider
	SetBlockSpider(spider *chain.BlockSpider)
}

type CommPlatform struct {
	Chain     string
	ChainName string
	Lock      sync.RWMutex

	HeightAlarmThr int
	heightAlarmSeq uint64

	lastNodeHeight      int
	nodeHeightUpdatedAt int64
}

func (p *CommPlatform) MonitorHeight(onAvailablityChanged func(bool), liveInterval time.Duration) {
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

	if IsTestNet(p.ChainName) || strings.Contains(p.ChainName, "evm") {
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

	if ShouldChainAlarm(p.ChainName) {
		p.monitorHeightChanging(height, liveInterval)
	}

	thr := 30

	if p.HeightAlarmThr > 0 {
		thr = p.HeightAlarmThr
	}

	ret := height - oldHeight
	if ret > thr {
		if ShouldChainAlarm(p.ChainName) {
			alarmMsg := fmt.Sprintf("请注意：%s链块高相差大于%d,相差%d，链上块高：%d,业务块高：%d", p.ChainName, thr, ret, height, oldHeight)
			alarmOpts := WithMsgLevel("FATAL")
			alarmOpts = WithAlarmChainName(p.ChainName)
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
		}
		if onAvailablityChanged != nil {
			onAvailablityChanged(false)
		}
	} else {
		if onAvailablityChanged != nil {
			onAvailablityChanged(true)
		}
	}
}

func (p *CommPlatform) monitorHeightChanging(height int, liveInterval time.Duration) {
	if p.lastNodeHeight != height {
		p.lastNodeHeight = height
		p.nodeHeightUpdatedAt = time.Now().Unix()
	}

	if p.nodeHeightUpdatedAt > 0 {
		updatedAt := time.Unix(p.nodeHeightUpdatedAt, 0)
		thr := liveInterval * 10
		minThr := time.Minute * 10
		if thr < minThr {
			thr = minThr
		}

		if time.Now().Sub(updatedAt) > thr {
			alarmMsg := fmt.Sprintf(
				"请注意：%s链的链上块高已经长时间不更新，上次更新时间：%s，缓存链上块高: %d。",
				p.ChainName, updatedAt.Format("2006-01-02 15:04:05"), height,
			)
			alarmOpts := WithMsgLevel("FATAL")
			LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts, WithAlarmChainName(p.ChainName))
		}
	}
}

func LiveInterval(p Platform) time.Duration {
	return time.Duration(p.Coin().LiveInterval) * time.Millisecond
}
