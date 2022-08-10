package subhandle

import (
	coins "block-crawling/internal/common"
	"block-crawling/internal/data"
	"sync"
)

type Platform interface {
	Coin() coins.Coin
	GetTransactions()
	SetRedisHeight()
	GetTransactionResultByTxhash()
}

type Platforms map[string]map[string]Platform

type CommPlatform struct {
	Height    int
	Chain     string
	ChainName string
	Lock      sync.RWMutex
}

func (p *CommPlatform) SetRedisHeight() {
	data.RedisClient.Set(data.CHAINNAME+p.ChainName, p.Height, 0)
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
