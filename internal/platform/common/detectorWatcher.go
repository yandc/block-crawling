package common

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/log"
	"errors"
	"fmt"
	"sync"

	"gitlab.bixin.com/mili/node-driver/detector"
	"go.uber.org/zap"
)

// DetectorZapWatcher watch detector and print via zap.
type DetectorZapWatcher struct {
	chainName string
	urls      []string
	rwLock    sync.RWMutex
}

// NewDectorZapWatcher create watcher.
func NewDectorZapWatcher(chainName string) *DetectorZapWatcher {
	return &DetectorZapWatcher{
		chainName: chainName,
	}
}

// OnNodesChange watch nodes changed.
func (d *DetectorZapWatcher) OnNodesChange(nodes []detector.Node) {
	urls := make([]string, 0, len(nodes))
	for _, n := range nodes {
		urls = append(urls, n.URL())
	}
	log.Debug(
		"NODES DETECTED",
		zap.String("chainName", d.chainName),
		zap.Strings("nodeUrls", urls),
	)
	d.rwLock.Lock()
	defer d.rwLock.Unlock()
	d.urls = urls
}

// OnNodeFailover watches node has been failovered.
func (d *DetectorZapWatcher) OnNodeFailover(current detector.Node, next detector.Node) {
	d.rwLock.RLock()
	defer d.rwLock.RUnlock()
	log.Debug(
		"NODE HAD BEEN FAILOVERED",
		zap.String("chainName", d.chainName),
		zap.String("current", current.URL()),
		zap.String("next", next.URL()),
		zap.Strings("nodeUrls", d.urls),
	)
}

// NodeRecoverIn common recover to embed into Node implementation.
type NodeRecoverIn struct {
	ChainName string
}

// Recover handle panic.
func (p *NodeRecoverIn) Recover(r interface{}) (err error) {
	if e, ok := r.(error); ok {
		log.Errore("IndexBlock error, chainName:"+p.ChainName, e)
		err = e
	} else {
		err = errors.New(fmt.Sprintf("%s", err))
		log.Errore("IndexBlock panic, chainName:"+p.ChainName, err)
	}

	// 程序出错 接入lark报警
	alarmMsg := fmt.Sprintf("请注意：%s链爬块失败, error：%s", p.ChainName, fmt.Sprintf("%s", r))
	alarmOpts := biz.WithMsgLevel("FATAL")
	biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	return
}
