package common

import (
	"block-crawling/internal/log"
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
