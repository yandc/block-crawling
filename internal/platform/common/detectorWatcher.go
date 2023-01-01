package common

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.bixin.com/mili/node-driver/chain"
	"gitlab.bixin.com/mili/node-driver/detector"
	"go.uber.org/zap"
)

// DetectorZapWatcher watch detector and print via zap.
type DetectorZapWatcher struct {
	chainName string
	urls      []string
	rwLock    sync.RWMutex

	numOfContinualFailed int32

	lastNotifiedTx string
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
	utils.UpdateRecordRPCURL(current.URL(), false)

	// 累加失败计数
	numOfContinualFailed := atomic.AddInt32(&d.numOfContinualFailed, 1)

	d.rwLock.RLock()
	defer d.rwLock.RUnlock()
	if int(numOfContinualFailed) >= len(d.urls) {
		if !strings.HasSuffix(d.chainName, "TEST") {
			log.Error(
				"ALL NODES HAD BEEN FAILOVERED",
				zap.String("chainName", d.chainName),
				zap.String("current", current.URL()),
				zap.String("next", next.URL()),
				zap.Strings("nodeUrls", d.urls),
			)
		}
		alarmMsg := fmt.Sprintf("请注意：%s链目前没有可用rpcURL", d.chainName)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, d.urls, alarmOpts)
	}
}

// OnNodeSuccess watches node has been used successfully.
func (d *DetectorZapWatcher) OnNodeSuccess(node detector.Node) {
	// 一旦有一个成功则重置计数器
	atomic.StoreInt32(&d.numOfContinualFailed, 0)
	utils.UpdateRecordRPCURL(node.URL(), true)
}

func (d *DetectorZapWatcher) URLs() []string {
	d.rwLock.RLock()
	defer d.rwLock.RUnlock()
	urls := make([]string, 0, len(d.urls))
	for _, url := range d.urls {
		urls = append(urls, url)
	}
	return urls
}

func (d *DetectorZapWatcher) OnSentTxFailed(txType chain.TxType, numOfContinuous int, txHashs []string) {
	if numOfContinuous > 2 {
		lastTxHash := txHashs[len(txHashs)-1]

		if d.lastNotifiedTx != "" && lastTxHash == d.lastNotifiedTx {
			// already notified.
			return
		}
		d.lastNotifiedTx = lastTxHash

		alarmMsg := fmt.Sprintf(
			"请注意：%s 链发出的交易连续失败达到 %d 次，交易类型：%s，失败交易列表：\n%s",
			d.chainName, numOfContinuous,
			txType,
			strings.Join(txHashs, "\n"),
		)
		alarmOpts := biz.WithMsgLevel("FATAL")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	}
}

// NodeRecoverIn common recover to embed into Node implementation.
type NodeDefaultIn struct {
	ChainName  string
	retryAfter time.Time
}

// Recover handle panic.
func (p *NodeDefaultIn) Recover(r interface{}) (err error) {
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

func (p *NodeDefaultIn) RetryAfter() time.Time {
	return p.retryAfter
}

func (p *NodeDefaultIn) SetRetryAfter(after time.Duration) {
	p.retryAfter = time.Now().Add(after)
}

func (p *NodeDefaultIn) ParseRetryAfter(header http.Header) {
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After
	retryAfter := header.Get("retry-after")
	if retryAfter == "" {
		return
	}
	if seconds, err := p.parseSeconds(retryAfter); err == nil {
		log.Debug("PARSED RETRY AFTER FROM HEADER", zap.String("chain", p.ChainName), zap.Duration("retryAfter", seconds))
		p.SetRetryAfter(seconds)
		return
	}
	if parsed, err := p.parseHTTPDate(retryAfter); err == nil {
		log.Debug("PARSED RETRY AFTER FROM HEADER", zap.String("chain", p.ChainName), zap.Time("retryAfter", parsed))
		p.retryAfter = parsed
	}
}

// parseSeconds parses the value as seconds.
func (p *NodeDefaultIn) parseSeconds(retryAfter string) (time.Duration, error) {
	seconds, err := strconv.ParseInt(retryAfter, 10, 64)
	if err != nil {
		return time.Duration(0), err
	}
	if seconds < 0 {
		return time.Duration(0), errors.New("negative seconds")
	}
	return time.Second * time.Duration(seconds), nil
}

// ParseHTTPDate parses the value as HTTP date.
func (p *NodeDefaultIn) parseHTTPDate(retryAfter string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC1123, retryAfter)
	if err != nil {
		return time.Time{}, err
	}
	return parsed, nil
}
