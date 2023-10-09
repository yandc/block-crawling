package common

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/data"
	"block-crawling/internal/log"
	"block-crawling/internal/utils"
	"context"
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

	lastNotifiedTx       string
	onAvailablityChanged func(bool)
}

// NewDectorZapWatcher create watcher.
func NewDectorZapWatcher(chainName string, onAvailablityChanged func(bool)) *DetectorZapWatcher {
	return &DetectorZapWatcher{
		chainName:            chainName,
		onAvailablityChanged: onAvailablityChanged,
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
		if !biz.IsTestNet(d.chainName) {
			log.Error(
				"ALL NODES HAD BEEN FAILOVERED",
				zap.String("chainName", d.chainName),
				zap.String("current", current.URL()),
				zap.String("next", next.URL()),
				zap.Strings("nodeUrls", d.urls),
			)
		}
		if biz.ShouldChainAlarm(d.chainName) {
			alarmMsg := fmt.Sprintf("请注意：%s链目前没有可用rpcURL", d.chainName)
			alarmOpts := biz.WithMsgLevel("FATAL")
			alarmOpts = biz.WithAlarmChainName(d.chainName)
			biz.LarkClient.NotifyLark(alarmMsg, nil, d.urls, alarmOpts)
		}
		if d.onAvailablityChanged != nil {
			d.onAvailablityChanged(false)
		}
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

func (d *DetectorZapWatcher) OnSentTxFailed(txType chain.TxType, numOfContinuous int, txs []*chain.Transaction) {
	if numOfContinuous > 2 {
		lastTx := txs[len(txs)-1]

		if d.lastNotifiedTx != "" && lastTx.Hash == d.lastNotifiedTx {
			// already notified.
			return
		}

		d.lastNotifiedTx = lastTx.Hash
		txHashs := d.formatTxWithSentEmail(txs)
		alarmMsg := fmt.Sprintf(
			"请注意：%s 链发出的交易连续失败达到 %d 次，交易类型：%s，失败交易列表：\n%s",
			d.chainName, numOfContinuous,
			txType,
			strings.Join(txHashs, "\n"),
		)
		alarmOpts := biz.WithMsgLevel("FATAL")
		alarmOpts = biz.WithAlarmChannel("txinput")
		biz.LarkClient.NotifyLark(alarmMsg, nil, nil, alarmOpts)
	}
}

func (d *DetectorZapWatcher) formatTxWithSentEmail(txs []*chain.Transaction) []string {
	uniqUUIDs := make(map[string]bool)
	for _, tx := range txs {
		uniqUUIDs[tx.Result.FromUID] = true
	}
	uuids := make([]string, 0, len(uniqUUIDs))
	for uuid := range uniqUUIDs {
		uuids = append(uuids, uuid)
	}
	userMap, err := data.UserRecordRepoInst.LoadByUUIDs(context.Background(), uuids)
	if err != nil {
		return []string{err.Error()}
	}
	lines := make([]string, 0, len(txs))
	for _, tx := range txs {
		if user, ok := userMap[tx.Result.FromUID]; ok {
			lines = append(lines, fmt.Sprintf("%s(%s)", tx.Hash, user.Email))
		} else {
			lines = append(lines, fmt.Sprintf("%s(%s)", tx.Hash, "unknown"))
		}
	}
	return lines
}

// NodeRecoverIn common recover to embed into Node implementation.
type NodeDefaultIn struct {
	ChainName       string
	RoundRobinProxy bool

	proxyIndex int32
	retryAfter time.Time
}

func (p *NodeDefaultIn) ProxyTransport(transport *http.Transport) *http.Transport {
	if !p.RoundRobinProxy {
		return transport
	}

	length := int32(len(biz.HTTPProxies))
	// No proxy is configured.
	if length == 0 {
		return transport
	}

	monoIdx := atomic.AddInt32(&p.proxyIndex, 1)
	proxyIndex := (monoIdx - 1) % length

	// Reset monotonous index to 0 when it's exceed the length.
	if monoIdx >= length {
		atomic.StoreInt32(&p.proxyIndex, 0)
	}

	var tp *http.Transport
	if transport == nil {
		tp = &http.Transport{
			Proxy: http.ProxyURL(biz.HTTPProxies[proxyIndex]),
		}
	} else {
		tp = &http.Transport{
			Proxy:                  http.ProxyURL(biz.HTTPProxies[proxyIndex]),
			ProxyConnectHeader:     transport.ProxyConnectHeader,
			GetProxyConnectHeader:  transport.GetProxyConnectHeader,
			DialContext:            transport.DialContext,
			Dial:                   transport.Dial,
			DialTLSContext:         transport.DialTLSContext,
			DialTLS:                transport.DialTLS,
			TLSClientConfig:        transport.TLSClientConfig,
			TLSHandshakeTimeout:    transport.TLSHandshakeTimeout,
			DisableKeepAlives:      transport.DisableKeepAlives,
			DisableCompression:     transport.DisableCompression,
			MaxIdleConns:           transport.MaxIdleConns,
			MaxIdleConnsPerHost:    transport.MaxIdleConnsPerHost,
			MaxConnsPerHost:        transport.MaxConnsPerHost,
			IdleConnTimeout:        transport.IdleConnTimeout,
			ResponseHeaderTimeout:  transport.ResponseHeaderTimeout,
			ExpectContinueTimeout:  transport.ExpectContinueTimeout,
			TLSNextProto:           transport.TLSNextProto,
			MaxResponseHeaderBytes: transport.MaxResponseHeaderBytes,
			WriteBufferSize:        transport.WriteBufferSize,
			ReadBufferSize:         transport.ReadBufferSize,
			ForceAttemptHTTP2:      transport.ForceAttemptHTTP2,
		}
	}
	return tp
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
	alarmMsg := fmt.Sprintf("请注意：%s链服务内部异常, error：%s", p.ChainName, fmt.Sprintf("%s", r))
	alarmOpts := biz.WithMsgLevel("FATAL")
	alarmOpts = biz.WithAlarmChainName(p.ChainName)
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
