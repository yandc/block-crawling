package ptesting

import (
	"block-crawling/internal/biz"
	"block-crawling/internal/log"

	"github.com/google/wire"
	"go.uber.org/zap"
)

var LarkProviderSet = wire.NewSet(NewDummyLark, wire.Bind(new(biz.Larker), new(*dummyLark)))

type dummyLark struct {
}

// MonitorLark implements biz.Larker
func (*dummyLark) MonitorLark(msg string, opts ...biz.AlarmOption) {
	log.Info("LARK ALARM", zap.String("msg", msg))
}

// NotifyLark implements biz.Larker
func (*dummyLark) NotifyLark(msg string, usableRPC []string, disabledRPC []string, opts ...biz.AlarmOption) {
	log.Info("LARK ALARM", zap.String("msg", msg))
}

// NewDummyLark new a lark.
func NewDummyLark() *dummyLark {
	l := &dummyLark{}
	biz.LarkClient = l
	return l
}
