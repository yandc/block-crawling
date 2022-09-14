package main

import (
	"block-crawling/internal/conf"
	"block-crawling/internal/platform"
	"block-crawling/internal/platform/bitcoin"
	"block-crawling/internal/subhandle"
	"flag"
	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/tracing"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	"github.com/go-kratos/kratos/v2/transport/http"
	"go.uber.org/zap"
	"os"
	"time"
)

// go build -ldflags "-X main.Version=x.y.z"
var (
	// Name is the name of the compiled software.
	Name string
	// Version is the version of the compiled software.
	Version string
	// flagconf is the config flag.
	flagconf string

	id, _ = os.Hostname()
)

func init() {
	flag.StringVar(&flagconf, "conf", "../../configs", "config path, eg: -conf config.yaml")
}

func newApp(logger log.Logger, gs *grpc.Server, hs *http.Server) *kratos.App {
	return kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Metadata(map[string]string{}),
		kratos.Logger(logger),
		/*kratos.Server(
			gs,
			hs,
		),*/
	)
}

func main() {
	flag.Parse()
	logger := log.With(log.NewStdLogger(os.Stdout),
		"ts", log.DefaultTimestamp,
		"caller", log.DefaultCaller,
		"service.id", id,
		"service.name", Name,
		"service.version", Version,
		"trace.id", tracing.TraceID(),
		"span.id", tracing.SpanID(),
	)
	c := config.New(
		config.WithSource(
			file.NewSource(flagconf),
		),
	)
	defer c.Close()

	if err := c.Load(); err != nil {
		panic(err)
	}

	var bc conf.Bootstrap
	if err := c.Scan(&bc); err != nil {
		panic(err)
	}

	app, cleanup, err := wireApp(bc.Server, bc.Data, bc.App, bc.AddressServer, bc.Lark, bc.Logger,
		bc.Transaction, bc.InnerNodeList, bc.InnerPublicNodeList, bc.Platform, bc.PlatformTest, logger)
	if err != nil {
		panic(err)
	}
	defer cleanup()

	//platform.DappReset()
	// start task
	start()

	// start and wait for stop signal
	if err := app.Run(); err != nil {
		panic(err)
	}
}

func start() {
	defer func() {
		if err := recover(); err != nil {
			log.Error("main panic:", zap.Any("", err))
		}
	}()
	//var quitChan = make(chan os.Signal)
	quit := make(chan int)
	innerquit := make(chan int)
	//signal.Notify(quitChan, syscall.SIGTERM, os.Interrupt)

	go func() {
		platforms := platform.Platforms
		platformsLen := len(platforms)
		for i := 0; i < platformsLen; i++ {
			p := platforms[i]
			if p == nil {
				continue
			}
			go p.GetTransactions()
		}
	}()

	// get inner memerypool
	go func() {
		platforms := platform.Platforms
		platformsLen := len(platforms)
		for i := 0; i < platformsLen; i++ {
			p := platforms[i]
			if p == nil {
				continue
			}
			go func(p subhandle.Platform) {
				log.Info("start main", zap.Any("platform", p))
				// get result
				resultPlan := time.NewTicker(time.Duration(10) * time.Minute)
				for true {
					select {
					case <-resultPlan.C:
						//if _, ok := p.(*bitcoin.Platform); !ok {
						go p.GetTransactionResultByTxhash()
						//}
					case <-quit:
						resultPlan.Stop()
						return
					}
				}
			}(p)
		}
	}()

	go func() {
		platforms := platform.InnerPlatforms
		platformsLen := len(platforms)
		for i := 0; i < platformsLen; i++ {
			p := platforms[i]
			if p == nil {
				continue
			}
			go func(p subhandle.Platform) {
				if btc, ok := p.(*bitcoin.Platform); ok {
					//go p.GetTransactionResultByTxhash()
					go btc.GetPendingTransactionsByInnerNode()
				}
				liveInterval := p.Coin().LiveInterval
				log.Info("start inner main", zap.Any("platform", p))
				pendingTransactions := time.NewTicker(time.Duration(liveInterval) * time.Millisecond)
				for true {
					select {
					case <-pendingTransactions.C:
						if btc, ok := p.(*bitcoin.Platform); ok {
							//go p.GetTransactionResultByTxhash()
							go btc.GetPendingTransactionsByInnerNode()
						}
					case <-innerquit:
						pendingTransactions.Stop()
						return
					}
				}
			}(p)
		}
	}()

}
