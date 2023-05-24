package main

import (
	"block-crawling/internal/conf"
	"block-crawling/internal/kanban"
	bizLog "block-crawling/internal/log"
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-kratos/kratos/v2"
	kLog "github.com/go-kratos/kratos/v2/log"

	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	"github.com/go-kratos/kratos/v2/middleware/tracing"
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

func newApp(
	logger kLog.Logger,
	ts *kanban.TimeMachine,
) *kratos.App {
	return kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Metadata(map[string]string{}),
		kratos.Logger(logger),
		kratos.Server(ts),
	)
}

func init() {
	flag.StringVar(&flagconf, "conf", "../../configs", "config path, eg: -conf config.yaml")
}

func main() {
	flag.Parse()

	logger := kLog.With(kLog.NewStdLogger(os.Stdout),
		"ts", kLog.DefaultTimestamp,
		"caller", kLog.DefaultCaller,
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

	ctx, cancel := context.WithCancel(context.Background())
	options := &kanban.Options{
		Context: ctx,
		Cancel:  cancel,
	}

	var bc conf.Bootstrap
	if err := c.Scan(&bc); err != nil {
		panic(err)
	}
	bizLog.BootstrapLogger(bc.Logger)
	app, cleanup, err := wireApp(
		bc.Server, bc.Data, bc.App, bc.AddressServer, bc.Lark, bc.Logger,
		bc.Transaction, &bc, logger, options,
	)
	if err != nil {
		panic(err)
	}
	defer cleanup()
	go func() {
		// start and wait for stop signal
		if err := app.Run(); err != nil {
			panic(err)
		}
	}()
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGALRM, syscall.SIGINT)
	select {
	case <-options.Context.Done():
	case <-sc:
		options.Cancel()
	}

	app.Stop()
}
