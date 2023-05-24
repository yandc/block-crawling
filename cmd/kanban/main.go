package main

import (
	"block-crawling/internal/conf"
	"block-crawling/internal/kanban"
	bizLog "block-crawling/internal/log"
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	kLog "github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/transport"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	"github.com/go-kratos/kratos/v2/middleware/tracing"
	"github.com/urfave/cli/v2"
)

// go build -ldflags "-X main.Version=x.y.z"
var (
	// Name is the name of the compiled software.
	Name string
	// Version is the version of the compiled software.
	Version string

	id, _ = os.Hostname()
)

type Subcommand int

const (
	SubcommandTimeMachine Subcommand = iota
	SubcommandCron
	SubcommandSync
)

func main() {
	yesterday := time.Now().Unix() - (24 * 3600)
	yesterdayStart := yesterday - yesterday%(24*3600)
	ctx, cancel := context.WithCancel(context.Background())
	options := &kanban.Options{
		Context: ctx,
		Cancel:  cancel,
	}

	app := &cli.App{
		Name: Name,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "conf",
				Aliases:  []string{"c"},
				Usage:    "Configuration directory",
				Value:    "../../confs",
				Required: true,
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "timemachine",
				Aliases: []string{"t", "tm"},
				Usage:   "Run time machine to fill historical data.",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "chain",
						Usage: "Chain name to run time machine.",
						Value: "ScrollL2TEST",
					},
				},
				Action: func(c *cli.Context) error {
					confPath := c.String("conf")
					options.ChainName = c.String("chain")
					run(confPath, SubcommandTimeMachine, options)
					return nil
				},
			},
			{
				Name:    "cron",
				Aliases: []string{"c"},
				Flags: []cli.Flag{
					&cli.Int64Flag{
						Name:  "time",
						Usage: "Which day do we aggerate at, the default is yesterday",
						Value: yesterdayStart,
					},
					&cli.Int64Flag{
						Name:  "start",
						Usage: "Which day do we start to aggerate",
						Value: 0,
					},
					&cli.Int64Flag{
						Name:  "stop",
						Usage: "Which day do we stop to aggerate",
						Value: yesterday,
					},
					&cli.StringFlag{
						Name:  "only",
						Usage: "Only run a operaton: day, accumulating, trending.",
					},
				},
				Usage: "The job runs everyday: create table and aggerate.",
				Action: func(c *cli.Context) error {
					confPath := c.String("conf")
					options.AggerateTime = c.Int64("time")
					options.AggerateOnly = c.String("only")
					if start := c.Int64("start"); start > 0 {
						options.AggerateRange = append(options.AggerateRange, start)
						options.AggerateRange = append(options.AggerateRange, c.Int64("stop"))
					}
					run(confPath, SubcommandCron, options)
					return nil
				},
			},
			{
				Name:    "sync",
				Aliases: []string{"s"},
				Usage:   "Synchronous user's records to kanban",
				Action: func(c *cli.Context) error {
					confPath := c.String("conf")
					run(confPath, SubcommandSync, options)
					return nil
				},
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(confPath string, sub Subcommand, options *kanban.Options) {
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
			file.NewSource(confPath),
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
	bizLog.BootstrapLogger(bc.Logger)
	app, cleanup, err := wireApp(
		bc.Server, bc.Data, bc.App, bc.AddressServer, bc.Lark, bc.Logger,
		bc.Transaction, &bc, logger, sub, options,
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

func newApp(
	logger kLog.Logger,
	ms *kanban.MigrateScheduler,
	as *kanban.Aggerator,
	ts *kanban.TimeMachine,
	ss *kanban.RecordSync,
	sub Subcommand,
) *kratos.App {
	srvs := make([]transport.Server, 0, 2)
	switch sub {
	case SubcommandTimeMachine:
		srvs = append(srvs, ts)
	case SubcommandCron:
		srvs = append(srvs, as, ms)
	case SubcommandSync:
		srvs = append(srvs, ss)
	}

	return kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Metadata(map[string]string{}),
		kratos.Logger(logger),
		kratos.Server(srvs...),
	)
}
