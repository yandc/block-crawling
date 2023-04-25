package ptesting

import (
	"block-crawling/internal/conf"
	bizLog "block-crawling/internal/log"
	"fmt"
	"net"
	"sync"

	"os"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/tracing"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	googleRPC "google.golang.org/grpc"
)

func newApp(logger log.Logger, runner *Runner, gs *grpc.Server) *kratos.App {
	return kratos.New(
		kratos.ID("autotest"),
		kratos.Name("autotest"),
		kratos.Version("0.1.0"),
		kratos.Metadata(map[string]string{}),
		kratos.Logger(logger),
		kratos.Server(
			gs,
			runner,
		),
	)
}

var grpcPort int = 8999

func RunTest(preparation Preparation) {
	bizLog.EnableSignalLog()
	logger := log.With(log.NewStdLogger(os.Stdout),
		"ts", log.DefaultTimestamp,
		"caller", log.DefaultCaller,
		"service.id", "autotest",
		"service.name", "autotest",
		"service.version", "autotest",
		"trace.id", tracing.TraceID(),
		"span.id", tracing.SpanID(),
	)
	c := config.New(
		config.WithSource(
			file.NewSource(preparation.Configs),
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
	if isGitLabCI() || isUsingDocker() {
		if isGitLabCI() {
			bc.Data.Database.Source = "host=db user=haobtc password=haobtc.asd dbname=blockcrawlingtest port=5432 sslmode=disable TimeZone=Asia/Shanghai"
			bc.Data.User.Source = "host=db user=haobtc password=haobtc.asd dbname=blockcrawlingtest port=5432 sslmode=disable TimeZone=Asia/Shanghai"
			bc.Data.Redis.Address = "cache:6379"
		} else {
			bc.Data.Database.Source = dockerDSN
			bc.Data.User.Source = dockerDSN
			bc.Data.Redis.Address = dockerRedis
		}
		var err error
		grpcPort, err = getUnusedPort()
		if err != nil {
			panic(err)
		}
		bc.Server.Grpc.Addr = fmt.Sprintf("127.0.0.1:%d", grpcPort)
	}
	bizLog.BootstrapLogger(&conf.Logger{
		DEBUG:      true,
		FileName:   "stderr",
		Level:      "debug",
		ArchiveDir: "",
		MaxSize:    0,
		MaxAge:     0,
		MaxBackups: 0,
	})
	cancel := NewCancellation()
	app, cleanup, err := wireApp(
		bc.Server, bc.Data, bc.App, bc.AddressServer, bc.Lark, bc.Logger,
		bc.Transaction, &bc, logger,
		preparation, cancel,
	)
	defer cleanup()
	if err != nil {
		panic(err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := app.Run(); err != nil {
			panic(err)
		}
	}()
	<-cancel.Context.Done()
	app.Stop()
	wg.Wait()
}

func isGitLabCI() bool {
	return os.Getenv("CI_JOB_STAGE") == "test"
}

const (
	dockerDSN   = "host=db user=haobtc password=haobtc.asd dbname=blockcrawlingtest port=5432 sslmode=disable TimeZone=Asia/Shanghai"
	dockerRedis = "redis:6379"
)

func isUsingDocker() bool {
	return os.Getenv("PTESTING_ENV") == "docker"
}

func getUnusedPort() (port int, err error) {
	var a *net.TCPAddr
	if a, err = net.ResolveTCPAddr("tcp", "127.0.0.1:0"); err == nil {
		var l *net.TCPListener
		if l, err = net.ListenTCP("tcp", a); err == nil {
			defer l.Close()
			return l.Addr().(*net.TCPAddr).Port, nil
		}
	}
	return
}

func DialGRPC() (*googleRPC.ClientConn, error) {
	return googleRPC.Dial(fmt.Sprintf("127.0.0.1:%d", grpcPort), googleRPC.WithInsecure())
}
