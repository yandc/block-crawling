package main

import (
	"block-crawling/internal/conf"
	"block-crawling/internal/data"
	bizLog "block-crawling/internal/log"
	"block-crawling/internal/platform"
	"flag"
	"os"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
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

func newApp(*data.Bundle, platform.PlatformContainer, platform.InnerPlatformContainer) *kratos.App {
	return kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Metadata(map[string]string{}),
	)
}

func main() {
	flag.Parse()
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
	bizLog.BootstrapLogger(bc.Logger)

	_, cleanup, err := wireApp(bc.Logger, bc.Data, bc.App, &bc)
	if err != nil {
		panic(err)
	}
	defer cleanup()
	//platform.MigrateRecord()
	//platform.DappReset()
	//platform.BtcReset()
	//platform.HandleAsset()
	//platform.HandleAssetByEventLog()
	platform.DeleteAsset()
	//platform.DeleteAndUpdateAsset()
	//platform.UpdateAsset()
	//platform.HandleTokenInfo()
	//platform.DeleteRecordData()
	//platform.CheckNonce()
}
