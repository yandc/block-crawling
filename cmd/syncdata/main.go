package main

import (
	"block-crawling/internal/conf"
	"block-crawling/internal/platform"
	"flag"
	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	"os"
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
	cleanup, err := wireApp(bc.Logger, bc.Data, bc.App, bc.InnerNodeList, bc.InnerPublicNodeList, bc.Platform, bc.PlatformTest)
	if err != nil {
		panic(err)
	}
	defer cleanup()
	//platform.MigrateRecord()
	//platform.DappReset()
	//platform.BtcReset()
	//platform.HandleAsset()
	platform.HandleAssetByEventLog()
	platform.DeleteAsset()
}
