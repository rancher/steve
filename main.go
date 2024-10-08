package main

import (
	"os"

	"github.com/rancher/dynamiclistener/server"
	"github.com/rancher/steve/pkg/debug"
	stevecli "github.com/rancher/steve/pkg/server/cli"
	"github.com/rancher/steve/pkg/version"
	"github.com/rancher/wrangler/v3/pkg/signals"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

var (
	config      stevecli.Config
	debugconfig debug.Config
)

func main() {
	app := cli.NewApp()
	app.Name = "steve"
	app.Version = version.FriendlyVersion()
	app.Usage = ""
	app.Flags = append(
		stevecli.Flags(&config),
		debug.Flags(&debugconfig)...)
	app.Action = run

	if err := app.Run(os.Args); err != nil {
		logrus.Fatal(err)
	}
}

func run(_ *cli.Context) error {
	ctx := signals.SetupSignalContext()
	debugconfig.MustSetupDebug()
	s, err := config.ToServer(ctx, config.SQLCache)
	if err != nil {
		return err
	}
	return s.ListenAndServe(ctx, config.HTTPSListenPort, config.HTTPListenPort, &server.ListenOpts{DisplayServerLogs: true})
}
