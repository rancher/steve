package main

import (
	"context"
	"os"

	"github.com/rancher/naok/pkg/server"
	"github.com/rancher/naok/pkg/version"
	"github.com/rancher/wrangler/pkg/signals"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

var (
	config server.Config
)

func main() {
	app := cli.NewApp()
	app.Name = "naok"
	app.Version = version.FriendlyVersion()
	app.Usage = ""
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "kubeconfig",
			EnvVar:      "KUBECONFIG",
			Value:       "",
			Destination: &config.Kubeconfig,
		},
		cli.StringFlag{
			Name:        "namespace",
			EnvVar:      "NAMESPACE",
			Value:       "default",
			Destination: &config.Namespace,
		},
		cli.StringFlag{
			Name:        "listen-address",
			EnvVar:      "LISTEN_ADDRESS",
			Value:       ":8080",
			Destination: &config.ListenAddress,
		},
		cli.BoolFlag{Name: "debug"},
	}
	app.Action = run

	if err := app.Run(os.Args); err != nil {
		logrus.Fatal(err)
	}
}

func run(c *cli.Context) error {
	if c.Bool("debug") {
		logrus.SetLevel(logrus.DebugLevel)
	}
	ctx := signals.SetupSignalHandler(context.Background())
	return server.Run(ctx, config)
}
