package main

import (
	"context"
	"flag"
	"os"

	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/version"
	"github.com/rancher/wrangler/pkg/signals"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"k8s.io/klog"
)

var (
	config server.Config
)

func main() {
	app := cli.NewApp()
	app.Name = "steve"
	app.Version = version.FriendlyVersion()
	app.Usage = ""
	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:        "authentication",
			Destination: &config.Authentication,
		},
		cli.StringFlag{
			Name:        "webhook-kubeconfig",
			EnvVar:      "WEBHOOK_KUBECONFIG",
			Value:       "webhook-kubeconfig.yaml",
			Destination: &config.WebhookKubeconfig,
		},
		cli.StringFlag{
			Name:        "kubeconfig",
			EnvVar:      "KUBECONFIG",
			Value:       "",
			Destination: &config.Kubeconfig,
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
	logging := flag.NewFlagSet("", flag.PanicOnError)
	klog.InitFlags(logging)
	if c.Bool("debug") {
		logrus.SetLevel(logrus.DebugLevel)
		if err := logging.Parse([]string{
			"-v=7",
		}); err != nil {
			return err
		}
	} else {
		if err := logging.Parse([]string{
			"-v=0",
		}); err != nil {
			return err
		}
	}
	ctx := signals.SetupSignalHandler(context.Background())
	return server.Run(ctx, config)
}
