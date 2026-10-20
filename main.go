package main

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rancher/dynamiclistener/server"
	"github.com/rancher/steve/pkg/debug"
	stevecli "github.com/rancher/steve/pkg/server/cli"
	"github.com/rancher/steve/pkg/version"
	"github.com/rancher/wrangler/v3/pkg/signals"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
)

var (
	config      stevecli.Config
	debugconfig debug.Config
)

func main() {
	cmd := &cli.Command{
		Name:    "steve",
		Version: version.FriendlyVersion(),
		Usage:   "",
		Flags: append(
			stevecli.Flags(&config),
			debug.Flags(&debugconfig)...),
		Action: run,
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		logrus.Fatal(err)
	}
}

func run(_ context.Context, _ *cli.Command) error {
	ctx := signals.SetupSignalContext()
	debugconfig.MustSetupDebug()
	s, err := config.ToServer(ctx)
	if err != nil {
		return err
	}
	if config.PprofEnabled {
		go func() {
			if err := http.ListenAndServe(config.PprofListenAddr, nil); err != nil {
				logrus.Fatalf("pprof server: %v\n", err)
			}
		}()
	}
	if config.MetricsEnabled {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			if err := http.ListenAndServe(config.MetricsListenAddr, mux); err != nil {
				logrus.Fatalf("metrics server: %v\n", err)
			}
		}()
	}
	return s.ListenAndServe(ctx, config.HTTPSListenPort, config.HTTPListenPort, &server.ListenOpts{DisplayServerLogs: true})
}
