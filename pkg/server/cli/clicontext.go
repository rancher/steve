package cli

import (
	authcli "github.com/rancher/steve/pkg/auth/cli"
	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/wrangler/pkg/kubeconfig"
	"github.com/rancher/wrangler/pkg/ratelimit"
	"github.com/urfave/cli"
)

type Config struct {
	KubeConfig      string
	HTTPSListenPort int
	HTTPListenPort  int
	DashboardURL    string

	WebhookConfig authcli.WebhookConfig
}

func (c *Config) MustServer() *server.Server {
	cc, err := c.ToServer()
	if err != nil {
		panic(err)
	}
	return cc
}

func (c *Config) ToServer() (*server.Server, error) {
	restConfig, err := kubeconfig.GetNonInteractiveClientConfig(c.KubeConfig).ClientConfig()
	if err != nil {
		return nil, err
	}
	restConfig.RateLimiter = ratelimit.None

	auth, err := c.WebhookConfig.WebhookMiddleware()
	if err != nil {
		return nil, err
	}

	return &server.Server{
		RestConfig:     restConfig,
		AuthMiddleware: auth,
		DashboardURL: func() string {
			return c.DashboardURL
		},
	}, nil
}

func Flags(config *Config) []cli.Flag {
	flags := []cli.Flag{
		cli.StringFlag{
			Name:        "kubeconfig",
			EnvVar:      "KUBECONFIG",
			Destination: &config.KubeConfig,
		},
		cli.IntFlag{
			Name:        "https-listen-port",
			Value:       8443,
			Destination: &config.HTTPSListenPort,
		},
		cli.IntFlag{
			Name:        "http-listen-port",
			Value:       8080,
			Destination: &config.HTTPListenPort,
		},
		cli.StringFlag{
			Name:        "dashboard-url",
			Value:       "https://releases.rancher.com/dashboard/latest/index.html",
			Destination: &config.DashboardURL,
		},
	}

	return append(flags, authcli.Flags(&config.WebhookConfig)...)
}
