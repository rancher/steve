package debug

import (
	"flag"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"k8s.io/klog"
)

type Config struct {
	Debug      bool
	DebugLevel int
	SQLCache   bool
}

func (c *Config) MustSetupDebug() {
	err := c.SetupDebug()
	if err != nil {
		panic("failed to setup debug logging: " + err.Error())
	}
}

func (c *Config) SetupDebug() error {
	logging := flag.NewFlagSet("", flag.PanicOnError)
	klog.InitFlags(logging)
	if c.Debug {
		logrus.SetLevel(logrus.DebugLevel)
		if err := logging.Parse([]string{
			fmt.Sprintf("-v=%d", c.DebugLevel),
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

	return nil
}

func Flags(config *Config) []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:        "debug",
			Destination: &config.Debug,
		},
		&cli.IntFlag{
			Name:        "debug-level",
			Value:       7,
			Destination: &config.DebugLevel,
		},
		&cli.BoolFlag{
			Name:        "sql-cache",
			Destination: &config.SQLCache,
		},
	}
}
