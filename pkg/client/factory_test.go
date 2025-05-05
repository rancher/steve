package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/rest"
)

func TestWithQPSAndBurst(t *testing.T) {
	fo := defaultFactoryOptions()

	assert.Equal(t, defaultQPS, fo.qps)
	assert.Equal(t, defaultBurst, fo.burst)

	WithQPSAndBurst(50, 20)(fo)

	assert.Equal(t, float32(50.0), fo.qps)
	assert.Equal(t, 20, fo.burst)
}

func TestNewFactoryWithOptions(t *testing.T) {
	cfg := &rest.Config{
		QPS:   5.0,
		Burst: 10,
	}
	f, err := NewFactory(cfg, false, WithQPSAndBurst(50, 20))
	assert.NoError(t, err)

	assert.Equal(t, float32(50.0), f.clientCfg.QPS)
	assert.Equal(t, 20, f.clientCfg.Burst)
}
