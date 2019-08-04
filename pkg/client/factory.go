package client

import (
	"github.com/rancher/naok/pkg/attributes"
	"github.com/rancher/norman/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

type Factory struct {
	client dynamic.Interface
}

func NewFactory(cfg *rest.Config) (*Factory, error) {
	newCfg := *cfg
	newCfg.QPS = 10000
	newCfg.Burst = 100
	c, err := dynamic.NewForConfig(&newCfg)
	if err != nil {
		return nil, err
	}
	return &Factory{
		client: c,
	}, nil
}

func (p *Factory) Client(ctx *types.APIRequest, s *types.Schema) (dynamic.ResourceInterface, error) {
	gvr := attributes.GVR(s)
	if len(ctx.Namespaces) > 0 {
		return p.client.Resource(gvr).Namespace(ctx.Namespaces[0]), nil
	}

	return p.client.Resource(gvr), nil
}
