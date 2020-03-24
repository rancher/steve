module github.com/rancher/steve

go 1.13

replace k8s.io/client-go => k8s.io/client-go v0.17.2

require (
	github.com/ghodss/yaml v1.0.0
	github.com/google/go-cmp v0.4.0 // indirect
	github.com/gorilla/mux v1.6.2
	github.com/gorilla/websocket v1.4.0
	github.com/json-iterator/go v1.1.9 // indirect
	github.com/pkg/errors v0.8.1
	github.com/rancher/dynamiclistener v0.2.1-0.20200213165308-111c5b43e932
	github.com/rancher/wrangler v0.5.2
	github.com/rancher/wrangler-api v0.5.0
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.4.0
	github.com/urfave/cli v1.22.2
	github.com/urfave/cli/v2 v2.1.1
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
	golang.org/x/sys v0.0.0-20200122134326-e047566fdf82 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/yaml.v2 v2.2.5 // indirect
	k8s.io/api v0.17.2
	k8s.io/apiextensions-apiserver v0.17.2
	k8s.io/apimachinery v0.17.2
	k8s.io/apiserver v0.17.2
	k8s.io/client-go v11.0.1-0.20190409021438-1a26190bd76a+incompatible
	k8s.io/klog v1.0.0
	k8s.io/kube-aggregator v0.17.2
	k8s.io/kube-openapi v0.0.0-20191107075043-30be4d16710a
)
