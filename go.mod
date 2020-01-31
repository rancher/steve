module github.com/rancher/steve

go 1.13

replace (
	github.com/rancher/dynamiclistener => ../dynamiclistener
	k8s.io/client-go => k8s.io/client-go v0.17.2
)

require (
	github.com/ghodss/yaml v1.0.0
	github.com/gorilla/mux v1.6.2
	github.com/gorilla/websocket v1.4.0
	github.com/pkg/errors v0.8.1
	github.com/rancher/dynamiclistener v0.2.1-0.20191204183509-ab900b52683c
	github.com/rancher/wrangler v0.4.1-0.20200131051624-f65ef17f3764
	github.com/rancher/wrangler-api v0.4.1
	github.com/sirupsen/logrus v1.4.2
	github.com/urfave/cli v1.22.2
	github.com/urfave/cli/v2 v2.1.1
	k8s.io/api v0.17.2
	k8s.io/apiextensions-apiserver v0.17.2
	k8s.io/apimachinery v0.17.2
	k8s.io/apiserver v0.17.2
	k8s.io/client-go v11.0.1-0.20190409021438-1a26190bd76a+incompatible
	k8s.io/klog v1.0.0
	k8s.io/kube-aggregator v0.17.2
	k8s.io/kube-openapi v0.0.0-20191107075043-30be4d16710a
)
