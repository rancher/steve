module github.com/rancher/steve

go 1.13

replace (
	github.com/crewjam/saml => github.com/rancher/saml v0.0.0-20180713225824-ce1532152fde
	github.com/knative/pkg => github.com/rancher/pkg v0.0.0-20190514055449-b30ab9de040e
	github.com/matryer/moq => github.com/rancher/moq v0.0.0-20190404221404-ee5226d43009

	k8s.io/client-go => github.com/rancher/client-go v1.20.0-rancher.1
)

require (
	github.com/adrg/xdg v0.3.1
	github.com/gorilla/mux v1.7.3
	github.com/gorilla/websocket v1.4.2
	github.com/hashicorp/golang-lru v0.5.3 // indirect
	github.com/imdario/mergo v0.3.8 // indirect
	github.com/pborman/uuid v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/rancher/apiserver v0.0.0-20210519053359-f943376c4b42
	github.com/rancher/dynamiclistener v0.2.1-0.20200714201033-9c1939da3af9
	github.com/rancher/kubernetes-provider-detector v0.1.2
	github.com/rancher/norman v0.0.0-20210423002317-8e6ffc77a819
	github.com/rancher/remotedialer v0.2.6-0.20210318171128-d1ebd5202be4
	github.com/rancher/wrangler v0.8.1-0.20210423003607-f71a90542852
	github.com/sirupsen/logrus v1.6.0
	github.com/urfave/cli v1.22.2
	github.com/urfave/cli/v2 v2.1.1
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	k8s.io/api v0.20.0
	k8s.io/apiextensions-apiserver v0.20.0
	k8s.io/apimachinery v0.20.0
	k8s.io/apiserver v0.20.0
	k8s.io/client-go v12.0.0+incompatible
	k8s.io/klog v1.0.0
	k8s.io/kube-aggregator v0.20.0
	k8s.io/kube-openapi v0.0.0-20201113171705-d219536bb9fd
)
