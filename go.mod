module github.com/rancher/naok

go 1.12

replace (
	github.com/rancher/norman => ../norman
	github.com/rancher/wrangler => ../wrangler
	github.com/rancher/wrangler-api => ../wrangler-api
	k8s.io/api => ../kuberlite/staging/src/k8s.io/api
	k8s.io/apimachinery => ../kuberlite/staging/src/k8s.io/apimachinery
	k8s.io/client-go => ../kuberlite/staging/src/k8s.io/client-go
)

require (
	github.com/gogo/protobuf v1.2.2-0.20190723190241-65acae22fc9d
	github.com/golang/protobuf v1.3.1
	github.com/gorilla/mux v1.7.3
	github.com/rancher/norman v0.0.0-20190704000224-043a1c919df3
	github.com/rancher/wrangler v0.1.7-0.20190905161106-749be31cca6c
	github.com/rancher/wrangler-api v0.1.5-0.20190619170228-c3525df45215
	github.com/sirupsen/logrus v1.4.2
	github.com/urfave/cli v1.20.0
	golang.org/x/exp v0.0.0-20190312203227-4b39c73a6495 // indirect
	k8s.io/api v0.0.0
	k8s.io/apiextensions-apiserver v0.0.0-20190409022649-727a075fdec8
	k8s.io/apimachinery v0.0.0
	k8s.io/apiserver v0.0.0-20190409021813-1ec86e4da56c
	k8s.io/client-go v11.0.1-0.20190409021438-1a26190bd76a+incompatible
	k8s.io/helm v2.14.3+incompatible
	k8s.io/klog v0.3.2
	k8s.io/kube-aggregator v0.0.0-20190409022021-00b8e31abe9d
	k8s.io/kube-openapi v0.0.0-20190816220812-743ec37842bf

)
