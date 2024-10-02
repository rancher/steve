module github.com/rancher/steve

go 1.22.0

toolchain go1.22.7

replace github.com/rancher/wrangler/v3 => ../wrangler

replace (
	github.com/crewjam/saml => github.com/rancher/saml v0.2.0
	github.com/knative/pkg => github.com/rancher/pkg v0.0.0-20181214184433-b04c0947ad2f
	github.com/matryer/moq => github.com/rancher/moq v0.0.0-20190404221404-ee5226d43009
)

require (
	github.com/adrg/xdg v0.5.0
	github.com/google/gnostic-models v0.6.8
	github.com/gorilla/mux v1.8.1
	github.com/gorilla/websocket v1.5.1
	github.com/pborman/uuid v1.2.1
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.19.1
	github.com/rancher/apiserver v0.0.0-20241009200134-5a4ecca7b988
	github.com/rancher/dynamiclistener v0.6.1-rc.2
	github.com/rancher/kubernetes-provider-detector v0.1.5
	github.com/rancher/lasso v0.0.0-20240924233157-8f384efc8813
	github.com/rancher/norman v0.0.0-20241001183610-78a520c160ab
	github.com/rancher/remotedialer v0.3.2
	github.com/rancher/wrangler/v3 v3.0.1-rc.2
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.9.0
	github.com/urfave/cli v1.22.14
	github.com/urfave/cli/v2 v2.27.4
	go.uber.org/mock v0.4.0
	golang.org/x/sync v0.8.0
	gopkg.in/yaml.v3 v3.0.1
	k8s.io/api v0.31.1
	k8s.io/apiextensions-apiserver v0.31.1
	k8s.io/apimachinery v0.31.1
	k8s.io/apiserver v0.31.1
	k8s.io/client-go v0.31.1
	k8s.io/klog v1.0.0
	k8s.io/kube-aggregator v0.31.1
	k8s.io/kube-openapi v0.0.0-20240411171206-dc4e619f62f3
	sigs.k8s.io/controller-runtime v0.19.0
)
