module github.com/rancher/naok

go 1.12

replace (
	github.com/rancher/norman => ../norman
	github.com/rancher/wrangler-api => ../wrangler-api
)

require (
	github.com/googleapis/gnostic v0.2.0
	github.com/gorilla/mux v1.7.3
	github.com/kisielk/errcheck v1.2.0 // indirect
	github.com/rancher/norman v0.0.0-20190704000224-043a1c919df3
	github.com/rancher/wrangler v0.1.4
	github.com/rancher/wrangler-api v0.1.5-0.20190619170228-c3525df45215
	github.com/sirupsen/logrus v1.4.2
	github.com/urfave/cli v1.20.0
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
	google.golang.org/appengine v1.5.0 // indirect
	k8s.io/api v0.0.0-20190409021203-6e4e0e4f393b
	k8s.io/apiextensions-apiserver v0.0.0-20190409022649-727a075fdec8
	k8s.io/apimachinery v0.0.0-20190404173353-6a84e37a896d
	k8s.io/apiserver v0.0.0-20190409021813-1ec86e4da56c
	k8s.io/client-go v11.0.1-0.20190409021438-1a26190bd76a+incompatible
	k8s.io/kube-aggregator v0.0.0-20190409022021-00b8e31abe9d
	k8s.io/kube-openapi v0.0.0-20190502190224-411b2483e503

)
