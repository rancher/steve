module github.com/rancher/steve

go 1.23.0

toolchain go1.23.6

// pin Kubernetes dependencies to v0.30.1, which is the version
// Rancher 2.9 is compiled against as newer lasso versions
// pull in newer Kubernetes dependencies
replace (
	github.com/google/cel-go => github.com/google/cel-go v0.17.8
	github.com/prometheus/client_golang => github.com/prometheus/client_golang v1.16.0
	github.com/prometheus/client_model => github.com/prometheus/client_model v0.6.1
	github.com/prometheus/common => github.com/prometheus/common v0.44.0
	k8s.io/api => k8s.io/api v0.30.1
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.30.1
	k8s.io/apimachinery => k8s.io/apimachinery v0.30.1
	k8s.io/apiserver => k8s.io/apiserver v0.30.1
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.30.1
	k8s.io/client-go => k8s.io/client-go v0.30.1
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.30.1
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.30.1
	k8s.io/code-generator => k8s.io/code-generator v0.30.1
	k8s.io/component-base => k8s.io/component-base v0.30.1
	k8s.io/component-helpers => k8s.io/component-helpers v0.30.1
	k8s.io/controller-manager => k8s.io/controller-manager v0.30.1
	k8s.io/cri-api => k8s.io/cri-api v0.30.1
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.30.1
	k8s.io/dynamic-resource-allocation => k8s.io/dynamic-resource-allocation v0.30.1
	k8s.io/endpointslice => k8s.io/endpointslice v0.30.1
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.30.1
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.30.1
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.30.1
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.30.1
	k8s.io/kubectl => k8s.io/kubectl v0.30.1
	k8s.io/kubelet => k8s.io/kubelet v0.30.1
	k8s.io/kubernetes => k8s.io/kubernetes v1.30.1
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.30.1
	k8s.io/metrics => k8s.io/metrics v0.30.1
	k8s.io/mount-utils => k8s.io/mount-utils v0.30.1
	k8s.io/pod-security-admission => k8s.io/pod-security-admission v0.30.1
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.30.1
	sigs.k8s.io/controller-runtime => sigs.k8s.io/controller-runtime v0.18.5
)

replace (
	github.com/crewjam/saml => github.com/rancher/saml v0.4.14
	github.com/knative/pkg => github.com/rancher/pkg v0.0.0-20181214184433-b04c0947ad2f
	github.com/matryer/moq => github.com/rancher/moq v0.0.0-20190404221404-ee5226d43009
)

require (
	github.com/adrg/xdg v0.4.0
	github.com/golang/protobuf v1.5.4
	github.com/google/gnostic-models v0.6.8
	github.com/gorilla/mux v1.8.1
	github.com/gorilla/websocket v1.5.1
	github.com/pborman/uuid v1.2.1
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.19.1
	github.com/rancher/apiserver v0.0.0-20240708202538-39a6f2535146
	github.com/rancher/dynamiclistener v0.6.1
	github.com/rancher/kubernetes-provider-detector v0.1.5
	github.com/rancher/lasso v0.0.0-20241202185148-04649f379358
	github.com/rancher/norman v0.0.0-20240708202514-a0127673d1b9
	github.com/rancher/remotedialer v0.3.2
	github.com/rancher/wrangler/v3 v3.0.1
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.9.0
	github.com/urfave/cli v1.22.16
	github.com/urfave/cli/v2 v2.27.5
	go.uber.org/mock v0.5.0
	golang.org/x/sync v0.11.0
	gopkg.in/yaml.v3 v3.0.1
	helm.sh/helm/v3 v3.13.0
	k8s.io/api v0.31.1
	k8s.io/apiextensions-apiserver v0.31.0
	k8s.io/apimachinery v0.31.1
	k8s.io/apiserver v0.30.1
	k8s.io/client-go v0.31.1
	k8s.io/helm v2.17.0+incompatible
	k8s.io/klog v1.0.0
	k8s.io/kube-aggregator v0.30.1
	k8s.io/kube-openapi v0.0.0-20240411171206-dc4e619f62f3
	k8s.io/utils v0.0.0-20241210054802-24370beab758
	modernc.org/sqlite v1.36.0
	sigs.k8s.io/controller-runtime v0.19.0
)

require (
	github.com/Masterminds/semver/v3 v3.2.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blang/semver/v4 v4.0.0 // indirect
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.5 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/emicklei/go-restful/v3 v3.11.0 // indirect
	github.com/evanphx/json-patch v5.9.0+incompatible // indirect
	github.com/evanphx/json-patch/v5 v5.9.0 // indirect
	github.com/felixge/httpsnoop v1.0.3 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/jsonpointer v0.19.6 // indirect
	github.com/go-openapi/jsonreference v0.20.2 // indirect
	github.com/go-openapi/swag v0.22.4 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.16.0 // indirect
	github.com/imdario/mergo v0.3.13 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/mxk/go-flowrate v0.0.0-20140419014527-cca7078d478f // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/xrash/smetrics v0.0.0-20240521201337-686a1a2994c1 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.44.0 // indirect
	go.opentelemetry.io/otel v1.19.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.19.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.19.0 // indirect
	go.opentelemetry.io/otel/metric v1.19.0 // indirect
	go.opentelemetry.io/otel/sdk v1.19.0 // indirect
	go.opentelemetry.io/otel/trace v1.19.0 // indirect
	go.opentelemetry.io/proto/otlp v1.0.0 // indirect
	golang.org/x/crypto v0.26.0 // indirect
	golang.org/x/exp v0.0.0-20231108232855-2478ac86f678 // indirect
	golang.org/x/net v0.28.0 // indirect
	golang.org/x/oauth2 v0.21.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/term v0.23.0 // indirect
	golang.org/x/text v0.17.0 // indirect
	golang.org/x/time v0.3.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20230726155614-23370e0ffb3e // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230822172742-b8732ec3820d // indirect
	google.golang.org/grpc v1.58.3 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	k8s.io/component-base v0.30.1 // indirect
	k8s.io/klog/v2 v2.130.1 // indirect
	modernc.org/libc v1.61.13 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.8.2 // indirect
	sigs.k8s.io/apiserver-network-proxy/konnectivity-client v0.29.0 // indirect
	sigs.k8s.io/cli-utils v0.37.2 // indirect
	sigs.k8s.io/json v0.0.0-20221116044647-bc3834ca7abd // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.4.1 // indirect
	sigs.k8s.io/yaml v1.4.0 // indirect
)
