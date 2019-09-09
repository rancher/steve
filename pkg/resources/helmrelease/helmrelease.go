package helmrelease

import (
	"net/http"

	"github.com/rancher/norman/pkg/types"
	v1 "github.com/rancher/wrangler-api/pkg/generated/controllers/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/helm/pkg/proto/hapi/chart"
)

type HelmRelease struct {
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	chart.Metadata `json:",inline"`
	ID             string       `json:"id,omitempty"`
	FirstDeployed  *metav1.Time `json:"firstDeployed,omitempty"`
	LastDeployed   *metav1.Time `json:"lastDeployed,omitempty"`
	Deleted        *metav1.Time `json:"deleted,omitempty"`
	Status         string       `json:"status,omitempty"`
	Manifest       string       `json:"manifest,omitempty"`
	ReadMe         string       `json:"readMe,omitempty"`
	Name           string       `json:"name,omitempty"`
	Version        int32        `json:"version,omitempty"`
}

func Register(schemas *types.Schemas, configMaps v1.ConfigMapClient, secrets v1.SecretClient) {
	schemas.MustImportAndCustomize(HelmRelease{}, func(schema *types.Schema) {
		schema.CollectionMethods = []string{http.MethodGet}
		schema.ResourceMethods = []string{http.MethodGet}
		schema.Store = &Store{
			configMaps: configMaps,
			secrets:    secrets,
		}
	})
}
