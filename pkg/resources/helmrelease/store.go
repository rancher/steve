package helmrelease

import (
	"github.com/rancher/norman/pkg/store/empty"
	"github.com/rancher/norman/pkg/types"
	v1 "github.com/rancher/wrangler-api/pkg/generated/controllers/core/v1"
	"github.com/rancher/wrangler/pkg/kv"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Store struct {
	empty.Store
	configMaps v1.ConfigMapClient
	secrets    v1.SecretClient
}

func (s *Store) ByID(apiOp *types.APIRequest, schema *types.Schema, id string) (types.APIObject, error) {
	var (
		data            string
		namespace, name = kv.Split(id, ":")
	)

	secret, err := s.secrets.Get(namespace, name, metav1.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return types.APIObject{}, err
	} else if errors.IsNotFound(err) {
		secret = nil
	}

	if secret == nil {
		configMap, err := s.configMaps.Get(apiOp.Namespaces[0], id, metav1.GetOptions{})
		if err != nil && !errors.IsNotFound(err) {
			return types.APIObject{}, err
		}

		if configMap == nil {
			return types.APIObject{}, nil
		}

		data = configMap.Data["release"]
		name = configMap.Name
	} else {
		data = string(secret.Data["release"])
		name = secret.Name
	}

	hr, err := ToRelease(data, name)
	if err != nil || hr == nil {
		return types.APIObject{}, err
	}

	return types.ToAPI(hr), nil
}

//func (s *Store) List(apiOp *types.APIRequest, schema *types.Schema, opt *types.QueryOptions) (types.APIObject, error) {
//
//}
//
//func (s *Store) Watch(apiOp *types.APIRequest, schema *types.Schema, w types.WatchRequest) (chan types.APIEvent, error) {
//
//}
