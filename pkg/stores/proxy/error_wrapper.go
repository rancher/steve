package proxy

import (
	"github.com/rancher/apiserver/pkg/apierror"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/wrangler/v2/pkg/schemas/validation"
	"k8s.io/apimachinery/pkg/api/errors"
)

type errorStore struct {
	types.Store
}

// ByID looks up a single object by its ID.
func (e *errorStore) ByID(apiOp *types.APIRequest, schema *types.APISchema, id string) (types.APIObject, error) {
	data, err := e.Store.ByID(apiOp, schema, id)
	return data, translateError(err)
}

// List returns a list of resources.
func (e *errorStore) List(apiOp *types.APIRequest, schema *types.APISchema) (types.APIObjectList, error) {
	data, err := e.Store.List(apiOp, schema)
	return data, translateError(err)
}

// Create creates a single object in the store.
func (e *errorStore) Create(apiOp *types.APIRequest, schema *types.APISchema, data types.APIObject) (types.APIObject, error) {
	data, err := e.Store.Create(apiOp, schema, data)
	return data, translateError(err)
}

// Update updates a single object in the store.
func (e *errorStore) Update(apiOp *types.APIRequest, schema *types.APISchema, data types.APIObject, id string) (types.APIObject, error) {
	data, err := e.Store.Update(apiOp, schema, data, id)
	return data, translateError(err)
}

// Delete deletes an object from a store.
func (e *errorStore) Delete(apiOp *types.APIRequest, schema *types.APISchema, id string) (types.APIObject, error) {
	data, err := e.Store.Delete(apiOp, schema, id)
	return data, translateError(err)

}

// Watch returns a channel of events for a list or resource.
func (e *errorStore) Watch(apiOp *types.APIRequest, schema *types.APISchema, wr types.WatchRequest) (chan types.APIEvent, error) {
	data, err := e.Store.Watch(apiOp, schema, wr)
	return data, translateError(err)
}

func translateError(err error) error {
	if apiError, ok := err.(errors.APIStatus); ok {
		status := apiError.Status()
		return apierror.NewAPIError(validation.ErrorCode{
			Status: int(status.Code),
			Code:   string(status.Reason),
		}, status.Message)
	}
	return err
}
