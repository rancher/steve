package counts

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/rancher/norman/pkg/store/empty"
	"github.com/rancher/norman/pkg/types"
	"golang.org/x/sync/errgroup"
)

var (
	ignore = map[string]bool{
		"count":  true,
		"schema": true,
	}
)

func Register(schemas *types.Schemas) {
	schemas.MustImportAndCustomize(Count{}, func(schema *types.Schema) {
		schema.CollectionMethods = []string{http.MethodGet}
		schema.ResourceMethods = []string{}
		schema.Store = &Store{}
	})
}

type Count struct {
	Counts map[string]ItemCount `json:"counts,omitempty"`
}

type ItemCount struct {
	Count    int    `json:"count,omitempty"`
	Revision string `json:"revision,omitempty"`
}

type Store struct {
	empty.Store
}

func (s *Store) ByID(apiOp *types.APIRequest, schema *types.Schema, id string) (types.APIObject, error) {
	c, err := s.getCount(apiOp, 750*time.Millisecond)
	return types.ToAPI(c), err
}

func (s *Store) List(apiOp *types.APIRequest, schema *types.Schema, opt *types.QueryOptions) (types.APIObject, error) {
	c, err := s.getCount(apiOp, 750*time.Millisecond)
	return types.ToAPI([]interface{}{c}), err
}

func (s *Store) Watch(apiOp *types.APIRequest, schema *types.Schema, w types.WatchRequest) (chan types.APIEvent, error) {
	return nil, nil
}

func (s *Store) getCount(apiOp *types.APIRequest, timeout time.Duration) (Count, error) {
	var countLock sync.Mutex
	counts := map[string]ItemCount{}

	errCtx, cancel := context.WithTimeout(apiOp.Context(), timeout)
	eg, errCtx := errgroup.WithContext(errCtx)
	defer cancel()

	for _, schema := range apiOp.Schemas.Schemas() {
		if ignore[schema.ID] {
			continue
		}

		if schema.Store == nil {
			continue
		}

		if apiOp.AccessControl.CanList(apiOp, schema) != nil {
			continue
		}

		current := schema
		eg.Go(func() error {
			list, err := current.Store.List(apiOp, current, nil)
			if err != nil {
				return err
			}
			if list.IsNil() {
				return nil
			}

			countLock.Lock()
			counts[current.ID] = ItemCount{
				Count:    len(list.List()),
				Revision: list.ListRevision,
			}
			countLock.Unlock()

			return nil
		})
	}

	if err := eg.Wait(); err != nil && err != context.Canceled {
		return Count{}, err
	}

	// in the case of cancellation go routines could still be running so we copy the map
	// to avoid returning a map that might get modified
	countLock.Lock()
	result := Count{
		Counts: map[string]ItemCount{},
	}
	for k, v := range counts {
		result.Counts[k] = v
	}
	countLock.Unlock()

	return result, nil
}
