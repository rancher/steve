package counts

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/rancher/naok/pkg/accesscontrol"
	"github.com/rancher/norman/pkg/store/empty"
	"github.com/rancher/norman/pkg/types"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

var (
	ignore = map[string]bool{
		"count":  true,
		"schema": true,
	}
	slow = map[string]bool{
		"io.k8s.api.management.cattle.io.v3.CatalogTemplateVersion": true,
		"io.k8s.api.management.cattle.io.v3.CatalogTemplate":        true,
	}
	listTimeout = 1750 * time.Millisecond
)

func Register(schemas *types.Schemas) {
	schemas.MustImportAndCustomize(Count{}, func(schema *types.Schema) {
		schema.CollectionMethods = []string{http.MethodGet}
		schema.ResourceMethods = []string{}
		schema.Attributes["access"] = accesscontrol.AccessListMap{
			"watch": accesscontrol.AccessList{
				{
					Namespace:    "*",
					ResourceName: "*",
				},
			},
		}
		schema.Store = &Store{}
	})
}

type Count struct {
	ID     string               `json:"id,omitempty"`
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
	c, err := s.getCount(apiOp, listTimeout, true)
	return types.ToAPI(c), err
}

func (s *Store) List(apiOp *types.APIRequest, schema *types.Schema, opt *types.QueryOptions) (types.APIObject, error) {
	c, err := s.getCount(apiOp, listTimeout, true)
	return types.ToAPI([]interface{}{c}), err
}

func (s *Store) Watch(apiOp *types.APIRequest, schema *types.Schema, w types.WatchRequest) (chan types.APIEvent, error) {
	c, err := s.getCount(apiOp, listTimeout*10, false)
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(apiOp.Context())

	child := make(chan Count)
	for name, countItem := range c.Counts {
		wg.Add(1)
		go func() {
			s.watchItem(apiOp.WithContext(ctx), name, countItem.Count, countItem.Revision, cancel, child)
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(child)
	}()

	result := make(chan types.APIEvent)
	go func() {
		defer close(result)

		result <- types.APIEvent{
			Name:         "resource.create",
			ResourceType: "count",
			Object:       types.ToAPI(c),
		}

		for change := range child {
			for k, v := range change.Counts {
				c.Counts[k] = v
			}

			result <- types.APIEvent{
				Name:         "resource.change",
				ResourceType: "count",
				Object:       types.ToAPI(c),
			}
		}
	}()

	return result, nil
}

func (s *Store) watchItem(apiOp *types.APIRequest, schemaID string, start int, revision string, cancel func(), counts chan Count) {
	schema := apiOp.Schemas.Schema(schemaID)
	if schema == nil || schema.Store == nil || apiOp.AccessControl.CanWatch(apiOp, schema) != nil {
		return
	}

	defer cancel()

	w, err := schema.Store.Watch(apiOp, schema, types.WatchRequest{Revision: revision})
	if err != nil {
		logrus.Errorf("failed to watch %s for counts: %v", schema.ID, err)
		return
	}

	for event := range w {
		if event.Revision == revision {
			continue
		}

		write := false
		if event.Name == "resource.create" {
			start++
			write = true
		} else if event.Name == "resource.remove" {
			start--
			write = true
		}
		if write {
			counts <- Count{Counts: map[string]ItemCount{
				schemaID: {
					Count:    start,
					Revision: event.Revision,
				},
			}}
		}
	}
}

func (s *Store) getCount(apiOp *types.APIRequest, timeout time.Duration, ignoreSlow bool) (Count, error) {
	var countLock sync.Mutex
	counts := map[string]ItemCount{}

	errCtx, cancel := context.WithTimeout(apiOp.Context(), timeout)
	eg, errCtx := errgroup.WithContext(errCtx)
	defer cancel()

	for _, schema := range apiOp.Schemas.Schemas() {
		if ignore[schema.ID] {
			continue
		}

		if ignoreSlow && slow[schema.ID] {
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

	var (
		err error
	)

	select {
	case err = <-future(eg.Wait):
	case <-errCtx.Done():
		err = errCtx.Err()
	}

	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		return Count{}, err
	}

	// in the case of cancellation go routines could still be running so we copy the map
	// to avoid returning a map that might get modified
	countLock.Lock()
	result := Count{
		ID:     "count",
		Counts: map[string]ItemCount{},
	}
	for k, v := range counts {
		result.Counts[k] = v
	}
	countLock.Unlock()

	return result, nil
}

func future(f func() error) chan error {
	result := make(chan error, 1)
	go func() {
		defer close(result)
		if err := f(); err != nil {
			result <- err
		}
	}()
	return result
}
