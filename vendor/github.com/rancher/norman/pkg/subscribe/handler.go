package subscribe

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rancher/norman/pkg/api/writer"
	"github.com/rancher/norman/pkg/httperror"
	"github.com/rancher/norman/pkg/parse"
	"github.com/rancher/norman/pkg/types"
	"github.com/rancher/norman/pkg/types/convert"
	"github.com/rancher/norman/pkg/types/slice"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

var upgrader = websocket.Upgrader{}

type Subscribe struct {
	ResourceTypes []string
	APIVersions   []string
	ProjectID     string `norman:"type=reference[/v3/schemas/project]"`
}

func Handler(apiOp *types.APIRequest) (types.APIObject, error) {
	err := handler(apiOp)
	if err != nil {
		logrus.Errorf("Error during subscribe %v", err)
	}
	return types.APIObject{}, err
}

func getMatchingSchemas(apiOp *types.APIRequest) []*types.Schema {
	resourceTypes := apiOp.Request.URL.Query()["resourceTypes"]

	var schemas []*types.Schema
	for _, schema := range apiOp.Schemas.Schemas() {
		if !matches(resourceTypes, schema.ID) {
			continue
		}
		if schema.Store != nil {
			schemas = append(schemas, schema)
		}
	}

	return schemas
}

func handler(apiOp *types.APIRequest) error {
	schemas := getMatchingSchemas(apiOp)
	if len(schemas) == 0 {
		return httperror.NewAPIError(httperror.NotFound, "no resources types matched")
	}

	c, err := upgrader.Upgrade(apiOp.Response, apiOp.Request, nil)
	if err != nil {
		return err
	}
	defer c.Close()

	cancelCtx, cancel := context.WithCancel(apiOp.Request.Context())
	readerGroup, ctx := errgroup.WithContext(cancelCtx)
	apiOp.Request = apiOp.Request.WithContext(ctx)

	go func() {
		for {
			if _, _, err := c.NextReader(); err != nil {
				cancel()
				c.Close()
				break
			}
		}
	}()

	events := make(chan types.APIObject)
	for _, schema := range schemas {
		if apiOp.AccessControl.CanWatch(apiOp, schema) == nil {
			streamStore(ctx, readerGroup, apiOp, schema, events)
		}
	}

	go func() {
		readerGroup.Wait()
		close(events)
	}()

	jsonWriter := writer.EncodingResponseWriter{
		ContentType: "application/json",
		Encoder:     types.JSONEncoder,
	}
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()

	done := false
	for !done {
		select {
		case item, ok := <-events:
			if !ok {
				done = true
				break
			}

			header := `{"name":"resource.change","data":`
			if item.Map()[".removed"] == true {
				header = `{"name":"resource.remove","data":`
			}
			schema := apiOp.Schemas.Schema(convert.ToString(item.Map()["type"]))
			if schema != nil {
				buffer := &bytes.Buffer{}
				if err := jsonWriter.VersionBody(apiOp, buffer, item); err != nil {
					cancel()
					continue
				}

				if err := writeData(c, header, buffer.Bytes()); err != nil {
					cancel()
				}
			}
		case <-t.C:
			if err := writeData(c, `{"name":"ping","data":`, []byte("{}")); err != nil {
				cancel()
			}
		}
	}

	// no point in ever returning null because the connection is hijacked and we can't write it
	return nil
}

func writeData(c *websocket.Conn, header string, buf []byte) error {
	messageWriter, err := c.NextWriter(websocket.TextMessage)
	if err != nil {
		return err
	}

	if _, err := messageWriter.Write([]byte(header)); err != nil {
		return err
	}
	if _, err := messageWriter.Write(buf); err != nil {
		return err
	}
	if _, err := messageWriter.Write([]byte(`}`)); err != nil {
		return err
	}
	return messageWriter.Close()
}

func watch(apiOp *types.APIRequest, schema *types.Schema, opts *types.QueryOptions) (chan types.APIObject, error) {
	c, err := schema.Store.Watch(apiOp, schema, opts)
	if err != nil {
		return nil, err
	}
	return types.APIChan(c, func(data types.APIObject) types.APIObject {
		return apiOp.FilterObject(nil, schema, data)
	}), nil
}

func streamStore(ctx context.Context, eg *errgroup.Group, apiOp *types.APIRequest, schema *types.Schema, result chan types.APIObject) {
	eg.Go(func() error {
		opts := parse.QueryOptions(apiOp, schema)
		events, err := watch(apiOp, schema, &opts)
		if err != nil || events == nil {
			if err != nil {
				logrus.Errorf("failed on subscribe %s: %v", schema.ID, err)
			}
			return err
		}

		logrus.Debugf("watching %s", schema.ID)

		for e := range events {
			result <- e
		}

		return errors.New("disconnect")
	})
}

func matches(items []string, item string) bool {
	if len(items) == 0 {
		return true
	}
	return slice.ContainsString(items, item)
}
