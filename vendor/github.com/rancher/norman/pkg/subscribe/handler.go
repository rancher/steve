package subscribe

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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

	events := make(chan types.APIEvent)
	for _, schema := range schemas {
		if apiOp.AccessControl.CanWatch(apiOp, schema) == nil {
			streamStore(ctx, readerGroup, apiOp, schema, events)
		}
	}

	go func() {
		readerGroup.Wait()
		close(events)
	}()

	capture := &Capture{}
	captureWriter := writer.EncodingResponseWriter{
		ContentType: "application/json",
		Encoder:     capture.Encoder,
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

			schema := apiOp.Schemas.Schema(convert.ToString(item.Object.Map()["type"]))
			if schema != nil {
				if err := captureWriter.VersionBody(apiOp, nil, item.Object); err != nil {
					cancel()
					continue
				}

				item.Object = types.ToAPI(capture.Object)
				if err := writeData(c, item); err != nil {
					cancel()
				}
			}
		case <-t.C:
			if err := writeData(c, types.APIEvent{Name: "ping"}); err != nil {
				cancel()
			}
		}
	}

	// no point in ever returning null because the connection is hijacked and we can't write it
	return nil
}

func writeData(c *websocket.Conn, event types.APIEvent) error {
	event.Data = event.Object.Raw()
	messageWriter, err := c.NextWriter(websocket.TextMessage)
	if err != nil {
		return err
	}
	defer messageWriter.Close()

	return json.NewEncoder(messageWriter).Encode(event)
}

func watch(apiOp *types.APIRequest, schema *types.Schema, opts *types.QueryOptions) (chan types.APIEvent, error) {
	c, err := schema.Store.Watch(apiOp, schema, opts)
	if err != nil {
		return nil, err
	}
	return types.APIChan(c, func(data types.APIEvent) types.APIEvent {
		data.Object = apiOp.FilterObject(nil, schema, data.Object)
		return data
	}), nil
}

func streamStore(ctx context.Context, eg *errgroup.Group, apiOp *types.APIRequest, schema *types.Schema, result chan types.APIEvent) {
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

type Capture struct {
	Object interface{}
}

func (c *Capture) Encoder(w io.Writer, obj interface{}) error {
	c.Object = obj
	return nil
}
