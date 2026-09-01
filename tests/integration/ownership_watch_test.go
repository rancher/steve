package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rancher/steve/pkg/auth"
	"github.com/rancher/steve/pkg/resources/ownership"
	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
)

// subscribeEvent is a minimal decoding of the JSON messages steve sends over
// a /v1/subscribe websocket -- see github.com/rancher/apiserver/pkg/subscribe.
type subscribeEvent struct {
	Name string `json:"name"`
	Data struct {
		ID       string `json:"id"`
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Error string `json:"error"`
	} `json:"data"`
}

// TestOwnershipFiltersWatchEvents is a regression test for
// rancher/rancher#56849: the per-user ownership.Filter that
// ListByPartitions applies to list/count results for resources like
// ext.cattle.io Tokens and Kubeconfigs was never applied to the watch path
// (Store.watch, used by both /v1/subscribe and WatchNames). A user
// subscribed to such a resource type would receive resource.change events
// for every object of that type, including ones owned by other users, even
// though the same user's list/get requests correctly filtered them out.
//
// This test registers an ownership.Filter for the Banana CRD's GVK (same
// approach as TestOwnershipCountMatchesList) and opens a real websocket
// /v1/subscribe connection as a non-admin user. It asserts that user only
// receives resource.change events for bananas they own, and that they do
// still receive events for their own bananas (a filter-too-aggressive bug
// would silently look the same as "no leak" if we only checked for the
// absence of other users' events).
func (i *IntegrationSuite) TestOwnershipFiltersWatchEvents() {
	ctx, cancel := context.WithTimeout(i.T().Context(), 60*time.Second)
	defer cancel()

	ownership.Register(bananaGVK, ownership.NewUserIDLabelFilter("cattle.io/user-id"))
	defer ownership.Unregister(bananaGVK)

	manifestsFile := "testdata/ownership/common.manifests.yaml"
	gvrs := make(map[k8sschema.GroupVersionResource]struct{})
	i.doManifest(ctx, manifestsFile, func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
		gvrs[gvr] = struct{}{}
		return i.doApply(ctx, obj, gvr)
	})
	defer i.doManifestReversed(ctx, manifestsFile, i.doDelete)

	impersonateOrAdmin := func(req *http.Request) (user.Info, bool, error) {
		info, ok, err := auth.Impersonation(req)
		if ok || err != nil {
			return info, ok, err
		}
		return auth.AlwaysAdmin(req)
	}
	authMiddleware := auth.ToMiddleware(auth.AuthenticatorFunc(impersonateOrAdmin))

	steveHandler, err := server.New(ctx, i.restCfg, &server.Options{
		SQLCacheFactoryOptions: factory.CacheFactoryOptions{
			GCInterval:  15 * time.Minute,
			GCKeepCount: 1000,
			UseTempDir:  true,
		},
		AuthMiddleware: authMiddleware,
	})
	i.Require().NoError(err)

	httpServer := httptest.NewServer(steveHandler)
	defer httpServer.Close()

	baseURL := httpServer.URL
	for gvr := range gvrs {
		i.waitForSchema(baseURL, gvr)
	}
	defer i.maybeStopAndDebug(baseURL)

	// Subscribe as user-alice. She owns banana-alice-1 and banana-alice-2,
	// and has RBAC list/watch access to all bananas (see
	// common.manifests.yaml) -- if the ownership filter were missing from
	// the watch path, she'd receive events for banana-bob-1 too.
	conn := i.dialSubscribe(baseURL, "user-alice", bananaSchemaID)
	defer conn.Close()

	events := make(chan subscribeEvent, 16)
	go func() {
		for {
			var evt subscribeEvent
			if err := conn.ReadJSON(&evt); err != nil {
				close(events)
				return
			}
			events <- evt
		}
	}()

	// Wait for the initial resource.start ack before mutating anything, so
	// we know the subscription is live.
	i.waitForSubscribeEvent(events, "resource.start", "", 10*time.Second)

	// The informer backing the watch is created lazily on first access and
	// takes a moment to fully sync -- if we mutate bananas before it's
	// ready, the mutations just land in its initial List instead of firing
	// as watch events. Force it to warm up by polling the regular list
	// endpoint until all four fixture bananas are visible.
	i.Require().Eventually(func() bool {
		return i.countBananasViaList(ctx, baseURL, "") == 4
	}, 15*time.Second, 200*time.Millisecond, "banana informer never finished its initial sync")

	// Update bob's banana first. If the fix regresses, this is the event
	// that would leak to alice.
	i.patchBananaLabel(ctx, "banana-bob-1", "leaked", "yes")

	// Then update alice's own banana. This must arrive -- it proves the
	// filter isn't just dropping every event (too aggressive == same
	// observable behavior as "no leak" if we only checked for bob's event).
	i.patchBananaLabel(ctx, "banana-alice-1", "own-update", "yes")

	// Collect every resource.change event that arrives within the window,
	// instead of discarding non-matching ones while searching for alice's
	// own event -- otherwise a leaked banana-bob-1 event that happens to
	// arrive before banana-alice-1's would be silently thrown away by the
	// search itself, defeating the whole point of the test.
	var changeEvents []subscribeEvent
	deadline := time.After(15 * time.Second)
collect:
	for {
		select {
		case evt, ok := <-events:
			if !ok {
				break collect
			}
			if evt.Name != "resource.change" {
				continue
			}
			changeEvents = append(changeEvents, evt)
			if evt.Data.Metadata.Name == "banana-alice-1" {
				// Give any already-in-flight events a brief moment to also
				// arrive before we stop collecting.
				select {
				case <-time.After(2 * time.Second):
				case <-deadline:
				}
				break collect
			}
		case <-deadline:
			break collect
		}
	}

	var sawOwnUpdate bool
	for _, evt := range changeEvents {
		require.NotEqual(i.T(), "banana-bob-1", evt.Data.Metadata.Name,
			"alice must not receive watch events for bananas she doesn't own")
		if evt.Data.Metadata.Name == "banana-alice-1" {
			sawOwnUpdate = true
		}
	}
	require.True(i.T(), sawOwnUpdate,
		"alice should receive resource.change events for her own bananas")
}

// dialSubscribe opens a websocket connection to steve's /v1/subscribe
// endpoint impersonating the given user, and sends the initial subscribe
// message for resourceType.
func (i *IntegrationSuite) dialSubscribe(baseURL, username, resourceType string) *websocket.Conn {
	wsURL := strings.Replace(baseURL, "http://", "ws://", 1)
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)
	wsURL += "/v1/subscribe"

	header := http.Header{}
	if username != "" {
		header.Set("Impersonate-User", username)
	}

	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if resp != nil {
		defer resp.Body.Close()
	}
	i.Require().NoError(err, "dialing %s", wsURL)

	payload, err := json.Marshal(map[string]string{"resourceType": resourceType})
	i.Require().NoError(err)
	i.Require().NoError(conn.WriteMessage(websocket.TextMessage, payload))

	return conn
}

// waitForSubscribeEvent blocks until an event of the given name arrives
// (optionally filtered further by the object name it concerns), or fails
// the test after timeout.
func (i *IntegrationSuite) waitForSubscribeEvent(events <-chan subscribeEvent, name, objectName string, timeout time.Duration) subscribeEvent {
	deadline := time.After(timeout)
	for {
		select {
		case evt, ok := <-events:
			if !ok {
				i.Require().FailNow(fmt.Sprintf("subscribe channel closed while waiting for %q event", name))
			}
			if evt.Name != name {
				continue
			}
			if objectName != "" && evt.Data.Metadata.Name != objectName {
				continue
			}
			return evt
		case <-deadline:
			i.Require().FailNow(fmt.Sprintf("timed out waiting for %q event (object=%q)", name, objectName))
			return subscribeEvent{}
		}
	}
}

func (i *IntegrationSuite) patchBananaLabel(ctx context.Context, name, labelKey, labelValue string) {
	gvr := k8sschema.GroupVersionResource{Group: bananaGVK.Group, Version: bananaGVK.Version, Resource: "bananas"}
	obj, err := i.client.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
	i.Require().NoError(err)

	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[labelKey] = labelValue
	obj.SetLabels(labels)

	_, err = i.client.Resource(gvr).Update(ctx, obj, metav1.UpdateOptions{})
	i.Require().NoError(err)
}
