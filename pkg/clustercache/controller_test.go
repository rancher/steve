package clustercache_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/clustercache"
	"github.com/rancher/steve/pkg/schema"
	"github.com/rancher/wrangler/v3/pkg/schemas"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

var testSchema = map[string]*types.APISchema{
	"rbac.authorization.k8s.io.clusterrole": &types.APISchema{
		Schema: &schemas.Schema{
			ID:         "rbac.authorization.k8s.io.clusterrole",
			PluralName: "rbac.authorization.k8s.io.clusterroles",
			Attributes: map[string]any{
				"group":      "rbac.authorization.k8s.io",
				"kind":       "ClusterRole",
				"namespaced": false,
				"resource":   "clusterroles",
				"verbs":      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				"version":    "v1",
			},
		},
	},
	"configmap": &types.APISchema{
		Schema: &schemas.Schema{
			ID:         "configmap",
			PluralName: "configmaps",
			Attributes: map[string]any{
				"kind":       "ConfigMap",
				"namespaced": true,
				"resource":   "configmaps",
				"verbs":      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				"version":    "v1",
			},
		},
	},
	"secret": &types.APISchema{
		Schema: &schemas.Schema{
			ID:         "secret",
			PluralName: "secrets",
			Attributes: map[string]any{
				"kind":       "Secret",
				"namespaced": true,
				"resource":   "secrets",
				"verbs":      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				"version":    "v1",
			},
		},
	},
	"service": &types.APISchema{
		Schema: &schemas.Schema{
			ID:         "service",
			PluralName: "services",
			Attributes: map[string]any{
				"kind":       "Service",
				"namespaced": true,
				"resource":   "services",
				"verbs":      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				"version":    "v1"},
		},
	},
}

func TestClusterCacheRateLimitingNotEnabled(t *testing.T) {
	var errorCount int32
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	// The cache makes spawns a Go routine that makes 2 requests for each schema
	// (list and watch).
	// This configures the test-server to rate-limit responses.
	requestCount := len(testSchema) * 2
	// The cache spawns a Go routine that makes 2 requests for each schema
	// (list and watch).
	tstSrv := startTestServer(t, rate.NewLimiter(rate.Limit(1000),
		requestCount-2), &errorCount)

	sf := schema.NewCollection(ctx, &types.APISchemas{}, fakeAccessSetLookup{})
	sf.Reset(testSchema)
	dc, err := dynamic.NewForConfig(&rest.Config{Host: tstSrv.URL})
	assert.NoError(t, err)
	cache := clustercache.NewClusterCache(ctx, dc)

	err = cache.OnSchemas(sf)
	assert.NoError(t, err)

	// We should make requestCount requests in a burst, but the server is
	// limited to requestCount-2 in a burst.
	//
	// We can't really know how many failed requests there will be as this will
	// be impacted by how quickly we start the Go routines (and thus the load on
	// the machine), but as the server is rate-limited to fewer than requests we
	// make, we get 429s.
	assert.NotZero(t, errorCount)
}

func TestClusterCacheRateLimitingEnabled(t *testing.T) {
	var errorCount int32
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	t.Setenv("RANCHER_CACHE_CLIENT_LIMIT", "3")
	requestCount := len(testSchema) * 2
	// The cache spawns a Go routine that makes 2 requests for each schema
	// (list and watch).
	tstSrv := startTestServer(t, rate.NewLimiter(rate.Limit(1000),
		requestCount-2), &errorCount)

	sf := schema.NewCollection(ctx, &types.APISchemas{}, fakeAccessSetLookup{})
	sf.Reset(testSchema)
	dc, err := dynamic.NewForConfig(&rest.Config{Host: tstSrv.URL})
	assert.NoError(t, err)
	cache := clustercache.NewClusterCache(ctx, dc)

	err = cache.OnSchemas(sf)
	assert.NoError(t, err)

	// The cache client is rate-limited to less than the number of requests to
	// be made.
	// The server will allow all requests in a burst, so we should get no 429
	// responses.
	assert.Zero(t, errorCount)
}

// This provides a fake K8s API server that uses the provided rate.Limit to
// rate-limit requests, responding with 429 if the rate-limiter is limiting
// requests.
//
// This only handles list and watch requests for secrets, services, configmaps
// and clusterroles.
//
// The errors value passed in will be incremented every time a 429 response is
// returned to the client (client-go will consume some 429 responses).
func startTestServer(t *testing.T, rl *rate.Limiter, errors *int32) *httptest.Server {
	start := time.Now()

	writeJSON := func(w http.ResponseWriter, s string) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(s))
	}

	writeChunks := func(w http.ResponseWriter, chunks []string) {
		w.Header().Set("Transfer-Encoding", "chunked")
		w.Header().Set("Content-Type", "application/json")
		for _, chunk := range chunks {
			time.Sleep(1 * time.Second)
			fmt.Fprintf(w, chunk)
			w.(http.Flusher).Flush()
		}
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := r.URL.Query()
		watched := values.Get("watch") == "true"
		t.Logf("Faking response to %s watch=%v", r.URL.Path, watched)

		if !rl.Allow() {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Header().Set("Retry-After", "1")
			atomic.AddInt32(errors, 1)
			return
		}

		switch {
		case r.URL.Path == "/api/v1/configmaps" && !watched:
			writeJSON(w, `{"kind":"ConfigMapList","apiVersion":"v1","metadata":{"resourceVersion":"839768"},"items":[{"metadata":{"name":"my-test-configmap","namespace":"default","uid":"05f64c34-711e-4bef-8655-c486fbd6761e","resourceVersion":"629061","creationTimestamp":"2025-06-16T07:41:19Z","managedFields":[{"manager":"kubectl-create","operation":"Update","apiVersion":"v1","time":"2025-06-16T07:41:19Z","fieldsType":"FieldsV1","fieldsV1":{"f:data":{".":{},"f:testing":{}}}}]},"data":{"testing":"tested"}}]}`)
		case r.URL.Path == "/api/v1/configmaps" && watched:
			chunks := []string{
				`{"type":"ADDED","object":{"apiVersion":"v1","data":{"value":"nothing"},"kind":"ConfigMap","metadata":{"creationTimestamp":"2025-06-18T15:47:32Z","name":"other-test-configmap","namespace":"default","resourceVersion":"855779","uid": "83b9f7c9-05aa-4a8d-bd01-adf1e1089c8d"}}}`,
			}
			writeChunks(w, chunks)
			return

		case r.URL.Path == "/apis/rbac.authorization.k8s.io/v1/clusterroles" && !watched:
			writeJSON(w, `{"kind":"ClusterRoleList","apiVersion":"rbac.authorization.k8s.io/v1","metadata":{"resourceVersion":"840190"},"items":[{"metadata":{"name":"admin","uid":"dfa4f720-857f-4a9a-96ae-2cebdd5b4166","resourceVersion":"1413","creationTimestamp":"2025-05-30T09:33:38Z","labels":{"kubernetes.io/bootstrapping":"rbac-defaults"},"annotations":{"rbac.authorization.kubernetes.io/autoupdate":"true"},"managedFields":[{"manager":"clusterrole-aggregation-controller","operation":"Apply","apiVersion":"rbac.authorization.k8s.io/v1","time":"2025-05-30T10:12:19Z","fieldsType":"FieldsV1","fieldsV1":{"f:rules":{}}},{"manager":"k3s","operation":"Update","apiVersion":"rbac.authorization.k8s.io/v1","time":"2025-05-30T09:33:38Z","fieldsType":"FieldsV1","fieldsV1":{"f:aggregationRule":{".":{},"f:clusterRoleSelectors":{}},"f:metadata":{"f:annotations":{".":{},"f:rbac.authorization.kubernetes.io/autoupdate":{}},"f:labels":{".":{},"f:kubernetes.io/bootstrapping":{}}}}}]},"rules":[{"verbs":["create","delete","deletecollection","patch","update"],"apiGroups":["cert-manager.io"],"resources":["certificates","certificaterequests","issuers"]}]}]}`)
		case r.URL.Path == "/apis/rbac.authorization.k8s.io/v1/clusterroles" && watched:
			chunks := []string{
				`{"type":"ADDED","object":{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRole","metadata":{"creationTimestamp":"2025-05-30T10:12:19Z","name":"cert-manager-view","resourceVersion":"1399","uid":"06c404a0-895c-45db-a506-9ab6c7336d0f"}}}`,
			}
			writeChunks(w, chunks)
			return
		case r.URL.Path == "/api/v1/secrets" && !watched:
			writeJSON(w, `{"kind":"SecretList","apiVersion":"v1","metadata":{"resourceVersion":"839768"},"items":[{"metadata":{"name":"my-test-secret","namespace":"default","uid":"975dab7f-0b57-4d57-aaf8-f95851fac6e5","resourceVersion":"629061","creationTimestamp":"2025-06-16T07:41:19Z","managedFields":[]},"data":{"testing":"tested"}}]}`)
			return
		case r.URL.Path == "/api/v1/secrets" && watched:
			chunks := []string{
				`{"type":"ADDED","object":{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"Secret","metadata":{"creationTimestamp":"2025-05-30T10:12:19Z","name":"new-test-secret","resourceVersion":"629062","uid":"338ca29a-44ef-45ce-9e21-6479766a008f"}}}`,
			}
			writeChunks(w, chunks)
			return
		case r.URL.Path == "/api/v1/services" && !watched:
			writeJSON(w, `{"kind":"ServiceList","apiVersion":"v1","metadata":{"resourceVersion":"839768"},"items":[{"metadata":{"name":"my-test-service","namespace":"default","uid":"8c0ab3eb-a5b7-428f-82f3-a1e77d11b596","resourceVersion":"629061","creationTimestamp":"2025-06-16T07:41:19Z","managedFields":[]}}]}`)
			return
		case r.URL.Path == "/api/v1/services" && watched:
			chunks := []string{
				`{"type":"ADDED","object":{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"Service","metadata":{"creationTimestamp":"2025-06-19T08:12:19Z","name":"new-test-service","resourceVersion":"629063","uid":"e5d5f856-c961-4df8-b1eb-234195b5556a"}}}`,
			}
			writeChunks(w, chunks)
			return

		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
	}))

	t.Cleanup(func() {
		t.Logf("%v errors in %v", *errors, time.Since(start))
		ts.Close()
	})

	return ts
}

type fakeAccessSetLookup struct {
}

func (a fakeAccessSetLookup) AccessFor(user user.Info) *accesscontrol.AccessSet {
	return nil
}

func (a fakeAccessSetLookup) PurgeUserData(id string) {
}
