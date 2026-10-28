package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
)

var testdataRelistDir = filepath.Join("testdata", "relist")

// relistTestConfig is the header of a relist .test.yaml file. It names the
// resource whose relist is under test (used for a direct apiserver list) and
// the steve list path to query.
type relistTestConfig struct {
	ListPath string `yaml:"listPath"`
	Group    string `yaml:"group"`
	Version  string `yaml:"version"`
	Resource string `yaml:"resource"`
}

// TestRelistRevisionConsistency is an end-to-end regression test for
// rancher/rancher#56187. During its initial full relist (Store.Replace), the
// sqlite (vai) cache re-adds every object. The buggy version iterated a map to
// do so, leaving the cache's advertised revision at an arbitrary per-object
// revision at or below the real collection revision. A later list supplying a
// valid, higher revision then failed checkRevision with "unknown revision"
// (HTTP 400) and never recovered.
//
// Each scenario's objects live in testdata/relist/*.test.yaml. The fixtures
// include an unrelated trailing write so the collection revision the informer
// syncs at is strictly greater than every target object's revision, which makes
// the bug deterministic.
func (i *IntegrationSuite) TestRelistRevisionConsistency() {
	ctx := i.T().Context()

	matches, err := filepath.Glob(filepath.Join(testdataRelistDir, "*.test.yaml"))
	i.Require().NoError(err)

	for _, match := range matches {
		name := strings.TrimSuffix(filepath.Base(match), ".test.yaml")
		i.Run(name, func() {
			i.testRelistScenario(ctx, match)
		})
	}
}

func (i *IntegrationSuite) testRelistScenario(ctx context.Context, scenarioFile string) {
	var cfg relistTestConfig
	gvrs := make(map[k8sschema.GroupVersionResource]struct{})
	i.doManifestWithHeader(ctx, scenarioFile,
		func(_ context.Context, header map[string]any) error {
			return mapToObj(header, &cfg)
		},
		func(ctx context.Context, obj *unstructured.Unstructured, gvr k8sschema.GroupVersionResource) error {
			gvrs[gvr] = struct{}{}
			return i.doApply(ctx, obj, gvr)
		},
	)
	defer i.doManifestReversed(ctx, scenarioFile, i.doDelete)

	targetGVR := k8sschema.GroupVersionResource{Group: cfg.Group, Version: cfg.Version, Resource: cfg.Resource}

	// Read the collection revision straight from the apiserver *before* steve
	// lists the type. Because the fixtures include a trailing unrelated write,
	// this revision is strictly greater than every target object's own revision.
	// Capturing it before steve starts guarantees steve's own relist syncs at a
	// revision at or above it.
	targetList, err := i.client.Resource(targetGVR).List(ctx, metav1.ListOptions{})
	i.Require().NoError(err)
	collectionRV := targetList.GetResourceVersion()
	wantCount := len(targetList.Items)

	steveHandler, err := server.New(ctx, i.restCfg, &server.Options{
		SQLCacheFactoryOptions: factory.CacheFactoryOptions{
			GCInterval:  15 * time.Minute,
			GCKeepCount: 1000,
			UseTempDir:  true,
		},
	})
	i.Require().NoError(err)

	httpServer := httptest.NewServer(steveHandler)
	defer httpServer.Close()

	baseURL := httpServer.URL
	for gvr := range gvrs {
		i.waitForSchema(baseURL, gvr)
	}
	defer i.maybeStopAndDebug(baseURL)

	// The first list triggers the informer's initial relist (Store.Replace).
	// Wait until every fixture is visible so the sync has completed, then read
	// the advertised revision.
	var advertisedRV string
	i.Require().Eventually(func() bool {
		data, rv := i.steveList(ctx, baseURL, cfg.ListPath, "")
		if len(data) < wantCount {
			return false
		}
		advertisedRV = rv
		return true
	}, 20*time.Second, 200*time.Millisecond, "informer never finished its initial relist")

	// (1) The advertised revision must be the collection revision from the
	// relist, not an arbitrary per-object revision below it.
	i.Require().GreaterOrEqual(mustAtoi(i.T(), advertisedRV), mustAtoi(i.T(), collectionRV),
		"advertised list revision %s must be >= the collection revision %s", advertisedRV, collectionRV)

	// (2) A list supplying the collection revision must succeed rather than fail
	// with "unknown revision". Before the fix the cache advertised a lower
	// revision, so checkRevision rejected this valid, higher revision with 400.
	status, body := i.steveGet(ctx, fmt.Sprintf("%s/v1/%s?revision=%s", baseURL, cfg.ListPath, collectionRV), "")
	i.Require().Equal(http.StatusOK, status,
		"list at the collection revision %s must not return unknown revision; body: %s", collectionRV, body)
}

// steveList GETs a steve list and returns its data and advertised revision.
func (i *IntegrationSuite) steveList(ctx context.Context, baseURL, listPath, user string) ([]map[string]any, string) {
	status, body := i.steveGet(ctx, fmt.Sprintf("%s/v1/%s", baseURL, listPath), user)
	i.Require().Equal(http.StatusOK, status, "listing %s; body: %s", listPath, body)
	var parsed struct {
		Data     []map[string]any `json:"data"`
		Revision string           `json:"revision"`
	}
	i.Require().NoError(json.Unmarshal([]byte(body), &parsed))
	return parsed.Data, parsed.Revision
}

func (i *IntegrationSuite) steveGet(ctx context.Context, url, user string) (int, string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	i.Require().NoError(err)
	if user != "" {
		req.Header.Set("Impersonate-User", user)
	}
	resp, err := http.DefaultClient.Do(req)
	i.Require().NoError(err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	i.Require().NoError(err)
	return resp.StatusCode, string(body)
}

func mustAtoi(t interface{ Fatalf(string, ...any) }, s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		t.Fatalf("resourceVersion %q is not numeric: %v", s, err)
	}
	return n
}
