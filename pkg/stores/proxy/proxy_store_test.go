package proxy

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/client"
	"github.com/rancher/wrangler/v2/pkg/schemas"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	schema2 "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
	clientgotesting "k8s.io/client-go/testing"
)

var c *watch.FakeWatcher

type testFactory struct {
	*client.Factory

	fakeClient *fake.FakeDynamicClient
}

func TestWatchNamesErrReceive(t *testing.T) {
	testClientFactory, err := client.NewFactory(&rest.Config{}, false)
	assert.Nil(t, err)

	fakeClient := fake.NewSimpleDynamicClient(runtime.NewScheme())
	c = watch.NewFakeWithChanSize(5, true)
	defer c.Stop()
	errMsgsToSend := []string{"err1", "err2", "err3"}
	c.Add(&v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "testsecret1"}})
	for index := range errMsgsToSend {
		c.Error(&metav1.Status{
			Message: errMsgsToSend[index],
		})
	}
	c.Add(&v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "testsecret2"}})
	fakeClient.PrependWatchReactor("*", func(action clientgotesting.Action) (handled bool, ret watch.Interface, err error) {
		return true, c, nil
	})
	testStore := Store{
		clientGetter: &testFactory{Factory: testClientFactory,
			fakeClient: fakeClient,
		},
	}
	apiSchema := &types.APISchema{Schema: &schemas.Schema{Attributes: map[string]interface{}{"table": "something"}}}
	wc, err := testStore.WatchNames(&types.APIRequest{Namespace: "", Schema: apiSchema, Request: &http.Request{}}, apiSchema, types.WatchRequest{}, sets.NewString("testsecret1", "testsecret2"))
	assert.Nil(t, err)

	eg := errgroup.Group{}
	eg.Go(func() error { return receiveUntil(wc, 5*time.Second) })

	err = eg.Wait()
	assert.Nil(t, err)

	assert.Equal(t, 0, len(c.ResultChan()), "Expected all secrets to have been received")
}

func TestByNames(t *testing.T) {
	s := Store{}
	apiSchema := &types.APISchema{Schema: &schemas.Schema{}}
	apiOp := &types.APIRequest{Namespace: "*", Schema: apiSchema, Request: &http.Request{}}
	names := sets.NewString("some-resource", "some-other-resource")
	result, warn, err := s.ByNames(apiOp, apiSchema, names)
	assert.NotNil(t, result)
	assert.Len(t, result.Items, 0)
	assert.Nil(t, err)
	assert.Nil(t, warn)
}

func (t *testFactory) TableAdminClientForWatch(ctx *types.APIRequest, schema *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error) {
	return t.fakeClient.Resource(schema2.GroupVersionResource{}), nil
}

func receiveUntil(wc chan watch.Event, d time.Duration) error {
	timer := time.NewTicker(d)
	defer timer.Stop()
	secretNames := []string{"testsecret1", "testsecret2"}
	errMsgs := []string{"err1", "err2", "err3"}
	for {
		select {
		case event, ok := <-wc:
			if !ok {
				return errors.New("watch chan should not have been closed")
			}

			if event.Type == watch.Error {
				status, ok := event.Object.(*metav1.Status)
				if !ok {
					continue
				}
				if strings.HasSuffix(status.Message, errMsgs[0]) {
					errMsgs = errMsgs[1:]
				}
			}
			secret, ok := event.Object.(*v1.Secret)
			if !ok {
				continue
			}
			if secret.Name == secretNames[0] {
				secretNames = secretNames[1:]
			}
			if len(secretNames) == 0 && len(errMsgs) == 0 {
				return nil
			}
			continue
		case <-timer.C:
			return errors.New("timed out waiting to receiving objects from chan")
		}
	}
}
