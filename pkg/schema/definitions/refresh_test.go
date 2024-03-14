package definitions

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rancher/steve/pkg/debounce"
	"github.com/stretchr/testify/require"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiregv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

type refreshable struct {
	wasRefreshed atomic.Bool
}

func (r *refreshable) Refresh() error {
	r.wasRefreshed.Store(true)
	return nil
}

func Test_onChangeCRD(t *testing.T) {
	internalRefresh := refreshable{}
	refresher := debounce.DebounceableRefresher{
		Refreshable: &internalRefresh,
	}
	refreshHandler := refreshHandler{
		debounceRef:      &refresher,
		debounceDuration: time.Microsecond * 5,
	}
	input := apiextv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-crd",
		},
	}
	output, err := refreshHandler.onChangeCRD("test-crd", &input)
	require.Nil(t, err)
	require.Equal(t, input, *output)
	// waiting to allow the debouncer to refresh the refreshable
	time.Sleep(time.Millisecond * 2)
	require.True(t, internalRefresh.wasRefreshed.Load())
}

func Test_onChangeAPIService(t *testing.T) {
	internalRefresh := refreshable{}
	refresher := debounce.DebounceableRefresher{
		Refreshable: &internalRefresh,
	}
	refreshHandler := refreshHandler{
		debounceRef:      &refresher,
		debounceDuration: time.Microsecond * 5,
	}
	input := apiregv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-apiservice",
		},
	}
	output, err := refreshHandler.onChangeAPIService("test-apiservice", &input)
	require.Nil(t, err)
	require.Equal(t, input, *output)
	// waiting to allow the debouncer to refresh the refreshable
	time.Sleep(time.Millisecond * 2)
	require.True(t, internalRefresh.wasRefreshed.Load())

}

func Test_startBackgroundRefresh(t *testing.T) {
	internalRefresh := refreshable{}
	refresher := debounce.DebounceableRefresher{
		Refreshable: &internalRefresh,
	}
	refreshHandler := refreshHandler{
		debounceRef:      &refresher,
		debounceDuration: time.Microsecond * 5,
	}
	ctx, cancel := context.WithCancel(context.Background())
	refreshHandler.startBackgroundRefresh(ctx, time.Microsecond*10)
	time.Sleep(time.Millisecond * 2)
	require.True(t, internalRefresh.wasRefreshed.Load())
	cancel()
}
