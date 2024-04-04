package definitions

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rancher/steve/pkg/debounce"
	"github.com/stretchr/testify/require"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiregv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

type refreshable struct {
	refreshChannel chan struct{}
}

func (r *refreshable) Refresh() error {
	r.refreshChannel <- struct{}{}
	return nil
}

func Test_onChangeCRD(t *testing.T) {
	t.Parallel()
	refreshChannel := make(chan struct{}, 1)
	defer close(refreshChannel)
	internalRefresh := refreshable{
		refreshChannel: refreshChannel,
	}
	refresher := debounce.DebounceableRefresher{
		Refreshable: &internalRefresh,
	}
	refreshHandler := refreshHandler{
		debounceRef:      &refresher,
		debounceDuration: time.Microsecond * 2,
	}
	input := apiextv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-crd",
		},
	}
	output, err := refreshHandler.onChangeCRD("test-crd", &input)
	require.Nil(t, err)
	require.Equal(t, input, *output)
	err = receiveWithTimeout(refreshChannel, time.Second*5)
	require.NoError(t, err)
}

func Test_onChangeAPIService(t *testing.T) {
	t.Parallel()
	refreshChannel := make(chan struct{}, 1)
	defer close(refreshChannel)
	internalRefresh := refreshable{
		refreshChannel: refreshChannel,
	}
	refresher := debounce.DebounceableRefresher{
		Refreshable: &internalRefresh,
	}
	refreshHandler := refreshHandler{
		debounceRef:      &refresher,
		debounceDuration: time.Microsecond * 2,
	}
	input := apiregv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-apiservice",
		},
	}
	output, err := refreshHandler.onChangeAPIService("test-apiservice", &input)
	require.Nil(t, err)
	require.Equal(t, input, *output)
	err = receiveWithTimeout(refreshChannel, time.Second*5)
	require.NoError(t, err)

}

func Test_startBackgroundRefresh(t *testing.T) {
	t.Parallel()
	refreshChannel := make(chan struct{}, 1)
	internalRefresh := refreshable{
		refreshChannel: refreshChannel,
	}
	refresher := debounce.DebounceableRefresher{
		Refreshable: &internalRefresh,
	}
	refreshHandler := refreshHandler{
		debounceRef:      &refresher,
		debounceDuration: time.Microsecond * 2,
	}
	ctx, cancel := context.WithCancel(context.Background())
	refreshHandler.startBackgroundRefresh(ctx, time.Microsecond*2)

	err := receiveWithTimeout(refreshChannel, time.Second*5)
	require.NoError(t, err)
	// we want to stop the refresher before closing the channel to avoid errors
	// since this just stops the background refresh from asking for a new refresh, we still
	// need to wait for any currently debounced refreshes to finish
	cancel()
	time.Sleep(time.Second * 1)
	close(refreshChannel)
}

func receiveWithTimeout(channel chan struct{}, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	select {
	case <-channel:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("channel did not recieve value in timeout %d", timeout)
	}
}
