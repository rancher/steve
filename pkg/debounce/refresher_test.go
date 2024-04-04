package debounce

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type refreshable struct {
	refreshChannel chan struct{}
	cancelChannel  chan struct{}
	retErr         error
}

func (r *refreshable) Refresh() error {
	r.refreshChannel <- struct{}{}
	return r.retErr
}

func (r *refreshable) onCancel() {
	r.cancelChannel <- struct{}{}
}

func TestRefreshAfter(t *testing.T) {
	t.Parallel()
	refreshChannel := make(chan struct{}, 1)
	cancelChannel := make(chan struct{}, 1)
	ref := refreshable{
		refreshChannel: refreshChannel,
		cancelChannel:  cancelChannel,
	}
	debounce := DebounceableRefresher{
		Refreshable: &ref,
		onCancel:    ref.onCancel,
	}
	debounce.RefreshAfter(time.Millisecond * 100)
	debounce.RefreshAfter(time.Millisecond * 10)
	err := receiveWithTimeout(cancelChannel, time.Second*5)
	require.NoError(t, err)
	err = receiveWithTimeout(refreshChannel, time.Second*5)
	require.NoError(t, err)
	close(refreshChannel)
	close(cancelChannel)

	// test the error case
	refreshChannel = make(chan struct{}, 1)
	defer close(refreshChannel)
	ref = refreshable{
		retErr:         fmt.Errorf("Some error"),
		refreshChannel: refreshChannel,
	}
	debounce = DebounceableRefresher{
		Refreshable: &ref,
	}
	debounce.RefreshAfter(time.Millisecond * 100)
	err = receiveWithTimeout(refreshChannel, time.Second*5)
	require.NoError(t, err)
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
