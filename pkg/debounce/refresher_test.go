package debounce

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type refreshable struct {
	wasRefreshed atomic.Bool
	retErr       error
}

func (r *refreshable) Refresh() error {
	r.wasRefreshed.Store(true)
	return r.retErr
}

func TestRefreshAfter(t *testing.T) {
	ref := refreshable{}
	debounce := DebounceableRefresher{
		Refreshable: &ref,
	}
	debounce.RefreshAfter(time.Millisecond * 2)
	debounce.RefreshAfter(time.Microsecond * 2)
	time.Sleep(time.Millisecond * 1)
	// test that the second refresh call overrode the first - Micro < Milli so this should have ran
	require.True(t, ref.wasRefreshed.Load())
	ref.wasRefreshed.Store(false)
	time.Sleep(time.Millisecond * 2)
	// test that the call was debounced - though we called this twice only one refresh should be called
	require.False(t, ref.wasRefreshed.Load())

	ref = refreshable{
		retErr: fmt.Errorf("Some error"),
	}
	debounce = DebounceableRefresher{
		Refreshable: &ref,
	}
	debounce.RefreshAfter(time.Microsecond * 2)
	// test the error case
	time.Sleep(time.Millisecond * 1)
	require.True(t, ref.wasRefreshed.Load())
}
