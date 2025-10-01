package debounce

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Refreshable represents an object which can be refreshed. This should be protected by a mutex for concurrent operation.
type Refreshable interface {
	Refresh() error
}

// DebounceableRefresher is used to debounce multiple attempts to refresh a refreshable type.
type DebounceableRefresher struct {
	*sync.Cond
	scheduled     *time.Timer
	shouldRefresh bool
}

func NewDebounceableRefresher(ctx context.Context, refreshable Refreshable, duration time.Duration) *DebounceableRefresher {
	dr := &DebounceableRefresher{Cond: sync.NewCond(&sync.Mutex{})}

	// Refresh loop
	go func() {
		for {
			// Check for condition while holding the lock
			dr.L.Lock()
			for !dr.shouldRefresh {
				// The lock is released while waiting, and acquired when returned
				dr.Wait()
				if ctx.Err() != nil {
					dr.L.Unlock()
					return
				}
			}
			dr.shouldRefresh = false
			dr.L.Unlock()

			if err := refreshable.Refresh(); err != nil {
				logrus.Errorf("failed to refresh with error: %v", err)
			}
		}
	}()

	// Cancellation and scheduling goroutine
	go func() {
		// Allow disabling ticker if a negative or zero duration is provided
		var tick <-chan time.Time
		if duration > 0 {
			ticker := time.NewTicker(duration)
			defer ticker.Stop()
			tick = ticker.C
		}
		for {
			select {
			case <-ctx.Done():
				// Signaling to wake the processing loop in case it's waiting, so it can observe the canceled context
				dr.signalRefresh()
				return
			case <-tick:
				dr.signalRefresh()
			}
		}
	}()
	return dr
}

func (dr *DebounceableRefresher) signalRefresh() {
	dr.L.Lock()
	dr.shouldRefresh = true
	dr.Signal()
	dr.L.Unlock()
}

func (dr *DebounceableRefresher) Schedule(after time.Duration) {
	dr.L.Lock()
	defer dr.L.Unlock()

	if dr.scheduled != nil {
		dr.scheduled.Stop()
	}
	dr.scheduled = time.AfterFunc(after, dr.signalRefresh)
}
