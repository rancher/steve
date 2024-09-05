package sqlproxy

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/rancher/lasso/pkg/cache/sql/informer"
)

var _ informer.Listener = (*debounceListener)(nil)

type debounceListener struct {
	debounceRate time.Duration
	notified     atomic.Bool
	ch           chan struct{}
}

func newDebounceListener(debounceRate time.Duration) *debounceListener {
	listener := &debounceListener{
		debounceRate: debounceRate,
		ch:           make(chan struct{}, 100),
	}
	return listener
}

func (d *debounceListener) Run(ctx context.Context) {
	d.ch <- struct{}{}
	ticker := time.NewTicker(d.debounceRate)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !d.notified.Swap(false) {
				continue
			}
			d.ch <- struct{}{}
		}
	}
}

func (d *debounceListener) Notify() {
	d.notified.Store(true)
}

func (d *debounceListener) Close() {
	close(d.ch)
}
