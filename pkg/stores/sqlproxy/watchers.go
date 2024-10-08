package sqlproxy

import (
	"context"
	"sync"
	"time"

	"github.com/rancher/lasso/pkg/cache/sql/informer"
)

var _ informer.Listener = (*debounceListener)(nil)

type debounceListener struct {
	lock     sync.Mutex
	notified bool

	debounceRate time.Duration
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
	ticker := time.NewTicker(d.debounceRate)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			close(d.ch)
			return
		case <-ticker.C:
			d.lock.Lock()
			if d.notified {
				d.ch <- struct{}{}
			}
			d.notified = false
			d.lock.Unlock()
		}
	}
}

func (d *debounceListener) NotifyNow() {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.ch <- struct{}{}
}

func (d *debounceListener) Notify() {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.notified = true
}
