package sqlproxy

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rancher/lasso/pkg/cache/sql/informer"
)

var _ informer.Listener = (*debounceListener)(nil)

type debounceListener struct {
	lock         sync.Mutex
	lastRevision string

	debounceRate time.Duration
	ch           chan string
}

func newDebounceListener(debounceRate time.Duration) *debounceListener {
	listener := &debounceListener{
		debounceRate: debounceRate,
		ch:           make(chan string, 100),
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
			if d.lastRevision != "" {
				d.ch <- d.lastRevision
				d.lastRevision = ""
			}
			d.lock.Unlock()
		}
	}
}

func (d *debounceListener) NotifyNow(revision string) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.ch <- revision
}

func (d *debounceListener) Notify(revision string) {
	fmt.Println("Notify(", revision, ")")
	d.lock.Lock()
	defer d.lock.Unlock()
	d.lastRevision = revision
}
