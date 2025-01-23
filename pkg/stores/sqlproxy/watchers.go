package sqlproxy

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rancher/steve/pkg/sqlcache/informer"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
)

var _ informer.Listener = (*debounceListener)(nil)

type debounceListener struct {
	lock         sync.Mutex
	lastRevision string

	debounceRate time.Duration
	ch           chan string

	filterName      string
	filterNamespace string
	filterSelector  string
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

func (d *debounceListener) Notify(revision string, oldObj any, newObj any) {
	if !d.matchFilters(oldObj) && !d.matchFilters(newObj) {
		return
	}

	fmt.Println("Notify(", revision, ")")
	d.lock.Lock()
	defer d.lock.Unlock()
	d.lastRevision = revision
}

func (d *debounceListener) matchFilters(obj any) bool {
	if obj == nil {
		return false
	}

	metadata, err := meta.Accessor(obj)
	if err != nil {
		return false
	}

	if d.filterName != "" && d.filterName != metadata.GetName() {
		return false
	}

	if d.filterNamespace != "" && d.filterNamespace != metadata.GetNamespace() {
		return false
	}

	if d.filterSelector != "" {
		selector, err := labels.Parse(d.filterSelector)
		if err != nil {
			fmt.Println("error parsing selector", err)
			return false
		}
		if !selector.Matches(labels.Set(metadata.GetLabels())) {
			return false
		}
	}

	return true
}
