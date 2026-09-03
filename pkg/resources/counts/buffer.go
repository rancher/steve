package counts

import (
	"context"
	"time"

	"github.com/rancher/apiserver/pkg/types"
)

// debounceDuration determines how long events will be held before they are sent to the consumer
const debounceDuration = 5 * time.Second

// countsBuffer creates an APIEvent channel with a buffered response time (i.e. replies are only sent once every second).
func countsBuffer(ctx context.Context, wake <-chan struct{}, snapshot func() (Count, bool), debounce time.Duration) chan types.APIEvent {
	result := make(chan types.APIEvent)
	go func() {
		defer close(result)
		debounceCounts(ctx, result, wake, snapshot, debounce)
	}()
	return result
}

// debounceCounts converts counts from snapshot into an APIEvent, and updates the result channel at a reduced pace
func debounceCounts(ctx context.Context, result chan types.APIEvent, wake <-chan struct{}, snapshot func() (Count, bool), debounce time.Duration) {
	t := time.NewTicker(debounce)
	defer t.Stop()

	emit := func() bool {
		count, ok := snapshot()
		if !ok {
			return true
		}
		select {
		case result <- toAPIEvent(count):
			return true
		case <-ctx.Done():
			return false
		}
	}

	select {
	case <-wake:
		if !emit() {
			return
		}
	case <-ctx.Done():
		return
	}

	for {
		select {
		case <-t.C:
			if !emit() {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func toAPIEvent(count Count) types.APIEvent {
	return types.APIEvent{
		Name:         "resource.change",
		ResourceType: "counts",
		Object:       toAPIObject(count),
	}
}
