package clustercache

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
)

// Limiter limits the number of concurrent calls to an external service
// and collects errors from failed calls.
type Limiter struct {
	semaphore chan struct{}

	mu sync.Mutex

	eg errgroup.Group
}

// NewLimiter creates a new Limiter with the specified concurrency limit.
func NewLimiter(limit int) *Limiter {
	if limit <= 0 {
		limit = 1
	}
	return &Limiter{
		semaphore: make(chan struct{}, limit),
	}
}

// Execute executes the given function (representing an external service call)
// while respecting the concurrency limit. If the function returns an error,
// it is collected by the Limiter.
func (sl *Limiter) Execute(ctx context.Context, serviceFunc func(ctx context.Context) error) {
	sl.eg.Go(func() error {
		select {
		case sl.semaphore <- struct{}{}:
			defer func() {
				<-sl.semaphore
			}()

			err := serviceFunc(ctx)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			// If the context is cancelled before acquiring a semaphore slot,
			// just exit the goroutine. No error needs to be recorded for this specific case,
			// as the call wasn't even initiated.
		}

		return nil
	})
}

// Wait blocks until all outstanding calls made via `Execute` have completed
// and returns a slice of all collected errors.
func (sl *Limiter) Wait() error {
	return sl.eg.Wait()
}
