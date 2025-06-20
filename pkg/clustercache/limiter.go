package clustercache

import (
	"context"
	"errors"
	"sync"
)

// Limiter limits the number of concurrent calls to an external service
// and collects errors from failed calls.
type Limiter struct {
	semaphore chan struct{}

	// Mutex to protect the error
	// errors.Join is _not_ Go routine safe
	err error
	mu  sync.Mutex

	wg sync.WaitGroup
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
	sl.wg.Add(1)

	go func() {
		defer sl.wg.Done()

		select {
		case sl.semaphore <- struct{}{}:
			defer func() {
				<-sl.semaphore
			}()

			err := serviceFunc(ctx)
			if err != nil {
				sl.mu.Lock()
				sl.err = errors.Join(sl.err, err)
				sl.mu.Unlock()
			}
		case <-ctx.Done():
			// If the context is cancelled before acquiring a semaphore slot,
			// just exit the goroutine. No error needs to be recorded for this specific case,
			// as the call wasn't even initiated.
		}
	}()
}

// Wait blocks until all outstanding calls made via `Execute` have completed
// and returns a slice of all collected errors.
func (sl *Limiter) Wait() error {
	sl.wg.Wait()
	sl.mu.Lock()
	defer sl.mu.Unlock()
	return sl.err
}
