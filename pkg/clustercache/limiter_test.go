package clustercache

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewLimiter(t *testing.T) {
	t.Run("PositiveLimit", func(t *testing.T) {
		limiter := NewLimiter(5)
		assert.NotNil(t, limiter)
		assert.Len(t, limiter.semaphore, 0)
		assert.Equal(t, 5, cap(limiter.semaphore))
	})

	t.Run("ZeroLimit", func(t *testing.T) {
		limiter := NewLimiter(0)
		assert.NotNil(t, limiter)
		assert.Equal(t, 1, cap(limiter.semaphore))
	})

	t.Run("NegativeLimit", func(t *testing.T) {
		limiter := NewLimiter(-3)
		assert.NotNil(t, limiter)
		assert.Equal(t, 1, cap(limiter.semaphore))
	})
}

func TestLimiter_Concurrency(t *testing.T) {
	totalExecutions := 10

	var activeExecutions int32
	var maxActiveExecutions int32

	limit := 3
	activeTracker := make(chan struct{}, limit)

	testCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limiter := NewLimiter(limit)
	for i := 0; i < totalExecutions; i++ {
		limiter.Execute(testCtx, func(ctx context.Context) error {
			select {
			case activeTracker <- struct{}{}:
				currentActive := atomic.AddInt32(&activeExecutions, 1)
				defer atomic.AddInt32(&activeExecutions, -1)

				for {
					oldMax := atomic.LoadInt32(&maxActiveExecutions)
					if currentActive > oldMax {
						if atomic.CompareAndSwapInt32(&maxActiveExecutions, oldMax, currentActive) {
							break
						}
					} else {
						break
					}
				}
				time.Sleep(50 * time.Millisecond)
				<-activeTracker
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}

	err := limiter.Wait()

	assert.Equal(t, int32(limit), maxActiveExecutions)
	assert.NoError(t, err)
}

func TestLimiter_ErrorCollection(t *testing.T) {
	limiter := NewLimiter(5)
	expectedErrors := 0
	var startedCount int32

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Execution 1: Success
	limiter.Execute(ctx, func(callCtx context.Context) error {
		return fakeService(callCtx, 1, false, 10*time.Millisecond, &startedCount)
	})

	// Execution 2: Failure
	limiter.Execute(ctx, func(callCtx context.Context) error {
		expectedErrors++
		return fakeService(callCtx, 2, true, 10*time.Millisecond, &startedCount)
	})

	// Execution 3: Success
	limiter.Execute(ctx, func(callCtx context.Context) error {
		return fakeService(callCtx, 3, false, 10*time.Millisecond, &startedCount)
	})

	// Execution 4: Failure
	limiter.Execute(ctx, func(callCtx context.Context) error {
		expectedErrors++
		return fakeService(callCtx, 4, true, 10*time.Millisecond, &startedCount)
	})

	err := limiter.Wait()

	assert.Equal(t, int32(4), startedCount)
	assert.ErrorContains(t, err, "error from service 2")
	assert.ErrorContains(t, err, "error from service 4")
}

func TestLimiter_ContextCancellation(t *testing.T) {
	t.Run("cancellation during execution", func(t *testing.T) {
		limiter := NewLimiter(2)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond) // Short timeout
		defer cancel()

		var startedCount int32

		// This call should be running when context is cancelled
		limiter.Execute(ctx, func(callCtx context.Context) error {
			return fakeService(callCtx, 1, false, 200*time.Millisecond, &startedCount)
		})

		// This call should complete before cancellation
		limiter.Execute(ctx, func(callCtx context.Context) error {
			return fakeService(callCtx, 2, false, 10*time.Millisecond, &startedCount)
		})

		err := limiter.Wait()

		assert.ErrorContains(t, err, "service 1 cancelled: context deadline exceeded")
		assert.Equal(t, int32(2), startedCount)
	})

	t.Run("cancellation before acquiring semaphore", func(t *testing.T) {
		limiter := NewLimiter(1)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		var startedCount int32

		// Execute 1: Will acquire semaphore and run (and likely be cancelled)
		limiter.Execute(ctx, func(callCtx context.Context) error {
			return fakeService(callCtx, 1, false, 100*time.Millisecond, &startedCount)
		})

		// Execute 2: Will wait for semaphore, but context will likely cancel before it acquires
		limiter.Execute(ctx, func(callCtx context.Context) error {
			return fakeService(callCtx, 2, false, 200*time.Millisecond, &startedCount)
		})

		err := limiter.Wait()

		// Assert that only the first call actually started its service function
		// (the second one was cancelled before acquiring the semaphore slot).
		assert.Equal(t, int32(1), startedCount, "Only the first call should have started")

		// The first call might be cancelled, leading to 1 error.
		// The second call's goroutine will exit via `<-ctx.Done()` *before* acquiring the semaphore,
		// and thus won't add an error to the `limiter.err`
		assert.ErrorContains(t, err, "service 2 cancelled: context deadline exceeded")
	})
}

func TestLimiter_NoExecutions(t *testing.T) {
	limiter := NewLimiter(3)
	err := limiter.Wait()

	assert.NoError(t, err)
}

func fakeService(ctx context.Context, id int, simulateError bool, duration time.Duration, startedCounter *int32) error {
	atomic.AddInt32(startedCounter, 1)
	select {
	case <-ctx.Done():
		return fmt.Errorf("service %d cancelled: %v", id, ctx.Err())
	case <-time.After(duration):
		if simulateError {
			return fmt.Errorf("error from service %d", id)
		}
		return nil
	}
}
