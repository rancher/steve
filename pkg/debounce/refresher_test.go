package debounce

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mockRefresher struct {
	refreshCount atomic.Int32
	// refreshCh signals back to the test goroutine every time Refresh() is called.
	refreshCh chan struct{}
}

// newMockRefresher creates a mock with a buffered channel to prevent blocking.
func newMockRefresher(buffer int) *mockRefresher {
	return &mockRefresher{
		refreshCh: make(chan struct{}, buffer),
	}
}

// Refresh increments the counter and sends a signal on the channel.
func (m *mockRefresher) Refresh() error {
	m.refreshCh <- struct{}{}
	m.refreshCount.Add(1)
	return nil
}

// getCount safely returns the current refresh count.
func (m *mockRefresher) getCount() int {
	return int(m.refreshCount.Load())
}

// waitForRefresh waits for a refresh signal from the channel or fails the test on timeout.
// This is the key to creating reliable, non-flaky concurrent tests.
func (m *mockRefresher) waitForRefresh(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-m.refreshCh:
		// Signal received, continue the test.
	case <-time.After(timeout):
		t.Fatalf("timed out after %v waiting for refresh", timeout)
	}
}

// TestSchedule_Single verifies that a single scheduled call triggers a refresh.
func TestSchedule_Single(t *testing.T) {
	mock := newMockRefresher(1)
	// Create a refresher with no periodic duration.
	dr := NewDebounceableRefresher(t.Context(), mock, 0)

	// Schedule a refresh to happen very soon.
	dr.Schedule(10 * time.Millisecond)

	// Wait for the refresh to occur, with a generous timeout.
	mock.waitForRefresh(t, 50*time.Millisecond)

	// Assert that Refresh() was called exactly once.
	require.Equal(t, 1, mock.getCount())
}

// TestSchedule_Debounce verifies that subsequent calls to Schedule reset the timer.
func TestSchedule_Debounce(t *testing.T) {
	mock := newMockRefresher(1)
	dr := NewDebounceableRefresher(t.Context(), mock, 0)

	dr.Schedule(50 * time.Millisecond)
	// This should cancel the first timer:
	dr.Schedule(100 * time.Millisecond)

	// Wait for the debounced refresh to occur.
	// The total wait time should be ~110ms from the start. We'll give it 150ms.
	mock.waitForRefresh(t, 150*time.Millisecond)

	// Assert that even with two calls to Schedule, only one refresh happened.
	require.Equal(t, 1, mock.getCount())
}

// TestPeriodic_Ticker verifies that refreshes occur at the specified interval.
func TestPeriodic_Ticker(t *testing.T) {
	// Use a buffered channel in the mock to handle rapid refreshes.
	mock := newMockRefresher(5)
	// Create a refresher with a 20ms periodic tick.
	_ = NewDebounceableRefresher(t.Context(), mock, 20*time.Millisecond)

	// Wait for 3 distinct refreshes to occur.
	mock.waitForRefresh(t, 50*time.Millisecond)
	mock.waitForRefresh(t, 50*time.Millisecond)
	mock.waitForRefresh(t, 50*time.Millisecond)

	// Assert that at least 3 refreshes have happened.
	// Depending on timing, it could be slightly more, so we check the final count.
	require.Equal(t, 3, mock.getCount())
}

// TestContext_Cancellation verifies that the refresher stops when the context is canceled.
func TestContext_Cancellation(t *testing.T) {
	mock := newMockRefresher(5)
	ctx, cancel := context.WithCancel(t.Context())

	// Create a refresher with a very fast ticker.
	NewDebounceableRefresher(ctx, mock, 20*time.Millisecond)

	// Wait for the first refresh to ensure the goroutines are running.
	mock.waitForRefresh(t, 50*time.Millisecond)
	require.Equal(t, 1, mock.getCount())

	cancel()
	time.Sleep(50 * time.Millisecond)

	require.Equal(t, 1, mock.getCount(), "refresh should not occur after context is canceled")
}
