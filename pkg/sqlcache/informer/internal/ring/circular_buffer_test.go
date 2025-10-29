package ring

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestWriteAndRead ensures the fundamental operations work sequentially.
func TestWriteAndRead(t *testing.T) {
	t.Parallel()
	cb := NewCircularBuffer[int](5)
	reader := cb.NewReader()

	cb.Write(101)

	val, err := reader.Read(t.Context())
	if err != nil {
		t.Fatalf("Read() returned an unexpected error: %v", err)
	}
	if val != 101 {
		t.Errorf("expected to read 101, got %d", val)
	}
}

// TestReadAfterClose ensures that reading on a closed Buffer allows reading the remaining items and won't block anymore
func TestReadAfterClose(t *testing.T) {
	t.Parallel()
	cb := NewCircularBuffer[int](5)
	reader := cb.NewReader()

	cb.Write(100)
	cb.Close()

	if err := cb.Write(200); !errors.Is(err, ErrBufferClosed) {
		t.Errorf("expected ErrBufferClosed, got %v", err)
	}

	if _, err := reader.Read(t.Context()); err != nil {
		t.Fatalf("Read() after close should allow reading remaining objects, but got error: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	go func() {
		defer cancel()
		if _, err := reader.Read(ctx); !errors.Is(err, ErrBufferClosed) {
			t.Errorf("expected ErrBufferClosed, got %v", err)
		}
	}()
	select {
	case <-ctx.Done():
	case <-time.After(200 * time.Millisecond):
		t.Errorf("expected Read on closed buffer to exit immediately")
	}
}

// TestConcurrentReads validates that multiple readers can consume the stream independently.
func TestConcurrentReads(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cb := NewCircularBuffer[int](100)
	numReaders := 5
	numItems := 100

	var wg, pendingReads sync.WaitGroup
	pendingReads.Add(numItems * numReaders)

	// Reader goroutines
	for i := range numReaders {
		reader := cb.NewReader()
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			var results []int
			for range numItems {
				val, err := reader.Read(ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						break
					}
					t.Errorf("[Reader %d] unexpected error: %v", readerID, err)
					return
				}
				results = append(results, val)
				pendingReads.Done()
			}

			// Validate results
			expected := make([]int, numItems)
			for j := range numItems {
				expected[j] = j
			}
			assert.Equal(t, expected, results)
		}(i)
	}

	for i := range numItems {
		cb.Write(i)
	}

	pendingReads.Wait()
	cancel()
	wg.Wait()
}

// TestBlockingRead verifies that a reader blocks until data is available.
func TestBlockingRead(t *testing.T) {
	t.Parallel()
	cb := NewCircularBuffer[string](3)
	reader := cb.NewReader()

	readResultChan := make(chan string)
	isBlockedChan := make(chan bool)

	// Start reader in a goroutine; it should block.
	go func() {
		close(isBlockedChan) // Signal that the goroutine has started and is about to block
		val, _ := reader.Read(t.Context())
		readResultChan <- val
	}()

	<-isBlockedChan                   // Wait for the goroutine to start
	time.Sleep(50 * time.Millisecond) // Give it time to actually block on cond.Wait()

	// Now, write the data that should unblock the reader.
	cb.Write("hello")

	select {
	case result := <-readResultChan:
		if result != "hello" {
			t.Errorf("expected to read 'hello', got '%s'", result)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("reader did not unblock and read the value in time")
	}
}

// TestLapDetection ensures that a slow reader gets an error when lapped.
func TestLapDetection(t *testing.T) {
	t.Parallel()
	cb := NewCircularBuffer[int](3) // Small buffer
	reader := cb.NewReader()

	cb.Write(1)
	cb.Write(2)
	cb.Write(3)
	cb.Write(4)
	cb.Write(5)

	_, err := reader.Read(t.Context())
	if !errors.Is(err, ErrSlowReader) {
		t.Fatalf("expected ErrSlowReader, got %v", err)
	}
}

// TestRewind validates the ability to search backwards and resume reading.
func TestRewind(t *testing.T) {
	t.Parallel()
	t.Run("Found", func(t *testing.T) {
		ctx := t.Context()
		cb := NewCircularBuffer[int](10)
		for i := 10; i <= 50; i += 10 { // 10, 20, 30, 40, 50
			cb.Write(i)
		}
		reader := cb.NewReader()

		found := reader.Rewind(func(v int) bool {
			return v == 30
		})
		if !found {
			t.Fatal("Rewind() failed to find value 30")
		}

		val, err := reader.Read(ctx)
		if err != nil {
			t.Fatalf("Read() after rewind returned an error: %v", err)
		}
		if val != 30 {
			t.Errorf("expected first read after rewind to be 30, got %d", val)
		}

		val, err = reader.Read(ctx)
		if err != nil {
			t.Fatalf("second Read() after rewind returned an error: %v", err)
		}
		if val != 40 {
			t.Errorf("expected second read after rewind to be 40, got %d", val)
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		ctx := t.Context()
		cb := NewCircularBuffer[int](10)
		cb.Write(10)
		cb.Write(20)
		reader := cb.NewReader()

		found := reader.Rewind(func(v int) bool {
			return v == 99
		})
		if found {
			t.Fatal("Rewind() should have returned false for a value not in the buffer")
		}

		cb.Write(30)
		val, err := reader.Read(ctx)
		if err != nil {
			t.Fatalf("Read() after failed rewind returned an error: %v", err)
		}
		if val != 30 {
			t.Errorf("expected to read 30 after failed rewind, got %d", val)
		}
	})
}

// TestReadWithContextCancellation verifies that a blocked Read can be cancelled.
func TestReadWithContextCancellation(t *testing.T) {
	t.Parallel()
	cb := NewCircularBuffer[int](5)
	reader := cb.NewReader()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	readErrChan := make(chan error)

	go func() {
		_, err := reader.Read(ctx)
		readErrChan <- err
	}()

	select {
	case err := <-readErrChan:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded error, got %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Read() did not return after context was cancelled")
	}
}
