package counts

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/stretchr/testify/assert"
)

func Test_countsBuffer(t *testing.T) {
	tests := []struct {
		name           string
		numInputEvents int
		overrideInput  map[int]int // events whose count we should override. Don't include an event >= numInputEvents
	}{
		{
			name:           "test basic input",
			numInputEvents: 1,
		},
		{
			name:           "test basic multiple input",
			numInputEvents: 3,
		},
		{
			name:           "test basic input which is overriden by later events",
			numInputEvents: 3,
			overrideInput: map[int]int{
				1: 17,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			debounce := 10 * time.Millisecond
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			counter := newFakeCounter()
			outputChannel := countsBuffer(ctx, counter.wake, counter.snapshot, debounce)

			counter.update("test", 1)

			// first event is not buffered, so we expect to receive it quicker than the debounce
			_, err := receiveWithTimeout(outputChannel, time.Second*1)
			assert.NoError(t, err, "Expected first event to be received quickly")

			// stream our standard count events
			for i := 0; i < test.numInputEvents; i++ {
				counter.update(strconv.Itoa(i), 1)
			}

			// stream any overrides, if applicable
			for key, value := range test.overrideInput {
				counter.update(strconv.Itoa(key), value)
			}

			// due to complexities of cycle calculation, give a slight delay for the event to actually stream
			output, err := receiveWithTimeout(outputChannel, debounce+time.Second)
			assert.NoError(t, err, "did not expect an error when receiving value from channel")
			outputCount := output.Object.Object.(Count)
			assert.Len(t, outputCount.Counts, test.numInputEvents)
			for outputID, outputItem := range outputCount.Counts {
				outputIdx, err := strconv.Atoi(outputID)
				assert.NoError(t, err, "couldn't convert output idx")
				nsTotal := 0
				for _, nsSummary := range outputItem.Namespaces {
					nsTotal += nsSummary.Count
				}
				if outputOverride, ok := test.overrideInput[outputIdx]; ok {
					assert.Equal(t, outputOverride, outputItem.Summary.Count, "expected overridden output count to be most recent value")
					assert.Equal(t, outputOverride, nsTotal, "expected overridden output namespace count to be most recent value")
				} else {
					assert.Equal(t, 1, outputItem.Summary.Count, "expected non-overridden output count to be 1")
					assert.Equal(t, 1, nsTotal, "expected non-overridden output namespace count to be 1")
				}
			}
		})
	}
}

func Test_countsBufferSnapshotsOncePerWindow(t *testing.T) {
	// the window is deliberately longer than the test can run: the point is that
	// updates cost nothing until it elapses, so letting a tick land mid-burst
	// would only make the assertion below flaky. Test_countsBuffer covers the
	// coalesced value actually reaching the consumer.
	debounce := time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	counter := newFakeCounter()
	outputChannel := countsBuffer(ctx, counter.wake, counter.snapshot, debounce)

	// the first update is emitted immediately, costing one snapshot
	counter.update("test", 1)
	_, err := receiveWithTimeout(outputChannel, time.Second)
	assert.NoError(t, err, "Expected first event to be received quickly")

	for i := 0; i < 1000; i++ {
		counter.update("test", i)
	}

	assert.Equal(t, 1, counter.snapshots(), "expected 1000 updates within one window to cost no additional snapshots")
}

func Test_countsBufferDoesNotBlockProducer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	counter := newFakeCounter()
	// deliberately never read from the returned channel
	countsBuffer(ctx, counter.wake, counter.snapshot, 10*time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10000; i++ {
			counter.update(strconv.Itoa(i%50), i)
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("producer blocked on an unread consumer")
	}
}

// fakeCounter mimics the producer side of Store.Watch: it records which schema
// IDs have changed and only materializes a copy of them when the debouncer asks.
type fakeCounter struct {
	lock         sync.Mutex
	counts       map[string]ItemCount
	changed      map[string]struct{}
	snapshotCall int

	wake chan struct{}
}

func newFakeCounter() *fakeCounter {
	return &fakeCounter{
		counts:  map[string]ItemCount{},
		changed: map[string]struct{}{},
		wake:    make(chan struct{}, 1),
	}
}

func (f *fakeCounter) update(id string, count int) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.counts[id] = createItemCount(count)
	f.changed[id] = struct{}{}
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

func (f *fakeCounter) snapshot() (Count, bool) {
	f.lock.Lock()
	defer f.lock.Unlock()

	if len(f.changed) == 0 {
		return Count{}, false
	}
	f.snapshotCall++

	changedCounts := make(map[string]ItemCount, len(f.changed))
	for id := range f.changed {
		itemCount := f.counts[id]
		changedCounts[id] = *itemCount.DeepCopy()
	}
	clear(f.changed)

	return Count{ID: "count", Counts: changedCounts}, true
}

func (f *fakeCounter) snapshots() int {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.snapshotCall
}

// receiveWithTimeout tries to get a value from input within duration. Returns an error if no input was received during that period
func receiveWithTimeout(input chan types.APIEvent, duration time.Duration) (*types.APIEvent, error) {
	select {
	case value := <-input:
		return &value, nil
	case <-time.After(duration):
		return nil, fmt.Errorf("timeout error, no value received after %f seconds", duration.Seconds())
	}
}

func createItemCount(countTotal int) ItemCount {
	return ItemCount{
		Summary: Summary{
			Count: countTotal,
		},
		Namespaces: map[string]Summary{
			"test": {
				Count: countTotal,
			},
		},
	}
}
