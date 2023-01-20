package counts

import (
	"fmt"
	"strconv"
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
			debounceDuration = 10 * time.Millisecond
			countsChannel := make(chan Count, 100)
			outputChannel := countsBuffer(countsChannel)

			countsChannel <- Count{
				ID:     "count",
				Counts: map[string]ItemCount{"test": createItemCount(1)},
			}

			// first event is not buffered, so we expect to receive it quicker than the debounce
			_, err := receiveWithTimeout(outputChannel, time.Millisecond*1)
			assert.NoError(t, err, "Expected first event to be received quickly")

			// stream our standard count events
			for i := 0; i < test.numInputEvents; i++ {
				countsChannel <- Count{
					ID:     "count",
					Counts: map[string]ItemCount{strconv.Itoa(i): createItemCount(1)},
				}
			}

			// stream any overrides, if applicable
			for key, value := range test.overrideInput {
				countsChannel <- Count{
					ID:     "count",
					Counts: map[string]ItemCount{strconv.Itoa(key): createItemCount(value)},
				}
			}

			// due to complexities of cycle calculation, give a slight delay for the event to actually stream
			output, err := receiveWithTimeout(outputChannel, debounceDuration+time.Millisecond*10)
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
