package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseRestartField(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedCount string
		expectedDur   time.Duration
		expectError   bool
	}{
		{
			name:          "no restarts",
			input:         "0",
			expectedCount: "0",
			expectedDur:   0,
			expectError:   false,
		},
		{
			name:          "single digit count no restart",
			input:         "1",
			expectedCount: "1",
			expectedDur:   0,
			expectError:   false,
		},
		{
			name:          "restart 5 minutes ago",
			input:         "1 (5m ago)",
			expectedCount: "1",
			expectedDur:   5 * time.Minute,
			expectError:   false,
		},
		{
			name:          "restart 2 hours ago",
			input:         "10 (2h ago)",
			expectedCount: "10",
			expectedDur:   2 * time.Hour,
			expectError:   false,
		},
		{
			name:          "restart 1 day ago",
			input:         "3 (1d ago)",
			expectedCount: "3",
			expectedDur:   24 * time.Hour,
			expectError:   false,
		},
		{
			name:          "restart 1 day 23 hours ago",
			input:         "3 (1d23h ago)",
			expectedCount: "3",
			expectedDur:   47 * time.Hour,
			expectError:   false,
		},
		{
			name:          "restart 30 days ago",
			input:         "5 (30d ago)",
			expectedCount: "5",
			expectedDur:   30 * 24 * time.Hour,
			expectError:   false,
		},
		{
			name:          "invalid format - no opening paren",
			input:         "1 5m ago)",
			expectedCount: "1 5m ago)",
			expectedDur:   0,
			expectError:   false,
		},
		{
			name:        "invalid format - no closing paren",
			input:       "1 (5m ago",
			expectError: true,
		},
		{
			name:        "invalid duration unit",
			input:       "1 (5x ago)",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count, dur, err := ParseRestartField(tt.input)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedCount, count)
				assert.Equal(t, tt.expectedDur, dur)
			}
		})
	}
}
