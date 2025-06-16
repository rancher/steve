package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseHumanDuration(t *testing.T) {
	testCases := []struct {
		name        string
		input       string
		expected    time.Duration
		expectedErr bool
	}{
		{
			name:     "days + hours + mins + secs",
			input:    "1d23h45m56s",
			expected: 24*time.Hour + 23*time.Hour + 45*time.Minute + 56*time.Second,
		},
		{
			name:     "hours + mins + secs",
			input:    "12h34m56s",
			expected: 12*time.Hour + 34*time.Minute + 56*time.Second,
		},
		{
			name:     "days + hours",
			input:    "1d2h",
			expected: 24*time.Hour + 2*time.Hour,
		},
		{
			name:     "hours + secs",
			input:    "1d2s",
			expected: 24*time.Hour + 2*time.Second,
		},
		{
			name:     "mins + secs",
			input:    "1d2m",
			expected: 24*time.Hour + 2*time.Minute,
		},
		{
			name:     "hours",
			input:    "1h",
			expected: 1 * time.Hour,
		},
		{
			name:     "mins",
			input:    "1m",
			expected: 1 * time.Minute,
		},
		{
			name:     "secs",
			input:    "0s",
			expected: 0 * time.Second,
		},
		{
			name:        "invalid input",
			input:       "<invalid>",
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			output, err := ParseHumanReadableDuration(tc.input)
			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.Equal(t, tc.expected, output)
			}
		})
	}
}
