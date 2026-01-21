package parsers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRestarts(t *testing.T) {
	// Mock Now for deterministic tests
	Now = func() time.Time {
		return time.Unix(1737462000, 0)
	}
	defer func() { Now = time.Now }()

	tests := []struct {
		name      string
		input     string
		wantCount int
		wantTime  bool
		wantErr   bool
	}{
		{
			name:      "no restarts",
			input:     "0",
			wantCount: 0,
			wantTime:  false,
		},
		{
			name:      "single restart with time",
			input:     "1 (3h37m ago)",
			wantCount: 1,
			wantTime:  true,
		},
		{
			name:      "multiple restarts with time",
			input:     "4 (3h38m ago)",
			wantCount: 4,
			wantTime:  true,
		},
		{
			name:    "invalid format",
			input:   "invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseRestarts(tt.input)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Len(t, result, 2)

			// Check count
			count, ok := result[0].(int64)
			require.True(t, ok)
			assert.Equal(t, int64(tt.wantCount), count)

			// Check timestamp
			if tt.wantTime {
				assert.NotNil(t, result[1])
			} else {
				assert.Nil(t, result[1])
			}
		})
	}
}
