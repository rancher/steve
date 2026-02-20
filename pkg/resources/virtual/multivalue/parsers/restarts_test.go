package parsers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRestarts(t *testing.T) {
	// Mock Now for deterministic tests
	fixedNow := time.Unix(1737462000, 0) // 2026-01-21 13:00:00 UTC
	Now = func() time.Time {
		return fixedNow
	}
	defer func() { Now = time.Now }()

	tests := []struct {
		name          string
		input         string
		wantCount     int
		wantTimestamp *int64 // nil means no timestamp expected
		wantErr       bool
	}{
		{
			name:          "no restarts",
			input:         "0",
			wantCount:     0,
			wantTimestamp: nil,
		},
		{
			name:          "single restart with time (3h37m ago)",
			input:         "1 (3h37m ago)",
			wantCount:     1,
			wantTimestamp: int64Ptr(fixedNow.Add(-3*time.Hour - 37*time.Minute).UnixMilli()),
		},
		{
			name:          "multiple restarts with time (3h38m ago)",
			input:         "4 (3h38m ago)",
			wantCount:     4,
			wantTimestamp: int64Ptr(fixedNow.Add(-3*time.Hour - 38*time.Minute).UnixMilli()),
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

			count, ok := result[0].(int64)
			require.True(t, ok)
			assert.Equal(t, int64(tt.wantCount), count)

			if tt.wantTimestamp != nil {
				require.NotNil(t, result[1])
				timestamp, ok := result[1].(int64)
				require.True(t, ok)
				assert.Equal(t, *tt.wantTimestamp, timestamp)
			} else {
				assert.Nil(t, result[1])
			}
		})
	}
}

// Helper function to get pointer to int64
func int64Ptr(v int64) *int64 {
	return &v
}

