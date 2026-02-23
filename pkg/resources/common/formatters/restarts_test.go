package formatters

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFormatRestarts(t *testing.T) {
	// Use current time and calculate expected results
	now := time.Now()

	tests := []struct {
		name         string
		input        []interface{}
		wantContains string // partial match for dynamic time
	}{
		{
			name:         "count only",
			input:        []interface{}{0, nil},
			wantContains: "0",
		},
		{
			name:         "count with recent timestamp (3 hours ago)",
			input:        []interface{}{4, float64(now.Add(-3 * time.Hour).UnixMilli())},
			wantContains: "4 (3h",
		},
		{
			name:         "count with old timestamp (48 hours ago)",
			input:        []interface{}{10, float64(now.Add(-48 * time.Hour).UnixMilli())},
			wantContains: "10 (2d",
		},
		{
			name:         "count with very recent timestamp (5 minutes ago)",
			input:        []interface{}{2, float64(now.Add(-5 * time.Minute).UnixMilli())},
			wantContains: "2 (5m",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatRestarts(tt.input)
			assert.Contains(t, result, tt.wantContains)
		})
	}
}
