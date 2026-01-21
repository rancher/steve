package formatters

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFormatRestarts(t *testing.T) {
	tests := []struct {
		name     string
		input    []interface{}
		wantLike string // partial match since time is dynamic
	}{
		{
			name:     "count only",
			input:    []interface{}{0, nil},
			wantLike: "0",
		},
		{
			name:     "count with recent timestamp",
			input:    []interface{}{4, float64(time.Now().Add(-3 * time.Hour).UnixMilli())},
			wantLike: "4 (3h",
		},
		{
			name:     "count with old timestamp",
			input:    []interface{}{10, float64(time.Now().Add(-48 * time.Hour).UnixMilli())},
			wantLike: "10 (2d",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatRestarts(tt.input)
			assert.Contains(t, result, tt.wantLike)
		})
	}
}
