package dates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsUnixMilli(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{
			name:  "valid unix milli timestamp",
			input: "1714567890123",
			want:  true,
		},
		{
			name:  "too short",
			input: "171456789012",
			want:  false,
		},
		{
			name:  "too long",
			input: "17145678901234",
			want:  false,
		},
		{
			name:  "contains non-digit character",
			input: "171456789012a",
			want:  false,
		},
		{
			name:  "empty string",
			input: "",
			want:  false,
		},
		{
			name:  "duration string",
			input: "5m",
			want:  false,
		},
		{
			name:  "RFC3339 timestamp",
			input: "2024-05-01T12:00:00Z",
			want:  false,
		},
		{
			name:  "negative number 13 chars",
			input: "-171456789012",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isUnixMilli(tt.input))
		})
	}
}
