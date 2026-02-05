package composite

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCompositeInt_From(t *testing.T) {
	tests := []struct {
		name     string
		input    []interface{}
		wantPrim int64
		wantSec  int64
	}{
		{
			name:     "both values present",
			input:    []interface{}{4, 1737462000000},
			wantPrim: 4,
			wantSec:  1737462000000,
		},
		{
			name:     "only primary value",
			input:    []interface{}{5},
			wantPrim: 5,
			wantSec:  0,
		},
		{
			name:     "empty array",
			input:    []interface{}{},
			wantPrim: 0,
			wantSec:  0,
		},
		{
			name:     "nil primary",
			input:    []interface{}{nil, 1000},
			wantPrim: 0,
			wantSec:  1000,
		},
		{
			name:     "nil secondary",
			input:    []interface{}{3, nil},
			wantPrim: 3,
			wantSec:  0,
		},
		{
			name:     "float64 values",
			input:    []interface{}{float64(10), float64(2000)},
			wantPrim: 10,
			wantSec:  2000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ci := CompositeInt{}.From(tt.input)
			assert.Equal(t, tt.wantPrim, ci.Primary)
			assert.Equal(t, tt.wantSec, ci.Secondary)
		})
	}
}

func TestCompositeInt_ToSlice(t *testing.T) {
	tests := []struct {
		name string
		ci   CompositeInt
		want []interface{}
	}{
		{
			name: "both values",
			ci:   CompositeInt{Primary: 4, Secondary: 1737462000000},
			want: []interface{}{int64(4), int64(1737462000000)},
		},
		{
			name: "zero secondary becomes nil",
			ci:   CompositeInt{Primary: 0, Secondary: 0},
			want: []interface{}{int64(0), nil},
		},
		{
			name: "only primary",
			ci:   CompositeInt{Primary: 5, Secondary: 0},
			want: []interface{}{int64(5), nil},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.ci.ToSlice()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCompositeInt_IsZero(t *testing.T) {
	tests := []struct {
		name string
		ci   CompositeInt
		want bool
	}{
		{
			name: "both zero",
			ci:   CompositeInt{Primary: 0, Secondary: 0},
			want: true,
		},
		{
			name: "primary non-zero",
			ci:   CompositeInt{Primary: 1, Secondary: 0},
			want: false,
		},
		{
			name: "secondary non-zero",
			ci:   CompositeInt{Primary: 0, Secondary: 1000},
			want: false,
		},
		{
			name: "both non-zero",
			ci:   CompositeInt{Primary: 4, Secondary: 1737462000000},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.ci.IsZero())
		})
	}
}

func TestCompositeInt_FormatAsRestarts(t *testing.T) {
	tests := []struct {
		name string
		ci   CompositeInt
		want string
	}{
		{
			name: "no restarts",
			ci:   CompositeInt{Primary: 0, Secondary: 0},
			want: "0",
		},
		{
			name: "restarts without time",
			ci:   CompositeInt{Primary: 5, Secondary: 0},
			want: "5",
		},
		{
			name: "restarts with recent time",
			ci:   CompositeInt{Primary: 4, Secondary: time.Now().Add(-3 * time.Hour).UnixMilli()},
			want: "4 (3h ago)", // Approximate - duration formatting may vary
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.ci.FormatAsRestarts()
			// For time-based tests, check prefix/suffix instead of exact match
			if tt.ci.Secondary != 0 {
				assert.Contains(t, got, fmt.Sprintf("%d (", tt.ci.Primary))
				assert.Contains(t, got, "ago)")
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
		want  int64
	}{
		{
			name:  "int",
			input: 42,
			want:  42,
		},
		{
			name:  "int64",
			input: int64(100),
			want:  100,
		},
		{
			name:  "float64",
			input: float64(3.7),
			want:  3,
		},
		{
			name:  "nil",
			input: nil,
			want:  0,
		},
		{
			name:  "string (unsupported)",
			input: "123",
			want:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToInt64(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}
