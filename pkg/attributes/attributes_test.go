package attributes

import (
	"testing"

	"github.com/rancher/apiserver/pkg/types"
	wschemas "github.com/rancher/wrangler/v3/pkg/schemas"
	"github.com/stretchr/testify/assert"
)

func newSchema() *types.APISchema {
	return &types.APISchema{Schema: &wschemas.Schema{}}
}

func TestAggregated(t *testing.T) {
	t.Run("nil schema is not aggregated", func(t *testing.T) {
		assert.False(t, Aggregated(nil))
	})

	t.Run("unset attribute defaults to not aggregated", func(t *testing.T) {
		assert.False(t, Aggregated(newSchema()))
	})

	t.Run("set true", func(t *testing.T) {
		s := newSchema()
		SetAggregated(s, true)
		assert.True(t, Aggregated(s))
	})

	t.Run("set false", func(t *testing.T) {
		s := newSchema()
		SetAggregated(s, false)
		assert.False(t, Aggregated(s))
	})

	t.Run("overwrite true with false", func(t *testing.T) {
		s := newSchema()
		SetAggregated(s, true)
		SetAggregated(s, false)
		assert.False(t, Aggregated(s))
	})
}
