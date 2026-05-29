package informer

import (
	"github.com/rancher/steve/pkg/sqlcache/sqlgen"
)

// indexedFieldAdapter adapts informer.IndexedField to sqlgen.IndexedField
type indexedFieldAdapter struct {
	inner IndexedField
}

func (a *indexedFieldAdapter) ColumnName() string { return a.inner.ColumnName() }
func (a *indexedFieldAdapter) ColumnType() string { return a.inner.ColumnType() }

// timestampFieldAdapter adapts ComputedField to sqlgen.TimestampField
type timestampFieldAdapter struct {
	inner *ComputedField
}

func (a *timestampFieldAdapter) ColumnName() string    { return a.inner.ColumnName() }
func (a *timestampFieldAdapter) ColumnType() string    { return a.inner.ColumnType() }
func (a *timestampFieldAdapter) IsTimestampField() bool { return a.inner.IsTimestamp }

// buildFieldRegistry creates a sqlgen.FieldRegistry from the indexer's indexedFields.
func (l *ListOptionIndexer) buildFieldRegistry() sqlgen.FieldRegistry {
	adapted := make(map[string]sqlgen.IndexedField, len(l.indexedFields))
	for k, v := range l.indexedFields {
		if cf, ok := v.(*ComputedField); ok && cf.IsTimestamp {
			adapted[k] = &timestampFieldAdapter{inner: cf}
		} else {
			adapted[k] = &indexedFieldAdapter{inner: v}
		}
	}
	return sqlgen.NewFieldRegistry(adapted)
}
