package sqlcache

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"strconv"
	"strings"
)

// ListOptions represents the query parameters that may be included in a list request.
type ListOptions struct {
	ChunkSize  int
	Resume     string
	Filters    []Filter
	Sort       Sort
	Pagination Pagination
	Revision   string
}

// Filter represents a field to filter by.
// A subfield in an object is represented in a request query using . notation, e.g. 'metadata.name'.
// The subfield is internally represented as a slice, e.g. [metadata, name].
type Filter struct {
	field []string
	match string
}

// Sort represents the criteria to sort on.
// The subfield to sort by is represented in a request query using . notation, e.g. 'metadata.name'.
// The subfield is internally represented as a slice, e.g. [metadata, name].
// The order is represented by prefixing the sort key by '-', e.g. sort=-metadata.name.
type Sort struct {
	primaryField   []string
	secondaryField []string
	primaryOrder   SortOrder
	secondaryOrder SortOrder
}

// SortOrder represents whether the list should be ascending or descending.
type SortOrder int

const (
	// ASC stands for ascending order.
	ASC SortOrder = iota
	// DESC stands for descending (reverse) order.
	DESC
)

// Pagination represents how to return paginated results.
type Pagination struct {
	pageSize int
	page     int
}

// ListOptionIndexer extends VersionedIndexer by allowing queries based on ListOption
type ListOptionIndexer struct {
	*VersionedIndexer

	fieldFuncs map[string]FieldFunc
	addField   *sql.Stmt
}

// FieldFunc is a function from an object to a filterable/sortable property. Result can be string, int or bool
type FieldFunc func(obj any) any

// NewListOptionIndexer returns a cache.Indexer on a Kubernetes resource that is also able to satisfy ListOption queries
func NewListOptionIndexer(example meta.Object, path string, fieldFuncs map[string]FieldFunc) (*ListOptionIndexer, error) {
	keyFunc := func(a any) (string, error) {
		o, ok := a.(meta.Object)
		if !ok {
			return "", errors.Errorf("Unexpected object does not conform to meta.Object: %v", a)
		}
		return o.GetName(), nil
	}

	return NewCustomListOptionIndexer(example, keyFunc, path, fieldFuncs, cache.Indexers{})
}

// NewCustomListOptionIndexer returns a cache.Indexer on a Kubernetes resource that is also able to satisfy ListOption queries
// with custom keyFunc and Indexers
func NewCustomListOptionIndexer(example meta.Object, keyFunc cache.KeyFunc, path string, fieldFuncs map[string]FieldFunc, indexers cache.Indexers) (*ListOptionIndexer, error) {
	versionFunc := func(a any) (int, error) {
		o, ok := a.(meta.Object)
		if !ok {
			return 0, errors.Errorf("Unexpected object does not conform to meta.Object: %v", a)
		}
		i, err := strconv.Atoi(o.GetResourceVersion())
		if err != nil {
			return 0, errors.Errorf("Unexpected non-integer version: %v", o.GetResourceVersion())
		}
		return i, nil
	}
	v, err := NewVersionedIndexer(example, keyFunc, versionFunc, path, indexers)
	if err != nil {
		return nil, err
	}

	l := &ListOptionIndexer{
		VersionedIndexer: v,
		fieldFuncs:       fieldFuncs,
	}
	l.RegisterAfterUpsert(l.AfterUpsert)

	err = l.InitExec(`CREATE TABLE fields (
    		name VARCHAR NOT NULL,
			key VARCHAR NOT NULL,
			version INTEGER NOT NULL,
            value VARCHAR,
			PRIMARY KEY (name, key, version),
            FOREIGN KEY (key, version) REFERENCES object_history (key, version) ON DELETE CASCADE 
	   )`)
	if err != nil {
		return nil, err
	}
	err = l.InitExec(`CREATE INDEX fields_value ON fields(value)`)
	if err != nil {
		return nil, err
	}

	l.addField = l.Prepare(`INSERT INTO fields(name, key, version, value) VALUES (?,?,?,?) ON CONFLICT DO UPDATE SET value = excluded.value`)

	return l, nil
}

/* Core methods */

// AfterUpsert saves sortable/filterable fields into tables
func (l *ListOptionIndexer) AfterUpsert(key string, obj any, tx *sql.Tx) error {
	version, err := l.versionFunc(obj)
	if err != nil {
		return err
	}

	for name, fieldFunc := range l.fieldFuncs {
		value := fieldFunc(obj)
		switch typedValue := value.(type) {
		case int, bool, string:
			_, err = tx.Stmt(l.addField).Exec(sanitize(name), key, version, fmt.Sprint(typedValue))
		case []string:
			_, err = tx.Stmt(l.addField).Exec(sanitize(name), key, version, strings.Join(typedValue, "|"))
		default:
			panic(errors.Errorf("FieldFunc returned a non-supported type value: %v", value))
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func sanitize(name string) string {
	return strings.ReplaceAll(name, "\"", ".")
}

// ListByOptions returns objects according to the ListOptions struct
func (l *ListOptionIndexer) ListByOptions(lo ListOptions) ([]any, error) {
	// compute list of interesting fields (filtered or sorted)
	fields := [][]string{}
	for _, filter := range lo.Filters {
		fields = append(fields, filter.field)
	}
	if len(lo.Sort.primaryField) > 0 {
		fields = append(fields, lo.Sort.primaryField)
	}
	if len(lo.Sort.secondaryField) > 0 {
		fields = append(fields, lo.Sort.secondaryField)
	}

	// compute join clauses (one per interesting field) and their corresponding parameters
	joinClauses := []string{}
	params := []any{}
	for _, field := range fields {
		columnName := toColumnName(field)
		joinClauses = append(joinClauses, fmt.Sprintf(`JOIN fields "f_%s" ON "f_%s".key = o.key AND "f_%s".version = o.version AND "f_%s".name = ?`, columnName, columnName, columnName, columnName))
		params = append(params, columnName)
	}

	// compute WHERE clauses (from lo.Filters and lo.Revision) - and their corresponding parameters
	whereClauses := []string{}
	for _, filter := range lo.Filters {
		columnName := toColumnName(filter.field)
		whereClauses = append(whereClauses, fmt.Sprintf(`"f_%s".value LIKE ?`, columnName))
		params = append(params, fmt.Sprintf("%%%s%%", filter.match))
	}
	if lo.Revision == "" {
		// latest
		whereClauses = append(whereClauses, "o.version = (SELECT MAX(o2.version) FROM object_history o2 WHERE o2.key = o.key)")
		whereClauses = append(whereClauses, "o.deleted_version IS NULL")
	} else {
		version, err := strconv.Atoi(lo.Revision)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not parse Revision %s", lo.Revision)
		}
		whereClauses = append(whereClauses, "o.version = (SELECT MAX(o2.version) FROM object_history o2 WHERE o2.key = o.key AND o2.version <= ?)")
		params = append(params, version)
		whereClauses = append(whereClauses, "(o.deleted_version IS NULL OR o.deleted_version > ?)")
		params = append(params, version)
	}

	// compute ORDER BY clauses (from lo.Sort)
	orderByClauses := []string{}
	if len(lo.Sort.primaryField) > 0 {
		columnName := toColumnName(lo.Sort.primaryField)
		direction := "ASC"
		if lo.Sort.primaryOrder == DESC {
			direction = "DESC"
		}
		orderByClauses = append(orderByClauses, fmt.Sprintf(`"f_%s".value %s`, columnName, direction))
	}
	if len(lo.Sort.secondaryField) > 0 {
		columnName := toColumnName(lo.Sort.secondaryField)
		direction := "ASC"
		if lo.Sort.secondaryOrder == DESC {
			direction = "DESC"
		}
		orderByClauses = append(orderByClauses, fmt.Sprintf(`"f_%s".value %s`, columnName, direction))
	}

	// compute LIMIT/OFFSET clauses (from lo.Pagination)
	limitClause := ""
	offsetClause := ""
	if lo.Pagination.pageSize >= 1 {
		limitClause = " LIMIT ?"
		params = append(params, lo.Pagination.pageSize)

		if lo.Pagination.page >= 1 {
			offsetClause = " OFFSET ?"
			params = append(params, lo.Pagination.pageSize*(lo.Pagination.page-1))
		}
	}

	// put the final query together
	stmt := `SELECT o.object FROM object_history o`
	if len(joinClauses) > 0 {
		stmt += " "
		stmt += strings.Join(joinClauses, " ")
	}
	if len(whereClauses) > 0 {
		stmt += " WHERE "
		stmt += strings.Join(whereClauses, " AND ")
	}
	if len(orderByClauses) > 0 {
		stmt += " ORDER BY "
		stmt += strings.Join(orderByClauses, ", ")
	}
	stmt += limitClause
	stmt += offsetClause

	result, err := l.QueryObjects(l.Prepare(stmt), params...)
	if err != nil {
		return nil, err
	}

	return result, nil
}

/* Utilities */

func toColumnName(s []string) string {
	return sanitize(strings.Join(s, "."))
}
