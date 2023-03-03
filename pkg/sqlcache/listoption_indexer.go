package sqlcache

import (
	"database/sql"
	"encoding/gob"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/rancher/steve/pkg/stores/partition/listprocessor"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"
	"strconv"
	"strings"
)

// ListOptionIndexer extends VersionedIndexer by allowing queries based on ListOption
type ListOptionIndexer struct {
	*VersionedIndexer

	fields      [][]string
	addField    *sql.Stmt
	lastVersion *sql.Stmt
}

// NewListOptionIndexer returns a SQLite-backed cache.Indexer of unstructured.Unstructured Kubernetes resources of a certain GVK
// ListOptionIndexer is also able to satisfy ListOption queries on indexed (sub)fields
// Fields are specified as slices (eg. "metadata.resourceVersion" is ["metadata", "resourceVersion"])
func NewListOptionIndexer(example *unstructured.Unstructured, keyFunc cache.KeyFunc, fields [][]string, path string) (*ListOptionIndexer, error) {
	// necessary in order to gob/ungob unstructured.Unstructured objects
	gob.Register(map[string]interface{}{})

	versionFunc := func(a any) (int, error) {
		o, ok := a.(*unstructured.Unstructured)
		if !ok {
			return 0, errors.Errorf("Unexpected object type, expected unstructured.Unstructured: %v", a)
		}
		i, err := strconv.Atoi(o.GetResourceVersion())
		if err != nil {
			return 0, errors.Errorf("Unexpected non-integer ResourceVersion: %v", o.GetResourceVersion())
		}
		return i, nil
	}
	v, err := NewVersionedIndexer(example, keyFunc, versionFunc, path, cache.Indexers{})
	if err != nil {
		return nil, err
	}

	completedFields := [][]string{{"metadata", "name"}, {"metadata", "namespace"}}
	for _, f := range fields {
		completedFields = append(completedFields, f)
	}

	l := &ListOptionIndexer{
		VersionedIndexer: v,
		fields:           completedFields,
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
	l.lastVersion = l.Prepare(`SELECT CAST(MAX(version) AS TEXT) FROM object_history`)

	return l, nil
}

/* Core methods */

// AfterUpsert saves sortable/filterable fields into tables
func (l *ListOptionIndexer) AfterUpsert(key string, obj any, tx *sql.Tx) error {
	version, err := l.versionFunc(obj)
	if err != nil {
		return err
	}

	for _, field := range l.fields {
		value, err := getField(obj, field)
		if err != nil {
			return err
		}
		switch typedValue := value.(type) {
		case nil:
			_, err = tx.Stmt(l.addField).Exec(toColumnName(field), key, version, "")
		case int, bool, string:
			_, err = tx.Stmt(l.addField).Exec(toColumnName(field), key, version, fmt.Sprint(typedValue))
		case []string:
			_, err = tx.Stmt(l.addField).Exec(toColumnName(field), key, version, strings.Join(typedValue, "|"))
		default:
			return errors.Errorf("%v has a non-supported type value: %v", toColumnName(field), value)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// ListByOptions returns objects according to the specified list options and partitions
// result is an unstructured.UnstructuredList, a revision string or an error
func (l *ListOptionIndexer) ListByOptions(lo listprocessor.ListOptions, partitions []listprocessor.Partition) (*unstructured.UnstructuredList, string, error) {
	// compute list of "interesting" fields (default plus filtering or sorting fields)
	fields := sets.NewString("metadata.name", "metadata.namespace")
	for _, filter := range lo.Filters {
		fields.Insert(toColumnName(filter.Field))
	}
	if len(lo.Sort.PrimaryField) > 0 {
		fields.Insert(toColumnName(lo.Sort.PrimaryField))
	}
	if len(lo.Sort.SecondaryField) > 0 {
		fields.Insert(toColumnName(lo.Sort.SecondaryField))
	}

	// compute join clauses (one per interesting field) and their corresponding parameters
	joinClauses := []string{}
	params := []any{}
	for _, field := range fields.List() {
		joinClauses = append(joinClauses, fmt.Sprintf(`JOIN fields "f_%s" ON "f_%s".key = o.key AND "f_%s".version = o.version AND "f_%s".name = ?`, field, field, field, field))
		params = append(params, field)
	}

	// compute WHERE clauses from lo (.Filters and .Revision) and their corresponding parameters
	whereClauses := []string{}
	for _, filter := range lo.Filters {
		columnName := toColumnName(filter.Field)
		whereClauses = append(whereClauses, fmt.Sprintf(`"f_%s".value LIKE ?`, columnName))
		params = append(params, fmt.Sprintf("%%%s%%", filter.Match))
	}
	if lo.Revision == "" {
		// latest
		whereClauses = append(whereClauses, "o.version = (SELECT MAX(o2.version) FROM object_history o2 WHERE o2.key = o.key)")
		whereClauses = append(whereClauses, "o.deleted_version IS NULL")
	} else {
		version, err := strconv.Atoi(lo.Revision)
		if err != nil {
			return nil, "", errors.Wrapf(err, "Could not parse Revision %s", lo.Revision)
		}
		whereClauses = append(whereClauses, "o.version = (SELECT MAX(o2.version) FROM object_history o2 WHERE o2.key = o.key AND o2.version <= ?)")
		params = append(params, version)
		whereClauses = append(whereClauses, "(o.deleted_version IS NULL OR o.deleted_version > ?)")
		params = append(params, version)
	}

	// compute WHERE clauses from partitions and their corresponding parameters
	partitionClauses := []string{}
	for _, partition := range partitions {
		if partition.Passthrough {
			// nothing to do, no extra filtering to apply by definition
		} else {
			// always filter by namespace
			singlePartitionClauses := []string{fmt.Sprintf(`"f_metadata.namespace".value = ?`)}
			params = append(params, partition.Namespace)

			// optionally filter by names
			if !partition.All {
				names := partition.Names

				if len(names) == 0 {
					// degenerate case, there will be no results
					singlePartitionClauses = append(singlePartitionClauses, "FALSE")
				} else {
					singlePartitionClauses = append(singlePartitionClauses, fmt.Sprintf(`"f_metadata.name".value IN (?%s)`, strings.Repeat(", ?", len(partition.Names)-1)))
					for name := range partition.Names {
						params = append(params, name)
					}
				}
			}

			partitionClauses = append(partitionClauses, strings.Join(singlePartitionClauses, " AND "))
		}
	}
	if len(partitions) == 0 {
		// degenerate case, there will be no results
		whereClauses = append(whereClauses, "FALSE")
	}
	if len(partitionClauses) == 1 {
		whereClauses = append(whereClauses, partitionClauses[0])
	}
	if len(partitionClauses) > 1 {
		whereClauses = append(whereClauses, "(\n      ("+strings.Join(partitionClauses, ") OR\n      (")+")\n)")
	}

	// compute ORDER BY clauses (from lo.Sort)
	orderByClauses := []string{}
	if len(lo.Sort.PrimaryField) > 0 {
		columnName := toColumnName(lo.Sort.PrimaryField)
		direction := "ASC"
		if lo.Sort.PrimaryOrder == listprocessor.DESC {
			direction = "DESC"
		}
		orderByClauses = append(orderByClauses, fmt.Sprintf(`"f_%s".value %s`, columnName, direction))
	}
	if len(lo.Sort.SecondaryField) > 0 {
		columnName := toColumnName(lo.Sort.SecondaryField)
		direction := "ASC"
		if lo.Sort.SecondaryOrder == listprocessor.DESC {
			direction = "DESC"
		}
		orderByClauses = append(orderByClauses, fmt.Sprintf(`"f_%s".value %s`, columnName, direction))
	}

	// compute LIMIT/OFFSET clauses (from lo.Pagination)
	limitClause := ""
	offsetClause := ""
	if lo.Pagination.PageSize >= 1 {
		limitClause = "\n  LIMIT ?"
		params = append(params, lo.Pagination.PageSize)

		if lo.Pagination.Page >= 1 {
			offsetClause = "\n  OFFSET ?"
			params = append(params, lo.Pagination.PageSize*(lo.Pagination.Page-1))
		}
	}

	// put the final query together
	stmt := `SELECT o.object FROM object_history o`
	if len(joinClauses) > 0 {
		stmt += "\n  "
		stmt += strings.Join(joinClauses, "\n  ")
	}
	if len(whereClauses) > 0 {
		stmt += "\n  WHERE\n    "
		stmt += strings.Join(whereClauses, " AND\n    ")
	}
	if len(orderByClauses) > 0 {
		stmt += "\n  ORDER BY "
		stmt += strings.Join(orderByClauses, ", ")
	}
	stmt += limitClause
	stmt += offsetClause

	logrus.Debugf("ListOptionIndexer prepared statement: %v", stmt)
	logrus.Debugf("Params: %v", params...)

	items, err := l.QueryObjects(l.Prepare(stmt), params...)
	if err != nil {
		return nil, "", err
	}

	version := lo.Revision

	if version == "" {
		versions, err := l.QueryStrings(l.lastVersion)
		if err != nil {
			return nil, "", err
		}
		version = versions[0]
	}

	return toUnstructuredList(items), version, nil
}

/* Utilities */

// toColumnName returns the column name corresponding to a field expressed as string slice
func toColumnName(s []string) string {
	return strings.ReplaceAll(strings.Join(s, "."), "\"", ".")
}

// getField extracts the value of a field expressed as a string slice from an unstructured object
func getField(a any, field []string) (any, error) {
	o, ok := a.(*unstructured.Unstructured)
	if !ok {
		return nil, errors.Errorf("Unexpected object type, expected unstructured.Unstructured: %v", a)
	}
	result, ok, err := unstructured.NestedFieldNoCopy(o.Object, field...)
	if !ok || err != nil {
		return nil, errors.Wrapf(err, "Could not extract field %v from object %v", field, o)
	}
	return result, nil
}

// toUnstructuredList turns a slice of unstructured objects into an unstructured.UnstructuredList
func toUnstructuredList(items []any) *unstructured.UnstructuredList {
	objectItems := make([]map[string]any, len(items))
	result := &unstructured.UnstructuredList{
		Items:  make([]unstructured.Unstructured, len(items)),
		Object: map[string]interface{}{"items": objectItems},
	}
	for i, item := range items {
		result.Items[i] = *item.(*unstructured.Unstructured)
		objectItems[i] = item.(*unstructured.Unstructured).Object
	}
	return result
}
