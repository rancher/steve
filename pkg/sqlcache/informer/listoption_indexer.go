package informer

import (
	"context"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/rancher/steve/pkg/sqlcache/db/transaction"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"

	"github.com/rancher/steve/pkg/sqlcache/db"
	"github.com/rancher/steve/pkg/sqlcache/partition"
)

// ListOptionIndexer extends Indexer by allowing queries based on ListOption
type ListOptionIndexer struct {
	*Indexer

	namespaced              bool
	indexedFields           []string
	fieldGetterForGroupName func(groupName string) ([][]string, error)

	addFieldQuery     string
	deleteFieldQuery  string
	upsertLabelsQuery string
	deleteLabelsQuery string

	addFieldStmt     *sql.Stmt
	deleteFieldStmt  *sql.Stmt
	upsertLabelsStmt *sql.Stmt
	deleteLabelsStmt *sql.Stmt
}

var (
	defaultIndexedFields   = []string{"metadata.name", "metadata.creationTimestamp"}
	defaultIndexNamespaced = "metadata.namespace"
	subfieldRegex          = regexp.MustCompile(`([a-zA-Z]+)|(\[[-a-zA-Z./]+])|(\[[0-9]+])`)

	ErrInvalidColumn = errors.New("supplied column is invalid")
)

const (
	matchFmt                 = `%%%s%%`
	strictMatchFmt           = `%s`
	escapeBackslashDirective = ` ESCAPE '\'` // The leading space is crucial for unit tests only '
	createFieldsTableFmt     = `CREATE TABLE "%s_fields" (
			key TEXT NOT NULL PRIMARY KEY,
            %s
	   )`
	createFieldsIndexFmt = `CREATE INDEX "%s_%s_index" ON "%s_fields"("%s")`

	failedToGetFromSliceFmt = "[listoption indexer] failed to get subfield [%s] from slice items: %w"

	createLabelsTableFmt = `CREATE TABLE IF NOT EXISTS "%s_labels" (
		key TEXT NOT NULL REFERENCES "%s"(key) ON DELETE CASCADE,
		label TEXT NOT NULL,
		value TEXT NOT NULL,
		PRIMARY KEY (key, label)
	)`
	createLabelsTableIndexFmt = `CREATE INDEX IF NOT EXISTS "%s_labels_index" ON "%s_labels"(label, value)`

	upsertLabelsStmtFmt = `REPLACE INTO "%s_labels"(key, label, value) VALUES (?, ?, ?)`
	deleteLabelsStmtFmt = `DELETE FROM "%s_labels" WHERE KEY = ?`
)

// NewListOptionIndexer returns a SQLite-backed cache.Indexer of unstructured.Unstructured Kubernetes resources of a certain GVK
// ListOptionIndexer is also able to satisfy ListOption queries on indexed (sub)fields.
// Fields are specified as slices (e.g. "metadata.resourceVersion" is ["metadata", "resourceVersion"])
func NewListOptionIndexer(ctx context.Context, fields [][]string, s Store, namespaced bool) (*ListOptionIndexer, error) {
	// necessary in order to gob/ungob unstructured.Unstructured objects
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})

	i, err := NewIndexer(ctx, cache.Indexers{}, s)
	if err != nil {
		return nil, err
	}

	var indexedFields []string
	for _, f := range defaultIndexedFields {
		indexedFields = append(indexedFields, f)
	}
	if namespaced {
		indexedFields = append(indexedFields, defaultIndexNamespaced)
	}
	for _, f := range fields {
		indexedFields = append(indexedFields, toColumnName(f))
	}

	l := &ListOptionIndexer{
		Indexer:       i,
		namespaced:    namespaced,
		indexedFields: indexedFields,
	}
	l.RegisterAfterUpsert(l.addIndexFields)
	l.RegisterAfterUpsert(l.addLabels)
	l.RegisterAfterDelete(l.deleteIndexFields)
	l.RegisterAfterDelete(l.deleteLabels)
	columnDefs := make([]string, len(indexedFields))
	for index, field := range indexedFields {
		column := fmt.Sprintf(`"%s" TEXT`, field)
		columnDefs[index] = column
	}

	dbName := db.Sanitize(i.GetName())
	columns := make([]string, len(indexedFields))
	qmarks := make([]string, len(indexedFields))
	setStatements := make([]string, len(indexedFields))

	err = l.WithTransaction(ctx, true, func(tx transaction.Client) error {
		_, err = tx.Exec(fmt.Sprintf(createFieldsTableFmt, dbName, strings.Join(columnDefs, ", ")))
		if err != nil {
			return err
		}

		for index, field := range indexedFields {
			// create index for field
			_, err = tx.Exec(fmt.Sprintf(createFieldsIndexFmt, dbName, field, dbName, field))
			if err != nil {
				return err
			}

			// format field into column for prepared statement
			column := fmt.Sprintf(`"%s"`, field)
			columns[index] = column

			// add placeholder for column's value in prepared statement
			qmarks[index] = "?"

			// add formatted set statement for prepared statement
			setStatement := fmt.Sprintf(`"%s" = excluded."%s"`, field, field)
			setStatements[index] = setStatement
		}
		createLabelsTableQuery := fmt.Sprintf(createLabelsTableFmt, dbName, dbName)
		_, err = tx.Exec(createLabelsTableQuery)
		if err != nil {
			return &db.QueryError{QueryString: createLabelsTableQuery, Err: err}
		}

		createLabelsTableIndexQuery := fmt.Sprintf(createLabelsTableIndexFmt, dbName, dbName)
		_, err = tx.Exec(createLabelsTableIndexQuery)
		if err != nil {
			return &db.QueryError{QueryString: createLabelsTableIndexQuery, Err: err}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	l.addFieldQuery = fmt.Sprintf(
		`INSERT INTO "%s_fields"(key, %s) VALUES (?, %s) ON CONFLICT DO UPDATE SET %s`,
		dbName,
		strings.Join(columns, ", "),
		strings.Join(qmarks, ", "),
		strings.Join(setStatements, ", "),
	)
	l.deleteFieldQuery = fmt.Sprintf(`DELETE FROM "%s_fields" WHERE key = ?`, dbName)

	l.addFieldStmt = l.Prepare(l.addFieldQuery)
	l.deleteFieldStmt = l.Prepare(l.deleteFieldQuery)

	l.upsertLabelsQuery = fmt.Sprintf(upsertLabelsStmtFmt, dbName)
	l.deleteLabelsQuery = fmt.Sprintf(deleteLabelsStmtFmt, dbName)
	l.upsertLabelsStmt = l.Prepare(l.upsertLabelsQuery)
	l.deleteLabelsStmt = l.Prepare(l.deleteLabelsQuery)

	return l, nil
}

func (l *ListOptionIndexer) SetFieldGetterForGroupName(fieldGetter func(groupName string) ([][]string, error)) {
	l.fieldGetterForGroupName = fieldGetter
}

/* Core methods */

// addIndexFields saves sortable/filterable fields into tables
func (l *ListOptionIndexer) addIndexFields(key string, obj any, tx transaction.Client) error {
	args := []any{key}
	for _, field := range l.indexedFields {
		value, err := getField(obj, field)
		if err != nil {
			logrus.Errorf("cannot index object of type [%s] with key [%s] for indexer [%s]: %v", l.GetType().String(), key, l.GetName(), err)
			return err
		}
		switch typedValue := value.(type) {
		case nil:
			args = append(args, "")
		case int, bool, string:
			args = append(args, fmt.Sprint(typedValue))
		case []string:
			args = append(args, strings.Join(typedValue, "|"))
		default:
			err2 := fmt.Errorf("field %v has a non-supported type value: %v", field, value)
			return err2
		}
	}

	_, err := tx.Stmt(l.addFieldStmt).Exec(args...)
	if err != nil {
		return &db.QueryError{QueryString: l.addFieldQuery, Err: err}
	}
	return nil
}

// labels are stored in tables that shadow the underlying object table for each GVK
func (l *ListOptionIndexer) addLabels(key string, obj any, tx transaction.Client) error {
	k8sObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("addLabels: unexpected object type, expected unstructured.Unstructured: %v", obj)
	}
	incomingLabels := k8sObj.GetLabels()
	for k, v := range incomingLabels {
		_, err := tx.Stmt(l.upsertLabelsStmt).Exec(key, k, v)
		if err != nil {
			return &db.QueryError{QueryString: l.upsertLabelsQuery, Err: err}
		}
	}
	return nil
}

func (l *ListOptionIndexer) deleteIndexFields(key string, tx transaction.Client) error {
	args := []any{key}

	_, err := tx.Stmt(l.deleteFieldStmt).Exec(args...)
	if err != nil {
		return &db.QueryError{QueryString: l.deleteFieldQuery, Err: err}
	}
	return nil
}

func (l *ListOptionIndexer) deleteLabels(key string, tx transaction.Client) error {
	_, err := tx.Stmt(l.deleteLabelsStmt).Exec(key)
	if err != nil {
		return &db.QueryError{QueryString: l.deleteLabelsQuery, Err: err}
	}
	return nil
}

// ListByOptions returns objects according to the specified list options and partitions.
// Specifically:
//   - an unstructured list of resources belonging to any of the specified partitions
//   - the total number of resources (returned list might be a subset depending on pagination options in lo)
//   - a continue token, if there are more pages after the returned one
//   - an error instead of all of the above if anything went wrong
func (l *ListOptionIndexer) ListByOptions(ctx context.Context, lo *sqltypes.ListOptions, partitions []partition.Partition, namespace string) (*unstructured.UnstructuredList, int, string, error) {
	queryInfo, err := l.constructQuery(lo, partitions, namespace, db.Sanitize(l.GetName()))
	if err != nil {
		return nil, 0, "", err
	}
	logrus.Debugf("ListOptionIndexer prepared statement: %v", queryInfo.query)
	logrus.Debugf("Params: %v", queryInfo.params)
	return l.executeQuery(ctx, queryInfo)
}

// QueryInfo is a helper-struct that is used to represent the core query and parameters when converting
// a filter from the UI into a sql query
type QueryInfo struct {
	query       string
	params      []any
	countQuery  string
	countParams []any
	limit       int
	offset      int
}

func (l *ListOptionIndexer) executeQuery(ctx context.Context, queryInfo *QueryInfo) (result *unstructured.UnstructuredList, total int, token string, err error) {
	stmt := l.Prepare(queryInfo.query)
	defer func() {
		cerr := l.CloseStmt(stmt)
		if cerr != nil {
			err = errors.Join(err, &db.QueryError{QueryString: queryInfo.query, Err: cerr})
		}
	}()

	var items []any
	err = l.WithTransaction(ctx, false, func(tx transaction.Client) error {
		txStmt := tx.Stmt(stmt)
		rows, err := txStmt.QueryContext(ctx, queryInfo.params...)
		if err != nil {
			return &db.QueryError{QueryString: queryInfo.query, Err: err}
		}
		items, err = l.ReadObjects(rows, l.GetType(), l.GetShouldEncrypt())
		if err != nil {
			return err
		}

		total = len(items)
		if queryInfo.countQuery != "" {
			countStmt := l.Prepare(queryInfo.countQuery)
			defer func() {
				cerr := l.CloseStmt(countStmt)
				if cerr != nil {
					err = errors.Join(err, &db.QueryError{QueryString: queryInfo.countQuery, Err: cerr})
				}
			}()
			txStmt := tx.Stmt(countStmt)
			rows, err := txStmt.QueryContext(ctx, queryInfo.countParams...)
			if err != nil {
				return &db.QueryError{QueryString: queryInfo.countQuery, Err: err}
			}
			total, err = l.ReadInt(rows)
			if err != nil {
				return fmt.Errorf("error reading query results: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		return nil, 0, "", err
	}

	continueToken := ""
	limit := queryInfo.limit
	offset := queryInfo.offset
	if limit > 0 && offset+len(items) < total {
		continueToken = fmt.Sprintf("%d", offset+limit)
	}

	return toUnstructuredList(items), total, continueToken, nil
}

func extractSubFields(fields string) []string {
	subfields := make([]string, 0)
	for _, subField := range subfieldRegex.FindAllString(fields, -1) {
		subfields = append(subfields, strings.TrimSuffix(subField, "."))
	}
	return subfields
}

// getField extracts the value of a field expressed as a string path from an unstructured object
func getField(a any, field string) (any, error) {
	subFields := extractSubFields(field)
	o, ok := a.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("unexpected object type, expected unstructured.Unstructured: %v", a)
	}

	var obj interface{}
	var found bool
	var err error
	obj = o.Object
	for i, subField := range subFields {
		switch t := obj.(type) {
		case map[string]interface{}:
			subField = strings.TrimSuffix(strings.TrimPrefix(subField, "["), "]")
			obj, found, err = unstructured.NestedFieldNoCopy(t, subField)
			if err != nil {
				return nil, err
			}
			if !found {
				// particularly with labels/annotation indexes, it is totally possible that some objects won't have these,
				// so either we this is not an error state or it could be an error state with a type that callers can check for
				return nil, nil
			}
		case []interface{}:
			if strings.HasPrefix(subField, "[") && strings.HasSuffix(subField, "]") {
				key, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(subField, "["), "]"))
				if err != nil {
					return nil, fmt.Errorf("[listoption indexer] failed to convert subfield [%s] to int in listoption index: %w", subField, err)
				}
				if key >= len(t) {
					return nil, fmt.Errorf("[listoption indexer] given index is too large for slice of len %d", len(t))
				}
				obj = fmt.Sprintf("%v", t[key])
			} else if i == len(subFields)-1 {
				// If the last layer is an array, return array.map(a => a[subfield])
				result := make([]string, len(t))
				for index, v := range t {
					itemVal, ok := v.(map[string]interface{})
					if !ok {
						return nil, fmt.Errorf(failedToGetFromSliceFmt, subField, err)
					}
					itemStr, ok := itemVal[subField].(string)
					if !ok {
						return nil, fmt.Errorf(failedToGetFromSliceFmt, subField, err)
					}
					result[index] = itemStr
				}
				return result, nil
			}
		default:
			return nil, fmt.Errorf("[listoption indexer] failed to parse subfields: %v", subFields)
		}
	}
	return obj, nil
}

// toColumnName returns the column name corresponding to a field expressed as string slice
func toColumnName(s []string) string {
	return db.Sanitize(smartJoin(s))
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
