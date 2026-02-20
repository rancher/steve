package informer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/sqlcache/db"
	"github.com/rancher/steve/pkg/sqlcache/informer/internal/ring"
	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

// ListOptionIndexer extends Indexer by allowing queries based on ListOption
type ListOptionIndexer struct {
	*Indexer

	namespaced     bool
	indexedFields  map[string]IndexedField // UI field ID -> field for O(1) lookups
	columnOrder    []string                // all UI field IDs (sorted, for deterministic iteration)
	uniqueColumns  []string                // unique database column names (for schema creation and value extraction)

	// lock protects latestRV
	lock     sync.RWMutex
	latestRV string

	eventLog *ring.CircularBuffer[*event]

	addFieldsStmt    db.Stmt
	deleteFieldsStmt db.Stmt
	dropFieldsStmt   db.Stmt
	upsertLabelsStmt db.Stmt
	deleteLabelsStmt db.Stmt
	dropLabelsStmt   db.Stmt
}

var (
	defaultIndexedFields = []IndexedField{
		&JSONPathField{Path: []string{"metadata", "name"}},
		&JSONPathField{Path: []string{"metadata", "creationTimestamp"}},
	}
	defaultIndexNamespaced = "metadata.namespace"
	immutableFields        = sets.New(
		"metadata.creationTimestamp",
		"metadata.namespace",
		"metadata.name",
		"id",
	)

	ErrTooOld = errors.New("resourceversion too old")
)

const (
	createFieldsTableFmt = `CREATE TABLE "%s_fields" (
		key TEXT NOT NULL REFERENCES "%s"(key) ON DELETE CASCADE,
		%s,
		PRIMARY KEY (key)
    )`
	createFieldsIndexFmt = `CREATE INDEX "%s_%s_index" ON "%s_fields"("%s")`
	deleteFieldsFmt      = `DELETE FROM "%s_fields"`
	dropFieldsFmt        = `DROP TABLE IF EXISTS "%s_fields"`

	createLabelsTableFmt = `CREATE TABLE IF NOT EXISTS "%s_labels" (
		key TEXT NOT NULL REFERENCES "%s"(key) ON DELETE CASCADE,
		label TEXT NOT NULL,
		value TEXT NOT NULL,
		PRIMARY KEY (key, label)
	)`
	createLabelsTableIndexFmt = `CREATE INDEX IF NOT EXISTS "%s_labels_index" ON "%s_labels"(label, value)`

	upsertLabelsStmtFmt = `
INSERT INTO "%s_labels" (key, label, value)
VALUES (?, ?, ?)
ON CONFLICT(key, label) DO UPDATE SET
  value = excluded.value`
	deleteLabelsStmtFmt = `DELETE FROM "%s_labels"`
	dropLabelsStmtFmt   = `DROP TABLE IF EXISTS "%s_labels"`
)

// event mimics watch.Event but replaces uses a metav1.Object instead of runtime.Object, as its guaranteed to be an actual Object, as Bookmark or Error are treated separately
type event struct {
	Type     watch.EventType
	Previous metav1.Object
	Object   metav1.Object
}

type ListOptionIndexerOptions struct {
	// Fields is a map of column name to IndexedField for filtering & sorting.
	// Each IndexedField specifies its column name, SQL type, and value extraction logic.
	Fields map[string]IndexedField
	// IsNamespaced determines whether the GVK for this ListOptionIndexer is
	// namespaced
	IsNamespaced bool
	// GCKeepCount is how many events to keep in memory
	GCKeepCount int
}

// NewListOptionIndexer returns a SQLite-backed cache.Indexer of unstructured.Unstructured Kubernetes resources of a certain GVK
// ListOptionIndexer is also able to satisfy ListOption queries on indexed (sub)fields.
func NewListOptionIndexer(ctx context.Context, s Store, opts ListOptionIndexerOptions) (*ListOptionIndexer, error) {
	i, err := NewIndexer(ctx, cache.Indexers{}, s)
	if err != nil {
		return nil, err
	}

	// Build the map from default fields + options
	indexedFields := make(map[string]IndexedField)

	// Add default fields
	for _, field := range defaultIndexedFields {
		fieldID := smartJoin(field.(*JSONPathField).Path)
		indexedFields[fieldID] = field
	}

	// Add namespace if namespaced
	if opts.IsNamespaced {
		field := &JSONPathField{Path: strings.Split(defaultIndexNamespaced, ".")}
		indexedFields[defaultIndexNamespaced] = field
	}

	// Merge in provided fields (overwrite if duplicate)
	for k, v := range opts.Fields {
		indexedFields[k] = v
	}

	// Sort keys for deterministic order. This ensures consistent SQL schema
	// generation and prepared statement parameter ordering across restarts.
	columnOrder := make([]string, 0, len(indexedFields))
	for name := range indexedFields {
		columnOrder = append(columnOrder, name)
	}
	slices.Sort(columnOrder)

	// Build list of unique database columns (deduplicating by ColumnName())
	// Multiple UI field IDs may map to the same database column
	seenColumns := make(map[string]bool)
	uniqueColumns := make([]string, 0)
	for _, mapKey := range columnOrder {
		field := indexedFields[mapKey]
		colName := field.ColumnName()
		if !seenColumns[colName] {
			seenColumns[colName] = true
			uniqueColumns = append(uniqueColumns, colName)
		}
	}
	// Sort unique columns for deterministic schema generation
	slices.Sort(uniqueColumns)

	maxEventHistory := opts.GCKeepCount
	if maxEventHistory <= 0 {
		maxEventHistory = 1000
	}

	l := &ListOptionIndexer{
		Indexer:       i,
		namespaced:    opts.IsNamespaced,
		indexedFields: indexedFields,
		columnOrder:   columnOrder,
		uniqueColumns: uniqueColumns,
		eventLog:      ring.NewCircularBuffer[*event](maxEventHistory),
	}
	l.RegisterAfterAdd(l.addIndexFields)
	l.RegisterAfterAdd(l.addLabels)
	l.RegisterAfterAdd(l.notifyEventAdded)
	l.RegisterAfterUpdate(l.addIndexFields)
	l.RegisterAfterUpdate(l.addLabels)
	l.RegisterAfterUpdate(l.notifyEventModified)
	l.RegisterAfterDelete(l.notifyEventDeleted)
	l.RegisterAfterDeleteAll(l.deleteFields)
	l.RegisterAfterDeleteAll(l.deleteLabels)
	l.RegisterBeforeDropAll(l.closeEventLog)
	l.RegisterBeforeDropAll(l.dropLabels)
	l.RegisterBeforeDropAll(l.dropFields)

	// Build column definitions using unique columns
	// Each IndexedField specifies its SQL type via ColumnType().
	columnDefs := make([]string, 0, len(uniqueColumns))
	for _, colName := range uniqueColumns {
		// Find a field with this column name to get the type
		var field IndexedField
		for _, mapKey := range columnOrder {
			if indexedFields[mapKey].ColumnName() == colName {
				field = indexedFields[mapKey]
				break
			}
		}
		columnDefs = append(columnDefs, fmt.Sprintf(`"%s" %s`, colName, field.ColumnType()))
	}

	dbName := db.Sanitize(i.GetName())
	columns := make([]string, 0, len(columnOrder))
	qmarks := make([]string, 0, len(columnOrder))
	setStatements := make([]string, 0, len(columnOrder))

	err = l.WithTransaction(ctx, true, func(tx db.TxClient) error {
		dropFieldsQuery := fmt.Sprintf(dropFieldsFmt, dbName)
		if _, err := tx.Exec(dropFieldsQuery); err != nil {
			return err
		}

		createFieldsTableQuery := fmt.Sprintf(createFieldsTableFmt, dbName, dbName, strings.Join(columnDefs, ", "))
		if _, err := tx.Exec(createFieldsTableQuery); err != nil {
			return err
		}

		// Create indexes and build prepared statement components for unique columns
		for _, actualColumnName := range uniqueColumns {
			// create index for field
			createFieldsIndexQuery := fmt.Sprintf(createFieldsIndexFmt, dbName, actualColumnName, dbName, actualColumnName)
			if _, err := tx.Exec(createFieldsIndexQuery); err != nil {
				return err
			}

			// format field into column for prepared statement
			column := fmt.Sprintf(`"%s"`, actualColumnName)
			columns = append(columns, column)

			// add placeholder for column's value in prepared statement
			qmarks = append(qmarks, "?")

			// add formatted set statement for prepared statement
			// optimization: avoid SET for fields which cannot change
			if !immutableFields.Has(actualColumnName) {
				setStatement := fmt.Sprintf(`"%s" = excluded."%s"`, actualColumnName, actualColumnName)
				setStatements = append(setStatements, setStatement)
			}
		}

		dropLabelsQuery := fmt.Sprintf(dropLabelsStmtFmt, dbName)
		if _, err := tx.Exec(dropLabelsQuery); err != nil {
			return err
		}

		createLabelsTableQuery := fmt.Sprintf(createLabelsTableFmt, dbName, dbName)
		if _, err := tx.Exec(createLabelsTableQuery); err != nil {
			return err
		}

		createLabelsTableIndexQuery := fmt.Sprintf(createLabelsTableIndexFmt, dbName, dbName)
		if _, err := tx.Exec(createLabelsTableIndexQuery); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	addFieldsOnConflict := "NOTHING"
	if len(setStatements) > 0 {
		addFieldsOnConflict = "UPDATE SET " + strings.Join(setStatements, ", ")
	}
	l.addFieldsStmt = l.Prepare(fmt.Sprintf(
		`INSERT INTO "%s_fields"(key, %s) VALUES (?, %s) ON CONFLICT DO %s`,
		dbName,
		strings.Join(columns, ", "),
		strings.Join(qmarks, ", "),
		addFieldsOnConflict,
	))
	l.deleteFieldsStmt = l.Prepare(fmt.Sprintf(deleteFieldsFmt, dbName))
	l.dropFieldsStmt = l.Prepare(fmt.Sprintf(dropFieldsFmt, dbName))

	l.upsertLabelsStmt = l.Prepare(fmt.Sprintf(upsertLabelsStmtFmt, dbName))
	l.deleteLabelsStmt = l.Prepare(fmt.Sprintf(deleteLabelsStmtFmt, dbName))
	l.dropLabelsStmt = l.Prepare(fmt.Sprintf(dropLabelsStmtFmt, dbName))

	return l, nil
}

func (l *ListOptionIndexer) GetLatestResourceVersion() []string {
	var latestRV []string

	l.lock.RLock()
	latestRV = []string{l.latestRV}
	l.lock.RUnlock()

	return latestRV
}

func (l *ListOptionIndexer) Watch(ctx context.Context, opts WatchOptions, eventsCh chan<- watch.Event) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	r := l.eventLog.NewReader()
	if targetRV := opts.ResourceVersion; targetRV != "" {
		found := r.Rewind(func(v *event) bool {
			return v.Object.GetResourceVersion() == targetRV
		})
		if !found {
			return ErrTooOld
		}

		// Discard the target object, as that's actually the last known resource version, we need to send the following ones
		if _, err := r.Read(ctx); err != nil {
			return err
		}
	}

	filter := opts.Filter
	for {
		e, err := r.Read(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if !filter.matches(e.Previous) && !filter.matches(e.Object) {
			continue
		}
		eventsCh <- watch.Event{
			Type:   e.Type,
			Object: e.Object.(runtime.Object).DeepCopyObject(),
		}
	}
}

func (l *ListOptionIndexer) decryptScanEvent(rows db.Rows, into runtime.Object) (watch.EventType, error) {
	var typ, rv string
	var serialized db.SerializedObject
	if err := rows.Scan(&typ, &rv, &serialized.Bytes, &serialized.Nonce, &serialized.KeyID); err != nil {
		return watch.Error, err
	}
	if err := l.Deserialize(serialized, into); err != nil {
		return watch.Error, err
	}
	return watch.EventType(typ), nil
}

/* Core methods */

func (l *ListOptionIndexer) notifyEventAdded(key string, obj any, _ db.TxClient) error {
	return l.notifyEvent(watch.Added, nil, obj)
}

func (l *ListOptionIndexer) notifyEventModified(key string, obj any, _ db.TxClient) error {
	oldObj, exists, err := l.GetByKey(key)
	if err != nil {
		return fmt.Errorf("error getting old object: %w", err)
	}

	if !exists {
		return fmt.Errorf("old object %q should be in store but was not", key)
	}

	return l.notifyEvent(watch.Modified, oldObj, obj)
}

func (l *ListOptionIndexer) notifyEventDeleted(key string, obj any, _ db.TxClient) error {
	oldObj, exists, err := l.GetByKey(key)
	if err != nil {
		return fmt.Errorf("error getting old object: %w", err)
	}

	if !exists {
		return fmt.Errorf("old object %q should be in store but was not", key)
	}
	return l.notifyEvent(watch.Deleted, oldObj, obj)
}

func (l *ListOptionIndexer) notifyEvent(eventType watch.EventType, old any, current any) error {
	obj, err := meta.Accessor(current)
	if err != nil {
		return err
	}

	var oldObj metav1.Object
	if old != nil {
		oldObj, err = meta.Accessor(old)
		if err != nil {
			return err
		}
	}

	latestRV := obj.GetResourceVersion()
	if err := l.eventLog.Write(&event{
		Type:     eventType,
		Previous: oldObj,
		Object:   obj,
	}); err != nil {
		return err
	}

	l.lock.Lock()
	defer l.lock.Unlock()
	l.latestRV = latestRV
	return nil
}

func (l *ListOptionIndexer) closeEventLog(_ db.TxClient) error {
	l.eventLog.Close()
	return nil
}

// addIndexFields saves sortable/filterable fields into tables
func (l *ListOptionIndexer) addIndexFields(key string, obj any, tx db.TxClient) error {
	unstrObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("expected unstructured.Unstructured, got %T", obj)
	}

	args := []any{key}

	// Iterate over unique columns to extract values
	// We need to find a field that maps to each unique column name
	for _, actualColumnName := range l.uniqueColumns {
		// Find a field with this column name
		var field IndexedField
		for _, mapKey := range l.columnOrder {
			if l.indexedFields[mapKey].ColumnName() == actualColumnName {
				field = l.indexedFields[mapKey]
				break
			}
		}
		value, err := field.GetValue(unstrObj)
		if err != nil {
			logrus.Errorf("cannot index field %v: %v", actualColumnName, err)
			args = append(args, normalizeValue(nil))
			continue
		}
		args = append(args, normalizeValue(value))
	}

	_, err := tx.Stmt(l.addFieldsStmt).Exec(args...)
	return err
}

// normalizeValue converts a value to a SQL-compatible type
func normalizeValue(value any) any {
	switch v := value.(type) {
	case nil:
		return ""
	case int, int64, float64, bool:
		return v
	case string:
		return v
	case []string:
		return strings.Join(v, "|")
	case []interface{}:
		strValues := make([]string, len(v))
		for i, item := range v {
			strValues[i] = fmt.Sprint(item)
		}
		return strings.Join(strValues, "|")
	default:
		return fmt.Sprint(v)
	}
}

// labels are stored in tables that shadow the underlying object table for each GVK
func (l *ListOptionIndexer) addLabels(key string, obj any, tx db.TxClient) error {
	k8sObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("addLabels: unexpected object type, expected unstructured.Unstructured: %v", obj)
	}
	incomingLabels := k8sObj.GetLabels()
	for k, v := range incomingLabels {
		if _, err := tx.Stmt(l.upsertLabelsStmt).Exec(key, k, v); err != nil {
			return err
		}
	}
	return nil
}

func (l *ListOptionIndexer) deleteFields(tx db.TxClient) error {
	_, err := tx.Stmt(l.deleteFieldsStmt).Exec()
	return err
}

func (l *ListOptionIndexer) dropFields(tx db.TxClient) error {
	_, err := tx.Stmt(l.dropFieldsStmt).Exec()
	return err
}

func (l *ListOptionIndexer) deleteLabels(tx db.TxClient) error {
	_, err := tx.Stmt(l.deleteLabelsStmt).Exec()
	return err
}

func (l *ListOptionIndexer) dropLabels(tx db.TxClient) error {
	_, err := tx.Stmt(l.dropLabelsStmt).Exec()
	return err
}

// ListByOptions returns objects according to the specified list options and partitions.
// Specifically:
//   - an unstructured list of resources belonging to any of the specified partitions
//   - the total number of resources (returned list might be a subset depending on pagination options in lo)
//   - a summary object, containing the possible values for each field specified in a summary= subquery
//   - a continue token, if there are more pages after the returned one
//   - an error instead of all of the above if anything went wrong
func (l *ListOptionIndexer) ListByOptions(ctx context.Context, lo *sqltypes.ListOptions, partitions []partition.Partition, namespace string) (list *unstructured.UnstructuredList, total int, summary *types.APISummary, continueToken string, err error) {
	dbName := db.Sanitize(l.GetName())
	if len(lo.SummaryFieldList) > 0 {
		if summary, err = l.ListSummaryFields(ctx, lo, partitions, dbName, namespace); err != nil {
			return
		}
	}
	var queryInfo *QueryInfo
	if queryInfo, err = l.constructQuery(lo, partitions, namespace, dbName); err != nil {
		return
	}
	logrus.Debugf("ListOptionIndexer prepared statement: %v", queryInfo.query)
	logrus.Debugf("Params: %v", queryInfo.params)
	logrus.Tracef("ListOptionIndexer prepared count-statement: %v", queryInfo.countQuery)
	logrus.Tracef("Params: %v", queryInfo.countParams)
	list, total, continueToken, err = l.executeQuery(ctx, queryInfo)
	return
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
		if cerr := stmt.Close(); cerr != nil && err == nil {
			err = errors.Join(err, cerr)
		}
	}()

	var items []any
	err = l.WithTransaction(ctx, false, func(tx db.TxClient) error {
		now := time.Now()
		rows, err := l.QueryForRows(ctx, tx.Stmt(stmt), queryInfo.params...)
		if err != nil {
			return err
		}
		elapsed := time.Since(now)
		logLongQuery(elapsed, queryInfo.query, queryInfo.params)
		items, err = l.ReadObjects(rows, l.GetType())
		if err != nil {
			return fmt.Errorf("read objects: %w", err)
		}

		total = len(items)
		if queryInfo.countQuery != "" {
			countStmt := l.Prepare(queryInfo.countQuery)
			defer func() {
				if cerr := countStmt.Close(); cerr != nil {
					err = errors.Join(err, cerr)
				}
			}()
			now = time.Now()
			rows, err := l.QueryForRows(ctx, tx.Stmt(countStmt), queryInfo.countParams...)
			if err != nil {
				return err
			}
			elapsed = time.Since(now)
			logLongQuery(elapsed, queryInfo.countQuery, queryInfo.countParams)
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

	l.lock.RLock()
	latestRV := l.latestRV
	l.lock.RUnlock()

	return toUnstructuredList(items, latestRV), total, continueToken, nil
}

func logLongQuery(elapsed time.Duration, query string, params []any) {
	threshold := 500 * time.Millisecond
	if elapsed < threshold {
		return
	}
	logrus.Debugf("Query took more than %v (took %v): %s with params %v", threshold, elapsed, query, params)
}

// toUnstructuredList turns a slice of unstructured objects into an unstructured.UnstructuredList
func toUnstructuredList(items []any, resourceVersion string) *unstructured.UnstructuredList {
	objectItems := make([]any, len(items))
	result := &unstructured.UnstructuredList{
		Items:  make([]unstructured.Unstructured, len(items)),
		Object: map[string]interface{}{"items": objectItems},
	}
	if resourceVersion != "" {
		result.SetResourceVersion(resourceVersion)
	}
	for i, item := range items {
		result.Items[i] = *item.(*unstructured.Unstructured)
		objectItems[i] = item.(*unstructured.Unstructured).Object
	}
	return result
}
