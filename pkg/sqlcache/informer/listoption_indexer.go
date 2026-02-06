package informer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/resources/virtual/multivalue/composite"
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

	namespaced    bool
	indexedFields []string
	typeGuidance  map[string]string

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
	defaultIndexedFields   = []string{"metadata.name", "metadata.creationTimestamp"}
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
	// Fields is a list of fields within the object that we want indexed for
	// filtering & sorting. Each field is specified as a slice.
	//
	// For example, .metadata.resourceVersion should be specified as []string{"metadata", "resourceVersion"}
	Fields [][]string
	// Used for specifying types of non-TEXT database fields.
	// The key is a fully-qualified field name, like 'metadata.fields[1]'.
	// The value is a type name, most likely "INT" but could be "REAL". The default type is "TEXT",
	// and we don't (currently) use NULL or BLOB types.
	TypeGuidance map[string]string
	// IsNamespaced determines whether the GVK for this ListOptionIndexer is
	// namespaced
	IsNamespaced bool
	// GCKeepCount is how many events to keep in memory
	GCKeepCount int
}

// isCompositeField checks if a field uses composite storage (split into multiple columns).
// Composite fields are stored as multiple values in separate database columns.
func (l *ListOptionIndexer) isCompositeField(field string) bool {
	return l.typeGuidance != nil && l.typeGuidance[field] == "COMPOSITE_INT"
}

// NewListOptionIndexer returns a SQLite-backed cache.Indexer of unstructured.Unstructured Kubernetes resources of a certain GVK
// ListOptionIndexer is also able to satisfy ListOption queries on indexed (sub)fields.
func NewListOptionIndexer(ctx context.Context, s Store, opts ListOptionIndexerOptions) (*ListOptionIndexer, error) {
	i, err := NewIndexer(ctx, cache.Indexers{}, s)
	if err != nil {
		return nil, err
	}

	var indexedFields []string
	for _, f := range defaultIndexedFields {
		indexedFields = append(indexedFields, f)
	}
	if opts.IsNamespaced {
		indexedFields = append(indexedFields, defaultIndexNamespaced)
	}
	for _, f := range opts.Fields {
		indexedFields = append(indexedFields, toColumnName(f))
	}

	maxEventHistory := opts.GCKeepCount
	if maxEventHistory <= 0 {
		maxEventHistory = 1000
	}

	l := &ListOptionIndexer{
		Indexer:       i,
		namespaced:    opts.IsNamespaced,
		indexedFields: indexedFields,
		typeGuidance:  opts.TypeGuidance,
		watchers:      make(map[*watchKey]*watcher),
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
	columnDefs := make([]string, 0, len(indexedFields)*2)
	for _, field := range indexedFields {
		typeName := "TEXT"
		newTypeName, ok := opts.TypeGuidance[field]
		if ok {
			typeName = newTypeName
		}
		if typeName == "COMPOSITE_INT" {
			// For COMPOSITE_INT, create two separate integer columns
			columnDefs = append(columnDefs, fmt.Sprintf(`"%s_0" INTEGER`, field))
			columnDefs = append(columnDefs, fmt.Sprintf(`"%s_1" INTEGER`, field))
		} else {
			column := fmt.Sprintf(`"%s" %s`, field, typeName)
			columnDefs = append(columnDefs, column)
		}
	}

	dbName := db.Sanitize(i.GetName())
	columns := make([]string, 0, len(indexedFields))
	qmarks := make([]string, 0, len(indexedFields))
	setStatements := make([]string, 0, len(indexedFields))

	err = l.WithTransaction(ctx, true, func(tx db.TxClient) error {
		dropFieldsQuery := fmt.Sprintf(dropFieldsFmt, dbName)
		if _, err := tx.Exec(dropFieldsQuery); err != nil {
			return err
		}

		createFieldsTableQuery := fmt.Sprintf(createFieldsTableFmt, dbName, dbName, strings.Join(columnDefs, ", "))
		if _, err := tx.Exec(createFieldsTableQuery); err != nil {
			return err
		}

		for _, field := range indexedFields {
			if opts.TypeGuidance[field] == "COMPOSITE_INT" {
				// For COMPOSITE_INT, create indexes for both split columns
				for _, suffix := range []string{"_0", "_1"} {
					col := field + suffix
					createFieldsIndexQuery := fmt.Sprintf(createFieldsIndexFmt, dbName, col, dbName, col)
					if _, err := tx.Exec(createFieldsIndexQuery); err != nil {
						return err
					}
				}

				// Add both columns to the prepared statement
				columns = append(columns, fmt.Sprintf(`"%s_0"`, field), fmt.Sprintf(`"%s_1"`, field))
				qmarks = append(qmarks, "?", "?")
				if !immutableFields.Has(field) {
					setStatements = append(setStatements,
						fmt.Sprintf(`"%s_0" = excluded."%s_0"`, field, field),
						fmt.Sprintf(`"%s_1" = excluded."%s_1"`, field, field))
				}
			} else {
				// create index for field
				createFieldsIndexQuery := fmt.Sprintf(createFieldsIndexFmt, dbName, field, dbName, field)
				if _, err := tx.Exec(createFieldsIndexQuery); err != nil {
					return err
				}

				// format field into column for prepared statement
				column := fmt.Sprintf(`"%s"`, field)
				columns = append(columns, column)

				// add placeholder for column's value in prepared statement
				qmarks = append(qmarks, "?")

				// add formatted set statement for prepared statement
				// optimization: avoid SET for fields which cannot change
				if !immutableFields.Has(field) {
					setStatement := fmt.Sprintf(`"%s" = excluded."%s"`, field, field)
					setStatements = append(setStatements, setStatement)
				}
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
	args := []any{key}
	for _, field := range l.indexedFields {
		value, err := getField(obj, field)
		if err != nil {
			logrus.Errorf("cannot index object of type [%s] with key [%s] for indexer [%s]: %v", l.GetType().String(), key, l.GetName(), err)
			return err
		}

		switch typedValue := value.(type) {
		case nil:
			if l.isCompositeField(field) {
				args = append(args, int64(0), int64(0))
				continue
			}
			args = append(args, "")
		case []interface{}:
			// Check if this is a composite field (splits into multiple columns)
			if l.isCompositeField(field) {
				// Wrap in CompositeInt for type-safe extraction
				ci := composite.CompositeInt{}.From(typedValue)
				args = append(args, ci.Primary, ci.Secondary)
				continue
			}
			// Regular array - join as pipe-separated string
			strValues := make([]string, len(typedValue))
			for i, v := range typedValue {
				strValues[i] = fmt.Sprint(v)
			}
			args = append(args, strings.Join(strValues, "|"))
		case int, bool, string, int64, float64:
			args = append(args, fmt.Sprint(typedValue))
		case []string:
			args = append(args, strings.Join(typedValue, "|"))
		default:
			err2 := fmt.Errorf("field %v has a non-supported type value: %v", field, value)
			return err2
		}
	}

	_, err := tx.Stmt(l.addFieldsStmt).Exec(args...)
	return err
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
