package informer

import (
	"context"
	"errors"
	"fmt"
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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

// ListOptionIndexer extends Indexer by allowing queries based on ListOption
type ListOptionIndexer struct {
	*Indexer

	namespaced    bool
	indexedFields []string

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
	columnDefs := make([]string, len(indexedFields))
	for index, field := range indexedFields {
		typeName := "TEXT"
		newTypeName, ok := opts.TypeGuidance[field]
		if ok {
			typeName = newTypeName
		}
		column := fmt.Sprintf(`"%s" %s`, field, typeName)
		columnDefs[index] = column
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
			args = append(args, "")
		case int, bool, string, int64, float64:
			args = append(args, fmt.Sprint(typedValue))
		case []string:
			args = append(args, strings.Join(typedValue, "|"))
		case []interface{}:
			var s []string
			for _, v := range typedValue {
				s = append(s, fmt.Sprint(v))
			}
			args = append(args, strings.Join(s, "|"))
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

// Augment the items in the list with the following approach:
// 1. Find all items that have a relationship block that includes `toType="pod"` and a non-empty selector field, and include the item's namespace in the namespace-search list
// 2. Search the DB for all pods that are in the namespace-search list -- grab the pod's namespace, name, and fields we want (health, state info)
// 3. Then walk this list again, and for each selector that can actually select one of the pods from step 2,
// add that information from the pod into a `metadata.associatedData` block.

func (l *ListOptionIndexer) AugmentList(ctx context.Context, list *unstructured.UnstructuredList) error {
	var namespaceSet = sets.Set[string]{}
	for _, data := range list.Items {
		relationships, found, err := unstructured.NestedFieldNoCopy(data.Object, "metadata", "relationships")
		if err != nil || !found {
			continue
		}
		for _, rel := range relationships.([]any) {
			rel2 := rel.(map[string]any)
			_, selectorOK := rel2["selector"]
			if !selectorOK {
				continue
			}
			toType, toTypeOK := rel2["toType"]
			if !toTypeOK || toType != "pod" {
				continue
			}
			toNamespace := ""
			toNamespaceAny, toNamespaceOK := rel2["toNamespace"]
			if toNamespaceOK {
				toNamespaceAsString, convOK := toNamespaceAny.(string)
				if convOK {
					toNamespace = toNamespaceAsString
				}

			}
			namespaceSet.Insert(toNamespace)
		}
	}
	if namespaceSet.Len() == 0 {
		return nil
	}
	// No need to sort this list as it goes into a SQL `metadata.namespace IN (...)` query
	namespaces := namespaceSet.UnsortedList()
	query, params, err := makeAugmentedDBQuery(namespaces)
	if err != nil {
		return err
	}
	err = l.finishAugmenting(ctx, list, query, params)
	if err != nil {
		logrus.Debugf("Error augmenting the info: %s\n", err)
	}
	return nil
}

func makeAugmentedDBQuery(namespaces []string) (string, []any, error) {
	if len(namespaces) == 0 {
		return "", nil, fmt.Errorf("nothing to select")
	}
	query := `SELECT f1."metadata.namespace" as NS,  f1."metadata.name" as POD, f1."metadata.state.name" AS STATENAME, f1."metadata.state.error" AS ERROR, f1."metadata.state.message" AS SMESSAGE, f1."metadata.state.transitioning" AS TRANSITIONING, lt1.label as LAB, lt1.value as VAL
    FROM "_v1_Pod_fields" f1
    JOIN "_v1_Pod_labels" lt1 ON f1.key = lt1.key
    WHERE f1."metadata.namespace" IN (?` + strings.Repeat(", ?", len(namespaces)-1) + ")"
	params := make([]any, len(namespaces), len(namespaces))
	for i, ns := range namespaces {
		params[i] = ns
	}
	return query, params, nil
}

func (l *ListOptionIndexer) finishAugmenting(ctx context.Context, list *unstructured.UnstructuredList, query string, params []any) error {
	stmt := l.Prepare(query)
	var err error
	defer func() {
		if cerr := stmt.Close(); cerr != nil && err == nil {
			err = errors.Join(err, cerr)
		}
	}()

	var items [][]string
	err = l.WithTransaction(ctx, false, func(tx db.TxClient) error {
		now := time.Now()
		rows, err := tx.Stmt(stmt).QueryContext(ctx, params...)
		if err != nil {
			return err
		}
		elapsed := time.Since(now)
		logLongQuery(elapsed, query, params)
		items, err = l.ReadPodInfoStrings(rows)
		if err != nil {
			return fmt.Errorf("finishAugmenting: error reading objects: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil
	}
	if len(items) == 0 {
		return nil
	}

	// And now plug in the new data into metadata.augmentedRelationships
	sortedItems := sortTheItems(items)
	podGVK := &schema.GroupVersionKind{
		Group:   "",
		Version: "v1",
		Kind:    "Pod",
	}
	//NextDeployment:
	for _, listItem := range list.Items {
		relationships, found, err := unstructured.NestedFieldNoCopy(listItem.Object, "metadata", "relationships")
		if err != nil || !found {
			// This should not have happened because we did a DB query on relationships
			continue
		}
		finalSelector := ""
		finalNamespace := ""
		for _, rel := range relationships.([]any) {
			rel2 := rel.(map[string]any)
			selector, selectorOK := rel2["selector"]
			if !selectorOK {
				continue
			}
			toType, toTypeOK := rel2["toType"]
			if !toTypeOK || toType != "pod" {
				continue
			}
			finalNamespace = rel2["toNamespace"].(string)
			finalSelector = selector.(string)
			break
		}
		if finalSelector == "" {
			continue
		}
		// Find the slice we care about
		podSelectorWrapper, ok := sortedItems[finalNamespace]
		if !ok {
			continue
		}
		selectorHash := make(map[string]string)
		selectorParts := strings.Split(finalSelector, ",")
		for _, selectorPart := range selectorParts {
			parts := strings.SplitN(selectorPart, "=", 2)
			if len(parts) != 2 {
				continue
			}
			selectorHash[parts[0]] = parts[1]
		}
		associationBlock := map[string]any{
			"GVK": map[string]any{
				"group":   podGVK.Group,
				"version": podGVK.Version,
				"kind":    podGVK.Kind,
			},
			"data": make([]any, 0),
		}
		for podName, podInfo := range *podSelectorWrapper {
			podSelectors := podInfo.labelAsSelectors
			acceptThis := true
			for deploymentLabel, deploymentValue := range selectorHash {
				if podSelectors[deploymentLabel] != deploymentValue {
					acceptThis = false
				}
			}
			if acceptThis {
				associationBlock["data"] = append(associationBlock["data"].([]any), map[string]any{
					"podName": podName,
					"state":   podInfo.stateInfo,
				})
			}
		}
		if len(associationBlock["data"].([]any)) > 0 {
			associationWrapper := []any{associationBlock}
			err = unstructured.SetNestedSlice(listItem.Object, associationWrapper, "metadata", "associatedData")
			if err != nil {
				logrus.Errorf("Can't set data: %s\n", err)
				return err
			}
		}
	}
	return nil
}

type podSelectorInfo struct {
	stateInfo        map[string]any
	labelAsSelectors map[string]string
}

// These need to be pointers so they can be updated while being created.
// There must be a better way to do this...
type podSelectorWrapper map[string]*podSelectorInfo

type podsByNamespace map[string]*podSelectorWrapper

func sortTheItems(items [][]string) podsByNamespace {
	itemsByNamespace := podsByNamespace{}
	for _, valueList := range items {
		namespaceName := valueList[0]
		psw, ok := itemsByNamespace[namespaceName]
		if !ok {
			psw = &podSelectorWrapper{}
			itemsByNamespace[namespaceName] = psw
		}
		podName := valueList[1]
		podBlock, ok := (*psw)[podName]
		if !ok {
			podBlock = &podSelectorInfo{
				// The state info is the same for every instance of
				// pod P with a different label, so we can assign it the first time
				// and don't need to check subsequent instances of pod P
				// Repeat: pod P repeats because the relational info is redundant when
				// we join pod fields with pod labels
				stateInfo: map[string]any{
					"name":          valueList[2],
					"error":         valueList[3],
					"message":       valueList[4],
					"transitioning": valueList[5],
				},
				labelAsSelectors: make(map[string]string),
			}
			(*psw)[podName] = podBlock
		}
		podBlock.labelAsSelectors[valueList[6]] = valueList[7]
	}
	return itemsByNamespace
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
