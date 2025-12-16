package informer

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/sqlcache/db"
	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
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

	// lock protects both latestRV and watchers
	lock     sync.RWMutex
	latestRV string
	watchers map[*watchKey]*watcher

	// gcInterval is how often to run the garbage collection
	gcInterval time.Duration
	// gcKeepCount is how many events to keep in _events table when gc runs
	gcKeepCount int

	upsertEventsStmt        db.Stmt
	findEventsRowByRVStmt   db.Stmt
	listEventsAfterStmt     db.Stmt
	deleteEventsByCountStmt db.Stmt
	dropEventsStmt          db.Stmt
	addFieldsStmt           db.Stmt
	deleteFieldsStmt        db.Stmt
	dropFieldsStmt          db.Stmt
	upsertLabelsStmt        db.Stmt
	deleteLabelsStmt        db.Stmt
	dropLabelsStmt          db.Stmt
}

var (
	defaultIndexedFields   = []string{"metadata.name", "metadata.creationTimestamp"}
	defaultIndexNamespaced = "metadata.namespace"
	immutableFields        = sets.New(
		"metadata.creationTimestamp",
		"metadata.namespace",
		"metadata.name",
		"id",
		"metadata.state.name",
	)
	subfieldRegex           = regexp.MustCompile(`([a-zA-Z]+)|(\[[-a-zA-Z./]+])|(\[[0-9]+])`)
	containsNonNumericRegex = regexp.MustCompile(`\D`)

	ErrInvalidColumn   = errors.New("supplied column is invalid")
	ErrTooOld          = errors.New("resourceversion too old")
	ErrUnknownRevision = errors.New("unknown revision")

	projectIDFieldLabel = "field.cattle.io/projectId"
	namespacesDbName    = "_v1_Namespace"
)

const (
	matchFmt                 = `%%%s%%`
	strictMatchFmt           = `%s`
	escapeBackslashDirective = ` ESCAPE '\'` // The leading space is crucial for unit tests only '

	// RV stands for ResourceVersion
	createEventsTableFmt = `CREATE TABLE "%s_events" (
                       rv TEXT NOT NULL,
                       type TEXT NOT NULL,
                       event BLOB NOT NULL,
                       eventnonce BLOB,
	               dekid BLOB,
                       PRIMARY KEY (rv, type)
          )`
	listEventsAfterFmt = `SELECT type, rv, event, eventnonce, dekid
	       FROM "%s_events"
	       WHERE rowid > ?
       `
	findEventsRowByRVFmt = `SELECT rowid
               FROM "%s_events"
               WHERE rv = ?
       `
	upsertEventsStmtFmt = `
INSERT INTO "%s_events" (rv, type, event, eventnonce, dekid)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(type, rv) DO UPDATE SET
  event = excluded.event,
  eventnonce = excluded.eventnonce,
  dekid = excluded.dekid`
	deleteEventsByCountFmt = `DELETE FROM "%s_events"
	WHERE rowid < (
	    SELECT MIN(rowid) FROM (
	        SELECT rowid FROM "%s_events" ORDER BY rowid DESC LIMIT ?
	    ) q
	)`
	dropEventsFmt = `DROP TABLE IF EXISTS "%s_events"`

	createFieldsTableFmt = `CREATE TABLE "%s_fields" (
		key TEXT NOT NULL REFERENCES "%s"(key) ON DELETE CASCADE,
		%s,
		PRIMARY KEY (key)
    )`
	createFieldsIndexFmt = `CREATE INDEX "%s_%s_index" ON "%s_fields"("%s")`
	deleteFieldsFmt      = `DELETE FROM "%s_fields"`
	dropFieldsFmt        = `DROP TABLE IF EXISTS "%s_fields"`

	failedToGetFromSliceFmt = "[listoption indexer] failed to get subfield [%s] from slice items"

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
	// GCInterval is how often to run the garbage collection
	GCInterval time.Duration
	// GCKeepCount is how many events to keep in _events table when gc runs
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

	l := &ListOptionIndexer{
		Indexer:       i,
		namespaced:    opts.IsNamespaced,
		indexedFields: indexedFields,
		watchers:      make(map[*watchKey]*watcher),
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
	l.RegisterBeforeDropAll(l.dropEvents)
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
		dropEventsQuery := fmt.Sprintf(dropEventsFmt, dbName)
		if _, err := tx.Exec(dropEventsQuery); err != nil {
			return err
		}

		createEventsTableQuery := fmt.Sprintf(createEventsTableFmt, dbName)
		if _, err := tx.Exec(createEventsTableQuery); err != nil {
			return err
		}

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

	l.upsertEventsStmt = l.Prepare(fmt.Sprintf(upsertEventsStmtFmt, dbName))
	l.listEventsAfterStmt = l.Prepare(fmt.Sprintf(listEventsAfterFmt, dbName))
	l.findEventsRowByRVStmt = l.Prepare(fmt.Sprintf(findEventsRowByRVFmt, dbName))
	l.deleteEventsByCountStmt = l.Prepare(fmt.Sprintf(deleteEventsByCountFmt, dbName, dbName))
	l.dropEventsStmt = l.Prepare(fmt.Sprintf(dropEventsFmt, dbName))

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

	l.gcInterval = opts.GCInterval
	l.gcKeepCount = opts.GCKeepCount

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

	// We can keep receiving events while replaying older events for the watcher.
	// By early registering this watcher, this channel will buffer any new events while we are still backfilling old events.
	// When we finish, calling backfillDone will write all events in the buffer, then listen to new events as normal.
	const maxBufferSize = 100
	watcherChannel, backfillDone, closeWatcher := watcherWithBackfill(ctx, eventsCh, maxBufferSize)
	defer closeWatcher()

	l.lock.Lock()
	latestRV := l.latestRV
	key := l.addWatcherLocked(watcherChannel, opts.Filter)
	l.lock.Unlock()
	defer l.removeWatcher(key)

	targetRV := opts.ResourceVersion
	if targetRV == "" {
		targetRV = latestRV
	}

	if err := l.WithTransaction(ctx, false, func(tx db.TxClient) error {
		var rowID int
		// use a closure to ensure rows is always closed immediately after it's needed
		if err := func() error {
			rows, err := l.QueryForRows(ctx, tx.Stmt(l.findEventsRowByRVStmt), targetRV)
			if err != nil {
				return err
			}
			defer rows.Close()

			if !rows.Next() {
				// query returned no results
				if targetRV != latestRV {
					return ErrTooOld
				}
				return nil
			}
			if err := rows.Scan(&rowID); err != nil {
				return fmt.Errorf("failed scan rowid: %w", err)
			}
			return nil
		}(); err != nil {
			return err
		}

		// Backfilling previous events from resourceVersion
		rows, err := l.QueryForRows(ctx, tx.Stmt(l.listEventsAfterStmt), rowID)
		if err != nil {
			return err
		}
		defer rows.Close()

		var latestRevisionReached bool
		for !latestRevisionReached && rows.Next() {
			obj := &unstructured.Unstructured{}
			eventType, err := l.decryptScanEvent(rows, obj)
			if err != nil {
				return fmt.Errorf("scanning event row: %w", err)
			}
			if obj.GetResourceVersion() == latestRV {
				// This iteration will be the last one, as we already reached the last event at the moment we started the loop
				latestRevisionReached = true
			}
			filter := opts.Filter
			if !matchFilter(filter.ID, filter.Namespace, filter.Selector, obj) {
				continue
			}

			ev := watch.Event{
				Type:   eventType,
				Object: obj,
			}
			select {
			case eventsCh <- ev:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return rows.Err()
	}); err != nil {
		return err
	}
	backfillDone()

	<-ctx.Done()
	return nil
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

// watcherWithBackfill creates a proxy channel that buffers events during a "backfill" phase
// and then seamlessly transitions to live event processing.
func watcherWithBackfill[T any](ctx context.Context, eventsCh chan<- T, maxBufferSize int) (chan T, func(), func()) {
	backfillCtx, signalBackfillDone := context.WithCancel(ctx)
	watcherCh := make(chan T)
	done := make(chan struct{})

	// The single proxy goroutine that manages all state.
	go func() {
		defer close(done)
		defer func() {
			// this goroutine can exit prematurely when the parent context is cancelled
			// this ensures the producer can finish writing and finish the cancellation sequence (closeWatcher is called from the parent)
			for range watcherCh {
			}
		}()

		var queue []T // Use a slice as an internal FIFO queue.

		// Phase 1: Accumulate while we're backfilling
	acc:
		for len(queue) < maxBufferSize { // Only accumulate until reaching max buffer size, then block ingestion instead
			select {
			case event, ok := <-watcherCh:
				if !ok {
					// writeChan was closed, assume that context is done, so the remaining queue will never be sent
					return
				}
				queue = append(queue, event)
			case <-backfillCtx.Done():
				break acc
			}
		}

		// Check backfill was completed, in case the above loop aborted early
		<-backfillCtx.Done()
		if ctx.Err() != nil {
			return
		}

		// Phase 2: start flushing while still accepting events from watcherCh
		for len(queue) > 0 {
			// Only accept new events from write buffer if the queue has space, blocking the sender (equivalent to a full buffered channel)
			// cases reading from a nil channel will be ignored
			var readChan <-chan T
			if len(queue) < maxBufferSize {
				readChan = watcherCh
			}

			select {
			case <-ctx.Done():
				return
			case event, ok := <-readChan: // This case is disabled if readChan is nil (queue is full)
				if !ok {
					// watcherCh was closed, assume that context is done, so the remaining queue will never be sent
					return
				}
				queue = append(queue, event)
			case eventsCh <- queue[0]:
				// We successfully sent the event, so we can remove it from the queue.
				queue = queue[1:]
			}
		}
		queue = nil // no longer needed, release the backing array for GC

		// Final phase: when flushing is completed, the original channel is piped to watcherCh
		for {
			select {
			case event, ok := <-watcherCh:
				if !ok {
					return // watcherCh was closed.
				}
				// Send event directly, blocking until the consumer is ready.
				select {
				case eventsCh <- event:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return watcherCh, signalBackfillDone, func() {
		close(watcherCh)
		<-done
	}
}

type watchKey struct {
	_ bool // ensure watchKey is NOT zero-sized to get unique pointers
}

type watcher struct {
	ch     chan<- watch.Event
	filter WatchFilter
}

func (l *ListOptionIndexer) addWatcherLocked(eventCh chan<- watch.Event, filter WatchFilter) *watchKey {
	key := new(watchKey)
	l.watchers[key] = &watcher{
		ch:     eventCh,
		filter: filter,
	}
	return key
}

func (l *ListOptionIndexer) removeWatcher(key *watchKey) {
	l.lock.Lock()
	delete(l.watchers, key)
	l.lock.Unlock()
}

/* Core methods */

func (l *ListOptionIndexer) notifyEventAdded(key string, obj any, tx db.TxClient) error {
	return l.notifyEvent(watch.Added, nil, obj, tx)
}

func (l *ListOptionIndexer) notifyEventModified(key string, obj any, tx db.TxClient) error {
	oldObj, exists, err := l.GetByKey(key)
	if err != nil {
		return fmt.Errorf("error getting old object: %w", err)
	}

	if !exists {
		return fmt.Errorf("old object %q should be in store but was not", key)
	}

	return l.notifyEvent(watch.Modified, oldObj, obj, tx)
}

func (l *ListOptionIndexer) notifyEventDeleted(key string, obj any, tx db.TxClient) error {
	oldObj, exists, err := l.GetByKey(key)
	if err != nil {
		return fmt.Errorf("error getting old object: %w", err)
	}

	if !exists {
		return fmt.Errorf("old object %q should be in store but was not", key)
	}
	return l.notifyEvent(watch.Deleted, oldObj, obj, tx)
}

func (l *ListOptionIndexer) notifyEvent(eventType watch.EventType, oldObj any, obj any, tx db.TxClient) error {
	acc, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	latestRV := acc.GetResourceVersion()

	err = l.upsertEvent(tx, eventType, latestRV, obj)
	if err != nil {
		return err
	}

	l.lock.RLock()
	for _, watcher := range l.watchers {
		if !matchWatch(watcher.filter.ID, watcher.filter.Namespace, watcher.filter.Selector, oldObj, obj) {
			continue
		}

		watcher.ch <- watch.Event{
			Type:   eventType,
			Object: obj.(runtime.Object).DeepCopyObject(),
		}
	}
	l.lock.RUnlock()

	l.lock.Lock()
	defer l.lock.Unlock()
	l.latestRV = latestRV
	return nil
}

func (l *ListOptionIndexer) upsertEvent(tx db.TxClient, eventType watch.EventType, latestRV string, obj any) error {
	serialized, err := l.Serialize(obj, l.GetShouldEncrypt())
	if err != nil {
		return err
	}
	_, err = tx.Stmt(l.upsertEventsStmt).Exec(latestRV, eventType, serialized.Bytes, serialized.Nonce, serialized.KeyID)
	return err
}

func (l *ListOptionIndexer) dropEvents(tx db.TxClient) error {
	_, err := tx.Stmt(l.dropEventsStmt).Exec()
	return err
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
	list, total, continueToken, err = l.executeQuery(ctx, queryInfo)
	return
}

type joinPart struct {
	joinCommand    string
	tableName      string
	tableNameAlias string
	onPrefix       string
	onField        string
	otherPrefix    string
	otherField     string
}

type filterComponentsT struct {
	joinParts    []joinPart
	whereClauses []string
	limitClause  string
	offsetClause string
	params       []any
	isEmpty      bool
}

func (f filterComponentsT) copy() filterComponentsT {
	return filterComponentsT{
		joinParts:    append([]joinPart{}, f.joinParts...),
		whereClauses: append([]string{}, f.whereClauses...),
		limitClause:  f.limitClause,
		offsetClause: f.offsetClause,
		params:       append([]any{}, f.params...),
		isEmpty:      f.isEmpty,
	}
}

func (l *ListOptionIndexer) ListSummaryFields(ctx context.Context, lo *sqltypes.ListOptions, partitions []partition.Partition, dbName string, namespace string) (*types.APISummary, error) {
	joinTableIndexByLabelName := make(map[string]int)
	mainFieldPrefix := "f1"
	filterComponents, err := l.constructSummaryTestFilters(lo, partitions, namespace, dbName, mainFieldPrefix, joinTableIndexByLabelName)
	if err != nil {
		return nil, err
	}
	countsByProperty := make(map[string]any)
	// We have to copy the current data-structures because processing other label summary-fields
	// could modify them, but we don't want to see those changes on subsequent fields
	for fieldNum, field := range lo.SummaryFieldList {
		//TODO: Don't make copies on the last run
		copyOfJoinTableIndexByLabelName := make(map[string]int)
		for k, v := range joinTableIndexByLabelName {
			copyOfJoinTableIndexByLabelName[k] = v
		}
		copyOfFilterComponents := filterComponents.copy()
		data, err := l.ListSummaryForField(ctx, field, fieldNum, dbName, &copyOfFilterComponents, mainFieldPrefix, copyOfJoinTableIndexByLabelName)
		if err != nil {
			return nil, err
		}
		for k, v := range data {
			countsByProperty[k] = v
		}
	}
	return convertMapToAPISummary(countsByProperty), nil
}

func convertMapToAPISummary(countsByProperty map[string]any) *types.APISummary {
	total := len(countsByProperty)
	blocksToSort := make([]types.SummaryEntry, 0, total)
	for property, v := range countsByProperty {
		fixedCounts := make(map[string]int)
		counts := v.(map[string]any)["counts"].(map[string]int)
		for k1, v1 := range counts {
			fixedCounts[k1] = v1
		}
		blocksToSort = append(blocksToSort, types.SummaryEntry{Property: property, Counts: fixedCounts})
	}

	sortedBlocks := slices.SortedFunc(slices.Values(blocksToSort), func(a, b types.SummaryEntry) int {
		return strings.Compare(a.Property, b.Property)
	})
	return &types.APISummary{SummaryItems: sortedBlocks}
}

func (l *ListOptionIndexer) ListSummaryForField(ctx context.Context, field []string, fieldNum int, dbName string, filterComponents *filterComponentsT, mainFieldPrefix string, joinTableIndexByLabelName map[string]int) (map[string]any, error) {
	queryInfo, err := l.constructSummaryQueryForField(field, fieldNum, dbName, filterComponents, mainFieldPrefix, joinTableIndexByLabelName)
	if err != nil {
		return nil, err
	}
	return l.executeSummaryQueryForField(ctx, queryInfo, field)
}

func (l *ListOptionIndexer) constructSummaryQueryForField(fieldParts []string, fieldNum int, dbName string, filterComponents *filterComponentsT, mainFieldPrefix string, joinTableIndexByLabelName map[string]int) (*QueryInfo, error) {
	columnName := toColumnName(fieldParts)
	var columnNameToDisplay string
	var err error
	if isLabelsFieldList(fieldParts) {
		columnNameToDisplay, err = getLabelColumnNameToDisplay(fieldParts)
	} else {
		columnNameToDisplay, err = l.getStandardColumnNameToDisplay(fieldParts, mainFieldPrefix)
	}
	if err != nil {
		return nil, err
	}
	if filterComponents.isEmpty {
		if !isLabelsFieldList(fieldParts) {
			// No need for a main-field prefix, so recalc
			var err error
			columnNameToDisplay, err = l.getStandardColumnNameToDisplay(fieldParts, "")
			if err != nil {
				//TODO: Prove that this can't happen
				return nil, err
			}
		}
		return l.constructSimpleSummaryQueryForField(fieldParts, dbName, columnName, columnNameToDisplay)
	}
	return l.constructComplexSummaryQueryForField(fieldParts, fieldNum, dbName, columnName, columnNameToDisplay, filterComponents, mainFieldPrefix, joinTableIndexByLabelName)
}

func (l *ListOptionIndexer) constructSimpleSummaryQueryForField(fieldParts []string, dbName, columnName, columnNameToDisplay string) (*QueryInfo, error) {
	if isLabelsFieldList(fieldParts) {
		return l.constructSimpleSummaryQueryForLabelField(fieldParts, dbName, columnName, columnNameToDisplay)
	}
	return l.constructSimpleSummaryQueryForStandardField(fieldParts, dbName, columnName, columnNameToDisplay)
}

// Pour everything into this function
func (l *ListOptionIndexer) constructComplexSummaryQueryForField(fieldParts []string, fieldNum int, dbName, columnName, columnNameToDisplay string, filterComponents *filterComponentsT, mainFieldPrefix string, joinTableIndexByLabelName map[string]int) (*QueryInfo, error) {
	const nl = "\n"
	isLabelField := isLabelsFieldList(fieldParts)
	withPrefix := fmt.Sprintf("w%d", fieldNum)
	withParts := make([]string, 0)
	// We don't use the main key directly, but we need it for SELECT-DISTINCT with the labels table.
	withParts = append(withParts, fmt.Sprintf("WITH %s(key, finalField) AS (\n", withPrefix))
	withParts = append(withParts, "\tSELECT")
	if len(filterComponents.joinParts) > 0 || isLabelField {
		withParts = append(withParts, " DISTINCT")
	}
	withParts = append(withParts, fmt.Sprintf(" %s.key,", mainFieldPrefix))
	targetField := columnNameToDisplay
	if isLabelField {
		labelName := fieldParts[2]
		jtIndex, ok := joinTableIndexByLabelName[labelName]
		if !ok {
			jtIndex = len(joinTableIndexByLabelName) + 1
			jtPrefix := fmt.Sprintf("lt%d", jtIndex)
			joinTableIndexByLabelName[labelName] = jtIndex
			filterComponents.joinParts = append(filterComponents.joinParts, joinPart{joinCommand: "LEFT OUTER JOIN", tableName: fmt.Sprintf("%s_labels", dbName), tableNameAlias: jtPrefix, onPrefix: "f1", onField: "key", otherPrefix: jtPrefix, otherField: "key"})
			filterComponents.whereClauses = append(filterComponents.whereClauses, fmt.Sprintf("%s.label = ?", jtPrefix))
			filterComponents.params = append(filterComponents.params, labelName)
		}
		targetField = fmt.Sprintf("lt%d.value", jtIndex)
	}
	withParts = append(withParts, fmt.Sprintf(` %s FROM "%s_fields" %s%s`, targetField, dbName, mainFieldPrefix, nl))
	for _, jp := range filterComponents.joinParts {
		withParts = append(withParts, fmt.Sprintf(`  %s "%s" %s ON %s.%s = %s.%s%s`,
			jp.joinCommand, jp.tableName, jp.tableNameAlias, jp.onPrefix, jp.onField, jp.otherPrefix, jp.otherField, nl))
	}
	if len(filterComponents.whereClauses) > 0 {
		if len(filterComponents.whereClauses) == 1 {
			withParts = append(withParts, fmt.Sprintf("\tWHERE %s\n", filterComponents.whereClauses[0]))
		} else {
			withParts = append(withParts, fmt.Sprintf("\tWHERE (%s)\n", strings.Join(filterComponents.whereClauses, ")\n\t\tAND (")))
		}
	}
	if filterComponents.limitClause != "" {
		withParts = append(withParts, "\tLIMIT ?\n")
	}
	if filterComponents.offsetClause != "" {
		withParts = append(withParts, "\tOFFSET ?\n")
	}
	withParts = append(withParts, ")\n")
	withParts = append(withParts, fmt.Sprintf("SELECT '%s' AS p, COUNT(*) AS c, %s.finalField AS k FROM %s\n", columnName, withPrefix, withPrefix))
	withParts = append(withParts, "\tWHERE k != \"\"\n\tGROUP BY k")
	query := strings.Join(withParts, "")
	queryInfo := &QueryInfo{}
	queryInfo.query = query
	queryInfo.params = filterComponents.params
	logrus.Debugf("Summary prepared statement: %v", queryInfo.query)
	logrus.Debugf("Summary params: %v", queryInfo.params)
	return queryInfo, nil
}

func (l *ListOptionIndexer) constructSimpleSummaryQueryForStandardField(fieldParts []string, dbName, columnName, columnNameToDisplay string) (*QueryInfo, error) {
	query := fmt.Sprintf(`SELECT '%s' AS p, COUNT(*) AS c, %s AS k
	FROM "%s_fields"
	WHERE k != ""
	GROUP BY k`,
		columnName, columnNameToDisplay, dbName)
	return &QueryInfo{query: query}, nil
}

func (l *ListOptionIndexer) constructSimpleSummaryQueryForLabelField(fieldParts []string, dbName, columnName, columnNameToDisplay string) (*QueryInfo, error) {
	query := fmt.Sprintf(`SELECT '%s' AS p, COUNT(*) AS c, value AS k
	FROM "%s_labels"
	WHERE label = ? AND k != ""
	GROUP BY k`,
		columnNameToDisplay, dbName)
	args := make([]any, 1)
	args[0] = fieldParts[2]
	return &QueryInfo{query: query, params: args}, nil
}

// Build the SELECT part for the initial WITH block
func (l *ListOptionIndexer) constructSummaryTestFilters(lo *sqltypes.ListOptions, partitions []partition.Partition, namespace string, dbName string, mainFieldPrefix string, joinTableIndexByLabelName map[string]int) (*filterComponentsT, error) {
	queryUsesLabels := hasLabelFilter(lo.Filters) || len(lo.ProjectsOrNamespaces.Filters) > 0

	joinParts := make([]joinPart, 0)
	whereClauses := make([]string, 0)
	limitClause := ""
	offsetClause := ""
	params := make([]any, 0)

	// 1. Do the filtering
	if queryUsesLabels {
		for _, orFilter := range lo.Filters {
			for _, filter := range orFilter.Filters {
				if isLabelFilter(&filter) {
					labelName := filter.Field[2]
					_, ok := joinTableIndexByLabelName[labelName]
					if !ok {
						// Make the lt index 1-based for readability
						jtIndex := len(joinTableIndexByLabelName) + 1
						joinTableIndexByLabelName[labelName] = jtIndex
						jtPrefix := fmt.Sprintf("lt%d", jtIndex)
						joinParts = append(joinParts,
							joinPart{joinCommand: "LEFT OUTER JOIN",
								tableName:      fmt.Sprintf("%s_labels", dbName),
								tableNameAlias: jtPrefix,
								onPrefix:       mainFieldPrefix,
								onField:        "key",
								otherPrefix:    jtPrefix,
								otherField:     "key",
							})
					}
				}
			}
		}
	}

	if len(lo.ProjectsOrNamespaces.Filters) > 0 {
		jtIndex := len(joinTableIndexByLabelName) + 1
		i, exists := joinTableIndexByLabelName[projectIDFieldLabel]
		if !exists {
			joinTableIndexByLabelName[projectIDFieldLabel] = jtIndex
		} else {
			jtIndex = i
		}
		jtPrefix := fmt.Sprintf("lt%d", jtIndex)
		joinParts = append(joinParts, joinPart{tableName: namespacesDbName,
			tableNameAlias: "nsf",
			onPrefix:       mainFieldPrefix,
			onField:        "metadata.namespace",
			otherPrefix:    "nsf",
			otherField:     "metadata.namespace"})
		joinParts = append(joinParts, joinPart{tableName: fmt.Sprintf("%s_labels", namespacesDbName),
			tableNameAlias: jtPrefix,
			onPrefix:       "nsf",
			onField:        "key",
			otherPrefix:    jtPrefix,
			otherField:     "key"})
	}

	// 2- Filtering: WHERE clauses (from lo.Filters)
	for _, orFilters := range lo.Filters {
		orClause, orParams, err := l.buildORClauseFromFilters(orFilters, dbName, mainFieldPrefix, true, joinTableIndexByLabelName)
		if err != nil {
			return nil, err
		}
		if orClause == "" {
			continue
		}
		whereClauses = append(whereClauses, orClause)
		params = append(params, orParams...)
	}

	// WHERE clauses (from lo.ProjectsOrNamespaces)
	if len(lo.ProjectsOrNamespaces.Filters) > 0 {
		projOrNsClause, projOrNsParams, err := l.buildClauseFromProjectsOrNamespaces(lo.ProjectsOrNamespaces, dbName, joinTableIndexByLabelName)
		if err != nil {
			return nil, err
		}
		whereClauses = append(whereClauses, projOrNsClause)
		params = append(params, projOrNsParams...)
	}

	// WHERE clauses (from namespace)
	if namespace != "" && namespace != "*" {
		whereClauses = append(whereClauses, fmt.Sprintf(`%s."metadata.namespace" = ?`, mainFieldPrefix))
		params = append(params, namespace)
	}

	// WHERE clauses (from partitions and their corresponding parameters)
	partitionClauses := []string{}
	for _, thisPartition := range partitions {
		if thisPartition.Passthrough {
			// nothing to do, no extra filtering to apply by definition
		} else {
			singlePartitionClauses := []string{}

			// filter by namespace
			if thisPartition.Namespace != "" && thisPartition.Namespace != "*" {
				singlePartitionClauses = append(singlePartitionClauses, fmt.Sprintf(`%s."metadata.namespace" = ?`, mainFieldPrefix))
				params = append(params, thisPartition.Namespace)
			}

			// optionally filter by names
			if !thisPartition.All {
				names := thisPartition.Names

				if len(names) == 0 {
					// degenerate case, there will be no results
					singlePartitionClauses = append(singlePartitionClauses, "FALSE")
				} else {
					singlePartitionClauses = append(singlePartitionClauses, fmt.Sprintf(`%s."metadata.name" IN (?%s)`, mainFieldPrefix, strings.Repeat(", ?", len(thisPartition.Names)-1)))
					// sort for reproducibility
					sortedNames := thisPartition.Names.UnsortedList()
					sort.Strings(sortedNames)
					for _, name := range sortedNames {
						params = append(params, name)
					}
				}
			}

			if len(singlePartitionClauses) > 0 {
				partitionClauses = append(partitionClauses, strings.Join(singlePartitionClauses, " AND "))
			}
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
	// 4- Pagination: LIMIT clause (from lo.Pagination)
	limit := lo.Pagination.PageSize
	if limit > 0 {
		limitClause = "\n  LIMIT ?"
		params = append(params, limit)
	}

	// OFFSET clause (from lo.Pagination)
	offset := 0
	if lo.Pagination.Page >= 1 {
		offset += lo.Pagination.PageSize * (lo.Pagination.Page - 1)
	}
	if offset > 0 {
		offsetClause = "\n  OFFSET ?"
		params = append(params, offset)
	}
	components := &filterComponentsT{
		joinParts:    joinParts,
		whereClauses: whereClauses,
		limitClause:  limitClause,
		offsetClause: offsetClause,
		params:       params,
		isEmpty:      len(joinParts) == 0 && len(whereClauses) == 0 && limitClause == "" && offsetClause == "",
	}
	return components, nil
}

// Helper functions for the summary-field methods
func getLabelColumnNameToDisplay(fieldParts []string) (string, error) {
	lastPart := fieldParts[2]
	columnNameToDisplay := ""
	const nameLimit = 63
	if len(lastPart) > nameLimit {
		return "", fmt.Errorf("label value %s..%s (%d chars, max %d) is too long", lastPart[0:10], lastPart[len(lastPart)-10:], len(lastPart), nameLimit)
	}
	simpleName := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	if simpleName.MatchString(lastPart) {
		columnNameToDisplay = strings.Join(fieldParts, ".")
	} else {
		compoundName := regexp.MustCompile(`[^a-zA-Z0-9_\-./]`)
		if compoundName.MatchString(lastPart) {
			return "", fmt.Errorf("invalid label name: %s", lastPart)
		}
		columnNameToDisplay = fmt.Sprintf("metadata.labels[%s]", lastPart)
	}
	return columnNameToDisplay, nil
}

func (l *ListOptionIndexer) getStandardColumnNameToDisplay(fieldParts []string, mainFieldPrefix string) (string, error) {
	columnName := toColumnName(fieldParts)
	var columnValueName string
	if mainFieldPrefix == "" {
		columnValueName = fmt.Sprintf("%q", columnName)
	} else {
		columnValueName = fmt.Sprintf("%s.%q", mainFieldPrefix, columnName)
	}
	err := l.validateColumn(columnName)
	if err == nil {
		return columnValueName, nil
	}
	if len(fieldParts) == 1 || containsNonNumericRegex.MatchString(fieldParts[len(fieldParts)-1]) {
		// Can't be a numeric-indexed field expression like spec.containers.image[2]
		return "", err
	}
	columnNameToValidate := toColumnName(fieldParts[:len(fieldParts)-1])
	if err2 := l.validateColumn(columnNameToValidate); err2 != nil {
		// Return the original error message
		return "", err
	}
	index, err2 := strconv.Atoi(fieldParts[len(fieldParts)-1])
	if err2 != nil {
		// Return the original error message
		return "", err
	}
	if mainFieldPrefix == "" {
		columnValueName = fmt.Sprintf(`extractBarredValue(%q, %d)`, columnNameToValidate, index)
	} else {
		columnValueName = fmt.Sprintf(`extractBarredValue(%s.%q, %d)`, mainFieldPrefix, columnNameToValidate, index)
	}
	return columnValueName, nil
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

func (l *ListOptionIndexer) constructQuery(lo *sqltypes.ListOptions, partitions []partition.Partition, namespace string, dbName string) (*QueryInfo, error) {
	unboundSortLabels := getUnboundSortLabels(lo)
	queryInfo := &QueryInfo{}
	queryUsesLabels := hasLabelFilter(lo.Filters) || len(lo.ProjectsOrNamespaces.Filters) > 0
	joinTableIndexByLabelName := make(map[string]int)

	l.lock.RLock()
	latestRV := l.latestRV
	l.lock.RUnlock()

	if len(lo.Revision) > 0 {
		currentRevision, err := strconv.ParseInt(latestRV, 10, 64)
		if err != nil {
			return nil, err
		}

		requestRevision, err := strconv.ParseInt(lo.Revision, 10, 64)
		if err != nil {
			return nil, err
		}

		if currentRevision < requestRevision {
			return nil, ErrUnknownRevision
		}
	}

	// First, what kind of filtering will we be doing?
	// 1- Intro: SELECT and JOIN clauses
	// There's a 1:1 correspondence between a base table and its _Fields table
	// but it's possible that a key has no associated labels, so if we're doing a
	// non-existence test on labels we need to do a LEFT OUTER JOIN
	query := ""
	params := []any{}
	whereClauses := []string{}
	joinPartsToUse := []string{}
	if len(unboundSortLabels) > 0 {
		withParts, withParams, _, joinParts, err := getWithParts(unboundSortLabels, joinTableIndexByLabelName, dbName, "o")
		if err != nil {
			return nil, err
		}
		query = "WITH " + strings.Join(withParts, ",\n") + "\n"
		params = withParams
		joinPartsToUse = joinParts
	}
	query += "SELECT "
	if queryUsesLabels {
		query += "DISTINCT "
	}
	query += fmt.Sprintf(`o.object, o.objectnonce, o.dekid FROM "%s" o`, dbName)
	query += "\n  "
	query += fmt.Sprintf(`JOIN "%s_fields" f ON o.key = f.key`, dbName)
	if len(joinPartsToUse) > 0 {
		query += "\n  "
		query += strings.Join(joinPartsToUse, "\n  ")
	}

	if queryUsesLabels {
		for _, orFilter := range lo.Filters {
			for _, filter := range orFilter.Filters {
				if isLabelFilter(&filter) {
					labelName := filter.Field[2]
					_, ok := joinTableIndexByLabelName[labelName]
					if !ok {
						// Make the lt index 1-based for readability
						jtIndex := len(joinTableIndexByLabelName) + 1
						joinTableIndexByLabelName[labelName] = jtIndex
						query += "\n  "
						query += fmt.Sprintf(`LEFT OUTER JOIN "%s_labels" lt%d ON o.key = lt%d.key`, dbName, jtIndex, jtIndex)
					}
				}
			}
		}
	}

	if len(lo.ProjectsOrNamespaces.Filters) > 0 {
		jtIndex := len(joinTableIndexByLabelName) + 1
		if _, exists := joinTableIndexByLabelName[projectIDFieldLabel]; !exists {
			joinTableIndexByLabelName[projectIDFieldLabel] = jtIndex
		}
		query += "\n  "
		query += fmt.Sprintf(`LEFT OUTER JOIN "%s_fields" nsf ON f."metadata.namespace" = nsf."metadata.name"`, namespacesDbName)
		query += "\n  "
		query += fmt.Sprintf(`LEFT OUTER JOIN "%s_labels" lt%d ON nsf.key = lt%d.key`, namespacesDbName, jtIndex, jtIndex)
	}

	// 2- Filtering: WHERE clauses (from lo.Filters)
	for _, orFilters := range lo.Filters {
		orClause, orParams, err := l.buildORClauseFromFilters(orFilters, dbName, "f", false, joinTableIndexByLabelName)
		if err != nil {
			return queryInfo, err
		}
		if orClause == "" {
			continue
		}
		whereClauses = append(whereClauses, orClause)
		params = append(params, orParams...)
	}

	// WHERE clauses (from lo.ProjectsOrNamespaces)
	if len(lo.ProjectsOrNamespaces.Filters) > 0 {
		projOrNsClause, projOrNsParams, err := l.buildClauseFromProjectsOrNamespaces(lo.ProjectsOrNamespaces, dbName, joinTableIndexByLabelName)
		if err != nil {
			return queryInfo, err
		}
		whereClauses = append(whereClauses, projOrNsClause)
		params = append(params, projOrNsParams...)
	}

	// WHERE clauses (from namespace)
	if namespace != "" && namespace != "*" {
		whereClauses = append(whereClauses, fmt.Sprintf(`f."metadata.namespace" = ?`))
		params = append(params, namespace)
	}

	// WHERE clauses (from partitions and their corresponding parameters)
	partitionClauses := []string{}
	for _, thisPartition := range partitions {
		if thisPartition.Passthrough {
			// nothing to do, no extra filtering to apply by definition
		} else {
			singlePartitionClauses := []string{}

			// filter by namespace
			if thisPartition.Namespace != "" && thisPartition.Namespace != "*" {
				singlePartitionClauses = append(singlePartitionClauses, fmt.Sprintf(`f."metadata.namespace" = ?`))
				params = append(params, thisPartition.Namespace)
			}

			// optionally filter by names
			if !thisPartition.All {
				names := thisPartition.Names

				if len(names) == 0 {
					// degenerate case, there will be no results
					singlePartitionClauses = append(singlePartitionClauses, "FALSE")
				} else {
					singlePartitionClauses = append(singlePartitionClauses, fmt.Sprintf(`f."metadata.name" IN (?%s)`, strings.Repeat(", ?", len(thisPartition.Names)-1)))
					// sort for reproducibility
					sortedNames := thisPartition.Names.UnsortedList()
					sort.Strings(sortedNames)
					for _, name := range sortedNames {
						params = append(params, name)
					}
				}
			}

			if len(singlePartitionClauses) > 0 {
				partitionClauses = append(partitionClauses, strings.Join(singlePartitionClauses, " AND "))
			}
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

	if len(whereClauses) > 0 {
		query += "\n  WHERE\n    "
		for index, clause := range whereClauses {
			query += fmt.Sprintf("(%s)", clause)
			if index == len(whereClauses)-1 {
				break
			}
			query += " AND\n    "
		}
	}

	// before proceeding, save a copy of the query and params without LIMIT/OFFSET/ORDER info
	// for COUNTing all results later
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", query)
	countParams := params[:]

	// 3- Sorting: ORDER BY clauses (from lo.Sort)
	if len(lo.SortList.SortDirectives) > 0 {
		orderByClauses := []string{}
		for _, sortDirective := range lo.SortList.SortDirectives {
			fields := sortDirective.Fields
			if isLabelsFieldList(fields) {
				clause, err := buildSortLabelsClause(fields[2], joinTableIndexByLabelName, sortDirective.Order == sqltypes.ASC, sortDirective.SortAsIP)
				if err != nil {
					return nil, err
				}
				orderByClauses = append(orderByClauses, clause)
			} else {
				fieldEntry, err := l.getValidFieldEntry("f", fields)
				if err != nil {
					return queryInfo, err
				}
				if sortDirective.SortAsIP {
					fieldEntry = fmt.Sprintf("inet_aton(%s)", fieldEntry)
				}
				direction := "ASC"
				if sortDirective.Order == sqltypes.DESC {
					direction = "DESC"
				}
				orderByClauses = append(orderByClauses, fmt.Sprintf("%s %s", fieldEntry, direction))
			}
		}
		query += "\n  ORDER BY "
		query += strings.Join(orderByClauses, ", ")
	} else {
		// make sure one default order is always picked
		if l.namespaced {
			// ID == metadata.namespace + "/" + metaqata.name
			query += "\n  ORDER BY f.\"id\" ASC "
		} else {
			query += "\n  ORDER BY f.\"metadata.name\" ASC "
		}
	}

	// 4- Pagination: LIMIT clause (from lo.Pagination)
	limitClause := ""
	limit := lo.Pagination.PageSize
	if limit > 0 {
		limitClause = "\n  LIMIT ?"
		params = append(params, limit)
	}

	// OFFSET clause (from lo.Pagination)
	offsetClause := ""
	offset := 0
	if lo.Pagination.Page >= 1 {
		offset += lo.Pagination.PageSize * (lo.Pagination.Page - 1)
	}
	if offset > 0 {
		offsetClause = "\n  OFFSET ?"
		params = append(params, offset)
	}
	if limit > 0 || offset > 0 {
		query += limitClause
		query += offsetClause
		queryInfo.countQuery = countQuery
		queryInfo.countParams = countParams
		queryInfo.limit = limit
		queryInfo.offset = offset
	}
	// Otherwise leave these as default values and the executor won't do pagination work

	logrus.Debugf("ListOptionIndexer prepared statement: %v", query)
	logrus.Debugf("Params: %v", params)
	queryInfo.query = query
	queryInfo.params = params

	return queryInfo, nil
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

type summaryInfo struct {
	Property string         `json:"property"`
	Counts   map[string]any `json:"counts"`
}
type fullSummary struct {
	Summary []summaryInfo `json:"summary"`
}

func (l *ListOptionIndexer) executeSummaryQueryForField(ctx context.Context, queryInfo *QueryInfo, field []string) (map[string]any, error) {
	stmt := l.Prepare(queryInfo.query)
	params := queryInfo.params
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
		logLongQuery(elapsed, queryInfo.query, params)
		items, err = l.ReadStringIntString(rows)
		if err != nil {
			return fmt.Errorf("executeSummaryQueryForField: read objects: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	propertyBlock := make(map[string]any)
	var countsBlock map[string]int
	for _, item := range items {
		propertyName := item[0]
		thisPBlock, ok := propertyBlock[propertyName]
		if !ok {
			propertyBlock[propertyName] = make(map[string]any)
			thisPBlock = propertyBlock[propertyName]
			thisPBlock.(map[string]any)["counts"] = make(map[string]int)
		}
		countsBlock = thisPBlock.(map[string]any)["counts"].(map[string]int)
		val, err := strconv.Atoi(item[1])
		if err != nil {
			return nil, err
		}
		countsBlock[item[2]] = val
	}

	return propertyBlock, nil
}

func (l *ListOptionIndexer) executeSummaryQuery(ctx context.Context, queryInfo *QueryInfo) (*types.APISummary, error) {
	stmt := l.Prepare(queryInfo.query)
	params := queryInfo.params
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
		logLongQuery(elapsed, queryInfo.query, params)
		items, err = l.ReadStringIntString(rows)
		if err != nil {
			return fmt.Errorf("executeSummaryQuery: read objects: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	countsByProperty := make(map[string]map[string]any)

	for _, item := range items {
		propertyName := item[0]
		propertyBlock, ok := countsByProperty[propertyName]
		if !ok {
			propertyBlock = make(map[string]any)
			countsByProperty[propertyName] = propertyBlock
			propertyBlock["property"] = propertyName
			propertyBlock["counts"] = make(map[string]any)
		}
		val, err := strconv.Atoi(item[1])
		if err != nil {
			return nil, err
		}
		propertyBlock["counts"].(map[string]any)[item[2]] = val
	}

	total := len(countsByProperty)
	blocksToSort := make([]types.SummaryEntry, 0, total)
	for _, v := range countsByProperty {
		property := v["property"].(string)
		fixedCounts := make(map[string]int)
		counts := v["counts"].(map[string]any)
		for k1, v1 := range counts {
			fixedV1, ok := v1.(int)
			if !ok {
				logrus.Errorf("Error converting value %v to int", v1)
				continue
			}
			fixedCounts[k1] = fixedV1
		}
		blocksToSort = append(blocksToSort, types.SummaryEntry{Property: property, Counts: fixedCounts})
	}
	sortedBlocks := slices.SortedFunc(slices.Values(blocksToSort), func(a, b types.SummaryEntry) int {
		return strings.Compare(a.Property, b.Property)
	})
	summary := types.APISummary{SummaryItems: sortedBlocks}
	return &summary, nil
}

func logLongQuery(elapsed time.Duration, query string, params []any) {
	threshold := 500 * time.Millisecond
	if elapsed < threshold {
		return
	}
	logrus.Debugf("Query took more than %v (took %v): %s with params %v", threshold, elapsed, query, params)
}

func (l *ListOptionIndexer) validateColumn(column string) error {
	for _, v := range l.indexedFields {
		if v == column {
			return nil
		}
	}
	return fmt.Errorf("column is invalid [%s]: %w", column, ErrInvalidColumn)
}

// Suppose the query access something like 'spec.containers[3].image' but only
// spec.containers.image is specified in the index.  If `spec.containers` is
// an array, then spec.containers.image is a pseudo-array of |-separated strings,
// and we can use our custom registered extractBarredValue function to extract the
// desired substring.
//
// The index can appear anywhere in the list of fields after the first entry,
// but we always end up with a |-separated list of substrings. Most of the time
// the index will be the second-last entry, but we lose nothing allowing for any
// position.
// Indices are 0-based.

func (l *ListOptionIndexer) getValidFieldEntry(prefix string, fields []string) (string, error) {
	columnName := toColumnName(fields)
	err := l.validateColumn(columnName)
	if err == nil {
		return fmt.Sprintf(`%s."%s"`, prefix, columnName), nil
	}
	if len(fields) <= 2 {
		return "", err
	}
	idx := -1
	for i := len(fields) - 1; i > 0; i-- {
		if !containsNonNumericRegex.MatchString(fields[i]) {
			idx = i
			break
		}
	}
	if idx == -1 {
		// We don't have an index onto a valid field
		return "", err
	}
	indexField := fields[idx]
	// fields[len(fields):] gives empty array
	otherFields := append(fields[0:idx], fields[idx+1:]...)
	leadingColumnName := toColumnName(otherFields)
	if l.validateColumn(leadingColumnName) != nil {
		// We have an index, but not onto a valid field
		return "", err
	}
	return fmt.Sprintf(`extractBarredValue(%s."%s", "%s")`, prefix, leadingColumnName, indexField), nil
}

// buildORClause creates an SQLite compatible query that ORs conditions built from passed filters
func (l *ListOptionIndexer) buildORClauseFromFilters(orFilters sqltypes.OrFilter, dbName string, mainFieldPrefix string, isSummaryFilter bool, joinTableIndexByLabelName map[string]int) (string, []any, error) {
	var params []any
	clauses := make([]string, 0, len(orFilters.Filters))
	var newParams []any
	var newClause string
	var err error

	for _, filter := range orFilters.Filters {
		if isLabelFilter(&filter) {
			var index int
			index, err = internLabel(filter.Field[2], joinTableIndexByLabelName, -1)
			if err != nil {
				return "", nil, err
			}
			newClause, newParams, err = l.getLabelFilter(index, filter, mainFieldPrefix, isSummaryFilter, dbName)
		} else {
			newClause, newParams, err = l.getFieldFilter(filter, mainFieldPrefix)
		}
		if err != nil {
			return "", nil, err
		}
		clauses = append(clauses, newClause)
		params = append(params, newParams...)
	}
	switch len(clauses) {
	case 0:
		return "", params, nil
	case 1:
		return clauses[0], params, nil
	}
	return fmt.Sprintf("(%s)", strings.Join(clauses, ") OR (")), params, nil
}

func (l *ListOptionIndexer) buildClauseFromProjectsOrNamespaces(orFilters sqltypes.OrFilter, dbName string, joinTableIndexByLabelName map[string]int) (string, []any, error) {
	var params []any
	var newParams []any
	var newClause string
	var err error
	var index int

	if len(orFilters.Filters) == 0 {
		return "", params, nil
	}

	clauses := make([]string, 0, len(orFilters.Filters))
	for _, filter := range orFilters.Filters {
		if isLabelFilter(&filter) {
			if index, err = internLabel(filter.Field[2], joinTableIndexByLabelName, -1); err != nil {
				return "", nil, err
			}
			newClause, newParams, err = l.getProjectsOrNamespacesLabelFilter(index, filter, dbName)
		} else {
			newClause, newParams, err = l.getProjectsOrNamespacesFieldFilter(filter)
		}
		if err != nil {
			return "", nil, err
		}
		clauses = append(clauses, newClause)
		params = append(params, newParams...)
	}

	if orFilters.Filters[0].Op == sqltypes.In {
		return fmt.Sprintf("(%s)", strings.Join(clauses, ") OR (")), params, nil
	}

	if orFilters.Filters[0].Op == sqltypes.NotIn {
		return fmt.Sprintf("(%s)", strings.Join(clauses, ") AND (")), params, nil
	}

	return "", nil, fmt.Errorf("project or namespaces supports only 'IN' or 'NOT IN' operation. op: %s is not valid",
		orFilters.Filters[0].Op)
}

func buildSortLabelsClause(labelName string, joinTableIndexByLabelName map[string]int, isAsc bool, sortAsIP bool) (string, error) {
	ltIndex, err := internLabel(labelName, joinTableIndexByLabelName, -1)
	fieldEntry := fmt.Sprintf("lt%d.value", ltIndex)
	if sortAsIP {
		fieldEntry = fmt.Sprintf("inet_aton(%s)", fieldEntry)
	}
	if err != nil {
		return "", err
	}
	dir := "ASC"
	nullsPosition := "LAST"
	if !isAsc {
		dir = "DESC"
		nullsPosition = "FIRST"
	}
	return fmt.Sprintf("%s %s NULLS %s", fieldEntry, dir, nullsPosition), nil
}

func getUnboundSortLabels(lo *sqltypes.ListOptions) []string {
	numSortDirectives := len(lo.SortList.SortDirectives)
	if numSortDirectives == 0 {
		return make([]string, 0)
	}
	unboundSortLabels := make(map[string]bool)
	for _, sortDirective := range lo.SortList.SortDirectives {
		fields := sortDirective.Fields
		if isLabelsFieldList(fields) {
			unboundSortLabels[fields[2]] = true
		}
	}
	if lo.Filters != nil {
		for _, andFilter := range lo.Filters {
			for _, orFilter := range andFilter.Filters {
				if isLabelFilter(&orFilter) {
					switch orFilter.Op {
					case sqltypes.In, sqltypes.Eq, sqltypes.Gt, sqltypes.Lt, sqltypes.Exists:
						delete(unboundSortLabels, orFilter.Field[2])
						// other ops don't necessarily select a label
					}
				}
			}
		}
	}
	return slices.Collect(maps.Keys(unboundSortLabels))
}

func getWithParts(unboundSortLabels []string, joinTableIndexByLabelName map[string]int, dbName string, mainFuncPrefix string) ([]string, []any, []string, []string, error) {
	numLabels := len(unboundSortLabels)
	parts := make([]string, numLabels)
	params := make([]any, numLabels)
	withNames := make([]string, numLabels)
	joinParts := make([]string, numLabels)
	for i, label := range unboundSortLabels {
		i1 := i + 1
		idx, err := internLabel(label, joinTableIndexByLabelName, i1)
		if err != nil {
			return parts, params, withNames, joinParts, err
		}
		parts[i] = fmt.Sprintf(`lt%d(key, value) AS (
SELECT key, value FROM "%s_labels"
  WHERE label = ?
)`, idx, dbName)
		params[i] = label
		withNames[i] = fmt.Sprintf("lt%d", idx)
		joinParts[i] = fmt.Sprintf("LEFT OUTER JOIN lt%d ON %s.key = lt%d.key", idx, mainFuncPrefix, idx)
	}

	return parts, params, withNames, joinParts, nil
}

// if nextNum <= 0 return an error message
func internLabel(labelName string, joinTableIndexByLabelName map[string]int, nextNum int) (int, error) {
	i, ok := joinTableIndexByLabelName[labelName]
	if ok {
		return i, nil
	}
	if nextNum <= 0 {
		return -1, fmt.Errorf("internal error: no join-table index given for label \"%s\"", labelName)
	}
	joinTableIndexByLabelName[labelName] = nextNum
	return nextNum, nil
}

// Possible ops from the k8s parser:
// KEY = and == (same) VALUE
// KEY != VALUE
// KEY exists []  # ,KEY, => this filter
// KEY ! []  # ,!KEY, => assert KEY doesn't exist
// KEY in VALUES
// KEY notin VALUES

func (l *ListOptionIndexer) getFieldFilter(filter sqltypes.Filter, prefix string) (string, []any, error) {
	opString := ""
	escapeString := ""
	fieldEntry, err := l.getValidFieldEntry(prefix, filter.Field)
	if err != nil {
		return "", nil, err
	}
	switch filter.Op {
	case sqltypes.Eq:
		if filter.Partial {
			opString = "LIKE"
			escapeString = escapeBackslashDirective
		} else {
			opString = "="
		}
		clause := fmt.Sprintf("%s %s ?%s", fieldEntry, opString, escapeString)
		return clause, []any{formatMatchTarget(filter)}, nil
	case sqltypes.NotEq:
		if filter.Partial {
			opString = "NOT LIKE"
			escapeString = escapeBackslashDirective
		} else {
			opString = "!="
		}
		clause := fmt.Sprintf("%s %s ?%s", fieldEntry, opString, escapeString)
		return clause, []any{formatMatchTarget(filter)}, nil

	case sqltypes.Lt, sqltypes.Gt:
		sym, target, err := prepareComparisonParameters(filter.Op, filter.Matches[0])
		if err != nil {
			return "", nil, err
		}
		clause := fmt.Sprintf("%s %s ?", fieldEntry, sym)
		return clause, []any{target}, nil

	case sqltypes.Exists, sqltypes.NotExists:
		return "", nil, errors.New("NULL and NOT NULL tests aren't supported for non-label queries")

	case sqltypes.In:
		fallthrough
	case sqltypes.NotIn:
		target := "()"
		if len(filter.Matches) > 0 {
			target = fmt.Sprintf("(?%s)", strings.Repeat(", ?", len(filter.Matches)-1))
		}
		opString = "IN"
		if filter.Op == sqltypes.NotIn {
			opString = "NOT IN"
		}
		clause := fmt.Sprintf("%s %s %s", fieldEntry, opString, target)
		matches := make([]any, len(filter.Matches))
		for i, match := range filter.Matches {
			matches[i] = match
		}
		return clause, matches, nil
	}

	return "", nil, fmt.Errorf("unrecognized operator: %s", opString)
}

func (l *ListOptionIndexer) getProjectsOrNamespacesFieldFilter(filter sqltypes.Filter) (string, []any, error) {
	opString := ""
	fieldEntry, err := l.getValidFieldEntry("nsf", filter.Field)
	if err != nil {
		return "", nil, err
	}
	switch filter.Op {
	case sqltypes.In:
		fallthrough
	case sqltypes.NotIn:
		target := "()"
		if len(filter.Matches) > 0 {
			target = fmt.Sprintf("(?%s)", strings.Repeat(", ?", len(filter.Matches)-1))
		}
		opString = "IN"
		if filter.Op == sqltypes.NotIn {
			opString = "NOT IN"
		}
		clause := fmt.Sprintf("%s %s %s", fieldEntry, opString, target)
		matches := make([]any, len(filter.Matches))
		for i, match := range filter.Matches {
			matches[i] = match
		}
		return clause, matches, nil
	}

	return "", nil, fmt.Errorf("unrecognized operator: %s", opString)
}

func (l *ListOptionIndexer) getProjectsOrNamespacesLabelFilter(index int, filter sqltypes.Filter, dbName string) (string, []any, error) {
	opString := ""
	labelName := filter.Field[2]
	target := "()"
	if len(filter.Matches) > 0 {
		target = fmt.Sprintf("(?%s)", strings.Repeat(", ?", len(filter.Matches)-1))
	}
	matches := make([]any, len(filter.Matches)+1)
	matches[0] = labelName
	for i, match := range filter.Matches {
		matches[i+1] = match
	}
	switch filter.Op {
	case sqltypes.In:
		clause := fmt.Sprintf(`lt%d.label = ? AND lt%d.value IN %s`, index, index, target)
		return clause, matches, nil
	case sqltypes.NotIn:
		clause1 := fmt.Sprintf(`(lt%d.label = ? AND lt%d.value NOT IN %s)`, index, index, target)
		clause2 := fmt.Sprintf(`(o.key NOT IN (SELECT f1.key FROM "%s_fields" f1
		LEFT OUTER JOIN "_v1_Namespace_fields" nsf1 ON f1."metadata.namespace" = nsf1."metadata.name"
		LEFT OUTER JOIN "_v1_Namespace_labels" lt%di1 ON nsf1.key = lt%di1.key
		WHERE lt%di1.label = ?))`, dbName, index, index, index)
		matches = append(matches, labelName)
		clause := fmt.Sprintf("%s OR %s", clause1, clause2)
		return clause, matches, nil
	}
	return "", nil, fmt.Errorf("unrecognized operator: %s", opString)
}

func (l *ListOptionIndexer) getLabelFilter(index int, filter sqltypes.Filter, mainFieldPrefix string, isSummaryFilter bool, dbName string) (string, []any, error) {
	opString := ""
	escapeString := ""
	matchFmtToUse := strictMatchFmt
	labelName := filter.Field[2]
	switch filter.Op {
	case sqltypes.Eq:
		if filter.Partial {
			opString = "LIKE"
			escapeString = escapeBackslashDirective
			matchFmtToUse = matchFmt
		} else {
			opString = "="
		}
		clause := fmt.Sprintf(`lt%d.label = ? AND lt%d.value %s ?%s`, index, index, opString, escapeString)
		return clause, []any{labelName, formatMatchTargetWithFormatter(filter.Matches[0], matchFmtToUse)}, nil

	case sqltypes.NotEq:
		if filter.Partial {
			opString = "NOT LIKE"
			escapeString = escapeBackslashDirective
			matchFmtToUse = matchFmt
		} else {
			opString = "!="
		}
		subFilter := sqltypes.Filter{
			Field: filter.Field,
			Op:    sqltypes.NotExists,
		}
		existenceClause, subParams, err := l.getLabelFilter(index, subFilter, mainFieldPrefix, isSummaryFilter, dbName)
		if err != nil {
			return "", nil, err
		}
		clause := fmt.Sprintf(`(%s) OR (lt%d.label = ? AND lt%d.value %s ?%s)`, existenceClause, index, index, opString, escapeString)
		params := append(subParams, labelName, formatMatchTargetWithFormatter(filter.Matches[0], matchFmtToUse))
		return clause, params, nil

	case sqltypes.Lt, sqltypes.Gt:
		sym, target, err := prepareComparisonParameters(filter.Op, filter.Matches[0])
		if err != nil {
			return "", nil, err
		}
		clause := fmt.Sprintf(`lt%d.label = ? AND lt%d.value %s ?`, index, index, sym)
		return clause, []any{labelName, target}, nil

	case sqltypes.Exists:
		clause := fmt.Sprintf(`lt%d.label = ?`, index)
		return clause, []any{labelName}, nil

	case sqltypes.NotExists:
		clause := ""
		if isSummaryFilter {
			mainFieldPrefix := "f1"
			newMainFieldPrefix := mainFieldPrefix + "1"
			clause = fmt.Sprintf(`%s.key NOT IN (SELECT %s.key FROM "%s_fields" %s
		LEFT OUTER JOIN "%s_labels" lt%di1 ON %s.key = lt%di1.key
		WHERE lt%di1.label = ?)`,
				mainFieldPrefix, newMainFieldPrefix, dbName, newMainFieldPrefix,
				dbName, index, newMainFieldPrefix, index,
				index)
		} else {
			clause = fmt.Sprintf(`o.key NOT IN (SELECT f1.key FROM "%s_fields" f1
		LEFT OUTER JOIN "%s_labels" lt%di1 ON f1.key = lt%di1.key
		WHERE lt%di1.label = ?)`, dbName, dbName, index, index, index)
		}
		return clause, []any{labelName}, nil

	case sqltypes.In:
		target := "(?"
		if len(filter.Matches) > 0 {
			target += strings.Repeat(", ?", len(filter.Matches)-1)
		}
		target += ")"
		clause := fmt.Sprintf(`lt%d.label = ? AND lt%d.value IN %s`, index, index, target)
		matches := make([]any, len(filter.Matches)+1)
		matches[0] = labelName
		for i, match := range filter.Matches {
			matches[i+1] = match
		}
		return clause, matches, nil

	case sqltypes.NotIn:
		target := "(?"
		if len(filter.Matches) > 0 {
			target += strings.Repeat(", ?", len(filter.Matches)-1)
		}
		target += ")"
		subFilter := sqltypes.Filter{
			Field: filter.Field,
			Op:    sqltypes.NotExists,
		}
		existenceClause, subParams, err := l.getLabelFilter(index, subFilter, mainFieldPrefix, isSummaryFilter, dbName)
		if err != nil {
			return "", nil, err
		}
		clause := fmt.Sprintf(`(%s) OR (lt%d.label = ? AND lt%d.value NOT IN %s)`, existenceClause, index, index, target)
		matches := append(subParams, labelName)
		for _, match := range filter.Matches {
			matches = append(matches, match)
		}
		return clause, matches, nil
	}
	return "", nil, fmt.Errorf("unrecognized operator: %s", opString)
}

func prepareComparisonParameters(op sqltypes.Op, target string) (string, float64, error) {
	num, err := strconv.ParseFloat(target, 32)
	if err != nil {
		return "", 0, err
	}
	switch op {
	case sqltypes.Lt:
		return "<", num, nil
	case sqltypes.Gt:
		return ">", num, nil
	}
	return "", 0, fmt.Errorf("unrecognized operator when expecting '<' or '>': '%s'", op)
}

func formatMatchTarget(filter sqltypes.Filter) string {
	format := strictMatchFmt
	if filter.Partial {
		format = matchFmt
	}
	return formatMatchTargetWithFormatter(filter.Matches[0], format)
}

func formatMatchTargetWithFormatter(match string, format string) string {
	// To allow matches on the backslash itself, the character needs to be replaced first.
	// Otherwise, it will undo the following replacements.
	match = strings.ReplaceAll(match, `\`, `\\`)
	match = strings.ReplaceAll(match, `_`, `\_`)
	match = strings.ReplaceAll(match, `%`, `\%`)
	return fmt.Sprintf(format, match)
}

// There are two kinds of string arrays to turn into a string, based on the last value in the array
// simple: ["a", "b", "conformsToIdentifier"] => "a.b.conformsToIdentifier"
// complex: ["a", "b", "foo.io/stuff"] => "a.b[foo.io/stuff]"

func smartJoin(s []string) string {
	if len(s) == 0 {
		return ""
	}
	if len(s) == 1 {
		return s[0]
	}
	lastBit := s[len(s)-1]
	simpleName := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	if simpleName.MatchString(lastBit) {
		return strings.Join(s, ".")
	}
	return fmt.Sprintf("%s[%s]", strings.Join(s[0:len(s)-1], "."), lastBit)
}

// toColumnName returns the column name corresponding to a field expressed as string slice
func toColumnName(s []string) string {
	return db.Sanitize(smartJoin(s))
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
				// Particularly with labels/annotation indexes, it is totally possible that some objects won't have these.
				// So either this is not an error state, or it could be an error state with a type that callers
				// will need to deal with somehow.
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
						return nil, fmt.Errorf(failedToGetFromSliceFmt, subField)
					}

					_, found := itemVal[subField]
					if found {
						itemStr, ok := itemVal[subField].(string)
						if !ok {
							return nil, fmt.Errorf(failedToGetFromSliceFmt, subField)
						}
						result[index] = itemStr
					} else {
						result[index] = ""
					}
				}
				return result, nil
			}
		default:
			return nil, fmt.Errorf("[listoption indexer] failed to parse subfields: %v", subFields)
		}
	}
	return obj, nil
}

func extractSubFields(fields string) []string {
	subfields := make([]string, 0)
	for _, subField := range subfieldRegex.FindAllString(fields, -1) {
		subfields = append(subfields, strings.TrimSuffix(subField, "."))
	}
	return subfields
}

func isLabelFilter(f *sqltypes.Filter) bool {
	return len(f.Field) >= 2 && f.Field[0] == "metadata" && f.Field[1] == "labels"
}

func hasLabelFilter(filters []sqltypes.OrFilter) bool {
	for _, outerFilter := range filters {
		for _, filter := range outerFilter.Filters {
			if isLabelFilter(&filter) {
				return true
			}
		}
	}
	return false
}

func isLabelsFieldList(fields []string) bool {
	return len(fields) == 3 && fields[0] == "metadata" && fields[1] == "labels"
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

func matchWatch(filterName string, filterNamespace string, filterSelector labels.Selector, oldObj any, obj any) bool {
	matchOld := false
	if oldObj != nil {
		matchOld = matchFilter(filterName, filterNamespace, filterSelector, oldObj)
	}
	return matchOld || matchFilter(filterName, filterNamespace, filterSelector, obj)
}

func matchFilter(filterName string, filterNamespace string, filterSelector labels.Selector, obj any) bool {
	if obj == nil {
		return false
	}
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return false
	}
	if filterName != "" && filterName != metadata.GetName() {
		return false
	}
	if filterNamespace != "" && filterNamespace != metadata.GetNamespace() {
		return false
	}
	if filterSelector != nil {
		if !filterSelector.Matches(labels.Set(metadata.GetLabels())) {
			return false
		}
	}
	return true
}

func (l *ListOptionIndexer) RunGC(ctx context.Context) {
	if l.gcInterval == 0 || l.gcKeepCount == 0 {
		return
	}

	ticker := time.NewTicker(l.gcInterval)
	defer ticker.Stop()

	logrus.Infof("Started SQL cache garbage collection for %s (interval=%s, keep=%d)", l.GetName(), l.gcInterval, l.gcKeepCount)
	defer logrus.Infof("Stopped SQL cache garbage collection for %s (interval=%s, keep=%d)", l.GetName(), l.gcInterval, l.gcKeepCount)

	for {
		select {
		case <-ticker.C:
			err := l.WithTransaction(ctx, true, func(tx db.TxClient) error {
				_, err := tx.Stmt(l.deleteEventsByCountStmt).Exec(l.gcKeepCount)
				return err
			})
			if err != nil {
				logrus.Errorf("garbage collection for %s: %v", l.GetName(), err)
			}
		case <-ctx.Done():
			return
		}
	}
}
