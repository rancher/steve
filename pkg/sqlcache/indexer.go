package sqlcache

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"k8s.io/client-go/tools/cache"
	"strings"
)

// Indexer is a SQLite-backed cache.Indexer which builds upon Store adding an index table
type Indexer struct {
	*Store
	indexers cache.Indexers

	deleteIndicesStmt   *sql.Stmt
	addIndexStmt        *sql.Stmt
	listByIndexStmt     *sql.Stmt
	listKeysByIndexStmt *sql.Stmt
	listIndexValuesStmt *sql.Stmt
}

// NewIndexer returns a cache.Indexer backed by SQLite for objects of the given example type
func NewIndexer(example any, keyFunc cache.KeyFunc, path string, indexers cache.Indexers) (*Indexer, error) {
	// sanity checks first
	for key := range indexers {
		if strings.Contains(key, `"`) {
			panic("Quote characters (\") in indexer names are not supported")
		}
	}

	s, err := NewStore(example, keyFunc, path)
	if err != nil {
		return nil, err
	}

	err = s.InitExec(`CREATE TABLE indices (
			name VARCHAR NOT NULL,
			value VARCHAR NOT NULL,
			key VARCHAR NOT NULL REFERENCES objects(key) ON DELETE CASCADE,
			PRIMARY KEY (name, value, key)
        )`)
	if err != nil {
		return nil, err
	}
	err = s.InitExec(`CREATE INDEX indices_name_value_index ON indices(name, value)`)
	if err != nil {
		return nil, err
	}

	i := &Indexer{
		Store:    s,
		indexers: indexers,
	}
	i.RegisterAfterUpsert(i.AfterUpsert)

	i.deleteIndicesStmt = s.Prepare(`DELETE FROM indices WHERE key = ?`)
	i.addIndexStmt = s.Prepare(`INSERT INTO indices(name, value, key) VALUES (?, ?, ?)`)
	i.listByIndexStmt = s.Prepare(`SELECT object FROM objects
			WHERE key IN (
			    SELECT key FROM indices
			    	WHERE name = ? AND value = ?
			)`)
	i.listKeysByIndexStmt = s.Prepare(`SELECT DISTINCT key FROM indices WHERE name = ? AND value = ?`)
	i.listIndexValuesStmt = s.Prepare(`SELECT DISTINCT value FROM indices WHERE name = ?`)

	return i, nil
}

/* Core methods */

// AfterUpsert updates indices of an object
func (i *Indexer) AfterUpsert(key string, obj any, tx *sql.Tx) error {
	// delete all
	_, err := tx.Stmt(i.deleteIndicesStmt).Exec(key)
	if err != nil {
		return err
	}

	// re-insert all
	for indexName, indexFunc := range i.indexers {
		values, err := indexFunc(obj)
		if err != nil {
			return err
		}

		for _, value := range values {
			_, err = tx.Stmt(i.addIndexStmt).Exec(indexName, value, key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

/* Satisfy cache.Indexer */

// Index returns a list of items that match the given object on the index function
func (i *Indexer) Index(indexName string, obj any) ([]any, error) {
	indexFunc := i.indexers[indexName]
	if indexFunc == nil {
		return nil, fmt.Errorf("Index with name %s does not exist", indexName)
	}

	values, err := indexFunc(obj)
	if err != nil {
		return nil, err
	}

	if len(values) == 0 {
		return nil, nil
	}

	// typical case
	if len(values) == 1 {
		return i.ByIndex(indexName, values[0])
	}

	// atypical case - more than one value to lookup
	// HACK: sql.Statement.Query does not allow to pass slices in as of go 1.19 - create an ad-hoc statement
	query := fmt.Sprintf(`
			SELECT object FROM objects
				WHERE key IN (
					SELECT key FROM indices
						WHERE name = ? AND value IN (?%s)
				)
		`, strings.Repeat(", ?", len(values)-1))
	stmt := i.Prepare(query)

	// HACK: Query will accept []any but not []string
	params := []any{indexName}
	for _, value := range values {
		params = append(params, value)
	}

	return i.QueryObjects(stmt, params...)
}

// ByIndex returns the stored objects whose set of indexed values
// for the named index includes the given indexed value
func (i *Indexer) ByIndex(indexName, indexedValue string) ([]any, error) {
	return i.QueryObjects(i.listByIndexStmt, indexName, indexedValue)
}

// IndexKeys returns a list of the Store keys of the objects whose indexed values in the given index include the given indexed value
func (i *Indexer) IndexKeys(indexName, indexedValue string) ([]string, error) {
	indexFunc := i.indexers[indexName]
	if indexFunc == nil {
		return nil, fmt.Errorf("Index with name %s does not exist", indexName)
	}

	return i.QueryStrings(i.listKeysByIndexStmt, indexName, indexedValue)
}

// ListIndexFuncValues wraps SafeListIndexFuncValues and panics in case of I/O errors
func (i *Indexer) ListIndexFuncValues(name string) []string {
	result, err := i.SafeListIndexFuncValues(name)
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in SafeListIndexFuncValues"))
	}
	return result
}

// SafeListIndexFuncValues returns all the indexed values of the given index
func (i *Indexer) SafeListIndexFuncValues(indexName string) ([]string, error) {
	return i.QueryStrings(i.listIndexValuesStmt, indexName)
}

// GetIndexers returns the indexers
func (i *Indexer) GetIndexers() cache.Indexers {
	return i.indexers
}

// AddIndexers adds more indexers to this Store.  If you call this after you already have data
// in the Store, the results are undefined.
func (i *Indexer) AddIndexers(newIndexers cache.Indexers) error {
	for k, v := range newIndexers {
		i.indexers[k] = v
	}
	return nil
}
