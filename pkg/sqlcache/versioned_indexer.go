package sqlcache

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"k8s.io/client-go/tools/cache"
)

// VersionedIndexer extends Indexer by storing a range of versions in addition to the latest one
type VersionedIndexer struct {
	*Indexer
	versionFunc VersionFunc

	addHistoryStmt    *sql.Stmt
	deleteHistoryStmt *sql.Stmt
	getByVersionStmt  *sql.Stmt
}

type VersionFunc func(obj any) (int, error)

// NewVersionedIndexer returns an Indexer that also stores a range of versions in addition to the latest one
func NewVersionedIndexer(example any, keyFunc cache.KeyFunc, versionFunc VersionFunc, path string, indexers cache.Indexers) (*VersionedIndexer, error) {
	i, err := NewIndexer(example, keyFunc, path, indexers)

	err = i.InitExec(`CREATE TABLE object_history (
			key VARCHAR NOT NULL,
			version INTEGER NOT NULL,
			deleted_version INTEGER DEFAULT NULL,
			object BLOB NOT NULL,
			PRIMARY KEY (key, version)
	   )`)
	if err != nil {
		return nil, err
	}
	err = i.InitExec(`CREATE INDEX object_history_version ON object_history(version)`)
	if err != nil {
		return nil, err
	}

	v := &VersionedIndexer{
		Indexer:     i,
		versionFunc: versionFunc,
	}
	v.RegisterAfterUpsert(v.AfterUpsert)
	v.RegisterAfterDelete(v.AfterDelete)

	v.addHistoryStmt = v.Prepare(`INSERT INTO object_history(key, version, deleted_version, object)
		SELECT ?, ?, NULL, object
			FROM objects
			WHERE key = ?
			ON CONFLICT
			    DO UPDATE SET object = excluded.object, deleted_version = NULL`)
	v.deleteHistoryStmt = v.Prepare(`UPDATE object_history SET deleted_version = (SELECT MAX(version) FROM object_history) WHERE key = ?`)
	v.getByVersionStmt = v.Prepare(`SELECT object FROM object_history WHERE key = ? AND version = ? AND (deleted_version IS NULL OR deleted_version > ?)`)

	return v, nil
}

/* Core methods */

// AfterUpsert appends the latest version to the history table
func (v *VersionedIndexer) AfterUpsert(key string, obj any, tx *sql.Tx) error {
	version, err := v.versionFunc(obj)
	if err != nil {
		return err
	}
	_, err = tx.Stmt(v.addHistoryStmt).Exec(key, version, key)
	return err
}

// AfterDelete updates the deleted flag on the history table
func (v *VersionedIndexer) AfterDelete(key string, tx *sql.Tx) error {
	_, err := tx.Stmt(v.deleteHistoryStmt).Exec(key)
	return err
}

// GetByKeyAndVersion returns the object associated with the given object's key and (exact) version
func (v *VersionedIndexer) GetByKeyAndVersion(key string, version int) (item any, exists bool, err error) {
	result, err := v.QueryObjects(v.getByVersionStmt, key, version, version)
	if err != nil {
		return nil, false, err
	}

	if len(result) == 0 {
		return nil, false, nil
	}

	return result[0], true, nil
}
