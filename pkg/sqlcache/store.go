package sqlcache

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"k8s.io/client-go/tools/cache"
	"os"
	"reflect"
)

// Store is a SQLite-backed cache.Store
type Store struct {
	typ     reflect.Type
	keyFunc cache.KeyFunc

	db           *sql.DB
	upsertStmt   *sql.Stmt
	deleteStmt   *sql.Stmt
	getStmt      *sql.Stmt
	listStmt     *sql.Stmt
	listKeysStmt *sql.Stmt

	afterUpsert []func(key string, obj any, tx *sql.Tx) error
	afterDelete []func(key string, tx *sql.Tx) error
}

// NewStore creates a SQLite-backed cache.Store for objects of the given example type
func NewStore(example any, keyFunc cache.KeyFunc, path string) (*Store, error) {
	err := os.RemoveAll(path)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", path+"?mode=rwc&_journal_mode=memory&_synchronous=off&_mutex=no&_foreign_keys=on")
	if err != nil {
		return nil, err
	}

	s := &Store{
		typ:         reflect.TypeOf(example),
		keyFunc:     keyFunc,
		db:          db,
		afterUpsert: []func(key string, obj any, tx *sql.Tx) error{},
		afterDelete: []func(key string, tx *sql.Tx) error{},
	}

	err = s.InitExec(`CREATE TABLE objects (
		key VARCHAR UNIQUE NOT NULL PRIMARY KEY,
		object BLOB
	)`)
	if err != nil {
		return nil, err
	}

	s.upsertStmt = s.Prepare(`INSERT INTO objects(key, object) VALUES (?, ?) ON CONFLICT DO UPDATE SET object = excluded.object`)
	s.deleteStmt = s.Prepare(`DELETE FROM objects WHERE key = ?`)
	s.getStmt = s.Prepare(`SELECT object FROM objects WHERE key = ?`)
	s.listStmt = s.Prepare(`SELECT object FROM objects`)
	s.listKeysStmt = s.Prepare(`SELECT key FROM objects`)

	return s, nil
}

/* Core methods */

// Upsert saves an obj with its key, or updates key with obj if it exists in this Store
func (s *Store) Upsert(key string, obj any) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Stmt(s.upsertStmt).Exec(key, s.toBytes(obj))
	if err != nil {
		return s.rollback(err, tx)
	}

	err = s.runAfterUpsert(key, obj, tx)
	if err != nil {
		return s.rollback(err, tx)
	}

	return tx.Commit()
}

// DeleteByKey deletes the object associated with key, if it exists in this Store
func (s *Store) DeleteByKey(key string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Stmt(s.deleteStmt).Exec(key)
	if err != nil {
		return s.rollback(err, tx)
	}

	err = s.runAfterDelete(key, tx)
	if err != nil {
		return s.rollback(err, tx)
	}

	return tx.Commit()
}

// GetByKey returns the object associated with the given object's key
func (s *Store) GetByKey(key string) (item any, exists bool, err error) {
	result, err := s.QueryObjects(s.getStmt, key)
	if err != nil {
		return nil, false, err
	}

	if len(result) == 0 {
		return nil, false, nil
	}

	return result[0], true, nil
}

// ReplaceByKey will delete the contents of the Store, using instead the given key to obj map
func (s *Store) ReplaceByKey(objects map[string]any) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	keys, err := s.QueryStrings(tx.Stmt(s.listKeysStmt))
	if err != nil {
		return s.rollback(err, tx)
	}

	for _, key := range keys {
		_, err = tx.Stmt(s.deleteStmt).Exec(key)
		if err != nil {
			return s.rollback(err, tx)
		}
		err = s.runAfterDelete(key, tx)
		if err != nil {
			return s.rollback(err, tx)
		}
	}

	for key, obj := range objects {
		_, err = tx.Stmt(s.upsertStmt).Exec(key, s.toBytes(obj))
		if err != nil {
			return s.rollback(err, tx)
		}
		err = s.runAfterUpsert(key, obj, tx)
		if err != nil {
			return s.rollback(err, tx)
		}
	}

	return tx.Commit()
}

// Close closes the database and prevents new queries from starting
func (s *Store) Close() error {
	return s.db.Close()
}

/* Satisfy cache.Store */

// Add saves an obj, or updates it if it exists in this Store
func (s *Store) Add(obj any) error {
	key, err := s.keyFunc(obj)
	if err != nil {
		return err
	}

	return s.Upsert(key, obj)
}

// Update saves an obj, or updates it if it exists in this Store
func (s *Store) Update(obj any) error {
	return s.Add(obj)
}

// Delete deletes the given object, if it exists in this Store
func (s *Store) Delete(obj any) error {
	key, err := s.keyFunc(obj)
	if err != nil {
		return err
	}

	return s.DeleteByKey(key)
}

// List returns a list of all the currently known objects
// Note: I/O errors will panic this function, as the interface signature does not allow returning errors
func (s *Store) List() []any {
	result, err := s.QueryObjects(s.listStmt)
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in Store.List"))
	}
	return result
}

// ListKeys returns a list of all the keys currently in this Store
// Note: I/O errors will panic this function, as the interface signature does not allow returning errors
func (s *Store) ListKeys() []string {
	result, err := s.QueryStrings(s.listKeysStmt)
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in Store.ListKeys"))
	}
	return result
}

// Get returns the object with the same key as obj
func (s *Store) Get(obj any) (item any, exists bool, err error) {
	key, err := s.keyFunc(obj)
	if err != nil {
		return nil, false, err
	}

	return s.GetByKey(key)
}

// Replace will delete the contents of the Store, using instead the given list
func (s *Store) Replace(objects []any, _ string) error {
	objectMap := map[string]any{}

	for _, object := range objects {
		key, err := s.keyFunc(object)
		if err != nil {
			return err
		}
		objectMap[key] = object
	}
	return s.ReplaceByKey(objectMap)
}

// Resync is a no-op and is deprecated
func (s *Store) Resync() error {
	return nil
}

/* Utilities */

// InitExec executes a statement as part of the DB initialization, closing the connection on error
func (s *Store) InitExec(stmt string) error {
	_, err := s.db.Exec(stmt)
	if err != nil {
		cerr := s.db.Close()
		if cerr != nil {
			return errors.Wrapf(cerr, "Error closing the DB during initialization")
		}
		return errors.Wrapf(err, "Error initializing Store DB")
	}
	return nil
}

// Prepare prepares a statement
func (s *Store) Prepare(stmt string) *sql.Stmt {
	prepared, err := s.db.Prepare(stmt)
	if err != nil {
		panic(errors.Errorf("Error preparing statement: %s\n%v", stmt, err))
	}
	return prepared
}

// QueryObjects runs a prepared statement that returns gobbed objects of type typ
func (s *Store) QueryObjects(stmt *sql.Stmt, params ...any) ([]any, error) {
	rows, err := stmt.Query(params...)
	if err != nil {
		return nil, err
	}

	var result []any
	for rows.Next() {
		var buf sql.RawBytes
		err := rows.Scan(&buf)
		if err != nil {
			return s.closeOnError(rows, err)
		}

		singleResult, err := s.fromBytes(buf)
		if err != nil {
			return s.closeOnError(rows, err)
		}
		result = append(result, singleResult.Elem().Interface())
	}
	err = rows.Err()
	if err != nil {
		if err != nil {
			return s.closeOnError(rows, err)
		}
		return nil, err
	}

	err = rows.Close()
	if err != nil {
		return nil, err
	}

	return result, nil
}

// QueryStrings runs a prepared statement that returns strings
func (s *Store) QueryStrings(stmt *sql.Stmt, params ...any) ([]string, error) {
	rows, err := stmt.Query(params...)
	if err != nil {
		return nil, err
	}

	var result []string
	for rows.Next() {
		var key string
		err := rows.Scan(&key)
		if err != nil {
			ce := rows.Close()
			if ce != nil {
				return nil, errors.Wrap(ce, "while handling "+err.Error())
			}
		}

		result = append(result, key)
	}
	err = rows.Err()
	if err != nil {
		ce := rows.Close()
		if ce != nil {
			return nil, errors.Wrap(ce, "while handling "+err.Error())
		}
	}

	err = rows.Close()
	if err != nil {
		return nil, err
	}

	return result, nil
}

// toBytes encodes an object to a byte slice
func (s *Store) toBytes(obj any) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(obj)
	if err != nil {
		panic(errors.Wrap(err, "Error while gobbing object"))
	}
	bb := buf.Bytes()
	return bb
}

// fromBytes decodes an object from a byte slice
func (s *Store) fromBytes(buf sql.RawBytes) (reflect.Value, error) {
	dec := gob.NewDecoder(bytes.NewReader(buf))
	singleResult := reflect.New(s.typ)
	err := dec.DecodeValue(singleResult)
	return singleResult, err
}

// rollback handles rollbacks and wraps errors if needed
func (s *Store) rollback(err error, tx *sql.Tx) error {
	rerr := tx.Rollback()
	if rerr != nil {
		return errors.Wrapf(rerr, "Error while rolling back from: %v", err)
	}
	return rerr
}

// RegisterAfterUpsert registers a func to be called after each upsert
func (s *Store) RegisterAfterUpsert(f func(key string, obj any, tx *sql.Tx) error) {
	s.afterUpsert = append(s.afterUpsert, f)
}

// runAfterUpsert executes functions registered to run after upsert
func (s *Store) runAfterUpsert(key string, obj any, tx *sql.Tx) error {
	for _, f := range s.afterUpsert {
		err := f(key, obj, tx)
		if err != nil {
			return err
		}
	}
	return nil
}

// RegisterAfterDelete registers a func to be called after each deletion
func (s *Store) RegisterAfterDelete(f func(key string, tx *sql.Tx) error) {
	s.afterDelete = append(s.afterDelete, f)
}

// runAfterDelete executes functions registered to run after upsert
func (s *Store) runAfterDelete(key string, tx *sql.Tx) error {
	for _, f := range s.afterDelete {
		err := f(key, tx)
		if err != nil {
			return err
		}
	}
	return nil
}

// closeOnError closes the sql.Rows object and wraps errors if needed
func (s *Store) closeOnError(rows *sql.Rows, err error) ([]any, error) {
	ce := rows.Close()
	if ce != nil {
		return nil, errors.Wrap(ce, "while handling "+err.Error())
	}

	return nil, err
}
