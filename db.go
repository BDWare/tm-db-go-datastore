package db

import (
	"errors"
	"fmt"

	ds "github.com/bdware/go-datastore"
	dskey "github.com/bdware/go-datastore/key"
	"github.com/bdware/go-datastore/query"
	"github.com/tendermint/tm-db"
)

const (
	// Backend type for bdware/go-datastore backed tm-db implementation
	GoDatastoreBackend db.BackendType = "go-datastore"
)

// https://github.com/tendermint/tm-db/blob/3157a928986298875ca48e6d5f77132a32dfb1f0/types.go#L5
var (
	// errBatchClosed is returned when a closed or written batch is used.
	errBatchClosed = errors.New("batch has been written or closed")

	// errKeyEmpty is returned when attempting to use an empty or nil key.
	errKeyEmpty = errors.New("key cannot be empty")

	// errValueNil is returned when attempting to set a nil value.
	errValueNil = errors.New("value cannot be nil")
)

// DB implements db.DB by wrapping a bdware/go-datastore Datastore instance
type DB struct {
	ds ds.Datastore
}

var _ db.DB = (*DB)(nil)

// New creates a new in-memory database.
func New(ds ds.Datastore) *DB {
	database := &DB{ds: ds}
	return database
}

// Get implements DB.
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	res, err := db.ds.Get(dskey.NewBytesKey(key))
	if err != nil {
		if err == ds.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return res, nil
}

// Has implements DB.
func (db *DB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errKeyEmpty
	}
	return db.ds.Has(dskey.NewBytesKey(key))
}

// Set implements DB.
func (db *DB) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	return db.set(dskey.NewBytesKey(key), value)
}

// SetSync implements DB.
func (db *DB) SetSync(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	k := dskey.NewBytesKey(key)
	if err := db.set(k, value); err != nil {
		return err
	}
	return db.ds.Sync(k)
}

func (db *DB) set(key dskey.Key, value []byte) error {
	return db.ds.Put(key, value)
}

// Delete implements DB.
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	return db.ds.Delete(dskey.NewBytesKey(key))
}

// DeleteSync implements DB.
func (db *DB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	k := dskey.NewBytesKey(key)
	if err := db.ds.Delete(k); err != nil {
		return err
	}
	return db.ds.Sync(k)
}

func (db *DB) delete(key dskey.Key) {
	db.ds.Delete(key)
}

func (db *DB) Datastore() ds.Datastore {
	return db.ds
}

// Close implements DB.
func (db *DB) Close() error {
	return db.ds.Close()
}

// Print implements DB.
func (db *DB) Print() error {

	if persDs, ok := db.ds.(ds.PersistentDatastore); ok {
		if size, err := persDs.DiskUsage(); err == nil {
			fmt.Printf("Disk usage: %v\n", size)
		}
	}

	qr, err := db.ds.Query(query.Query{})
	if err != nil {
		return err
	}
	for r := range qr.Next() {
		if r.Error != nil {
			return r.Error
		}
		fmt.Printf("[%X]:\t[%X]\n", r.Entry.Key.Bytes(), r.Entry.Value)
	}

	return nil
}

// Stats implements DB.
func (db *DB) Stats() map[string]string {
	stats := make(map[string]string)
	if persDs, ok := db.ds.(ds.PersistentDatastore); ok {
		if size, err := persDs.DiskUsage(); err == nil {
			stats["database.diskusage"] = fmt.Sprintf("%d", size)
		}
	}
	return stats
}

// NewBatch implements DB.
func (db *DB) NewBatch() db.Batch {
	b, err := newBatch(db.ds)
	if err != nil {
		panic(err)
	}
	return b
}

// iterator implements DB.
func (db *DB) Iterator(start, end []byte) (db.Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	return newIterator(db.ds, start, end, false)
}

// ReverseIterator implements DB.
// Takes out a read-lock on the database until the iterator is closed.
func (db *DB) ReverseIterator(start, end []byte) (db.Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	return newIterator(db.ds, start, end, true)
}
