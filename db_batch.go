package db

import (
	"github.com/bdware/go-datastore"
	dskey "github.com/bdware/go-datastore/key"
	db "github.com/tendermint/tm-db"
)

type batch struct {
	ds    datastore.Datastore
	batch datastore.Batch
}

var _ db.Batch = (*batch)(nil)

func newBatch(ds datastore.Datastore) (*batch, error) {
	bds, ok := ds.(datastore.Batching)
	if !ok {
		return nil, datastore.ErrBatchUnsupported
	}
	b, err := bds.Batch()
	if err != nil {
		return nil, err
	}
	return &batch{
		ds:    ds,
		batch: b,
	}, nil
}

// Set implements batch.
func (b *batch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.batch == nil {
		return errBatchClosed
	}
	return b.batch.Put(dskey.NewBytesKey(key), value)
}

// Delete implements batch.
func (b *batch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.batch == nil {
		return errBatchClosed
	}
	return b.batch.Delete(dskey.NewBytesKey(key))
}

// Write implements batch.
func (b *batch) Write() error {
	if err := b.write(); err != nil {
		return err
	}
	return b.Close()
}

// WriteSync implements batch.
func (b *batch) WriteSync() error {
	if err := b.write(); err != nil {
		return err
	}
	if err := b.ds.Sync(dskey.NewBytesKey([]byte{})); err != nil {
		return err
	}
	return b.Close()
}

func (b *batch) write() error {
	if b.batch == nil {
		return errBatchClosed
	}
	return b.batch.Commit()
}

// Close implements batch.
func (b *batch) Close() error {
	b.batch = nil
	return nil
}
