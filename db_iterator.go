package db

import (
	"github.com/bdware/go-datastore"
	"github.com/bdware/go-datastore/key"
	"github.com/bdware/go-datastore/query"
	db "github.com/tendermint/tm-db"
)

type iterator struct {
	res       query.Results
	cur       *query.Result
	start     []byte
	end       []byte
	isReverse bool
	isInvalid bool
}

var _ db.Iterator = (*iterator)(nil)

func newIterator(ds datastore.Datastore, start, end []byte, isReverse bool) (*iterator, error) {
	rnge := query.Range{}
	if start != nil {
		rnge.Start = key.NewBytesKey(start)
	}
	if end != nil {
		rnge.End = key.NewBytesKey(end)
	}
	q := query.Query{Range: rnge}

	if isReverse {
		q.Orders = []query.Order{query.OrderByKeyDescending{}}
	}

	res, err := ds.Query(q)
	if err != nil {
		return nil, err
	}
	itr := &iterator{
		res:       res,
		cur:       nil,
		start:     start,
		end:       end,
		isReverse: isReverse,
		isInvalid: false,
	}
	itr.Next()
	return itr, nil
}

// Domain implements iterator.
func (itr *iterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

// Valid implements iterator.
func (itr *iterator) Valid() bool {

	// Once invalid, forever invalid.
	if itr.isInvalid {
		return false
	}

	// If the current result has error, invalid.
	if itr.cur != nil && itr.cur.Error != nil {
		itr.isInvalid = true
		return false
	}

	// Valid
	return true
}

// Key implements iterator.
func (itr *iterator) Key() []byte {
	//  Panics if the iterator is invalid.
	itr.assertIsValid()
	return itr.cur.Key.Bytes()
}

// Value implements iterator.
func (itr *iterator) Value() []byte {
	// Panics if the iterator is invalid.
	itr.assertIsValid()
	return itr.cur.Value
}

// Next implements iterator.
func (itr *iterator) Next() {
	// If Valid returns false, this method will panic.
	itr.assertIsValid()
	if cur, ok := itr.res.NextSync(); !ok {
		itr.isInvalid = true
	} else {
		itr.cur = &cur
	}
}

// Error implements iterator.
func (itr *iterator) Error() error {
	if itr.cur == nil {
		return nil
	}
	return itr.cur.Error
}

// Close implements iterator.
func (itr *iterator) Close() error {
	return itr.res.Close()
}

func (itr iterator) assertIsValid() {
	if !itr.Valid() {
		panic("iterator is invalid")
	}
}
