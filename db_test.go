// Copyright for portions of this source code are held by Tendermint as part of the tm-db project.
// All other copyright for this source code are held by [The BDWare Authors, 2021]. All rights reserved.
// Use of this source code is governed by MIT license that can be found in the LICENSE file.

// https://github.com/tendermint/tm-db/blob/3157a928986298875ca48e6d5f77132a32dfb1f0/db_test.go
package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const commonTestDir string = "db_common_test"

// https://github.com/tendermint/tm-db/blob/3157a928986298875ca48e6d5f77132a32dfb1f0/db_test.go#L11
func TestDBIteratorSingleKey(t *testing.T) {
	db, dir, err := newTempDB(commonTestDir)
	require.NoError(t, err)
	defer cleanupDBDir(db, dir)

	err = db.SetSync(bz("1"), bz("value_1"))
	assert.NoError(t, err)
	itr, err := db.Iterator(nil, nil)
	assert.NoError(t, err)

	checkValid(t, itr, true)
	checkNext(t, itr, false)
	checkValid(t, itr, false)
	checkNextPanics(t, itr)

	// Once invalid...
	checkInvalid(t, itr)
}

// https://github.com/tendermint/tm-db/blob/3157a928986298875ca48e6d5f77132a32dfb1f0/db_test.go#L33
func TestDBIteratorTwoKeys(t *testing.T) {
	db, dir, err := newTempDB(commonTestDir)
	require.NoError(t, err)
	defer cleanupDBDir(db, dir)

	err = db.SetSync(bz("1"), bz("value_1"))
	assert.NoError(t, err)

	err = db.SetSync(bz("2"), bz("value_1"))
	assert.NoError(t, err)

	{ // Fail by calling Next too much
		itr, err := db.Iterator(nil, nil)
		assert.NoError(t, err)
		checkValid(t, itr, true)

		checkNext(t, itr, true)
		checkValid(t, itr, true)

		checkNext(t, itr, false)
		checkValid(t, itr, false)

		checkNextPanics(t, itr)

		// Once invalid...
		checkInvalid(t, itr)
	}
}

// https://github.com/tendermint/tm-db/blob/3157a928986298875ca48e6d5f77132a32dfb1f0/db_test.go#L65
func TestDBIteratorMany(t *testing.T) {
	db, dir, err := newTempDB(commonTestDir)
	require.NoError(t, err)
	defer cleanupDBDir(db, dir)

	keys := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		keys[i] = []byte{byte(i)}
	}

	value := []byte{5}
	for _, k := range keys {
		err := db.Set(k, value)
		assert.NoError(t, err)
	}

	itr, err := db.Iterator(nil, nil)
	assert.NoError(t, err)

	defer itr.Close()
	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value = itr.Value()
		value1, err := db.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, value1, value)
	}
}

// https://github.com/tendermint/tm-db/blob/3157a928986298875ca48e6d5f77132a32dfb1f0/db_test.go#L97
func TestDBIteratorEmpty(t *testing.T) {
	db, dir, err := newTempDB(commonTestDir)
	require.NoError(t, err)
	defer cleanupDBDir(db, dir)

	itr, err := db.Iterator(nil, nil)
	assert.NoError(t, err)

	checkInvalid(t, itr)
}

// https://github.com/tendermint/tm-db/blob/3157a928986298875ca48e6d5f77132a32dfb1f0/db_test.go#L111
func TestDBIteratorEmptyBeginAfter(t *testing.T) {
	db, dir, err := newTempDB(commonTestDir)
	require.NoError(t, err)
	defer cleanupDBDir(db, dir)

	itr, err := db.Iterator(bz("1"), nil)
	assert.NoError(t, err)

	checkInvalid(t, itr)
}

// https://github.com/tendermint/tm-db/blob/3157a928986298875ca48e6d5f77132a32dfb1f0/db_test.go#L125
func TestDBIteratorNonemptyBeginAfter(t *testing.T) {
	db, dir, err := newTempDB(commonTestDir)
	require.NoError(t, err)
	defer cleanupDBDir(db, dir)

	err = db.SetSync(bz("1"), bz("value_1"))
	assert.NoError(t, err)
	itr, err := db.Iterator(bz("2"), nil)
	assert.NoError(t, err)

	checkInvalid(t, itr)
}
