// Copyright for portions of this source code are held by Tendermint as part of the tm-db project.
// All other copyright for this source code are held by [The BDWare Authors, 2021]. All rights reserved.
// Use of this source code is governed by MIT license that can be found in the LICENSE file.

// https://github.com/tendermint/tm-db/blob/9d720ea4c79af7e7066b3107ad1353e07750806c/goleveldb_test.go
package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb/opt"
	db "github.com/tendermint/tm-db"
)

// https://github.com/tendermint/tm-db/blob/9d720ea4c79af7e7066b3107ad1353e07750806c/goleveldb_test.go#L11
func TestGoLevelDBNewGoLevelDB(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	defer cleanupGoLevelDBDir("", name)

	// Test we can't open the db twice for writing
	wr1, err := db.NewGoLevelDB(name, "")
	require.Nil(t, err)
	_, err = db.NewGoLevelDB(name, "")
	require.NotNil(t, err)
	wr1.Close() // Close the db to release the lock

	// Test we can open the db twice for reading only
	ro1, err := db.NewGoLevelDBWithOpts(name, "", &opt.Options{ReadOnly: true})
	require.Nil(t, err)
	defer ro1.Close()
	ro2, err := db.NewGoLevelDBWithOpts(name, "", &opt.Options{ReadOnly: true})
	require.Nil(t, err)
	defer ro2.Close()
}

// https://github.com/tendermint/tm-db/blob/9d720ea4c79af7e7066b3107ad1353e07750806c/goleveldb_test.go#L31
func BenchmarkGoLevelDBRandomReadsWrites(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	db, err := db.NewGoLevelDB(name, "")
	if err != nil {
		b.Fatal(err)
	}
	defer cleanupGoLevelDBDir("", name)

	b.ResetTimer()
	benchmarkRandomReadsWrites(b, db)
}

func BenchmarkGoLevelDBRangeScans1M(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	db, err := db.NewGoLevelDB(name, "")
	if err != nil {
		b.Fatal(err)
	}
	defer cleanupGoLevelDBDir("", name)

	b.ResetTimer()
	benchmarkRangeScans(b, db, int64(1e6))
}

func BenchmarkGoLevelDBRangeScans10M(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	db, err := db.NewGoLevelDB(name, "")
	if err != nil {
		b.Fatal(err)
	}
	defer cleanupGoLevelDBDir("", name)

	b.ResetTimer()
	benchmarkRangeScans(b, db, int64(10e6))
}

// https://github.com/tendermint/tm-db/blob/9d720ea4c79af7e7066b3107ad1353e07750806c/backend_test.go#L29:6
func cleanupGoLevelDBDir(dir, name string) {
	err := os.RemoveAll(filepath.Join(dir, name) + ".db")
	if err != nil {
		//panic(err)
	}
}

func BenchmarkGoDatastoreWithGoLevelDBRandomReadsWrites(b *testing.B) {
	db, dir, err := newTempDB(fmt.Sprintf("test_%x", randStr(12)))
	if err != nil {
		b.Fatal(err)
	}
	defer cleanupDBDir(db, dir)

	b.ResetTimer()
	benchmarkRandomReadsWrites(b, db)
}

func BenchmarkGoDatastoreWithGoLevelDBRangeScans1M(b *testing.B) {
	db, dir, err := newTempDB(fmt.Sprintf("test_%x", randStr(12)))
	if err != nil {
		b.Fatal(err)
	}
	defer cleanupDBDir(db, dir)

	b.ResetTimer()
	benchmarkRangeScans(b, db, int64(1e6))
}

func BenchmarkGoDatastoreWithGoLevelDBRangeScans10M(b *testing.B) {
	db, dir, err := newTempDB(fmt.Sprintf("test_%x", randStr(12)))
	if err != nil {
		b.Fatal(err)
	}
	defer cleanupDBDir(db, dir)

	b.ResetTimer()
	benchmarkRangeScans(b, db, int64(10e6))
}
