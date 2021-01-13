// Copyright for portions of this source code are held by Tendermint as part of the tm-db project.
// All other copyright for this source code are held by [The BDWare Authors, 2021]. All rights reserved.
// Use of this source code is governed by MIT license that can be found in the LICENSE file.

// https://github.com/tendermint/tm-db/blob/3157a928986298875ca48e6d5f77132a32dfb1f0/test_helpers.go
package db

import "math/rand"

const (
	strChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz" // 62 characters
)

// For testing convenience.
func bz(s string) []byte {
	return []byte(s)
}

// Str constructs a random alphanumeric string of given length.
func randStr(length int) string {
	chars := []byte{}
MAIN_LOOP:
	for {
		val := rand.Int63() // nolint:gosec // G404: Use of weak random number generator
		for i := 0; i < 10; i++ {
			v := int(val & 0x3f) // rightmost 6 bits
			if v >= 62 {         // only 62 characters in strChars
				val >>= 6
				continue
			} else {
				chars = append(chars, strChars[v])
				if len(chars) == length {
					break MAIN_LOOP
				}
				val >>= 6
			}
		}
	}

	return string(chars)
}
