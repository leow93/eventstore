package storage

import "github.com/cespare/xxhash/v2"

// / hashStreamName creates a fast, zero-allocation uint64 hash.
func hashStreamName(name string) uint64 {
	return xxhash.Sum64String(name)
}
