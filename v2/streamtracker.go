package storage

import (
	"sync"
)

type trackerShard struct {
	mu      sync.RWMutex
	streams map[uint64]uint64
}

type StreamTracker struct {
	shards [shardCount]*trackerShard
}

func NewStreamTracker() *StreamTracker {
	st := &StreamTracker{}
	for i := range shardCount {
		st.shards[i] = &trackerShard{
			streams: make(map[uint64]uint64),
		}
	}
	return st
}

// GetHash remains the same
func (s *StreamTracker) GetHash(streamName string) uint64 {
	return hashStreamName(streamName)
}

// GetLock returns the specific RWMutex protecting the shard for this hash.
// The Writer MUST call this and Lock() it before calling Get/Update.
func (s *StreamTracker) GetLock(h uint64) *sync.RWMutex {
	return &s.shards[h%shardCount].mu
}

// GetCurrentVersion - Caller must hold the RLock or Lock from GetLock()
func (s *StreamTracker) GetCurrentVersion(h uint64) (uint64, bool) {
	shard := s.shards[h%shardCount]
	ver, ok := shard.streams[h]
	return ver, ok
}

// UpdateVersion - Caller must hold the Lock from GetLock()
func (s *StreamTracker) UpdateVersion(h uint64, version uint64) {
	shard := s.shards[h%shardCount]
	shard.streams[h] = version
}
