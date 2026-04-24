package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_StreamTracker(t *testing.T) {
	t.Run("inital version is 0, ok false", func(t *testing.T) {
		st := NewStreamTracker()

		v, ok := st.GetCurrentVersion(34)
		assert.Equal(t, uint64(0), v)
		assert.False(t, ok)
	})

	t.Run("returns version", func(t *testing.T) {
		st := NewStreamTracker()
		stream := "mystream"
		h := st.GetHash(stream)
		st.UpdateVersion(h, 12)

		v, ok := st.GetCurrentVersion(h)
		assert.Equal(t, uint64(12), v)
		assert.True(t, ok)
	})
}
