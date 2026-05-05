package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetCategory(t *testing.T) {
	tests := []struct {
		streamName string
		expected   string
		err        error
	}{
		{"user-123", "user", nil},
		{"inventory-abc-456", "inventory", nil},
		{"global", "global", nil},
		{"default-", "default", nil},
		{"-badprefix", "", ErrStreamHasNoCategory{"-badprefix"}},
	}

	for _, tc := range tests {
		t.Run(tc.streamName, func(t *testing.T) {
			c, err := GetCategory(tc.streamName)
			require.Equal(t, tc.err, err)
			assert.Equal(t, tc.expected, c)
		})
	}
}
