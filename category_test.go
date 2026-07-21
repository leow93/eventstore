package eventstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetCategory(t *testing.T) {
	cases := []struct {
		name        string
		streamName  string
		expected    string
		expectedErr error
	}{
		{
			name:        "simple category and id",
			streamName:  "user-123",
			expected:    "user",
			expectedErr: nil,
		},
		{
			name:        "splits on the first dash only",
			streamName:  "inventory-abc-456",
			expected:    "inventory",
			expectedErr: nil,
		},
		{
			name:        "stream name with no dash is its own category",
			streamName:  "global",
			expected:    "global",
			expectedErr: nil,
		},
		{
			name:        "trailing dash with empty id",
			streamName:  "default-",
			expected:    "default",
			expectedErr: nil,
		},
		{
			name:        "leading dash has no category",
			streamName:  "-badprefix",
			expected:    "",
			expectedErr: ErrStreamHasNoCategory{stream: "-badprefix"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := GetCategory(tc.streamName)

			if tc.expectedErr != nil {
				require.Error(t, err)
				assert.Equal(t, tc.expectedErr, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.expected, actual)
		})
	}
}
