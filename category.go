package eventstore

import (
	"fmt"
	"strings"
)

type ErrStreamHasNoCategory struct {
	stream string
}

func (e ErrStreamHasNoCategory) Error() string {
	return fmt.Sprintf("stream %s has no category", e.stream)
}

// GetCategory extracts the category from a stream name. The category is the
// portion before the first dash; a stream name with no dash is its own category.
// A stream name that begins with a dash (empty category) is invalid.
func GetCategory(streamName string) (string, error) {
	parts := strings.SplitN(streamName, "-", 2)
	if len(parts) > 0 && parts[0] != "" {
		return parts[0], nil
	}
	return "", ErrStreamHasNoCategory{streamName}
}
