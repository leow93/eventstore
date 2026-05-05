package storage

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

func GetCategory(streamName string) (string, error) {
	parts := strings.SplitN(streamName, "-", 2)
	if len(parts) > 0 && parts[0] != "" {
		return parts[0], nil
	}
	return "", ErrStreamHasNoCategory{streamName}
}
