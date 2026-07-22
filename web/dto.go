package main

import (
	"encoding/base64"
	"encoding/json"
	"unicode/utf8"

	"github.com/leow93/eventstore"
)

// eventDTO is the JSON shape the UI consumes for a single event. Payload and Meta
// are emitted as JSON values (see toJSONValue) so the browser can render them
// directly whether they hold JSON documents, plain text, or opaque bytes.
type eventDTO struct {
	StreamName     string          `json:"streamName"`
	EventType      string          `json:"eventType"`
	Position       uint64          `json:"position"`
	GlobalPosition uint64          `json:"globalPosition"`
	Timestamp      uint64          `json:"timestamp"`
	Payload        json.RawMessage `json:"payload"`
	Meta           json.RawMessage `json:"meta"`
}

func toDTO(e *eventstore.Event) eventDTO {
	return eventDTO{
		StreamName:     e.StreamName,
		EventType:      e.EventType,
		Position:       e.Position,
		GlobalPosition: e.GlobalPosition,
		Timestamp:      e.Timestamp,
		Payload:        toJSONValue(e.Payload),
		Meta:           toJSONValue(e.Meta),
	}
}

// toJSONValue turns a raw byte field into a valid JSON value:
//   - empty            -> null
//   - valid JSON       -> the document itself (objects/arrays render structured)
//   - valid UTF-8 text -> a JSON string
//   - anything else    -> a base64 JSON string
//
// It always returns something json.Marshal-safe so the API response never fails
// on user-supplied payloads.
func toJSONValue(b []byte) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("null")
	}
	if json.Valid(b) {
		return json.RawMessage(b)
	}
	if utf8.Valid(b) {
		encoded, _ := json.Marshal(string(b))
		return encoded
	}
	encoded, _ := json.Marshal(base64.StdEncoding.EncodeToString(b))
	return encoded
}
