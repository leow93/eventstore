package storage

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvent_Encode(t *testing.T) {
	evt := &Event{
		GlobalPosition: 42,
		StreamName:     "invoice-123",
		EventType:      "InvoiceIssued",
		Position:       5,
		Timestamp:      1670000000,
		Payload:        []byte(`{"amount": 100}`),
		Meta:           []byte(`{"correlation_id": "abc-123"}`),
	}

	data := evt.Encode()

	// Verify Total Length
	totalLen := binary.LittleEndian.Uint32(data[0:4])
	expectedTotalLen := uint32(len(data) - 4)
	if totalLen != expectedTotalLen {
		t.Fatalf("expected total length %d, got %d", expectedTotalLen, totalLen)
	}

	// Start the cursor exactly where TotalLength left off
	offset := 4

	// Verify Stream Name
	streamNameLen := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2
	streamName := string(data[offset : offset+int(streamNameLen)])
	if streamName != evt.StreamName {
		t.Fatalf("expected stream name %s, got %s", evt.StreamName, streamName)
	}
	offset += int(streamNameLen)

	// Verify Event Type
	eventTypeLen := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2
	eventType := string(data[offset : offset+int(eventTypeLen)])
	if eventType != evt.EventType {
		t.Fatalf("expected event type %s, got %s", evt.EventType, eventType)
	}
	offset += int(eventTypeLen)

	// Verify Position
	pos := binary.LittleEndian.Uint64(data[offset : offset+8])
	if pos != evt.Position {
		t.Fatalf("expected position %d, got %d", evt.Position, pos)
	}
	offset += 8

	// Verify GlobalPosition
	globalPos := binary.LittleEndian.Uint64(data[offset : offset+8])
	if globalPos != evt.GlobalPosition {
		t.Fatalf("expected global position %d, got %d", evt.GlobalPosition, globalPos)
	}
	offset += 8

	// Verify Timestamp
	ts := binary.LittleEndian.Uint64(data[offset : offset+8])
	if ts != evt.Timestamp {
		t.Fatalf("expected timestamp %d, got %d", evt.Timestamp, ts)
	}
	offset += 8

	// Verify Payload
	payloadLen := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4
	payload := data[offset : offset+int(payloadLen)]
	if string(payload) != string(evt.Payload) {
		t.Fatalf("expected payload %s, got %s", string(evt.Payload), string(payload))
	}
	offset += int(payloadLen)

	// Verify Meta
	metaLen := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4
	meta := data[offset : offset+int(metaLen)]
	if string(meta) != string(evt.Meta) {
		t.Fatalf("expected meta %s, got %s", string(evt.Meta), string(meta))
	}
}

func TestEvent_Decode(t *testing.T) {
	validEvt := &Event{
		StreamName: "test-stream",
		EventType:  "TestFired",
		Payload:    []byte(`{"data": 1}`),
		Meta:       []byte(`{"actor": "user"}`),
	}
	validData := validEvt.Encode()
	tests := []struct {
		name          string
		makeData      func() []byte
		expectedError error
		result        *Event
	}{
		{
			name: "data with length of 0 cannot be decoded",
			makeData: func() []byte {
				return []byte{}
			},
			expectedError: ErrDataTooShortForTotalLen,
			result:        nil,
		},
		{
			name: "data with length of 3 cannot be decoded",
			makeData: func() []byte {
				return validData[0:3]
			},
			expectedError: ErrDataTooShortForTotalLen,
			result:        nil,
		},
		{
			name: "data with length less than total length cannot be decoded",
			makeData: func() []byte {
				return validData[:len(validData)-1] // chop off the last byte
			},
			expectedError: ErrDataSliceSmallerThanTotalLen,
			result:        nil,
		},
		{
			name: "data with half the total",
			makeData: func() []byte {
				return validData[:len(validData)/2] // chop in half
			},
			expectedError: ErrDataSliceSmallerThanTotalLen,
			result:        nil,
		},
		{
			name: "decodes the event as expected",
			makeData: func() []byte {
				return validData
			},

			expectedError: nil,
			result:        validEvt,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ev, err := Decode(test.makeData())
			require.Equal(t, test.expectedError, err)
			assert.Equal(t, test.result, ev)
		})
	}
}
