package eventstore

import (
	"encoding/binary"
	"errors"
)

// Event represents the logical structure of our event data.
type Event struct {
	StreamName     string
	EventType      string
	Position       uint64 // Position in the stream
	GlobalPosition uint64 // Position in the event store. Defined by a monotonically increasing counter.
	Timestamp      uint64
	Payload        []byte
	Meta           []byte
}

// Encode serializes the event into a raw byte slice based on our binary layout.
// When reading raw bytes from a file, the system needs to know exactly where one field ends and the next begins.
// Since strings and payloads are variable in length, we must precede them with their length. Storing the Total Record
// Length at the very beginning of the record also allows us to skip over records during sequential scans without
// reading their contents.
//
// Here is the binary layout for a single Event Record:
//
// TotalLength (uint32 - 4 bytes)
// StreamNameLength (uint16 - 2 bytes)
// StreamName (Variable bytes)
// EventTypeLength (uint16 - 2 bytes)
// EventType (Variable bytes)
// Position (uint64 - 8 bytes)
// GlobalPosition (uint64 - 8 bytes)
// Timestamp (uint64 - 8 bytes)
// PayloadLength (uint32 - 4 bytes)
// Payload (Variable bytes)
// MetaLength (uint32 - 4 bytes)
// Meta (Variable bytes)
func (e *Event) Encode() []byte {
	streamNameLen := uint16(len(e.StreamName))
	eventTypeLen := uint16(len(e.EventType))
	payloadLen := uint32(len(e.Payload))
	metaLen := uint32(len(e.Meta))

	// Calculate total length (including the 4 bytes for TotalLength itself):
	// 4 (TotalLength) + 2 (StreamNameLen) + len(StreamName) + 2 (EventTypeLen) + len(EventType) +
	// 8 (Position) + 8 (GlobalPosition) + 8 (Timestamp) + 4 (PayloadLen) + len(Payload) + 4 (MetaLen) + len(Meta)
	totalLen := 4 + 2 + len(e.StreamName) + 2 + len(e.EventType) + 8 + 8 + 8 + 4 + len(e.Payload) + 4 + len(e.Meta)

	buf := make([]byte, totalLen)

	offset := 0

	// 1. Total Length
	binary.LittleEndian.PutUint32(buf[offset:], uint32(totalLen))
	offset += 4

	// 2. Stream Name
	binary.LittleEndian.PutUint16(buf[offset:], streamNameLen)
	offset += 2
	copy(buf[offset:], e.StreamName)
	offset += int(streamNameLen)

	// 3. Event Type
	binary.LittleEndian.PutUint16(buf[offset:], eventTypeLen)
	offset += 2
	copy(buf[offset:], e.EventType)
	offset += int(eventTypeLen)

	// 4. Position, GlobalPosition & Timestamp
	binary.LittleEndian.PutUint64(buf[offset:], e.Position)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], e.GlobalPosition)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], e.Timestamp)
	offset += 8

	// 5. Payload
	binary.LittleEndian.PutUint32(buf[offset:], payloadLen)
	offset += 4
	copy(buf[offset:], e.Payload)
	offset += int(payloadLen)

	// 6. Meta
	binary.LittleEndian.PutUint32(buf[offset:], metaLen)
	offset += 4
	copy(buf[offset:], e.Meta)

	return buf
}

// TotalSize returns the encoded size of the event in bytes.
// There are 40 bytes of fixed-size fields, plus the variable-length fields.
func (e *Event) TotalSize() uint32 {
	return uint32(40 + len(e.StreamName) + len(e.EventType) + len(e.Payload) + len(e.Meta))
}

var (
	ErrDataTooShortForTotalLen      = errors.New("length of data too small for total length")
	ErrDataSliceSmallerThanTotalLen = errors.New("data slice smaller than encoded total length")
)

func Decode(data []byte) (*Event, error) {
	if len(data) < 4 {
		return nil, ErrDataTooShortForTotalLen
	}
	totalLen := binary.LittleEndian.Uint32(data[0:4])
	if len(data) < int(totalLen) {
		return nil, ErrDataSliceSmallerThanTotalLen
	}
	evt := &Event{}
	offset := 4

	streamNameLen := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2
	evt.StreamName = string(data[offset : offset+int(streamNameLen)])
	offset += int(streamNameLen)

	eventTypeLen := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2
	evt.EventType = string(data[offset : offset+int(eventTypeLen)])
	offset += int(eventTypeLen)

	evt.Position = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	evt.GlobalPosition = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	evt.Timestamp = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	payloadLen := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4
	evt.Payload = make([]byte, payloadLen)
	copy(evt.Payload, data[offset:offset+int(payloadLen)])
	offset += int(payloadLen)

	metaLen := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4
	evt.Meta = make([]byte, metaLen)
	copy(evt.Meta, data[offset:offset+int(metaLen)])

	return evt, nil
}
