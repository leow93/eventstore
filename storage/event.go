package storage

import "encoding/binary"

// Event represents the logical structure of our event data.
type Event struct {
	StreamName     string
	EventType      string
	Position       uint64 // Position in the stream
	GlobalPosition uint64 // Position in the event store. Define by a monotonically increasing counter which will have gaps.
	Timestamp      uint64
	Payload        []byte
	Meta           []byte
}

// Encode serializes the event into a raw byte slice based on our binary layout.
// When reading raw bytes from a file, the system needs to know exactly where one field ends and the next begins.
// Since strings and payloads are variable in length, we must precede them with their length. Storing the Total Record Length at the very beginning of the record is also a pro-tip—it allows you to easily skip over records during sequential scans if you don't need to read their contents.
// Here is the binary layout for a single Event Record:
//
// TotalLength (uint32 - 4 bytes)
// StreamNameLength (uint16 - 2 bytes)
// StreamName (Variable bytes)
// EventTypeLength (uint16 - 2 bytes)
// EventType (Variable bytes)
// Position (uint64 - 8 bytes)
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

	// Calculate total length (excluding the 4 bytes for TotalLength itself)
	// 2 (StreamNameLen) + len(StreamName) + 2 (EventTypeLen) + len(EventType) + 8 (Position) + 8 (GlobalPosition) + 8 (Timestamp) + 4 (PayloadLen) + len(Payload) + 4 (MetaLen)+ len(Meta)
	totalLen := 2 + len(e.StreamName) + 2 + len(e.EventType) + 8 + 8 + 8 + 4 + len(e.Payload) + 4 + len(e.Meta)

	// Allocate exact buffer size: 4 (TotalLength) + totalLen
	buf := make([]byte, 4+totalLen)

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
