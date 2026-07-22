package eventstore

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// crc32cTable is the Castagnoli polynomial table used for per-record checksums.
// CRC-32C is hardware-accelerated on x86-64 (SSE4.2) and ARM64, so it is
// effectively free relative to the fsync that dominates a write.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// minRecordSize is the encoded size of a record whose variable-length fields are
// all empty: the fixed fields plus the 4-byte length prefix and 4-byte trailing
// CRC. Any declared TotalLength below this is an impossible/corrupt length prefix.
const minRecordSize = 44

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
// CRC (uint32 - 4 bytes) - CRC-32C over every preceding byte of the record
//
// The CRC is the last field so that a torn/short write fails the TotalLength-based
// length check (recoverable — truncate the tail) while a bit-flip inside an
// otherwise-complete record fails the CRC check (unrecoverable — fail loudly).
func (e *Event) Encode() []byte {
	streamNameLen := uint16(len(e.StreamName))
	eventTypeLen := uint16(len(e.EventType))
	payloadLen := uint32(len(e.Payload))
	metaLen := uint32(len(e.Meta))

	// Calculate total length (including the 4 bytes for TotalLength itself and the
	// trailing 4-byte CRC):
	// 4 (TotalLength) + 2 (StreamNameLen) + len(StreamName) + 2 (EventTypeLen) + len(EventType) +
	// 8 (Position) + 8 (GlobalPosition) + 8 (Timestamp) + 4 (PayloadLen) + len(Payload) + 4 (MetaLen) + len(Meta) + 4 (CRC)
	totalLen := 4 + 2 + len(e.StreamName) + 2 + len(e.EventType) + 8 + 8 + 8 + 4 + len(e.Payload) + 4 + len(e.Meta) + 4

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

	// 7. CRC-32C over every preceding byte (length prefix through meta).
	crc := crc32.Checksum(buf[:totalLen-4], crc32cTable)
	binary.LittleEndian.PutUint32(buf[totalLen-4:], crc)

	return buf
}

// TotalSize returns the encoded size of the event in bytes.
// There are 44 bytes of fixed-size fields (including the trailing 4-byte CRC),
// plus the variable-length fields.
func (e *Event) TotalSize() uint32 {
	return uint32(minRecordSize + len(e.StreamName) + len(e.EventType) + len(e.Payload) + len(e.Meta))
}

var (
	ErrDataTooShortForTotalLen      = errors.New("length of data too small for total length")
	ErrDataSliceSmallerThanTotalLen = errors.New("data slice smaller than encoded total length")
	ErrChecksumMismatch             = errors.New("event checksum mismatch")
)

func Decode(data []byte) (*Event, error) {
	if len(data) < 4 {
		return nil, ErrDataTooShortForTotalLen
	}
	totalLen := binary.LittleEndian.Uint32(data[0:4])
	if totalLen < minRecordSize {
		// An impossibly small declared length: a corrupt or partially-written
		// length prefix. Treat as a short/torn record rather than trusting it.
		return nil, ErrDataTooShortForTotalLen
	}
	if len(data) < int(totalLen) {
		return nil, ErrDataSliceSmallerThanTotalLen
	}

	// Verify the trailing CRC before trusting any field length. A short/torn
	// record is caught by the checks above; a bit-flip in a complete record is
	// caught here, so callers can tell a recoverable torn tail from real
	// corruption.
	storedCRC := binary.LittleEndian.Uint32(data[totalLen-4 : totalLen])
	if crc32.Checksum(data[:totalLen-4], crc32cTable) != storedCRC {
		return nil, ErrChecksumMismatch
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
