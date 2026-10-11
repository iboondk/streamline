// Package wire decodes the Confluent Schema Registry wire format:
// one magic byte (0), a 4-byte big-endian schema id, then the Avro payload.
package wire

import (
	"encoding/binary"
	"errors"
)

// ErrBadMagic is returned when a frame is shorter than the 5-byte header or
// does not start with the magic byte 0.
var ErrBadMagic = errors.New("wire: frame too short or bad magic byte")

// Parse splits a framed message into its schema id and Avro payload.
// The returned payload aliases b.
func Parse(b []byte) (schemaID uint32, payload []byte, err error) {
	if len(b) < 5 || b[0] != 0 {
		return 0, nil, ErrBadMagic
	}
	return binary.BigEndian.Uint32(b[1:5]), b[5:], nil
}
