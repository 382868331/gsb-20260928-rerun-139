package columnar

import (
	"encoding/binary"
	"math"
)

const (
	keyNull byte = iota
	keyInt64
	keyFloat
	keyBool
	keyString
)

func appendGroupKeyPart(buffer []byte, value any) []byte {
	switch typed := value.(type) {
	case nil:
		return append(buffer, keyNull)
	case int64:
		buffer = append(buffer, keyInt64)
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], uint64(typed)^(1<<63))
		return append(buffer, encoded[:]...)
	case float64:
		buffer = append(buffer, keyFloat)
		bits := math.Float64bits(typed)
		if bits&(1<<63) != 0 {
			bits = ^bits
		} else {
			bits |= 1 << 63
		}
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], bits)
		return append(buffer, encoded[:]...)
	case bool:
		buffer = append(buffer, keyBool)
		if typed {
			return append(buffer, 1)
		}
		return append(buffer, 0)
	case string:
		buffer = append(buffer, keyString)
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(typed)))
		buffer = append(buffer, length[:]...)
		return append(buffer, typed...)
	default:
		return append(buffer, keyNull)
	}
}
