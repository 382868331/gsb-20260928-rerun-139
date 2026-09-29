package coldstore

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"time"
)

// Segment layout:
//
//	magic    [4]byte  = "CLS1"
//	kind     uint8
//	rows     uint32
//	nullmap  bytes    ceil(rows/8), 1 = null
//	values   type-specific encoding, one fixed-width or length-prefixed
//	         entry per row regardless of nullness
//	crc      uint32   IEEE CRC-32 of every preceding byte
const (
	segmentMagic = "CLS1"
	headerLen    = 4 + 1 + 4
)

var crcTable = crc32.MakeTable(crc32.IEEE)

func encodeSegment(data ColumnData) ([]byte, error) {
	n := data.Len()
	if n < 0 || n > math.MaxUint32 {
		return nil, &SchemaError{Reason: "batch has invalid row count"}
	}
	nullBytes := (n + 7) / 8
	values, err := encodeValues(data, n)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, headerLen+nullBytes+len(values)+4)
	out = append(out, segmentMagic...)
	out = append(out, byte(data.Kind))
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(n))
	out = append(out, lenBuf[:]...)
	mapStart := len(out)
	out = append(out, make([]byte, nullBytes)...)
	for i := 0; i < n; i++ {
		if data.IsNull(i) {
			out[mapStart+i/8] |= 1 << uint(i%8)
		}
	}
	out = append(out, values...)
	binary.LittleEndian.PutUint32(lenBuf[:], crc32.Checksum(out, crcTable))
	out = append(out, lenBuf[:]...)
	return out, nil
}

func encodeValues(data ColumnData, n int) ([]byte, error) {
	switch data.Kind {
	case KindInt64:
		if len(data.Int64s) != n {
			return nil, &SchemaError{Reason: "int64 column length mismatch"}
		}
		buf := make([]byte, 0, n*8)
		for _, v := range data.Int64s {
			buf = binary.LittleEndian.AppendUint64(buf, uint64(v))
		}
		return buf, nil
	case KindFloat64:
		if len(data.Float64s) != n {
			return nil, &SchemaError{Reason: "float64 column length mismatch"}
		}
		buf := make([]byte, 0, n*8)
		for _, v := range data.Float64s {
			buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(v))
		}
		return buf, nil
	case KindBool:
		if len(data.Bools) != n {
			return nil, &SchemaError{Reason: "bool column length mismatch"}
		}
		buf := make([]byte, n)
		for i, v := range data.Bools {
			if v {
				buf[i] = 1
			}
		}
		return buf, nil
	case KindString:
		if len(data.Strings) != n {
			return nil, &SchemaError{Reason: "string column length mismatch"}
		}
		buf := make([]byte, 0, n*4)
		for _, v := range data.Strings {
			if len(v) > math.MaxUint32 {
				return nil, &SchemaError{Reason: "string value too long"}
			}
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(v)))
			buf = append(buf, v...)
		}
		return buf, nil
	case KindTimestamp:
		if len(data.Timestamps) != n {
			return nil, &SchemaError{Reason: "timestamp column length mismatch"}
		}
		buf := make([]byte, 0, n*8)
		for _, v := range data.Timestamps {
			buf = binary.LittleEndian.AppendUint64(buf, uint64(v.UnixNano()))
		}
		return buf, nil
	default:
		return nil, &SchemaError{Reason: fmt.Sprintf("unknown column kind %d", data.Kind)}
	}
}

// decodeSegment decodes one segment. It verifies the trailing CRC before
// touching the payload, so any truncated or flipped byte is reported.
func decodeSegment(r io.Reader, expected Kind) (ColumnData, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return ColumnData{}, err
	}
	if len(raw) < headerLen+4 {
		return ColumnData{}, fmt.Errorf("segment too short: %d bytes", len(raw))
	}
	body := raw[:len(raw)-4]
	want := binary.LittleEndian.Uint32(raw[len(raw)-4:])
	if got := crc32.Checksum(body, crcTable); got != want {
		return ColumnData{}, fmt.Errorf("checksum mismatch")
	}
	if string(body[0:4]) != segmentMagic {
		return ColumnData{}, fmt.Errorf("bad magic")
	}
	kind := Kind(body[4])
	if kind != expected {
		return ColumnData{}, fmt.Errorf("kind %s does not match schema %s", kind, expected)
	}
	n := int(binary.LittleEndian.Uint32(body[5:9]))
	nullBytes := (n + 7) / 8
	pos := headerLen
	if len(body) < pos+nullBytes {
		return ColumnData{}, fmt.Errorf("truncated null bitmap")
	}
	nullmap := body[pos : pos+nullBytes]
	pos += nullBytes
	nulls := make([]bool, n)
	for i := 0; i < n; i++ {
		nulls[i] = nullmap[i/8]&(1<<uint(i%8)) != 0
	}
	payload := body[pos:]
	data := ColumnData{Kind: kind, Null: nulls}
	switch kind {
	case KindInt64:
		vals, perr := decodeFixed(payload, n, 8)
		if perr != nil {
			return ColumnData{}, perr
		}
		data.Int64s = make([]int64, n)
		for i := range data.Int64s {
			data.Int64s[i] = int64(binary.LittleEndian.Uint64(vals[i*8:]))
		}
	case KindFloat64:
		vals, perr := decodeFixed(payload, n, 8)
		if perr != nil {
			return ColumnData{}, perr
		}
		data.Float64s = make([]float64, n)
		for i := range data.Float64s {
			data.Float64s[i] = math.Float64frombits(binary.LittleEndian.Uint64(vals[i*8:]))
		}
	case KindBool:
		vals, perr := decodeFixed(payload, n, 1)
		if perr != nil {
			return ColumnData{}, perr
		}
		data.Bools = make([]bool, n)
		for i := range data.Bools {
			if vals[i] > 1 {
				return ColumnData{}, fmt.Errorf("invalid bool byte %d", vals[i])
			}
			data.Bools[i] = vals[i] == 1
		}
	case KindString:
		vals, perr := decodeStrings(payload, n)
		if perr != nil {
			return ColumnData{}, perr
		}
		data.Strings = vals
	case KindTimestamp:
		vals, perr := decodeFixed(payload, n, 8)
		if perr != nil {
			return ColumnData{}, perr
		}
		data.Timestamps = make([]time.Time, n)
		for i := range data.Timestamps {
			data.Timestamps[i] = time.Unix(0, int64(binary.LittleEndian.Uint64(vals[i*8:]))).UTC()
		}
	default:
		return ColumnData{}, fmt.Errorf("unknown column kind %d", kind)
	}
	return data, nil
}

func decodeFixed(payload []byte, n, width int) ([]byte, error) {
	if len(payload) != n*width {
		return nil, fmt.Errorf("payload length %d, want %d", len(payload), n*width)
	}
	return payload, nil
}

func decodeStrings(payload []byte, n int) ([]string, error) {
	out := make([]string, n)
	pos := 0
	for i := 0; i < n; i++ {
		if pos+4 > len(payload) {
			return nil, fmt.Errorf("truncated string length at row %d", i)
		}
		size := int(binary.LittleEndian.Uint32(payload[pos:]))
		pos += 4
		if pos+size > len(payload) {
			return nil, fmt.Errorf("truncated string body at row %d", i)
		}
		out[i] = string(payload[pos : pos+size])
		pos += size
	}
	if pos != len(payload) {
		return nil, fmt.Errorf("%d trailing bytes after strings", len(payload)-pos)
	}
	return out, nil
}
