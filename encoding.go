package columnar

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
)

func crc32Checksum(data []byte) uint32 { return crc32.ChecksumIEEE(data) }

const (
	columnMagic = "CCOL1\x00"
	columnEnd   = "CCOE1\x00"
)

func bitmapByte(row int) int { return row / 8 }

func bitmapBit(row int) byte { return 1 << uint(row%8) }

func encodeColumn(typ Type, values []any) ([]byte, error) {
	w := newChecksumWriter()
	if _, err := io.WriteString(w, columnMagic); err != nil {
		return nil, err
	}
	if err := w.WriteByte(byte(typ)); err != nil {
		return nil, err
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(len(values))); err != nil {
		return nil, err
	}
	bitmap := make([]byte, (len(values)+7)/8)
	for i, value := range values {
		if value != nil {
			bitmap[bitmapByte(i)] |= bitmapBit(i)
		}
	}
	if _, err := w.Write(bitmap); err != nil {
		return nil, err
	}
	for i, value := range values {
		if value == nil {
			continue
		}
		if err := writeValue(w, typ, value, i); err != nil {
			return nil, err
		}
	}
	checksum := w.Sum32()
	if err := binary.Write(w.buffer, binary.LittleEndian, checksum); err != nil {
		return nil, err
	}
	if _, err := io.WriteString(w.buffer, columnEnd); err != nil {
		return nil, err
	}
	return w.buffer.Bytes(), nil
}

type checksumWriter struct {
	buffer *sizedBuffer
	hash   hash.Hash32
}

func newChecksumWriter() *checksumWriter {
	return &checksumWriter{buffer: newSizedBuffer(), hash: crc32.NewIEEE()}
}

func (w *checksumWriter) Write(p []byte) (int, error) {
	n, err := w.buffer.Write(p)
	if n > 0 {
		if _, hashErr := w.hash.Write(p[:n]); hashErr != nil {
			return n, hashErr
		}
	}
	return n, err
}

func (w *checksumWriter) WriteByte(value byte) error {
	_, err := w.Write([]byte{value})
	return err
}

func (w *checksumWriter) Sum32() uint32 { return w.hash.Sum32() }

func writeValue(w io.Writer, typ Type, value any, row int) error {
	switch typ {
	case TypeInt64:
		typed, ok := value.(int64)
		if !ok {
			return valueTypeError(row, typ, value)
		}
		return binary.Write(w, binary.LittleEndian, typed)
	case TypeFloat64:
		typed, ok := value.(float64)
		if !ok {
			return valueTypeError(row, typ, value)
		}
		return binary.Write(w, binary.LittleEndian, typed)
	case TypeBool:
		typed, ok := value.(bool)
		if !ok {
			return valueTypeError(row, typ, value)
		}
		var encoded byte
		if typed {
			encoded = 1
		}
		_, err := w.Write([]byte{encoded})
		return err
	case TypeString:
		typed, ok := value.(string)
		if !ok {
			return valueTypeError(row, typ, value)
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(len(typed))); err != nil {
			return err
		}
		_, err := io.WriteString(w, typed)
		return err
	default:
		return fmt.Errorf("unsupported column type %s", typ)
	}
}

func valueTypeError(row int, expected Type, value any) error {
	return fmt.Errorf("row %d: expected %s, got %T", row, expected, value)
}

func decodeColumn(typ Type, data []byte) ([]any, error) {
	if len(data) < len(columnMagic)+1+8+len(columnEnd)+4 {
		return nil, fmt.Errorf("column file is truncated")
	}
	offset := 0
	if string(data[:len(columnMagic)]) != columnMagic {
		return nil, fmt.Errorf("invalid column magic")
	}
	offset += len(columnMagic)
	if Type(data[offset]) != typ {
		return nil, fmt.Errorf("column type mismatch: file has %s, expected %s", Type(data[offset]), typ)
	}
	offset++
	rowCount := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	bitmapLength := int((rowCount + 7) / 8)
	if rowCount > uint64(maxInt()/8) || len(data) < offset+bitmapLength+4+len(columnEnd) {
		return nil, fmt.Errorf("column file is truncated")
	}
	payloadEnd := len(data) - 4 - len(columnEnd)
	storedChecksum := binary.LittleEndian.Uint32(data[payloadEnd : payloadEnd+4])
	actualChecksum := crc32.ChecksumIEEE(data[:payloadEnd])
	if storedChecksum != actualChecksum {
		return nil, fmt.Errorf("column checksum mismatch: file is corrupted")
	}
	if string(data[payloadEnd+4:]) != columnEnd {
		return nil, fmt.Errorf("invalid column trailer")
	}
	bitmap := data[offset : offset+bitmapLength]
	position := offset + bitmapLength
	values := make([]any, rowCount)
	for i := uint64(0); i < rowCount; i++ {
		if bitmap[bitmapByte(int(i))]&bitmapBit(int(i)) == 0 {
			continue
		}
		value, next, err := readValue(typ, data, position, int(i))
		if err != nil {
			return nil, err
		}
		if next > payloadEnd {
			return nil, fmt.Errorf("column data is truncated at row %d", i)
		}
		values[i] = value
		position = next
	}
	if position != payloadEnd {
		return nil, fmt.Errorf("column has %d unexpected trailing data bytes", payloadEnd-position)
	}
	return values, nil
}

func readValue(typ Type, data []byte, position int, row int) (any, int, error) {
	switch typ {
	case TypeInt64:
		if position+8 > len(data) {
			return nil, position, fmt.Errorf("row %d is truncated", row)
		}
		value := int64(binary.LittleEndian.Uint64(data[position:]))
		return value, position + 8, nil
	case TypeFloat64:
		if position+8 > len(data) {
			return nil, position, fmt.Errorf("row %d is truncated", row)
		}
		bits := binary.LittleEndian.Uint64(data[position:])
		return float64FromBits(bits), position + 8, nil
	case TypeBool:
		if position >= len(data) {
			return nil, position, fmt.Errorf("row %d is truncated", row)
		}
		switch data[position] {
		case 0:
			return false, position + 1, nil
		case 1:
			return true, position + 1, nil
		default:
			return nil, position, fmt.Errorf("row %d has invalid boolean value %d", row, data[position])
		}
	case TypeString:
		if position+4 > len(data) {
			return nil, position, fmt.Errorf("row %d string length is truncated", row)
		}
		length := int(binary.LittleEndian.Uint32(data[position:]))
		position += 4
		if position+length > len(data) {
			return nil, position, fmt.Errorf("row %d string value is truncated", row)
		}
		return string(data[position : position+length]), position + length, nil
	default:
		return nil, position, fmt.Errorf("unsupported column type %s", typ)
	}
}

type sizedBuffer struct {
	data []byte
}

func newSizedBuffer() *sizedBuffer { return &sizedBuffer{data: make([]byte, 0, 256)} }

func (b *sizedBuffer) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *sizedBuffer) Bytes() []byte { return b.data }
