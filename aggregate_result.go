package columnar

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
)

const (
	aggregateResultMagic = "CAGR1\x00"
	aggregateResultEnd   = "CAGE1\x00"
)

type aggregateResultWriter struct {
	file *os.File
	hash interface {
		Write([]byte) (int, error)
		Sum32() uint32
	}
	schema Schema
	rows   uint64
}

func newAggregateResultWriter(path string, schema Schema) (*aggregateResultWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.Create(path + ".tmp")
	if err != nil {
		return nil, err
	}
	writer := &aggregateResultWriter{file: file, hash: crc32.NewIEEE(), schema: schema}
	if err := writer.write([]byte(aggregateResultMagic)); err != nil {
		return nil, writer.fail()
	}
	schemaData, err := json.Marshal(schema.Columns)
	if err != nil {
		return nil, writer.fail()
	}
	var length [4]byte
	binary.LittleEndian.PutUint32(length[:], uint32(len(schemaData)))
	if err := writer.write(length[:]); err != nil || writer.write(schemaData) != nil {
		return nil, writer.fail()
	}
	return writer, nil
}

func (writer *aggregateResultWriter) write(p []byte) error {
	if _, err := writer.file.Write(p); err != nil {
		return err
	}
	_, err := writer.hash.Write(p)
	return err
}

func (writer *aggregateResultWriter) writeRow(row []any) error {
	if len(row) != len(writer.schema.Columns) {
		return fmt.Errorf("aggregate result row has %d values, schema has %d", len(row), len(writer.schema.Columns))
	}
	for i, value := range row {
		if err := writer.writeResultValue(writer.schema.Columns[i].Type, value); err != nil {
			return err
		}
	}
	writer.rows++
	return nil
}

func (writer *aggregateResultWriter) writeResultValue(typ Type, value any) error {
	if value == nil {
		return writer.write([]byte{keyNull})
	}
	switch typ {
	case TypeInt64:
		typed, ok := value.(int64)
		if !ok {
			return valueTypeError(int(writer.rows), typ, value)
		}
		var encoded [9]byte
		encoded[0] = keyInt64
		binary.LittleEndian.PutUint64(encoded[1:], uint64(typed))
		return writer.write(encoded[:])
	case TypeFloat64:
		typed, ok := value.(float64)
		if !ok {
			return valueTypeError(int(writer.rows), typ, value)
		}
		var encoded [9]byte
		encoded[0] = keyFloat
		binary.LittleEndian.PutUint64(encoded[1:], math.Float64bits(typed))
		return writer.write(encoded[:])
	case TypeBool:
		typed, ok := value.(bool)
		if !ok {
			return valueTypeError(int(writer.rows), typ, value)
		}
		encoded := [2]byte{keyBool, 0}
		if typed {
			encoded[1] = 1
		}
		return writer.write(encoded[:])
	case TypeString:
		typed, ok := value.(string)
		if !ok {
			return valueTypeError(int(writer.rows), typ, value)
		}
		var header [5]byte
		header[0] = keyString
		binary.LittleEndian.PutUint32(header[1:], uint32(len(typed)))
		if err := writer.write(header[:]); err != nil {
			return err
		}
		return writer.write([]byte(typed))
	default:
		return fmt.Errorf("unsupported result type %s", typ)
	}
}

func (writer *aggregateResultWriter) finish() (int64, uint32, uint64, error) {
	var count [8]byte
	binary.LittleEndian.PutUint64(count[:], writer.rows)
	if err := writer.write(count[:]); err != nil {
		return 0, 0, 0, writer.fail()
	}
	checksum := writer.hash.Sum32()
	if err := binary.Write(writer.file, binary.LittleEndian, checksum); err != nil {
		return 0, 0, 0, writer.fail()
	}
	if _, err := io.WriteString(writer.file, aggregateResultEnd); err != nil {
		return 0, 0, 0, writer.fail()
	}
	if err := writer.file.Sync(); err != nil {
		return 0, 0, 0, writer.fail()
	}
	info, err := writer.file.Stat()
	if err != nil {
		return 0, 0, 0, writer.fail()
	}
	if err := writer.file.Close(); err != nil {
		return 0, 0, 0, err
	}
	tempPath := writer.file.Name()
	finalPath := stringsTrimSuffix(tempPath, ".tmp")
	if err := os.Chmod(tempPath, 0o644); err != nil {
		_ = os.Remove(tempPath)
		return 0, 0, 0, err
	}
	if err := os.Rename(tempPath, finalPath); err != nil {
		_ = os.Remove(tempPath)
		return 0, 0, 0, err
	}
	if err := syncDir(filepath.Dir(finalPath)); err != nil {
		return 0, 0, 0, err
	}
	return info.Size(), checksum, writer.rows, nil
}

func (writer *aggregateResultWriter) fail() error {
	name := writer.file.Name()
	_ = writer.file.Close()
	_ = os.Remove(name)
	return fmt.Errorf("aggregate result write failed")
}

func (writer *aggregateResultWriter) close() {
	name := writer.file.Name()
	_ = writer.file.Close()
	_ = os.Remove(name)
}

func stringsTrimSuffix(value string, suffix string) string {
	if len(value) >= len(suffix) && value[len(value)-len(suffix):] == suffix {
		return value[:len(value)-len(suffix)]
	}
	return value
}

func readPublishedAggregate(path string, size int64, checksum uint32) (PublishedAggregate, error) {
	info, err := os.Stat(path)
	if err != nil {
		return PublishedAggregate{}, err
	}
	if info.Size() != size {
		return PublishedAggregate{}, fmt.Errorf("published aggregate result size mismatch")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return PublishedAggregate{}, err
	}
	payloadEnd := len(data) - 4 - len(aggregateResultEnd)
	if len(data) < len(aggregateResultMagic)+4+8+4+len(aggregateResultEnd) {
		return PublishedAggregate{}, fmt.Errorf("aggregate result is truncated")
	}
	if crc32.ChecksumIEEE(data[:payloadEnd]) != checksum || string(data[payloadEnd+4:]) != aggregateResultEnd {
		return PublishedAggregate{}, fmt.Errorf("published aggregate result is corrupted")
	}
	if string(data[:len(aggregateResultMagic)]) != aggregateResultMagic {
		return PublishedAggregate{}, fmt.Errorf("invalid aggregate result magic")
	}
	position := len(aggregateResultMagic)
	schemaLength := int(binary.LittleEndian.Uint32(data[position:]))
	position += 4
	if position+schemaLength > payloadEnd-8 {
		return PublishedAggregate{}, fmt.Errorf("aggregate result schema is truncated")
	}
	var columns []Column
	if err := json.Unmarshal(data[position:position+schemaLength], &columns); err != nil {
		return PublishedAggregate{}, err
	}
	position += schemaLength
	schema, err := NewSchema(columns)
	if err != nil {
		return PublishedAggregate{}, err
	}
	rowEnd := payloadEnd - 8
	rows := make([][]any, 0)
	for position < rowEnd {
		row := make([]any, len(columns))
		for i, column := range columns {
			value, next, err := readResultValue(column, data, position, len(rows))
			if err != nil {
				return PublishedAggregate{}, err
			}
			row[i] = value
			position = next
		}
		rows = append(rows, row)
	}
	storedRows := binary.LittleEndian.Uint64(data[payloadEnd-8 : payloadEnd])
	if uint64(len(rows)) != storedRows || position != rowEnd {
		return PublishedAggregate{}, fmt.Errorf("aggregate result row count is corrupted")
	}
	return PublishedAggregate{Schema: schema, Rows: rows}, nil
}

func readResultValue(column Column, data []byte, position int, row int) (any, int, error) {
	if position >= len(data) {
		return nil, position, fmt.Errorf("aggregate result row %d is truncated", row)
	}
	tag := data[position]
	position++
	if tag == keyNull {
		return nil, position, nil
	}
	switch column.Type {
	case TypeInt64:
		if tag != keyInt64 || position+8 > len(data) {
			return nil, position, fmt.Errorf("aggregate result int64 is truncated")
		}
		return int64(binary.LittleEndian.Uint64(data[position:])), position + 8, nil
	case TypeFloat64:
		if tag != keyFloat || position+8 > len(data) {
			return nil, position, fmt.Errorf("aggregate result float64 is truncated")
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(data[position:])), position + 8, nil
	case TypeBool:
		if tag != keyBool || position >= len(data) || data[position] > 1 {
			return nil, position, fmt.Errorf("aggregate result bool is corrupted")
		}
		return data[position] == 1, position + 1, nil
	case TypeString:
		if tag != keyString || position+4 > len(data) {
			return nil, position, fmt.Errorf("aggregate result string length is truncated")
		}
		length := int(binary.LittleEndian.Uint32(data[position:]))
		position += 4
		if position+length > len(data) {
			return nil, position, fmt.Errorf("aggregate result string is truncated")
		}
		return string(data[position : position+length]), position + length, nil
	default:
		return nil, position, fmt.Errorf("unsupported result type %s", column.Type)
	}
}
