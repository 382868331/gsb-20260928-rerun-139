package columnar

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
)

const (
	spillMagic   = "CSPL1\x00"
	spillEnd     = "CSPE1\x00"
	frameCRCSeed = uint32(0x434f4c00)
)

// spillFrame is one sorted group record inside a spilled run.
type spillFrame struct {
	key    string
	groups []any
	accs   []aggAccumulator
}

type runWriter struct {
	file       *os.File
	frameCount uint32
}

func createRunWriter(path string) (*runWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	if _, err := io.WriteString(file, spillMagic); err != nil {
		file.Close()
		return nil, err
	}
	return &runWriter{file: file}, nil
}

func (writer *runWriter) writeFrame(frame spillFrame, _ []namedAggregate) error {
	payload := encodeFramePayload(frame)
	header := make([]byte, 4)
	binary.LittleEndian.PutUint32(header, uint32(len(payload)))
	if _, err := writer.file.Write(header); err != nil {
		return err
	}
	if _, err := writer.file.Write(payload); err != nil {
		return err
	}
	checksum := spillChecksum(payload)
	binary.LittleEndian.PutUint32(header, checksum)
	if _, err := writer.file.Write(header); err != nil {
		return err
	}
	writer.frameCount++
	return nil
}

func (writer *runWriter) close() error {
	trailer := make([]byte, 0, 4+len(spillEnd))
	countBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBytes, writer.frameCount)
	trailer = append(trailer, countBytes...)
	trailer = append(trailer, spillEnd...)
	if _, err := writer.file.Write(trailer); err != nil {
		return err
	}
	if err := writer.file.Sync(); err != nil {
		return err
	}
	return writer.file.Close()
}

func encodeFramePayload(frame spillFrame) []byte {
	payload := make([]byte, 0, 64)
	keyBytes := []byte(frame.key)
	length := make([]byte, 4)
	binary.LittleEndian.PutUint32(length, uint32(len(keyBytes)))
	payload = append(payload, length...)
	payload = append(payload, keyBytes...)
	for i := range frame.accs {
		acc := frame.accs[i]
		payload = appendInt64(payload, acc.countRows)
		payload = appendInt64(payload, acc.countNonNull)
		payload = appendInt64(payload, acc.intSum)
		payload = appendFloat64(payload, acc.floatSum)
		var hasFloat byte
		if acc.hasFloat {
			hasFloat = 1
		}
		payload = append(payload, hasFloat)
		payload = appendCodecValue(payload, acc.minValue)
		payload = appendCodecValue(payload, acc.maxValue)
	}
	return payload
}

func appendInt64(buffer []byte, value int64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], uint64(value))
	return append(buffer, encoded[:]...)
}

func appendFloat64(buffer []byte, value float64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], math.Float64bits(value))
	return append(buffer, encoded[:]...)
}

func appendCodecValue(buffer []byte, value any) []byte {
	switch typed := value.(type) {
	case nil:
		return append(buffer, keyNull)
	case int64:
		buffer = append(buffer, keyInt64)
		return appendInt64(buffer, typed)
	case float64:
		buffer = append(buffer, keyFloat)
		return appendFloat64(buffer, typed)
	case bool:
		buffer = append(buffer, keyBool)
		if typed {
			return append(buffer, 1)
		}
		return append(buffer, 0)
	case string:
		buffer = append(buffer, keyString)
		var encoded [4]byte
		binary.LittleEndian.PutUint32(encoded[:], uint32(len(typed)))
		buffer = append(buffer, encoded[:]...)
		return append(buffer, typed...)
	default:
		return append(buffer, keyNull)
	}
}

func spillChecksum(payload []byte) uint32 {
	checksum := crc32Checksum(payload)
	return checksum ^ frameCRCSeed
}

type runReader struct {
	file      *os.File
	remaining uint32
}

func openRunReader(path string, wantFrames uint32) (*runReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	size := info.Size()
	trailerLen := 4 + len(spillEnd)
	if size < int64(len(spillMagic)+trailerLen) {
		file.Close()
		return nil, fmt.Errorf("spill run %s is truncated", path)
	}
	trailer := make([]byte, trailerLen)
	if _, err := file.ReadAt(trailer, size-int64(trailerLen)); err != nil {
		file.Close()
		return nil, err
	}
	if string(trailer[4:]) != spillEnd {
		file.Close()
		return nil, fmt.Errorf("spill run %s has an invalid trailer", path)
	}
	header := make([]byte, len(spillMagic))
	if _, err := file.ReadAt(header, 0); err != nil {
		file.Close()
		return nil, err
	}
	if string(header) != spillMagic {
		file.Close()
		return nil, fmt.Errorf("spill run %s has an invalid header", path)
	}
	frameCount := binary.LittleEndian.Uint32(trailer[:4])
	if frameCount != wantFrames {
		file.Close()
		return nil, fmt.Errorf("spill run %s declares %d frames, checkpoint records %d", path, frameCount, wantFrames)
	}
	if _, err := file.Seek(int64(len(spillMagic)), io.SeekStart); err != nil {
		file.Close()
		return nil, err
	}
	return &runReader{
		file:      file,
		remaining: frameCount,
	}, nil
}

func (reader *runReader) close() error { return reader.file.Close() }

func (reader *runReader) Close() error { return reader.close() }

func (reader *runReader) next(aggs []namedAggregate) (spillFrame, error) {
	if reader.remaining == 0 {
		return spillFrame{}, io.EOF
	}
	header := make([]byte, 4)
	if _, err := io.ReadFull(reader.file, header); err != nil {
		return spillFrame{}, fmt.Errorf("spill run frame header is truncated: %w", err)
	}
	payloadLength := int(binary.LittleEndian.Uint32(header))
	if payloadLength == 0 {
		return spillFrame{}, fmt.Errorf("spill run frame is truncated")
	}
	payload := make([]byte, payloadLength)
	if _, err := io.ReadFull(reader.file, payload); err != nil {
		return spillFrame{}, fmt.Errorf("spill run frame payload is truncated: %w", err)
	}
	checksumBytes := make([]byte, 4)
	if _, err := io.ReadFull(reader.file, checksumBytes); err != nil {
		return spillFrame{}, fmt.Errorf("spill run frame checksum is truncated: %w", err)
	}
	checksum := binary.LittleEndian.Uint32(checksumBytes)
	if spillChecksum(payload) != checksum {
		return spillFrame{}, fmt.Errorf("spill run frame checksum mismatch: temporary file is corrupted")
	}
	frame, err := decodeFramePayload(payload, aggs)
	if err != nil {
		return spillFrame{}, err
	}
	reader.remaining--
	return frame, nil
}

func decodeFramePayload(payload []byte, aggs []namedAggregate) (spillFrame, error) {
	position := 0
	if len(payload) < 4 {
		return spillFrame{}, fmt.Errorf("spill frame is truncated")
	}
	keyLength := int(binary.LittleEndian.Uint32(payload))
	position += 4
	if position+keyLength > len(payload) {
		return spillFrame{}, fmt.Errorf("spill frame key is truncated")
	}
	frame := spillFrame{
		key:  string(payload[position : position+keyLength]),
		accs: make([]aggAccumulator, len(aggs)),
	}
	position += keyLength
	for i := range aggs {
		acc, next, err := decodeAccumulator(payload, position)
		if err != nil {
			return spillFrame{}, err
		}
		frame.accs[i] = acc
		position = next
	}
	if position != len(payload) {
		return spillFrame{}, fmt.Errorf("spill frame has %d unexpected trailing bytes", len(payload)-position)
	}
	return frame, nil
}

func decodeAccumulator(buffer []byte, position int) (aggAccumulator, int, error) {
	acc := aggAccumulator{}
	if position+33 > len(buffer) {
		return acc, position, fmt.Errorf("spill frame accumulator is truncated")
	}
	acc.countRows = int64(binary.LittleEndian.Uint64(buffer[position:]))
	acc.countNonNull = int64(binary.LittleEndian.Uint64(buffer[position+8:]))
	acc.intSum = int64(binary.LittleEndian.Uint64(buffer[position+16:]))
	acc.floatSum = math.Float64frombits(binary.LittleEndian.Uint64(buffer[position+24:]))
	acc.hasFloat = buffer[position+32] != 0
	position += 33
	var err error
	acc.minValue, position, err = readCodecValue(buffer, position)
	if err != nil {
		return acc, position, err
	}
	acc.maxValue, position, err = readCodecValue(buffer, position)
	if err != nil {
		return acc, position, err
	}
	acc.hasMinMax = acc.minValue != nil || acc.maxValue != nil
	return acc, position, nil
}

func readCodecValue(buffer []byte, position int) (any, int, error) {
	if position >= len(buffer) {
		return nil, position, fmt.Errorf("spill frame value is truncated")
	}
	tag := buffer[position]
	position++
	switch tag {
	case keyNull:
		return nil, position, nil
	case keyInt64:
		if position+8 > len(buffer) {
			return nil, position, fmt.Errorf("spill frame int64 value is truncated")
		}
		value := int64(binary.LittleEndian.Uint64(buffer[position:]))
		return value, position + 8, nil
	case keyFloat:
		if position+8 > len(buffer) {
			return nil, position, fmt.Errorf("spill frame float64 value is truncated")
		}
		value := math.Float64frombits(binary.LittleEndian.Uint64(buffer[position:]))
		return value, position + 8, nil
	case keyBool:
		if position >= len(buffer) {
			return nil, position, fmt.Errorf("spill frame bool value is truncated")
		}
		if buffer[position] > 1 {
			return nil, position, fmt.Errorf("spill frame has invalid boolean value %d", buffer[position])
		}
		return buffer[position] == 1, position + 1, nil
	case keyString:
		if position+4 > len(buffer) {
			return nil, position, fmt.Errorf("spill frame string length is truncated")
		}
		length := int(binary.LittleEndian.Uint32(buffer[position:]))
		position += 4
		if position+length > len(buffer) {
			return nil, position, fmt.Errorf("spill frame string value is truncated")
		}
		return string(buffer[position : position+length]), position + length, nil
	default:
		return nil, position, fmt.Errorf("spill frame has invalid value type %d", tag)
	}
}

func decodeGroupKey(key string, groupSchema Schema) ([]any, error) {
	values := make([]any, 0, len(groupSchema.Columns))
	buffer := []byte(key)
	position := 0
	for columnIndex := 0; columnIndex < len(groupSchema.Columns); columnIndex++ {
		if position >= len(buffer) {
			return nil, fmt.Errorf("group key %d is truncated", columnIndex)
		}
		tag := buffer[position]
		position++
		switch tag {
		case keyNull:
			values = append(values, nil)
		case keyInt64:
			if position+8 > len(buffer) {
				return nil, fmt.Errorf("group key int64 is truncated")
			}
			bits := binary.BigEndian.Uint64(buffer[position:]) ^ (1 << 63)
			values = append(values, int64(bits))
			position += 8
		case keyFloat:
			if position+8 > len(buffer) {
				return nil, fmt.Errorf("group key float64 is truncated")
			}
			bits := binary.BigEndian.Uint64(buffer[position:])
			if bits&(1<<63) != 0 {
				bits ^= 1 << 63
			} else {
				bits = ^bits
			}
			values = append(values, math.Float64frombits(bits))
			position += 8
		case keyBool:
			if position >= len(buffer) {
				return nil, fmt.Errorf("group key bool is truncated")
			}
			values = append(values, buffer[position] == 1)
			position++
		case keyString:
			if position+4 > len(buffer) {
				return nil, fmt.Errorf("group key string length is truncated")
			}
			length := int(binary.BigEndian.Uint32(buffer[position:]))
			position += 4
			if position+length > len(buffer) {
				return nil, fmt.Errorf("group key string value is truncated")
			}
			values = append(values, string(buffer[position:position+length]))
			position += length
		default:
			return nil, fmt.Errorf("group key has invalid type tag %d", tag)
		}
	}
	if position != len(buffer) {
		return nil, fmt.Errorf("group key has unexpected trailing bytes")
	}
	return values, nil
}
