package columnar

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"hash/crc32"
)

const aggregateResultEnd = "CAGE1\x00"

type persistedResult struct {
	Columns []Column
	Rows    [][]any
}

func encodeAggregateRows(plan aggregationPlan, rows [][]any) ([]byte, error) {
	var body bytes.Buffer
	body.WriteString(aggregateResultMagic)
	schema := plan.outputSchema
	metadata, err := json.Marshal(schema.Columns)
	if err != nil {
		return nil, err
	}
	var length [4]byte
	putUint32(length[:], uint32(len(metadata)))
	body.Write(length[:])
	body.Write(metadata)
	if err := gob.NewEncoder(&body).Encode(persistedResult{Columns: schema.Columns, Rows: rows}); err != nil {
		return nil, err
	}
	data := body.Bytes()
	checksum := crc32.ChecksumIEEE(data)
	var checksumBytes [4]byte
	putUint32(checksumBytes[:], checksum)
	data = append(data, checksumBytes[:]...)
	data = append(data, aggregateResultEnd...)
	return data, nil
}

func decodeAggregateRows(data []byte) (AggregateResult, error) {
	if len(data) < len(aggregateResultMagic)+4+4+len(aggregateResultEnd) {
		return AggregateResult{}, fmt.Errorf("aggregate result is truncated")
	}
	if string(data[:len(aggregateResultMagic)]) != aggregateResultMagic {
		return AggregateResult{}, fmt.Errorf("invalid aggregate result magic")
	}
	payloadEnd := len(data) - 4 - len(aggregateResultEnd)
	if string(data[payloadEnd+4:]) != aggregateResultEnd {
		return AggregateResult{}, fmt.Errorf("invalid aggregate result trailer")
	}
	storedChecksum := readUint32(data[payloadEnd : payloadEnd+4])
	if crc32Checksum(data[:payloadEnd]) != storedChecksum {
		return AggregateResult{}, fmt.Errorf("aggregate result checksum mismatch: file is corrupted")
	}
	offset := len(aggregateResultMagic)
	metadataLength := int(readUint32(data[offset : offset+4]))
	offset += 4
	if metadataLength > payloadEnd-offset {
		return AggregateResult{}, fmt.Errorf("aggregate result metadata is truncated")
	}
	var columns []Column
	if err := json.Unmarshal(data[offset:offset+metadataLength], &columns); err != nil {
		return AggregateResult{}, fmt.Errorf("invalid aggregate result schema: %w", err)
	}
	offset += metadataLength
	var persisted persistedResult
	if err := gob.NewDecoder(bytes.NewReader(data[offset:payloadEnd])).Decode(&persisted); err != nil {
		return AggregateResult{}, fmt.Errorf("invalid aggregate result rows: %w", err)
	}
	schema, err := NewSchema(columns)
	if err != nil {
		return AggregateResult{}, err
	}
	return AggregateResult{Schema: schema, Rows: persisted.Rows}, nil
}

func putUint32(target []byte, value uint32) {
	target[0] = byte(value)
	target[1] = byte(value >> 8)
	target[2] = byte(value >> 16)
	target[3] = byte(value >> 24)
}

func readUint32(data []byte) uint32 {
	return uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
}
