package columnar

import (
	"fmt"
)

type Reader struct {
	store  *fileStore
	schema Schema
}

type Result struct {
	Schema          Schema
	Rows            [][]any
	SegmentsScanned []string
	ColumnsRead     []string
}

type ScanOptions struct {
	Columns   []string
	Filter    *Filter
	Reference bool
}

func OpenReader(root string) (*Reader, error) {
	store, err := newFileStore(root)
	if err != nil {
		return nil, err
	}
	current, err := store.readManifest()
	if err != nil {
		return nil, err
	}
	schema, _ := NewSchema(current.Columns)
	return &Reader{store: store, schema: schema}, nil
}

func (r *Reader) Schema() Schema { return r.schema }

func (r *Reader) Scan(options ScanOptions) (Result, error) {
	current, err := r.store.readManifest()
	if err != nil {
		return Result{}, err
	}
	predicates, err := options.Filter.bind(r.schema)
	if err != nil {
		return Result{}, err
	}
	projectedSchema, projectedIndexes, err := r.schema.Project(options.Columns)
	if err != nil && len(options.Columns) > 0 {
		return Result{}, err
	}
	if len(options.Columns) == 0 {
		projectedSchema = r.schema
		projectedIndexes = make([]int, len(r.schema.Columns))
		for i := range projectedIndexes {
			projectedIndexes[i] = i
		}
	}
	neededIndexes := append([]int(nil), projectedIndexes...)
	needed := make(map[int]struct{}, len(neededIndexes))
	for _, index := range neededIndexes {
		needed[index] = struct{}{}
	}
	for _, predicate := range predicates {
		if _, ok := needed[predicate.column]; !ok {
			needed[predicate.column] = struct{}{}
			neededIndexes = append(neededIndexes, predicate.column)
		}
	}
	result := Result{Schema: projectedSchema}
	for _, segment := range current.Segments {
		if !options.Reference && segmentCanSkip(current.Columns, segment, predicates) {
			continue
		}
		for _, fileInfo := range segment.Files {
			if err := r.store.validateFileMetadata(fileInfo.Path, fileInfo.Size); err != nil {
				return Result{}, fmt.Errorf("segment %s: %w", segment.ID, err)
			}
		}
		readIndexes := neededIndexes
		if options.Reference {
			readIndexes = make([]int, len(current.Columns))
			for i := range readIndexes {
				readIndexes[i] = i
			}
		}
		columns, err := r.readSegmentColumns(current, segment, readIndexes)
		if err != nil {
			return Result{}, fmt.Errorf("segment %s: %w", segment.ID, err)
		}
		result.SegmentsScanned = append(result.SegmentsScanned, segment.ID)
		for rowIndex := uint64(0); rowIndex < segment.Rows; rowIndex++ {
			rowColumns := make([]any, len(current.Columns))
			for columnIndex, values := range columns {
				rowColumns[columnIndex] = values[rowIndex]
			}
			if !matches(rowColumns, predicates) {
				continue
			}
			row := make([]any, len(projectedIndexes))
			for i, sourceIndex := range projectedIndexes {
				row[i] = columns[sourceIndex][rowIndex]
			}
			result.Rows = append(result.Rows, row)
		}
	}
	result.ColumnsRead = make([]string, 0, len(needed))
	for index := range needed {
		result.ColumnsRead = append(result.ColumnsRead, r.schema.Columns[index].Name)
	}
	if options.Reference {
		result.ColumnsRead = make([]string, len(r.schema.Columns))
		for i, column := range r.schema.Columns {
			result.ColumnsRead[i] = column.Name
		}
	}
	sortStrings(result.ColumnsRead)
	return result, nil
}

func (r *Reader) readSegmentColumns(current manifest, segment segmentInfo, indexes []int) (map[int][]any, error) {
	columns := make(map[int][]any, len(indexes))
	for _, index := range indexes {
		if index < 0 || index >= len(segment.Files) {
			return nil, fmt.Errorf("invalid column index %d", index)
		}
		fileInfo := segment.Files[index]
		data, err := r.store.readColumnFile(fileInfo.Path)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", current.Columns[index].Name, err)
		}
		if int64(len(data)) != fileInfo.Size || crc32Checksum(data) != fileInfo.CRC32 {
			return nil, fmt.Errorf("column %q file metadata does not match: segment is corrupted", current.Columns[index].Name)
		}
		values, err := decodeColumn(current.Columns[index].Type, data)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", current.Columns[index].Name, err)
		}
		if uint64(len(values)) != segment.Rows {
			return nil, fmt.Errorf("column %q has %d rows, manifest declares %d", current.Columns[index].Name, len(values), segment.Rows)
		}
		columns[index] = values
	}
	return columns, nil
}

func segmentCanSkip(columns []Column, segment segmentInfo, predicates []boundPredicate) bool {
	for _, predicate := range predicates {
		if predicate.value == nil {
			return true
		}
		if canSkip(segment.Stats[columns[predicate.column].Name], predicate) {
			return true
		}
	}
	return false
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j-1] > values[j]; j-- {
			values[j-1], values[j] = values[j], values[j-1]
		}
	}
}
