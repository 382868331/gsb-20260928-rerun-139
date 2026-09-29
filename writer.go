package columnar

import (
	"fmt"
	"os"
	"path/filepath"
)

type Writer struct {
	store  *fileStore
	schema Schema
	batch  *Batch
}

func Create(root string, schema Schema) (*Writer, error) {
	store, err := newFileStore(root)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(root, "manifest.json")); err == nil {
		return nil, fmt.Errorf("dataset already exists at %s", root)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	initial := manifest{Version: manifestVersion, Columns: schema.Columns, Segments: nil}
	if err := store.writeManifest(initial); err != nil {
		return nil, err
	}
	return &Writer{store: store, schema: schema, batch: NewBatch(schema)}, nil
}

func OpenWriter(root string) (*Writer, error) {
	store, err := newFileStore(root)
	if err != nil {
		return nil, err
	}
	current, err := store.readManifest()
	if err != nil {
		return nil, err
	}
	schema, _ := NewSchema(current.Columns)
	return &Writer{store: store, schema: schema, batch: NewBatch(schema)}, nil
}

func (w *Writer) Schema() Schema { return w.schema }

func (w *Writer) Append(values ...any) error {
	return w.batch.Append(values...)
}

func (w *Writer) Flush() error {
	if w.batch.Len() == 0 {
		return nil
	}
	current, err := w.store.readManifest()
	if err != nil {
		return err
	}
	id, err := w.store.nextSegmentID(current)
	if err != nil {
		return err
	}
	segmentDir := filepath.Join(w.store.root, "segments", id)
	if err := os.MkdirAll(segmentDir, 0o755); err != nil {
		return err
	}
	files := make([]segmentFile, len(w.schema.Columns))
	statistics := make(map[string]stats, len(w.schema.Columns))
	for columnIndex, column := range w.schema.Columns {
		values := make([]any, w.batch.Len())
		for rowIndex := range values {
			values[rowIndex] = w.batch.rows[rowIndex][columnIndex]
		}
		encoded, err := encodeColumn(column.Type, values)
		if err != nil {
			return err
		}
		name := fmt.Sprintf("%04d.col", columnIndex)
		path := filepath.Join(segmentDir, name)
		if err := atomicWriteFile(path, encoded, false); err != nil {
			return err
		}
		files[columnIndex] = segmentFile{
			Path:  filepath.ToSlash(filepath.Join("segments", id, name)),
			Size:  int64(len(encoded)),
			CRC32: crc32Checksum(encoded),
		}
		statistics[column.Name] = buildStats(column.Type, values)
	}
	if err := syncDir(segmentDir); err != nil {
		return err
	}
	segment := segmentInfo{ID: id, Rows: uint64(w.batch.Len()), Files: files, Stats: statistics}
	current.Segments = append(current.Segments, segment)
	sortedSegmentIDs(current.Segments)
	if err := w.store.publishSegment(segment, current); err != nil {
		return err
	}
	w.batch = NewBatch(w.schema)
	return nil
}

func (w *Writer) Close() error {
	return w.Flush()
}
