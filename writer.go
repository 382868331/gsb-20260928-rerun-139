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
	return w.FlushAs("")
}

// FlushAs writes the pending rows as a new immutable segment with the given
// stable segment identity. An empty id uses the next generated id.
func (w *Writer) FlushAs(id string) error {
	if w.batch.Len() == 0 {
		return nil
	}
	current, err := w.store.readManifest()
	if err != nil {
		return err
	}
	if id == "" {
		generated, err := w.store.nextSegmentID(current)
		if err != nil {
			return err
		}
		id = generated
	} else {
		if err := validateStableSegmentID(id); err != nil {
			return err
		}
		if _, exists := segmentByID(current.Segments, id); exists {
			return fmt.Errorf("segment %q already exists; use ReplaceSegment", id)
		}
	}
	return w.flushSegment(current, id, 1, true)
}

// ReplaceSegment writes the pending rows as a new revision of an existing
// segment identity. Old column files are removed atomically with the manifest
// publish and the dataset revision advances.
func (w *Writer) ReplaceSegment(id string) error {
	if w.batch.Len() == 0 {
		return nil
	}
	if err := validateStableSegmentID(id); err != nil {
		return err
	}
	current, err := w.store.readManifest()
	if err != nil {
		return err
	}
	old, exists := segmentByID(current.Segments, id)
	if !exists {
		return fmt.Errorf("segment %q does not exist; use FlushAs", id)
	}
	return w.flushSegment(current, id, old.Rev+1, false)
}

// WithdrawSegment removes a segment by its stable identity. A withdrawn id can
// be appended again later as a fresh revision.
func (w *Writer) WithdrawSegment(id string) error {
	if err := validateStableSegmentID(id); err != nil {
		return err
	}
	current, err := w.store.readManifest()
	if err != nil {
		return err
	}
	_, exists := segmentByID(current.Segments, id)
	if !exists {
		return fmt.Errorf("segment %q does not exist", id)
	}
	segments := make([]segmentInfo, 0, len(current.Segments)-1)
	for _, segment := range current.Segments {
		if segment.ID != id {
			segments = append(segments, segment)
		}
	}
	current.Segments = segments
	current.Revision++
	w.store.mu.Lock()
	if err := w.store.writeManifestLocked(current); err != nil {
		w.store.mu.Unlock()
		return err
	}
	segmentDir := filepath.Join(w.store.root, "segments", id)
	if err := syncDir(filepath.Join(w.store.root, "segments")); err != nil {
		w.store.mu.Unlock()
		return err
	}
	w.store.mu.Unlock()
	_ = os.RemoveAll(segmentDir)
	return nil
}

func (w *Writer) flushSegment(current manifest, id string, revision uint64, newIdentity bool) error {
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
	segment := segmentInfo{ID: id, Rev: revision, Rows: uint64(w.batch.Len()), Files: files, Stats: statistics}
	if newIdentity {
	current.Segments = append(current.Segments, segment)
	} else {
		for index := range current.Segments {
			if current.Segments[index].ID == id {
				current.Segments[index] = segment
				break
			}
		}
	}
	sortedSegmentIDs(current.Segments)
	current.Revision++
	if err := w.store.publishSegment(segment, current); err != nil {
		return err
	}
	w.batch = NewBatch(w.schema)
	return nil
}

func validateStableSegmentID(id string) error {
	if id == "" {
		return fmt.Errorf("segment id must not be empty")
	}
	if id == "." || id == ".." {
		return fmt.Errorf("invalid segment id %q", id)
	}
	for _, char := range id {
		if char == '/' || char == '\\' || char == 0 || char == filepath.Separator {
			return fmt.Errorf("invalid segment id %q", id)
		}
	}
	return nil
}

func segmentByID(segments []segmentInfo, id string) (segmentInfo, bool) {
	for _, segment := range segments {
		if segment.ID == id {
			if segment.Rev == 0 {
				segment.Rev = 1
			}
			return segment, true
		}
	}
	return segmentInfo{}, false
}

func (w *Writer) Close() error {
	return w.Flush()
}
