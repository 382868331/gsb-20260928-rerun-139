package coldstore

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

// Writer appends columnar batches. Segments are durable before Close,
// but batches are only published when Close replaces the manifest;
// a crash before that leaves no manifest and no valid data.
type Writer struct {
	s      *Store
	schema Schema
	m      *manifest
	failed bool
}

// NewWriter creates a writer for a brand-new dataset with schema.
func NewWriter(s *Store, schema Schema) (*Writer, error) {
	if len(schema.Columns) == 0 {
		return nil, &SchemaError{Reason: "schema has no columns"}
	}
	names := map[string]bool{}
	for _, c := range schema.Columns {
		if c.Name == "" {
			return nil, &SchemaError{Reason: "column has empty name"}
		}
		if names[c.Name] {
			return nil, &SchemaError{Reason: "duplicate column " + c.Name}
		}
		names[c.Name] = true
	}
	if _, err := os.Stat(filepath.Join(s.root, manifestName)); err == nil {
		return nil, fmt.Errorf("coldstore: %s already published; open it with OpenWriter to append", s.root)
	}
	m := &manifest{Version: 1, Columns: append([]Column(nil), schema.Columns...)}
	return &Writer{s: s, schema: schema, m: m}, nil
}

// OpenWriter opens an existing published dataset for appending.
func OpenWriter(s *Store) (*Writer, error) {
	m, err := s.readManifest()
	if err != nil {
		return nil, err
	}
	return &Writer{s: s, schema: m.schema(), m: m}, nil
}

// Append validates the batch against the schema, encodes and fsyncs every
// column segment, and records metadata in memory. The batch is not
// visible to readers until Close publishes the manifest.
func (w *Writer) Append(b Batch) error {
	if w.failed {
		return fmt.Errorf("coldstore: writer is unusable after a previous error")
	}
	if err := w.validate(b); err != nil {
		w.failed = true
		return err
	}
	nextID := len(w.m.Batches)
	dir := filepath.Join(w.s.root, "batch-"+strconv.Itoa(nextID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		w.failed = true
		return err
	}
	segments := make(map[string]segmentManifest, len(b.Columns))
	for i, col := range w.schema.Columns {
		data := b.Columns[i]
		raw, err := encodeSegment(data)
		if err != nil {
			w.failed = true
			return err
		}
		name := fmt.Sprintf("col-%d.bin", i)
		tmp := filepath.Join(dir, "."+name+".tmp")
		if err := writeFileSync(tmp, raw); err != nil {
			w.failed = true
			return err
		}
		final := filepath.Join(dir, name)
		if err := os.Rename(tmp, final); err != nil {
			w.failed = true
			return err
		}
		sm := statsFromColumn(data)
		sm.File = name
		segments[col.Name] = sm
	}
	if err := syncDir(dir); err != nil {
		w.failed = true
		return err
	}
	w.m.Batches = append(w.m.Batches, batchManifest{
		ID:       nextID,
		Rows:     b.Rows,
		Segments: segments,
	})
	return nil
}

func writeFileSync(path string, raw []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, werr := f.Write(raw)
	serr := f.Sync()
	cerr := f.Close()
	if werr != nil {
		os.Remove(path)
		return werr
	}
	if serr != nil {
		os.Remove(path)
		return serr
	}
	if cerr != nil {
		os.Remove(path)
		return cerr
	}
	return nil
}

func (w *Writer) validate(b Batch) error {
	if len(b.Columns) != len(w.schema.Columns) {
		return &SchemaError{Reason: fmt.Sprintf("batch has %d columns, schema has %d", len(b.Columns), len(w.schema.Columns))}
	}
	for i, col := range w.schema.Columns {
		d := b.Columns[i]
		if d.Kind != col.Kind {
			return &SchemaError{Reason: fmt.Sprintf("column %q: kind %s, want %s", col.Name, d.Kind, col.Kind)}
		}
		if d.Len() != b.Rows {
			return &SchemaError{Reason: fmt.Sprintf("column %q has %d cells, batch rows %d", col.Name, d.Len(), b.Rows)}
		}
		if len(d.Null) != 0 && len(d.Null) != b.Rows {
			return &SchemaError{Reason: fmt.Sprintf("column %q null mask length %d, want %d", col.Name, len(d.Null), b.Rows)}
		}
	}
	return nil
}

// Close publishes all appended batches atomically via the manifest.
func (w *Writer) Close() error {
	if w.failed {
		return fmt.Errorf("coldstore: writer failed earlier; manifest was not published")
	}
	sort.Slice(w.m.Batches, func(i, j int) bool { return w.m.Batches[i].ID < w.m.Batches[j].ID })
	w.failed = true
	return w.s.publishManifest(w.m)
}
