package coldstore

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ReadOptions selects which columns and rows to read. Columns not named
// are never opened on disk. Filters reference columns implicitly: those
// columns are read even when absent from Columns because they drive the
// selection, but they are not returned.
//
// Segment pruning uses manifest min/max statistics to skip whole
// batches before any segment is opened. A skipped segment cannot be
// checked for corruption; segments that are actually read always report
// CorruptSegmentError instead of being silently dropped.
type ReadOptions struct {
	Columns []string
	Filters []Filter
}

// RowBatch is a filtered read result. It preserves the original column
// positions of the schema; unrequested columns are nil. Sel lists the
// source row indices that passed every filter.
type RowBatch struct {
	BatchID int
	Rows    int
	Sel     []int
	Columns []ColumnData
}

// Reader scans a published store.
type Reader struct {
	s      *Store
	m      *manifest
	schema Schema
}

// NewReader opens a store and reads its manifest.
func NewReader(s *Store) (*Reader, error) {
	m, err := s.readManifest()
	if err != nil {
		return nil, err
	}
	return &Reader{s: s, m: m, schema: m.schema()}, nil
}

// Schema returns the published schema.
func (r *Reader) Schema() Schema { return r.schema }

// Read scans every batch honoring opt. It reads only the requested plus
// filter columns, prunes batches via statistics, and returns a materialized
// projection per surviving batch.
func (r *Reader) Read(opt ReadOptions) ([]RowBatch, error) {
	projectIdx := make([]int, 0, len(opt.Columns))
	for _, name := range opt.Columns {
		idx, ok := r.schema.indexOf(name)
		if !ok {
			return nil, fmt.Errorf("coldstore: unknown column %q", name)
		}
		projectIdx = append(projectIdx, idx)
	}
	filterIdx := make([]int, len(opt.Filters))
	for i, f := range opt.Filters {
		idx, ok := r.schema.indexOf(f.Column)
		if !ok {
			return nil, fmt.Errorf("coldstore: filter on unknown column %q", f.Column)
		}
		filterIdx[i] = idx
	}

	needCol := map[int]bool{}
	for _, idx := range projectIdx {
		needCol[idx] = true
	}
	for _, idx := range filterIdx {
		needCol[idx] = true
	}

	var out []RowBatch
	for bi := range r.m.Batches {
		bm := r.m.Batches[bi]
		pruned, err := r.batchCanMatch(bm, opt.Filters, filterIdx)
		if err != nil {
			return nil, err
		}
		if pruned {
			continue
		}
		masks := make([][]bool, len(opt.Filters))
		cache := map[int]ColumnData{}
		for fi, idx := range filterIdx {
			col := r.schema.Columns[idx]
			data, err := r.loadSegment(bm, idx, col)
			if err != nil {
				return nil, err
			}
			cache[idx] = data
			mask, err := opt.Filters[fi].Evaluate(data)
			if err != nil {
				return nil, fmt.Errorf("coldstore: filter on %q: %w", col.Name, err)
			}
			masks[fi] = mask
		}
		sel := intersectMasks(bm.Rows, masks)
		if len(sel) == 0 {
			continue
		}
		rb := RowBatch{BatchID: bm.ID, Rows: bm.Rows, Sel: sel,
			Columns: make([]ColumnData, len(r.schema.Columns))}
		for _, idx := range projectIdx {
			data, ok := cache[idx]
			if !ok {
				col := r.schema.Columns[idx]
				d, err := r.loadSegment(bm, idx, col)
				if err != nil {
					return nil, err
				}
				data = d
			}
			rb.Columns[idx] = projectSelection(data, sel)
		}
		out = append(out, rb)
	}
	return out, nil
}

func (r *Reader) loadSegment(bm batchManifest, idx int, col Column) (ColumnData, error) {
	sm, ok := bm.Segments[col.Name]
	if !ok || sm.File == "" {
		return ColumnData{}, &CorruptSegmentError{
			Batch: bm.ID, Column: col.Name, Reason: "no segment listed in manifest"}
	}
	path := filepath.Join(r.s.root, "batch-"+itoa(bm.ID), sm.File)
	f, err := os.Open(path)
	if err != nil {
		return ColumnData{}, &CorruptSegmentError{
			Path: path, Batch: bm.ID, Column: col.Name,
			Reason: "cannot open segment: " + err.Error()}
	}
	defer f.Close()
	data, err := decodeSegment(f, col.Kind)
	if err != nil {
		return ColumnData{}, &CorruptSegmentError{
			Path: path, Batch: bm.ID, Column: col.Name, Reason: err.Error()}
	}
	if data.Len() != bm.Rows {
		return ColumnData{}, &CorruptSegmentError{
			Path: path, Batch: bm.ID, Column: col.Name,
			Reason: fmt.Sprintf("decoded %d rows, manifest says %d", data.Len(), bm.Rows)}
	}
	return data, nil
}

// batchCanMatch returns true if the batch can be skipped. Pruning is
// conservative: nulls never satisfy filters, and equality/range checks
// use min/max only when statistics are present.
func (r *Reader) batchCanMatch(bm batchManifest, filters []Filter, idxs []int) (bool, error) {
	for fi, idx := range idxs {
		col := r.schema.Columns[idx]
		sm := bm.Segments[col.Name]
		if sm.File == "" {
			return false, &CorruptSegmentError{
				Batch: bm.ID, Column: col.Name, Reason: "no segment listed in manifest"}
		}
		if sm.NonNull == 0 {
			return true, nil
		}
		if prunedByStats(sm, col.Kind, filters[fi]) {
			return true, nil
		}
	}
	return false, nil
}

func intersectMasks(rows int, masks [][]bool) []int {
	sel := make([]int, 0, rows)
outer:
	for i := 0; i < rows; i++ {
		for _, m := range masks {
			if !m[i] {
				continue outer
			}
		}
		sel = append(sel, i)
	}
	return sel
}

func projectSelection(data ColumnData, sel []int) ColumnData {
	out := ColumnData{Kind: data.Kind, Null: make([]bool, len(sel))}
	for j, i := range sel {
		out.Null[j] = data.IsNull(i)
	}
	switch data.Kind {
	case KindInt64:
		out.Int64s = make([]int64, len(sel))
		for j, i := range sel {
			out.Int64s[j] = data.Int64s[i]
		}
	case KindFloat64:
		out.Float64s = make([]float64, len(sel))
		for j, i := range sel {
			out.Float64s[j] = data.Float64s[i]
		}
	case KindBool:
		out.Bools = make([]bool, len(sel))
		for j, i := range sel {
			out.Bools[j] = data.Bools[i]
		}
	case KindString:
		out.Strings = make([]string, len(sel))
		for j, i := range sel {
			out.Strings[j] = data.Strings[i]
		}
	case KindTimestamp:
		out.Timestamps = make([]time.Time, len(sel))
		for j, i := range sel {
			out.Timestamps[j] = data.Timestamps[i]
		}
	}
	return out
}

func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}
