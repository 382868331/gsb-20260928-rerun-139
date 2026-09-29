package coldstore_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"coldstore"
	"coldstore/refrow"
)

var schema = coldstore.Schema{Columns: []coldstore.Column{
	{Name: "id", Kind: coldstore.KindInt64},
	{Name: "score", Kind: coldstore.KindFloat64},
	{Name: "name", Kind: coldstore.KindString},
	{Name: "active", Kind: coldstore.KindBool},
}}

func sampleBatches() []coldstore.Batch {
	return []coldstore.Batch{
		{
			Rows: 5,
			Columns: []coldstore.ColumnData{
				{Kind: coldstore.KindInt64, Int64s: []int64{0, 1, 2, 3, 4},
					Null: []bool{false, false, true, false, false}},
				{Kind: coldstore.KindFloat64, Float64s: []float64{0, 9.5, 0, 7.2, 3.1},
					Null: []bool{false, false, false, true, false}},
				{Kind: coldstore.KindString, Strings: []string{"", "a", "b", "c", "d"},
					Null: []bool{true, false, false, false, false}},
				{Kind: coldstore.KindBool, Bools: []bool{false, true, false, true, false},
					Null: []bool{false, true, false, false, true}},
			},
		},
		{
			Rows: 4,
			Columns: []coldstore.ColumnData{
				{Kind: coldstore.KindInt64, Int64s: []int64{10, 20, 30, 40}, Null: []bool{false, false, false, false}},
				{Kind: coldstore.KindFloat64, Float64s: []float64{1.1, 2.2, 3.3, 4.4}, Null: make([]bool, 4)},
				{Kind: coldstore.KindString, Strings: []string{"e", "f", "g", "h"}, Null: []bool{false, false, true, false}},
				{Kind: coldstore.KindBool, Bools: []bool{true, false, true, false}, Null: []bool{false, false, false, false}},
			},
		},
	}
}

func writeStore(t *testing.T, batches []coldstore.Batch) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	s, err := coldstore.CreateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := coldstore.NewWriter(s, schema)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range batches {
		if err := w.Append(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func openReader(t *testing.T, dir string) *coldstore.Reader {
	t.Helper()
	s, err := coldstore.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	r, err := coldstore.NewReader(s)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// Nulls and ordinary zeros must round-trip independently for every kind.
func TestRoundTripNullVsZero(t *testing.T) {
	dir := writeStore(t, sampleBatches())
	r := openReader(t, dir)
	out, err := r.Read(coldstore.ReadOptions{Columns: []string{"id", "score", "name", "active"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("got %d batches", len(out))
	}
	b0 := out[0]
	idCol := b0.Columns[0]
	if idCol.IsNull(0) || idCol.Int64s[0] != 0 {
		t.Fatal("ordinary int64 zero at row 0 lost")
	}
	if !idCol.IsNull(2) {
		t.Fatal("null id at row 2 not preserved")
	}
	scoreCol := b0.Columns[1]
	if scoreCol.IsNull(0) || scoreCol.Float64s[0] != 0 {
		t.Fatal("ordinary float64 zero lost")
	}
	if !scoreCol.IsNull(3) {
		t.Fatal("null score not preserved")
	}
	nameCol := b0.Columns[2]
	if !nameCol.IsNull(0) || nameCol.Strings[0] != "" {
		t.Fatal("empty string null flag wrong")
	}
	actCol := b0.Columns[3]
	if actCol.IsNull(0) || actCol.Bools[0] != false {
		t.Fatal("ordinary bool false lost")
	}
	if !actCol.IsNull(1) {
		t.Fatal("null bool not preserved")
	}
}

// Filtering must use the named column's positions; a null never matches
// even when its stored value is zero.
func TestFilterNullSemanticsAndAlignment(t *testing.T) {
	dir := writeStore(t, sampleBatches())
	r := openReader(t, dir)

	out, err := r.Read(coldstore.ReadOptions{
		Columns: []string{"id"},
		Filters: []coldstore.Filter{{Column: "id", Op: coldstore.OpEq, Value: int64(0)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || len(out[0].Sel) != 1 {
		t.Fatalf("id == 0 should match only the non-null zero, got %+v", out)
	}
	if got := out[0].Sel[0]; got != 0 {
		t.Fatalf("matched wrong row %d (column alignment broken)", got)
	}

	// != must not return nulls either.
	out, err = r.Read(coldstore.ReadOptions{
		Columns: []string{"id"},
		Filters: []coldstore.Filter{{Column: "id", Op: coldstore.OpNe, Value: int64(0)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, rb := range out {
		for _, ri := range rb.Sel {
			if rb.BatchID == 0 && ri == 2 {
				t.Fatal("null id leaked through != filter")
			}
			count++
		}
	}
	if count != 7 { // 3 non-null batch0 + 4 batch1
		t.Fatalf("!= matched %d rows, want 7", count)
	}
}

func TestFilterMatchesRowReference(t *testing.T) {
	batches := sampleBatches()
	dir := writeStore(t, batches)
	r := openReader(t, dir)
	ref := refrow.FromBatches(schema, batches)

	filters := []coldstore.Filter{
		{Column: "score", Op: coldstore.OpGe, Value: float64(3.0)},
		{Column: "active", Op: coldstore.OpEq, Value: true},
	}
	out, err := r.Read(coldstore.ReadOptions{Columns: []string{"id", "name"}, Filters: filters})
	if err != nil {
		t.Fatal(err)
	}
	refSel := ref.Filter(filters)
	var gotRows []int
	for _, rb := range out {
		base := 0
		for bi := 0; bi < rb.BatchID; bi++ {
			base += batches[bi].Rows
		}
		for _, ri := range rb.Sel {
			gotRows = append(gotRows, base+ri)
		}
	}
	if len(gotRows) != len(refSel) {
		t.Fatalf("columnar %v != row reference %v", gotRows, refSel)
	}
	for i := range gotRows {
		if gotRows[i] != refSel[i] {
			t.Fatalf("at %d columnar %d != reference %d", i, gotRows[i], refSel[i])
		}
	}
	// Projection alignment: returned id/name come from the selected rows.
	for _, rb := range out {
		for j, ri := range rb.Sel {
			if rb.Columns[0].Int64s[j] != batches[rb.BatchID].Columns[0].Int64s[ri] {
				t.Fatal("projected id misaligned with selection")
			}
		}
	}
}

func TestOnlyRequestedColumnsRead(t *testing.T) {
	dir := writeStore(t, sampleBatches())
	r := openReader(t, dir)
	out, err := r.Read(coldstore.ReadOptions{Columns: []string{"name"}})
	if err != nil {
		t.Fatal(err)
	}
	nameIdx, _ := schema.ColumnIndex("name")
	if out[0].Columns[nameIdx].Kind != coldstore.KindString {
		t.Fatal("expected name column at its schema position")
	}
	idIdx, _ := schema.ColumnIndex("id")
	actIdx, _ := schema.ColumnIndex("active")
	if out[0].Columns[idIdx].Kind != 0 || out[0].Columns[actIdx].Kind != 0 {
		t.Fatal("unrequested columns were populated")
	}
}

func TestSegmentPruningSkipsWholeBatch(t *testing.T) {
	dir := writeStore(t, sampleBatches())
	r := openReader(t, dir)
	// batch0 ids 0..4, batch1 ids 10..40; no batch0 row can match.
	out, err := r.Read(coldstore.ReadOptions{
		Columns: []string{"id"},
		Filters: []coldstore.Filter{{Column: "id", Op: coldstore.OpEq, Value: int64(30)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].BatchID != 1 || len(out[0].Sel) != 1 || out[0].Sel[0] != 2 {
		t.Fatalf("pruning result wrong: %+v", out)
	}
}

func TestCorruptSegmentReportedNotSkipped(t *testing.T) {
	dir := writeStore(t, sampleBatches())
	seg := filepath.Join(dir, "batch-0", "col-0.bin")
	raw, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xFF // flip a CRC byte
	if err := os.WriteFile(seg, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	r := openReader(t, dir)
	_, err = r.Read(coldstore.ReadOptions{Columns: []string{"id"}})
	var corrupt *coldstore.CorruptSegmentError
	if !errors.As(err, &corrupt) {
		t.Fatalf("expected CorruptSegmentError, got %v", err)
	}
}

func TestUnreadCorruptColumnDoesNotMaskOthers(t *testing.T) {
	dir := writeStore(t, sampleBatches())
	seg := filepath.Join(dir, "batch-0", "col-3.bin") // active column
	raw, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xFF
	if err := os.WriteFile(seg, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	r := openReader(t, dir)
	// Reading only id must not open the damaged active segment.
	if _, err := r.Read(coldstore.ReadOptions{Columns: []string{"id"}}); err != nil {
		t.Fatalf("unneeded column read: %v", err)
	}
	// But once the damaged column is needed it must surface the error.
	_, err = r.Read(coldstore.ReadOptions{Columns: []string{"active"}})
	var corrupt *coldstore.CorruptSegmentError
	if !errors.As(err, &corrupt) {
		t.Fatalf("expected corruption error, got %v", err)
	}
}

func TestInterruptedWriteNotPublished(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s, err := coldstore.CreateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := coldstore.NewWriter(s, schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(sampleBatches()[0]); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash: Close is never called. Segment exists, but no manifest.
	if _, err := os.Stat(filepath.Join(dir, "batch-0", "col-0.bin")); err != nil {
		t.Fatalf("segment should be on disk: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "manifest.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manifest must not exist before publish, got %v", err)
	}
	if _, err := coldstore.OpenStore(dir); err == nil {
		t.Fatal("interrupted dataset opened as valid data")
	}
}

func TestSchemaValidation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s, err := coldstore.CreateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := coldstore.NewWriter(s, schema)
	if err != nil {
		t.Fatal(err)
	}
	bad := coldstore.Batch{Rows: 1, Columns: []coldstore.ColumnData{
		{Kind: coldstore.KindString, Strings: []string{"x"}}, // wrong kind
		{Kind: coldstore.KindFloat64, Float64s: []float64{1}}, {Kind: coldstore.KindString, Strings: []string{"x"}}, {Kind: coldstore.KindBool, Bools: []bool{true}},
	}}
	if err := w.Append(bad); err == nil {
		t.Fatal("expected schema mismatch error")
	}
}
