package coldstore_test

import (
	"path/filepath"
	"testing"
	"time"

	"coldstore"
)

func TestAppendAfterPublish(t *testing.T) {
	dir := writeStore(t, sampleBatches()[:1])

	s, err := coldstore.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := coldstore.OpenWriter(s)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(sampleBatches()[1]); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r := openReader(t, dir)
	out, err := r.Read(coldstore.ReadOptions{Columns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, rb := range out {
		total += rb.Rows
	}
	if total != 9 {
		t.Fatalf("appended rows total %d, want 9", total)
	}
}

func TestTimestampNullRoundTrip(t *testing.T) {
	sch := coldstore.Schema{Columns: []coldstore.Column{{Name: "at", Kind: coldstore.KindTimestamp}}}
	now := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	b := coldstore.Batch{Rows: 3, Columns: []coldstore.ColumnData{{
		Kind:       coldstore.KindTimestamp,
		Timestamps: []time.Time{{}, now, {}},
		Null:       []bool{true, false, true},
	}}}
	dir := filepath.Join(t.TempDir(), "store")
	s, err := coldstore.CreateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := coldstore.NewWriter(s, sch)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r := openReader(t, dir)
	out, err := r.Read(coldstore.ReadOptions{
		Columns: []string{"at"},
		Filters: []coldstore.Filter{{Column: "at", Op: coldstore.OpGe, Value: now}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || len(out[0].Sel) != 1 || out[0].Sel[0] != 1 {
		t.Fatalf("timestamp filter wrong: %+v", out)
	}
	if !out[0].Columns[0].Timestamps[0].Equal(now) {
		t.Fatal("timestamp value lost")
	}
}
