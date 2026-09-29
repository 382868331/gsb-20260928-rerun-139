package columnar

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func testSchema(t *testing.T) Schema {
	t.Helper()
	schema, err := NewSchema([]Column{
		{Name: "id", Type: TypeInt64},
		{Name: "age", Type: TypeInt64},
		{Name: "active", Type: TypeBool},
		{Name: "name", Type: TypeString},
	})
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func writeTestDataset(t *testing.T, root string) {
	t.Helper()
	writer, err := Create(root, testSchema(t))
	if err != nil {
		t.Fatal(err)
	}
	rows := [][]any{
		{int64(1), int64(0), true, "zero"},
		{int64(2), nil, true, "null-age"},
		{int64(3), int64(5), false, "inactive"},
		{int64(4), int64(10), true, ""},
	}
	for _, row := range rows[:2] {
		if err := writer.Append(row...); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows[2:] {
		if err := writer.Append(row...); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestColumnScanMatchesRowReferenceAndPreservesNulls(t *testing.T) {
	root := t.TempDir()
	writeTestDataset(t, root)

	filter, err := NewFilter(Predicate{Column: "active", Op: OpEq, Value: true})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReader(root)
	if err != nil {
		t.Fatal(err)
	}
	columnResult, err := reader.Scan(ScanOptions{Columns: []string{"name", "age"}, Filter: filter})
	if err != nil {
		t.Fatal(err)
	}
	referenceResult, err := reader.Scan(ScanOptions{Columns: []string{"name", "age"}, Filter: filter, Reference: true})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(columnResult.Rows, referenceResult.Rows) {
		t.Fatalf("column scan %#v != row reference %#v", columnResult.Rows, referenceResult.Rows)
	}
	want := [][]any{
		{"zero", int64(0)},
		{"null-age", nil},
		{"", int64(10)},
	}
	if !reflect.DeepEqual(columnResult.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", columnResult.Rows, want)
	}
}

func TestScanReadsOnlyRequiredColumnsAndSegments(t *testing.T) {
	root := t.TempDir()
	writeTestDataset(t, root)
	filter, err := NewFilter(Predicate{Column: "age", Op: OpGe, Value: int64(10)})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReader(root)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Scan(ScanOptions{Columns: []string{"name"}, Filter: filter})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Rows, [][]any{{""}}) {
		t.Fatalf("rows = %#v", result.Rows)
	}
	if !reflect.DeepEqual(result.SegmentsScanned, []string{"0000000002"}) {
		t.Fatalf("scanned segments = %#v", result.SegmentsScanned)
	}
	if !reflect.DeepEqual(result.ColumnsRead, []string{"age", "name"}) {
		t.Fatalf("read columns = %#v", result.ColumnsRead)
	}
}

func TestCorruptedColumnFileReturnsError(t *testing.T) {
	root := t.TempDir()
	writeTestDataset(t, root)
	path := filepath.Join(root, "segments", "0000000001", "0000.col")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReader(root)
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.Scan(ScanOptions{Columns: []string{"id"}})
	if err == nil {
		t.Fatal("corruption was silently ignored")
	}
}

func TestMissingUnprojectedColumnInScannedSegmentReturnsError(t *testing.T) {
	root := t.TempDir()
	writeTestDataset(t, root)
	if err := os.Remove(filepath.Join(root, "segments", "0000000002", "0002.col")); err != nil {
		t.Fatal(err)
	}
	filter, err := NewFilter(Predicate{Column: "age", Op: OpGe, Value: int64(10)})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReader(root)
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.Scan(ScanOptions{Columns: []string{"name"}, Filter: filter})
	if err == nil {
		t.Fatal("missing non-projected column was silently ignored")
	}
}

func TestUnpublishedSegmentIsNotValidData(t *testing.T) {
	root := t.TempDir()
	writeTestDataset(t, root)
	orphan := filepath.Join(root, "segments", "0000000003", "0000.col")
	if err := os.MkdirAll(filepath.Dir(orphan), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(orphan, []byte("interrupted"), 0o644); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReader(root)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Scan(ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 4 {
		t.Fatalf("got %d rows including unpublished data", len(result.Rows))
	}
}

func TestNullNeverEqualsZeroOrMatchesComparisons(t *testing.T) {
	schema, err := NewSchema([]Column{{Name: "value", Type: TypeInt64}})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	writer, err := Create(root, schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(int64(0)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(nil); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	for _, operator := range []Operator{OpEq, OpNe, OpLt, OpLe, OpGt, OpGe} {
		filter, err := NewFilter(Predicate{Column: "value", Op: operator, Value: int64(0)})
		if err != nil {
			t.Fatal(err)
		}
		reader, err := OpenReader(root)
		if err != nil {
			t.Fatal(err)
		}
		result, err := reader.Scan(ScanOptions{Filter: filter})
		if err != nil {
			t.Fatal(err)
		}
		wantCount := 0
		if operator == OpEq || operator == OpLe || operator == OpGe {
			wantCount = 1
		}
		if len(result.Rows) != wantCount {
			t.Fatalf("operator %s matched %d rows, want %d", operator, len(result.Rows), wantCount)
		}
	}
}
