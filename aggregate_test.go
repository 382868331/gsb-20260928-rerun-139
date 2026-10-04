package columnar

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
)

type cancelAfterContext struct {
	context.Context
	calls atomic.Int32
	after int32
}

func (c *cancelAfterContext) Err() error {
	if c.calls.Add(1) > c.after {
		return context.Canceled
	}
	return nil
}

func aggregateTestSchema(t *testing.T) Schema {
	t.Helper()
	schema, err := NewSchema([]Column{
		{Name: "g", Type: TypeInt64},
		{Name: "s", Type: TypeString},
		{Name: "v", Type: TypeInt64},
	})
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func writeAggregateDataset(t *testing.T, root string) {
	t.Helper()
	writer, err := Create(root, aggregateTestSchema(t))
	if err != nil {
		t.Fatal(err)
	}
	segments := [][][]any{
		{
			{int64(0), "", int64(2)},
			{nil, "x", int64(3)},
			{int64(1), "", nil},
			{int64(1), "", int64(4)},
		},
		{
			{int64(0), "", int64(0)},
			{nil, "x", nil},
			{int64(2), "z", int64(1)},
			{int64(2), "z", int64(7)},
			{int64(3), "hot", int64(10)},
			{int64(3), "hot", int64(20)},
			{int64(3), "hot", int64(30)},
		},
	}
	for _, rows := range segments {
		for _, row := range rows {
			if err := writer.Append(row...); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.Flush(); err != nil {
			t.Fatal(err)
		}
	}
}

func aggregateOptions() AggregateOptions {
	return AggregateOptions{
		GroupBy: []string{"g", "s"},
		Aggregates: []AggregationSpec{
			{Func: AggCount, Name: "rows"},
			{Column: "v", Func: AggCount, Name: "v_count"},
			{Column: "v", Func: AggSum, Name: "v_sum"},
			{Column: "v", Func: AggAvg, Name: "v_avg"},
			{Column: "v", Func: AggMin, Name: "v_min"},
			{Column: "v", Func: AggMax, Name: "v_max"},
		},
		MaxGroups: 2,
	}
}

func expectedAggregateRows(rows [][]any) [][]any {
	type state struct {
		count   int64
		nonNull int64
		sum     int64
		min     int64
		max     int64
	}
	keyIndex := map[string]*state{}
	var keys []string
	for _, row := range rows {
		key := fmt.Sprintf("%v\x00%v", row[0], row[1])
		current, ok := keyIndex[key]
		if !ok {
			current = &state{}
			keyIndex[key] = current
			keys = append(keys, key)
		}
		current.count++
		if row[2] != nil {
			value := row[2].(int64)
			current.nonNull++
			current.sum += value
			if current.nonNull == 1 || value < current.min {
				current.min = value
			}
			if current.nonNull == 1 || value > current.max {
				current.max = value
			}
		}
	}
	result := make([][]any, 0, len(keys))
	for _, key := range keys {
		var parts []any
		var g int64
		var s string
		fmt.Sscanf(key, "%v\x00%v", &g, &s)
		if key == "<nil>\x00x" {
			parts = []any{nil, "x"}
		} else {
			parts = []any{g, s}
		}
		current := keyIndex[key]
		var avg any
		var minimum any
		var maximum any
		if current.nonNull > 0 {
			avg = float64(current.sum) / float64(current.nonNull)
			minimum = current.min
			maximum = current.max
		}
		result = append(result, append(parts, current.count, current.nonNull, current.sum, avg, minimum, maximum))
	}
	sortRows(result)
	return result
}

func TestAggregateMatchesReferenceAndDistinguishesIdentities(t *testing.T) {
	root := t.TempDir()
	writeAggregateDataset(t, root)
	reader, err := OpenReader(root)
	if err != nil {
		t.Fatal(err)
	}
	options := aggregateOptions()
	result, err := reader.Aggregate(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := reader.Aggregate(context.Background(), AggregateOptions{
		GroupBy:    options.GroupBy,
		Aggregates: options.Aggregates,
		MaxGroups:  options.MaxGroups,
		Reference:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	scan, err := reader.Scan(ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := expectedAggregateRows(scan.Rows)
	if !result.Spilled {
		t.Fatal("high-cardinality aggregate did not spill")
	}
	if !reflect.DeepEqual(result.Rows, reference.Rows) {
		t.Fatalf("aggregate %#v != reference %#v", result.Rows, reference.Rows)
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("aggregate rows = %#v, want %#v", result.Rows, want)
	}
	if !reflect.DeepEqual(result.ColumnsRead, []string{"g", "s", "v"}) {
		t.Fatalf("read columns = %#v", result.ColumnsRead)
	}
	if !reflect.DeepEqual(reference.ColumnsRead, []string{"g", "s", "v"}) {
		t.Fatalf("reference read columns = %#v", reference.ColumnsRead)
	}
}

func TestAggregateProjectsOnlyRequiredColumnsWithFilter(t *testing.T) {
	root := t.TempDir()
	schema, err := NewSchema([]Column{
		{Name: "id", Type: TypeInt64},
		{Name: "group", Type: TypeInt64},
		{Name: "amount", Type: TypeInt64},
		{Name: "unused", Type: TypeString},
	})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := Create(root, schema)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range [][]any{{int64(1), int64(0), int64(4), "a"}, {int64(2), int64(0), int64(6), "b"}} {
		if err := writer.Append(row...); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	filter, err := NewFilter(Predicate{Column: "id", Op: OpGe, Value: int64(2)})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReader(root)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Aggregate(context.Background(), AggregateOptions{
		GroupBy:    []string{"group"},
		Aggregates: []AggregationSpec{{Column: "amount", Func: AggSum, Name: "total"}},
		Filter:     filter,
		MaxGroups:  10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Rows, [][]any{{int64(0), int64(6)}}) {
		t.Fatalf("rows = %#v", result.Rows)
	}
	sort.Strings(result.ColumnsRead)
	if !reflect.DeepEqual(result.ColumnsRead, []string{"amount", "group", "id"}) {
		t.Fatalf("read columns = %#v", result.ColumnsRead)
	}
}

func TestAggregationResumeDoesNotDoubleCountAndKeepsOldVersionOnCorruption(t *testing.T) {
	root := t.TempDir()
	writeAggregateDataset(t, root)
	reader, err := OpenReader(root)
	if err != nil {
		t.Fatal(err)
	}
	options := aggregateOptions()
	options.MaxGroups = 20
	if err := reader.RunAggregation(context.Background(), "metrics", options); err != nil {
		t.Fatal(err)
	}
	first, err := ReadPublishedAggregate(root, "metrics")
	if err != nil {
		t.Fatal(err)
	}

	canceled := &cancelAfterContext{Context: context.Background(), after: 8}
	if err := reader.RunAggregation(&cancelAfterContext{Context: context.Background(), after: 8}, "metrics", options); err == nil {
		t.Fatal("canceled aggregation unexpectedly succeeded")
	}
	if _, err := ReadPublishedAggregate(root, "metrics"); err != nil {
		t.Fatalf("old published version disappeared after cancellation: %v", err)
	}

	runRoot := filepath.Join(root, "aggregates", "metrics", "run")
	stateFiles, err := filepath.Glob(filepath.Join(runRoot, "segments", "*", "*.gob"))
	if err != nil || len(stateFiles) == 0 {
		t.Fatalf("no confirmed intermediate state found: files=%v err=%v", stateFiles, err)
	}
	if err := reader.ResumeAggregation(context.Background(), "metrics"); err != nil {
		t.Fatal(err)
	}
	resumed, err := ReadPublishedAggregate(root, "metrics")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Rows, resumed.Rows) {
		t.Fatalf("resumed rows = %#v, want %#v", resumed.Rows, first.Rows)
	}
	if pointer, err := readAggregatePointer(root, "metrics"); err != nil || pointer.Version != 2 {
		t.Fatalf("resumed pointer version = %#v, err = %v", pointer, err)
	}

	if err := reader.RunAggregation(canceled, "metrics", options); err == nil {
		t.Fatal("second canceled aggregation unexpectedly succeeded")
	}
	stateFiles, err = filepath.Glob(filepath.Join(runRoot, "segments", "*", "*.gob"))
	if err != nil || len(stateFiles) == 0 {
		t.Fatalf("no confirmed intermediate state found after second cancellation: files=%v err=%v", stateFiles, err)
	}
	if err := os.WriteFile(stateFiles[0], []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := reader.ResumeAggregation(context.Background(), "metrics"); err != nil {
		t.Fatal(err)
	}
	second, err := ReadPublishedAggregate(root, "metrics")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Rows, second.Rows) {
		t.Fatalf("rebuilt rows = %#v, want %#v", second.Rows, first.Rows)
	}
	oldPath := filepath.Join(root, "aggregates", "metrics", "versions", "0000000001", "result.cols")
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("old published version was removed: %v", err)
	}
	pointer, err := readAggregatePointer(root, "metrics")
	if err != nil {
		t.Fatal(err)
	}
	if pointer.Version != 3 {
		t.Fatalf("published version = %d, want 3", pointer.Version)
	}
}
