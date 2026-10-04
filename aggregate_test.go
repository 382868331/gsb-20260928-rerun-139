package columnar

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func aggregateTestSchema(t *testing.T) Schema {
	t.Helper()
	schema, err := NewSchema([]Column{
		{Name: "group", Type: TypeString},
		{Name: "value", Type: TypeInt64},
		{Name: "unused", Type: TypeBool},
	})
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func writeAggregateDataset(t *testing.T, root string, rowsPerGroup int) {
	t.Helper()
	schema := aggregateTestSchema(t)
	writer, err := Create(root, schema)
	if err != nil {
		t.Fatal(err)
	}
	groups := []string{"a", "", "b", "a"}
	for segmentIndex, group := range groups {
		for i := 0; i < rowsPerGroup; i++ {
			var value any
			if segmentIndex == 1 {
				value = nil
			} else {
				value = int64(segmentIndex + i)
			}
			if err := writer.Append(group, value, true); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func sortedRows(rows [][]any) {
	sort.Slice(rows, func(i, j int) bool { return rows[i][0].(string) < rows[j][0].(string) })
}

func TestAggregateMatchesReferenceAndKeepsNullAndEmptyStringDistinct(t *testing.T) {
	root := t.TempDir()
	writeAggregateDataset(t, root, 3)
	reader, err := OpenReader(root)
	if err != nil {
		t.Fatal(err)
	}
	request := AggregateRequest{
		GroupBy: []string{"group"},
		Aggs: []AggregateSpec{
			{Func: AggCountStar, As: "all"},
			{Column: "value", Func: AggCount, As: "non_null"},
			{Column: "value", Func: AggSum, As: "total"},
			{Column: "value", Func: AggMin, As: "low"},
			{Column: "value", Func: AggMax, As: "high"},
		},
		MaxInMemoryGroups: 1,
		SpillDirectory:    t.TempDir(),
	}
	got, err := reader.Aggregate(request)
	if err != nil {
		t.Fatal(err)
	}
	want, err := reader.Aggregate(AggregateRequest{
		GroupBy:   request.GroupBy,
		Aggs:      request.Aggs,
		Reference: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	sortedRows(got.Rows)
	sortedRows(want.Rows)
	if !reflect.DeepEqual(got.Rows, want.Rows) {
		t.Fatalf("spilled %#v != reference %#v", got.Rows, want.Rows)
	}
	if !got.Spilled {
		t.Fatal("expected high-cardinality aggregation to spill")
	}
	if !reflect.DeepEqual(got.ColumnsRead, []string{"group", "value"}) {
		t.Fatalf("columns read = %#v", got.ColumnsRead)
	}
	if len(got.Rows) != 3 {
		t.Fatalf("empty string and null-group rows produced %d groups", len(got.Rows))
	}
	emptyRow := got.Rows[0]
	if emptyRow[0] != "" || emptyRow[1] != int64(3) || emptyRow[2] != int64(0) || emptyRow[3] != nil || emptyRow[4] != nil {
		t.Fatalf("empty group aggregate = %#v", emptyRow)
	}
}

func TestSingleHotGroupDoesNotSpillAcrossSegments(t *testing.T) {
	root := t.TempDir()
	schema, err := NewSchema([]Column{{Name: "g", Type: TypeInt64}, {Name: "v", Type: TypeInt64}})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := Create(root, schema)
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 20; i++ {
		if err := writer.Append(int64(1), i); err != nil {
			t.Fatal(err)
		}
		if i%5 == 4 {
			if err := writer.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReader(root)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Aggregate(AggregateRequest{
		GroupBy:           []string{"g"},
		Aggs:              []AggregateSpec{{Column: "v", Func: AggSum, As: "total"}},
		MaxInMemoryGroups: 1,
		SpillDirectory:    t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Spilled {
		t.Fatal("repeated hot group caused a spill")
	}
	if !reflect.DeepEqual(result.Rows, [][]any{{int64(1), int64(190)}}) {
		t.Fatalf("rows = %#v", result.Rows)
	}
}

func TestAggregationJobResumeDoesNotReconfirmSegmentsAfterCancellation(t *testing.T) {
	dataset := t.TempDir()
	state := t.TempDir()
	writeAggregateDataset(t, dataset, 1)
	reader, err := OpenReader(dataset)
	if err != nil {
		t.Fatal(err)
	}
	request := AggregateRequest{
		GroupBy:           []string{"group"},
		Aggs:              []AggregateSpec{{Column: "value", Func: AggSum, As: "total"}},
		MaxInMemoryGroups: 1,
	}
	job := AggregationJob{DatasetRoot: dataset, StateRoot: state, Request: request}
	completed, err := reader.RunAggregationJob(context.Background(), job)
	if err != nil || !completed.Complete {
		t.Fatalf("initial completion: %+v %v", completed, err)
	}
	oldResult := filepath.Join(state, completed.Checkpoint.ResultPath)
	oldData, err := os.ReadFile(oldResult)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := OpenWriter(dataset)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Append("a", int64(100), true); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	cancelContext, cancel := context.WithCancel(context.Background())
	cancel()
	cancelled, err := reader.ResumeAggregationJob(cancelContext, job)
	if err != context.Canceled || cancelled.Complete {
		t.Fatalf("cancellation = %+v %v", cancelled, err)
	}
	if !reflect.DeepEqual(cancelled.Result.Rows, completed.Result.Rows) {
		t.Fatal("old published aggregate was not retained on cancellation")
	}
	newData, readErr := os.ReadFile(oldResult)
	if readErr != nil || !reflect.DeepEqual(newData, oldData) {
		t.Fatal("old result file changed during canceled update")
	}
	resumed, err := reader.ResumeAggregationJob(context.Background(), job)
	if err != nil || !resumed.Complete {
		t.Fatalf("resume = %+v %v", resumed, err)
	}
	if filepath.Join(state, resumed.Checkpoint.ResultPath) == oldResult {
		t.Fatal("completed update did not publish a new result version")
	}
	var updated []any
	for _, row := range resumed.Result.Rows {
		if row[0] == "a" {
			updated = row
		}
	}
	if !reflect.DeepEqual(updated, []any{"a", int64(103)}) {
		t.Fatalf("updated a group = %#v, all = %#v", updated, resumed.Result.Rows)
	}
}

func TestCorruptConfirmedAggregateStateLeavesOldPublishedResultReadable(t *testing.T) {
	dataset := t.TempDir()
	state := t.TempDir()
	writeAggregateDataset(t, dataset, 1)
	reader, err := OpenReader(dataset)
	if err != nil {
		t.Fatal(err)
	}
	job := AggregationJob{
		DatasetRoot: dataset,
		StateRoot:   state,
		Request: AggregateRequest{
			GroupBy: []string{"group"},
			Aggs:    []AggregateSpec{{Column: "value", Func: AggSum, As: "total"}},
		},
	}
	completed, err := reader.RunAggregationJob(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, filepath.FromSlash(completed.Checkpoint.StatePath)), []byte{0}, 0o644); err != nil {
		t.Fatal(err)
	}
	writer, err := OpenWriter(dataset)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Append("z", int64(1), true); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = reader.ResumeAggregationJob(context.Background(), job)
	if err == nil {
		t.Fatal("corrupt aggregate state was silently accepted")
	}
	old, _, err := readAggregationCheckpoint(filepath.Join(state, "checkpoint.json"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = readPublishedAggregate(filepath.Join(state, old.ResultPath), old.ResultSize, old.ResultCRC32)
	if err != nil {
		t.Fatalf("old published result became unreadable: %v", err)
	}
}
