package columnar

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func TestGroupingKeepsZeroNullAndEmptyStringDistinct(t *testing.T) {
	root := t.TempDir()
	schema, err := NewSchema([]Column{
		{Name: "g", Type: TypeInt64},
		{Name: "v", Type: TypeFloat64},
	})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := Create(root, schema)
	if err != nil {
		t.Fatal(err)
	}
	rows := [][]any{
		{int64(0), 1.0},
		{int64(0), 2.0},
		{nil, 3.0},
		{nil, nil},
	}
	for i, row := range rows {
		if err := writer.Append(row...); err != nil {
			t.Fatal(err)
		}
		if i == 1 {
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
	request := AggregateRequest{
		GroupBy:           []string{"g"},
		Aggs:              []AggregateSpec{{Func: AggCountStar, As: "n"}, {Column: "v", Func: AggCount, As: "c"}, {Column: "v", Func: AggAvg, As: "a"}},
		MaxInMemoryGroups: 1,
		SpillDirectory:    t.TempDir(),
	}
	got, err := reader.Aggregate(request)
	if err != nil {
		t.Fatal(err)
	}
	want, err := reader.Aggregate(AggregateRequest{GroupBy: request.GroupBy, Aggs: request.Aggs, Reference: true})
	if err != nil {
		t.Fatal(err)
	}
	sortRowsByKey(got.Rows)
	sortRowsByKey(want.Rows)
	if !reflect.DeepEqual(got.Rows, want.Rows) {
		t.Fatalf("spilled %#v != reference %#v", got.Rows, want.Rows)
	}
	if !reflect.DeepEqual(got.Rows, [][]any{
		{nil, int64(2), int64(1), 3.0},
		{int64(0), int64(2), int64(2), 1.5},
	}) {
		t.Fatalf("zero and null groups collapsed: %#v", got.Rows)
	}
}

func TestGlobalAggregateWithoutGroupBy(t *testing.T) {
	root := t.TempDir()
	writeAggregateDataset(t, root, 2)
	reader, err := OpenReader(root)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Aggregate(AggregateRequest{
		Aggs: []AggregateSpec{
			{Func: AggCountStar, As: "all"},
			{Column: "value", Func: AggCount, As: "c"},
			{Column: "value", Func: AggSum, As: "s"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Rows, [][]any{{int64(8), int64(6), int64(13)}}) {
		t.Fatalf("global aggregate = %#v", result.Rows)
	}
}

func TestHighCardinalitySpillMatchesReference(t *testing.T) {
	root := t.TempDir()
	schema, err := NewSchema([]Column{{Name: "g", Type: TypeInt64}, {Name: "v", Type: TypeInt64}})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := Create(root, schema)
	if err != nil {
		t.Fatal(err)
	}
	random := rand.New(rand.NewSource(7))
	for i := 0; i < 500; i++ {
		group := int64(random.Intn(130))
		var value any
		if random.Intn(5) == 0 {
			value = nil
		} else {
			value = random.Int63n(1000) - 500
		}
		if err := writer.Append(group, value); err != nil {
			t.Fatal(err)
		}
		if i%37 == 36 {
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
	aggs := []AggregateSpec{
		{Func: AggCountStar, As: "n"},
		{Column: "v", Func: AggCount, As: "c"},
		{Column: "v", Func: AggSum, As: "s"},
		{Column: "v", Func: AggMin, As: "lo"},
		{Column: "v", Func: AggMax, As: "hi"},
	}
	spilled, err := reader.Aggregate(AggregateRequest{GroupBy: []string{"g"}, Aggs: aggs, MaxInMemoryGroups: 16, SpillDirectory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	reference, err := reader.Aggregate(AggregateRequest{GroupBy: []string{"g"}, Aggs: aggs, Reference: true})
	if err != nil {
		t.Fatal(err)
	}
	sortRowsByKey(spilled.Rows)
	sortRowsByKey(reference.Rows)
	if !reflect.DeepEqual(spilled.Rows, reference.Rows) {
		t.Fatalf("spilled aggregation disagrees with reference")
	}
	if !spilled.Spilled {
		t.Fatal("expected spills with 130 groups and a 16 group budget")
	}
}

func TestCancellationMidwayResumesWithoutDoubleCounting(t *testing.T) {
	dataset := t.TempDir()
	state := t.TempDir()
	writeAggregateDataset(t, dataset, 2)
	reader, err := OpenReader(dataset)
	if err != nil {
		t.Fatal(err)
	}
	job := AggregationJob{
		DatasetRoot: dataset,
		StateRoot:   state,
		Request: AggregateRequest{
			GroupBy:           []string{"group"},
			Aggs:              []AggregateSpec{{Column: "value", Func: AggSum, As: "total"}},
			MaxInMemoryGroups: 1,
		},
	}
	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()
	progress, err := reader.RunAggregationJob(cancelledContext, job)
	if err != context.Canceled || progress.Complete {
		t.Fatalf("cancelled run = %+v %v", progress, err)
	}
	resumed, err := reader.ResumeAggregationJob(context.Background(), job)
	if err != nil || !resumed.Complete {
		t.Fatalf("resume = %+v %v", resumed, err)
	}
	final, err := reader.Aggregate(AggregateRequest{
		GroupBy:   []string{"group"},
		Aggs:      []AggregateSpec{{Column: "value", Func: AggSum, As: "total"}},
		Reference: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	sortRowsByKey(resumed.Result.Rows)
	sortRowsByKey(final.Rows)
	if !reflect.DeepEqual(resumed.Result.Rows, final.Rows) {
		t.Fatalf("resumed %#v != one-shot reference %#v", resumed.Result.Rows, final.Rows)
	}
	if _, err := os.Stat(filepath.Join(state, resumed.Checkpoint.ResultPath)); err != nil {
		t.Fatalf("published result missing: %v", err)
	}
}

func sortRowsByKey(rows [][]any) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i][0] == nil {
			return rows[j][0] != nil
		}
		if rows[j][0] == nil {
			return false
		}
		switch left := rows[i][0].(type) {
		case int64:
			return left < rows[j][0].(int64)
		case string:
			return left < rows[j][0].(string)
		default:
			return false
		}
	})
}
