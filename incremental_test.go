package columnar

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func newIncrementalDataset(t *testing.T, root string) (*Writer, Schema) {
	t.Helper()
	schema, err := NewSchema([]Column{
		{Name: "g", Type: TypeString},
		{Name: "v", Type: TypeInt64},
	})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := Create(root, schema)
	if err != nil {
		t.Fatal(err)
	}
	return writer, schema
}

func flushSegment(t *testing.T, writer *Writer, id string, rows ...[]any) {
	t.Helper()
	for _, row := range rows {
		if err := writer.Append(row...); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.FlushAs(id); err != nil {
		t.Fatal(err)
	}
}

func incrementalJob(dataset, state string) IncrementalAggregationJob {
	return IncrementalAggregationJob{
		DatasetRoot: dataset,
		StateRoot:   state,
		Request: AggregateRequest{
			GroupBy:           []string{"g"},
			Aggs:              []AggregateSpec{{Column: "v", Func: AggSum, As: "total"}},
			MaxInMemoryGroups: 1,
		},
	}
}

func sortedIncrementalRows(rows [][]any) [][]any {
	sort.Slice(rows, func(i, j int) bool { return rows[i][0].(string) < rows[j][0].(string) })
	return rows
}

func statusMap(changes []GroupChange) map[string]string {
	result := make(map[string]string, len(changes))
	for _, change := range changes {
		result[change.Key[0].(string)] = change.Status
	}
	return result
}

func TestIncrementalPreviewPublishAndReadOldReleases(t *testing.T) {
	dataset, state := t.TempDir(), t.TempDir()
	writer, _ := newIncrementalDataset(t, dataset)
	flushSegment(t, writer, "s1", []any{"a", int64(1)}, []any{"a", int64(2)})
	flushSegment(t, writer, "s2", []any{"b", int64(10)})
	reader, err := OpenReader(dataset)
	if err != nil {
		t.Fatal(err)
	}
	job := incrementalJob(dataset, state)
	preview, err := reader.PreviewIncrementalAggregation(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if preview.BaseRelease != 0 || preview.Release != 1 {
		t.Fatalf("unexpected release numbers: %+v", preview)
	}
	ids := []string{preview.Changes[0].ID, preview.Changes[1].ID}
	if !reflect.DeepEqual(ids, []string{"s1", "s2"}) {
		t.Fatalf("changes = %+v", preview.Changes)
	}
	if !preview.Spilled {
		t.Fatal("expected hot-group spilling with a one-group memory budget")
	}
	release, err := reader.PublishIncrementalAggregation(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sortedIncrementalRows(release.Result.Rows), [][]any{{"a", int64(3)}, {"b", int64(10)}}) {
		t.Fatalf("first release = %#v", release.Result.Rows)
	}

	flushSegment(t, writer, "s3", []any{"a", int64(100)}, []any{"c", int64(7)})
	reader, _ = OpenReader(dataset)
	preview, err = reader.PreviewIncrementalAggregation(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if preview.BaseRelease != 1 || preview.Release != 2 || len(preview.Changes) != 1 || preview.Changes[0].ID != "s3" {
		t.Fatalf("second preview = %+v", preview)
	}
	statuses := statusMap(preview.AffectedGroups)
	if statuses["a"] != "updated" || statuses["c"] != "appeared" {
		t.Fatalf("affected groups = %+v", preview.AffectedGroups)
	}
	if _, exists := statuses["b"]; exists {
		t.Fatalf("unchanged group reported as affected: %+v", statuses)
	}
	if _, err := reader.PublishIncrementalAggregation(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	old, err := reader.IncrementalReleaseByNumber(job, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sortedIncrementalRows(old.Result.Rows), [][]any{{"a", int64(3)}, {"b", int64(10)}}) {
		t.Fatalf("old release changed: %#v", old.Result.Rows)
	}
	current, exists, err := reader.CurrentIncrementalRelease(job)
	if err != nil || !exists || current.Record.Number != 2 {
		t.Fatalf("current release = %+v exists=%v err=%v", current, exists, err)
	}
	if !reflect.DeepEqual(sortedIncrementalRows(current.Result.Rows), [][]any{{"a", int64(103)}, {"b", int64(10)}, {"c", int64(7)}}) {
		t.Fatalf("current release = %#v", current.Result.Rows)
	}
}

func TestIncrementalReplaceWithdrawCancelAndReopenIdempotent(t *testing.T) {
	dataset, state := t.TempDir(), t.TempDir()
	writer, _ := newIncrementalDataset(t, dataset)
	flushSegment(t, writer, "s1", []any{"a", int64(1)})
	flushSegment(t, writer, "s2", []any{"b", int64(5)}, []any{"c", int64(9)})
	reader, _ := OpenReader(dataset)
	job := incrementalJob(dataset, state)
	if _, err := reader.PublishIncrementalAggregation(context.Background(), job); err != nil {
		t.Fatal(err)
	}

	if err := writer.Append("b", int64(50)); err != nil {
		t.Fatal(err)
	}
	if err := writer.ReplaceSegment("s2"); err != nil {
		t.Fatal(err)
	}
	reader, _ = OpenReader(dataset)
	preview, err := reader.PreviewIncrementalAggregation(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if len(preview.Changes) != 1 || preview.Changes[0].Kind != SegmentReplaced || preview.Changes[0].OldRevision != 1 || preview.Changes[0].NewRevision != 2 {
		t.Fatalf("replace change = %+v", preview.Changes)
	}
	if !reflect.DeepEqual(sortedIncrementalRows(preview.Result.Rows), [][]any{{"a", int64(1)}, {"b", int64(50)}}) {
		t.Fatalf("replace preview = %#v", preview.Result.Rows)
	}
	statuses := statusMap(preview.AffectedGroups)
	if statuses["b"] != "updated" || statuses["c"] != "disappeared" {
		t.Fatalf("replace affected groups = %+v", preview.AffectedGroups)
	}
	if err := reader.CancelIncrementalAggregation(job); err != nil {
		t.Fatal(err)
	}
	current, exists, err := reader.CurrentIncrementalRelease(job)
	if err != nil || !exists || current.Record.Number != 1 {
		t.Fatalf("cancel changed current release: %+v %v %v", current, exists, err)
	}
	if !reflect.DeepEqual(sortedIncrementalRows(current.Result.Rows), [][]any{{"a", int64(1)}, {"b", int64(5)}, {"c", int64(9)}}) {
		t.Fatalf("cancel did not restore old result: %#v", current.Result.Rows)
	}
	if _, err := os.Stat(filepath.Join(state, "stage")); !os.IsNotExist(err) {
		t.Fatalf("stage left behind after cancel: %v", err)
	}

	if err := writer.WithdrawSegment("s2"); err != nil {
		t.Fatal(err)
	}
	reader, _ = OpenReader(dataset)
	preview, err = reader.PreviewIncrementalAggregation(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if len(preview.Changes) != 1 || preview.Changes[0].Kind != SegmentWithdrawn || preview.Changes[0].ID != "s2" {
		t.Fatalf("withdraw change = %+v", preview.Changes)
	}
	statuses = statusMap(preview.AffectedGroups)
	if statuses["b"] != "disappeared" || statuses["c"] != "disappeared" {
		t.Fatalf("withdraw groups = %+v", preview.AffectedGroups)
	}
	if !reflect.DeepEqual(preview.Result.Rows, [][]any{{"a", int64(1)}}) {
		t.Fatalf("withdraw preview = %#v", preview.Result.Rows)
	}
	if _, err := reader.PublishIncrementalAggregation(context.Background(), job); err != nil {
		t.Fatal(err)
	}

	flushSegment(t, writer, "s2", []any{"a", int64(2)})
	reopened, err := OpenReader(dataset)
	if err != nil {
		t.Fatal(err)
	}
	firstPreview, err := reopened.PreviewIncrementalAggregation(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	secondPreview, err := reopened.PreviewIncrementalAggregation(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(secondPreview.Result.Rows, firstPreview.Result.Rows) {
		t.Fatalf("repeated preview is not idempotent: %#v vs %#v", firstPreview.Result.Rows, secondPreview.Result.Rows)
	}
	releaseOne, err := reopened.PublishIncrementalAggregation(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	releaseTwo, err := reopened.PublishIncrementalAggregation(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(releaseOne.Result.Rows, releaseTwo.Result.Rows) || releaseTwo.Record.Number != releaseOne.Record.Number {
		t.Fatalf("repeated publish created a new release: %+v %+v", releaseOne.Record, releaseTwo.Record)
	}
	if !reflect.DeepEqual(releaseTwo.Result.Rows, [][]any{{"a", int64(3)}}) {
		t.Fatalf("re-append after withdraw = %#v", releaseTwo.Result.Rows)
	}
}

func TestIncrementalOneShotReferenceMatchesBoundedSpill(t *testing.T) {
	dataset, state := t.TempDir(), t.TempDir()
	writer, _ := newIncrementalDataset(t, dataset)
	groups := []string{"a", "b", "c", "d", "e"}
	for segmentIndex, group := range groups {
		for i := 0; i < 3; i++ {
			if err := writer.Append(group, int64(segmentIndex*10+i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	reader, _ := OpenReader(dataset)
	job := incrementalJob(dataset, state)
	release, err := reader.PublishIncrementalAggregation(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := reader.Aggregate(AggregateRequest{
		GroupBy:   []string{"g"},
		Aggs:      []AggregateSpec{{Column: "v", Func: AggSum, As: "total"}},
		Reference: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sortedIncrementalRows(release.Result.Rows), sortedIncrementalRows(reference.Rows)) {
		t.Fatalf("incremental %#v != reference %#v", release.Result.Rows, reference.Rows)
	}
}

func TestIncrementalPreviewAcrossCloseAndReopenIsIdempotent(t *testing.T) {
	dataset, state := t.TempDir(), t.TempDir()
	writer, _ := newIncrementalDataset(t, dataset)
	flushSegment(t, writer, "s1", []any{"a", int64(1)})
	reader, _ := OpenReader(dataset)
	job := incrementalJob(dataset, state)
	if _, err := reader.PublishIncrementalAggregation(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	flushSegment(t, writer, "s2", []any{"a", int64(2)}, []any{"b", int64(4)})

	firstReader, err := OpenReader(dataset)
	if err != nil {
		t.Fatal(err)
	}
	preview, err := firstReader.PreviewIncrementalAggregation(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sortedIncrementalRows(preview.Result.Rows), [][]any{{"a", int64(3)}, {"b", int64(4)}}) {
		t.Fatalf("preview = %#v", preview.Result.Rows)
	}
	reopened, err := OpenReader(dataset)
	if err != nil {
		t.Fatal(err)
	}
	repreview, err := reopened.PreviewIncrementalAggregation(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(repreview.Result.Rows, preview.Result.Rows) || repreview.Release != preview.Release {
		t.Fatalf("preview changed after reopen: %#v vs %#v", repreview.Result.Rows, preview.Result.Rows)
	}
	release, err := reopened.PublishIncrementalAggregation(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	again, err := reopened.PublishIncrementalAggregation(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if again.Record.Number != release.Record.Number || !reflect.DeepEqual(again.Result.Rows, release.Result.Rows) {
		t.Fatalf("publish after reopen was not idempotent")
	}
}
