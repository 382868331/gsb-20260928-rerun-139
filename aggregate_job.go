package columnar

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

const aggregateManifestVersion = 1

type aggregateJobManifest struct {
	Version    int
	Options    AggregateOptions
	Filters    []Predicate
	Columns    []Column
	Completed  []completedSegment
	ResultSize int64
	ResultCRC  uint32
}

type completedSegment struct {
	ID    string
	Files []aggregateStateFile
}

type aggregateStateFile struct {
	Path  string
	Size  int64
	CRC32 uint32
}

type aggregatePointer struct {
	Version int
	Result  string
}

func (r *Reader) RunAggregation(ctx context.Context, name string, options AggregateOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateAggregateName(name); err != nil {
		return err
	}
	current, err := r.store.readManifest()
	if err != nil {
		return err
	}
	plan, _, err := buildAggregationPlan(r.schema, options)
	if err != nil {
		return err
	}
	runRoot := filepath.Join(r.store.root, "aggregates", name, "run")
	if err := os.RemoveAll(runRoot); err != nil {
		return err
	}
	if err := os.MkdirAll(runRoot, 0o755); err != nil {
		return err
	}
	var filters []Predicate
	if options.Filter != nil {
		filters = append(filters, options.Filter.predicates...)
	}
	job := aggregateJobManifest{Version: aggregateManifestVersion, Options: options, Filters: filters, Columns: r.schema.Columns}
	if err := writeJSON(filepath.Join(runRoot, "job.json.tmp"), job); err != nil {
		return err
	}
	if err := processAggregationSegments(ctx, r, current, plan, options, runRoot, &job); err != nil {
		return err
	}
	rows, err := mergeAggregationRun(ctx, plan, runRoot, &job)
	if err != nil {
		return err
	}
	resultBytes, err := encodeAggregateRows(plan, rows)
	if err != nil {
		return err
	}
	job.ResultSize = int64(len(resultBytes))
	job.ResultCRC = crc32Checksum(resultBytes)
	if err := writeJSON(filepath.Join(runRoot, "job.json.tmp"), job); err != nil {
		return err
	}
	version := nextAggregateVersion(r.store.root, name)
	versionDir := fmt.Sprintf("%010d", version)
	versionRoot := filepath.Join(r.store.root, "aggregates", name, "versions", versionDir)
	if err := os.MkdirAll(versionRoot, 0o755); err != nil {
		return err
	}
	if err := atomicWriteFile(filepath.Join(versionRoot, "result.cols"), resultBytes, true); err != nil {
		return err
	}
	pointer := aggregatePointer{Version: version, Result: filepath.ToSlash(filepath.Join("aggregates", name, "versions", versionDir, "result.cols"))}
	if err := publishAggregatePointer(r.store.root, name, pointer); err != nil {
		return err
	}
	if err := os.RemoveAll(runRoot); err != nil {
		return err
	}
	return syncDir(filepath.Join(r.store.root, "aggregates", name))
}

func (r *Reader) ResumeAggregation(ctx context.Context, name string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateAggregateName(name); err != nil {
		return err
	}
	current, err := r.store.readManifest()
	if err != nil {
		return err
	}
	runRoot := filepath.Join(r.store.root, "aggregates", name, "run")
	job, err := readAggregateJob(filepath.Join(runRoot, "job.json.tmp"))
	if err != nil {
		return err
	}
	plan, _, err := buildAggregationPlan(r.schema, job.Options)
	if err != nil {
		return err
	}
	if len(job.Filters) > 0 {
		filter, err := NewFilter(job.Filters...)
		if err != nil {
			return err
		}
		job.Options.Filter = filter
	}
	if err := validateCompletedSegments(r.store.root, runRoot, job.Completed); err != nil {
		if err := os.RemoveAll(runRoot); err != nil {
			return err
		}
		return r.RunAggregation(ctx, name, job.Options)
	}
	if err := processAggregationSegments(ctx, r, current, plan, job.Options, runRoot, &job); err != nil {
		return err
	}
	rows, err := mergeAggregationRun(ctx, plan, runRoot, &job)
	if err != nil {
		return err
	}
	resultBytes, err := encodeAggregateRows(plan, rows)
	if err != nil {
		return err
	}
	job.ResultSize = int64(len(resultBytes))
	job.ResultCRC = crc32Checksum(resultBytes)
	if err := writeJSON(filepath.Join(runRoot, "job.json.tmp"), job); err != nil {
		return err
	}
	version := nextAggregateVersion(r.store.root, name)
	versionDir := fmt.Sprintf("%010d", version)
	versionRoot := filepath.Join(r.store.root, "aggregates", name, "versions", versionDir)
	if err := os.MkdirAll(versionRoot, 0o755); err != nil {
		return err
	}
	if err := atomicWriteFile(filepath.Join(versionRoot, "result.cols"), resultBytes, true); err != nil {
		return err
	}
	pointer := aggregatePointer{Version: version, Result: filepath.ToSlash(filepath.Join("aggregates", name, "versions", versionDir, "result.cols"))}
	return publishAggregatePointer(r.store.root, name, pointer)
}

func ReadPublishedAggregate(root string, name string) (AggregateResult, error) {
	if err := validateAggregateName(name); err != nil {
		return AggregateResult{}, err
	}
	pointer, err := readAggregatePointer(root, name)
	if err != nil {
		return AggregateResult{}, err
	}
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(pointer.Result)))
	if err != nil {
		return AggregateResult{}, err
	}
	return decodeAggregateRows(data)
}

func processAggregationSegments(ctx context.Context, r *Reader, current manifest, plan aggregationPlan, options AggregateOptions, runRoot string, job *aggregateJobManifest) error {
	completed := make(map[string]struct{}, len(job.Completed))
	for _, segment := range job.Completed {
		completed[segment.ID] = struct{}{}
	}
	predicates, err := options.Filter.bind(r.schema)
	if err != nil {
		return err
	}
	neededIndexes := neededIndexesForPlan(plan, predicates)
	for _, segment := range current.Segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, ok := completed[segment.ID]; ok {
			continue
		}
		segmentTempRoot := filepath.Join(runRoot, "segments", segment.ID+".tmp")
		segmentFinalRoot := filepath.Join(runRoot, "segments", segment.ID)
		if err := os.RemoveAll(segmentTempRoot); err != nil {
			return err
		}
		if err := os.RemoveAll(segmentFinalRoot); err != nil {
			return err
		}
		if !options.Reference && segmentCanSkip(current.Columns, segment, predicates) {
			job.Completed = append(job.Completed, completedSegment{ID: segment.ID})
			if err := writeJSON(filepath.Join(runRoot, "job.json.tmp"), *job); err != nil {
				return err
			}
			continue
		}
		columns, err := r.readSegmentColumns(current, segment, neededIndexes)
		if err != nil {
			return fmt.Errorf("segment %s: %w", segment.ID, err)
		}
		segmentRoot := segmentTempRoot
		if err := os.MkdirAll(segmentRoot, 0o755); err != nil {
			return err
		}
		agg := newHashAggregator(plan, plan.maxGroups, segmentRoot)
		agg.spillRoot = segmentRoot
		agg.spilled = true
		for rowIndex := uint64(0); rowIndex < segment.Rows; rowIndex++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			row := make([]any, len(current.Columns))
			for columnIndex, values := range columns {
				row[columnIndex] = values[rowIndex]
			}
			if !matches(row, predicates) {
				continue
			}
			if err := agg.addRow(row); err != nil {
				return err
			}
		}
		if err := agg.flush(); err != nil {
			return err
		}
		if err := agg.closeAndSyncFiles(); err != nil {
			return err
		}
		if err := syncDir(segmentRoot); err != nil {
			return err
		}
		files, err := collectStateFiles(segmentRoot, filepath.Join("segments", segment.ID+".tmp"))
		if err != nil {
			return err
		}
		finalRoot := filepath.Join(runRoot, "segments", segment.ID)
		if err := os.Rename(segmentRoot, finalRoot); err != nil {
			return err
		}
		if err := syncDir(filepath.Join(runRoot, "segments")); err != nil {
			return err
		}
		for i := range files {
			files[i].Path = filepath.Join("segments", segment.ID, filepath.Base(files[i].Path))
		}
		job.Completed = append(job.Completed, completedSegment{ID: segment.ID, Files: files})
		sort.Slice(job.Completed, func(i, j int) bool { return job.Completed[i].ID < job.Completed[j].ID })
		if err := writeJSON(filepath.Join(runRoot, "job.json.tmp"), *job); err != nil {
			return err
		}
	}
	return nil
}

func neededIndexesForPlan(plan aggregationPlan, predicates []boundPredicate) []int {
	needed := map[int]struct{}{}
	for _, index := range plan.groupIndexes {
		needed[index] = struct{}{}
	}
	for _, resolved := range plan.specs {
		if resolved.hasColumn {
			needed[resolved.columnIndex] = struct{}{}
		}
	}
	for _, predicate := range predicates {
		needed[predicate.column] = struct{}{}
	}
	indexes := make([]int, 0, len(needed))
	for index := range needed {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	return indexes
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return atomicWriteFile(path, data, true)
}

func validateAggregateName(name string) error {
	if name == "" {
		return fmt.Errorf("aggregate name is empty")
	}
	if name == "." || name == ".." || filepath.Base(name) != name {
		return fmt.Errorf("invalid aggregate name %q", name)
	}
	return nil
}

func schemasEqual(left Schema, right Schema) bool {
	if len(left.Columns) != len(right.Columns) {
		return false
	}
	for i := range left.Columns {
		if left.Columns[i] != right.Columns[i] {
			return false
		}
	}
	return true
}

func publishAggregatePointer(root string, name string, pointer aggregatePointer) error {
	directory := filepath.Join(root, "aggregates", name)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(pointer, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tempPath := filepath.Join(directory, "current.json.tmp")
	if err := os.WriteFile(tempPath, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tempPath, filepath.Join(directory, "current.json")); err != nil {
		return err
	}
	return syncDir(directory)
}
