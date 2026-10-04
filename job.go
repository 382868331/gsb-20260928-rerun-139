package columnar

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type AggregationJob struct {
	DatasetRoot string
	StateRoot   string
	Request     AggregateRequest
}

type AggregationCheckpoint struct {
	Version           int              `json:"version"`
	DatasetRoot       string           `json:"dataset_root"`
	Request           AggregateRequest `json:"request"`
	ConfirmedSegments []string         `json:"confirmed_segments"`
	Complete          bool             `json:"complete"`
	StatePath         string           `json:"state_path"`
	StateFrames       uint32           `json:"state_frames"`
	ResultPath        string           `json:"result_path"`
	ResultRows        uint64           `json:"result_rows"`
	ResultCRC32       uint32           `json:"result_crc32"`
	ResultSize        int64            `json:"result_size"`
}

type PublishedAggregate struct {
	Schema Schema
	Rows   [][]any
}

type AggregationProgress struct {
	Checkpoint AggregationCheckpoint
	Result     PublishedAggregate
	Complete   bool
}

const aggregationCheckpointVersion = 1

func (job AggregationJob) stateRoot() string {
	if job.StateRoot != "" {
		return filepath.Clean(job.StateRoot)
	}
	return filepath.Join(filepath.Clean(job.DatasetRoot), "aggregate-state", "default")
}

func (job AggregationJob) checkpointPath() string {
	return filepath.Join(job.stateRoot(), "checkpoint.json")
}

func (r *Reader) RunAggregationJob(ctx context.Context, job AggregationJob) (AggregationProgress, error) {
	return r.ResumeAggregationJob(ctx, job)
}

func (r *Reader) ResumeAggregationJob(ctx context.Context, job AggregationJob) (progress AggregationProgress, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if job.DatasetRoot == "" {
		job.DatasetRoot = r.store.root
	}
	stateRoot := job.stateRoot()
	if err := os.MkdirAll(filepath.Join(stateRoot, "runs"), 0o755); err != nil {
		return AggregationProgress{}, err
	}
	checkpoint, checkpointExists, err := readAggregationCheckpoint(job.checkpointPath())
	if err != nil {
		return AggregationProgress{}, err
	}
	if checkpointExists {
		if checkpoint.DatasetRoot != "" && filepath.Clean(checkpoint.DatasetRoot) != filepath.Clean(job.DatasetRoot) {
			return AggregationProgress{}, fmt.Errorf("aggregate checkpoint belongs to dataset %q", checkpoint.DatasetRoot)
		}
		job.Request = checkpoint.Request
		progress.Checkpoint = checkpoint
		if checkpoint.ResultPath != "" {
			result, readErr := readPublishedAggregate(filepath.Join(stateRoot, filepath.FromSlash(checkpoint.ResultPath)), checkpoint.ResultSize, checkpoint.ResultCRC32)
			if readErr != nil {
				return AggregationProgress{}, readErr
			}
			progress.Result = result
		}
	}
	current, err := r.store.readManifest()
	if err != nil {
		return AggregationProgress{}, err
	}
	groupSchema, groupIndexes, aggs, predicates, outputSchema, err := bindAggregateRequest(r.schema, job.Request)
	if err != nil {
		return AggregationProgress{}, err
	}
	confirmed := make(map[string]struct{}, len(checkpoint.ConfirmedSegments))
	for _, id := range checkpoint.ConfirmedSegments {
		confirmed[id] = struct{}{}
	}
	readIndexes := requiredColumns(groupIndexes, aggs, predicates)
	if job.Request.Reference {
		readIndexes = make([]int, len(current.Columns))
		for i := range readIndexes {
			readIndexes[i] = i
		}
	}
	spillDir := job.Request.SpillDirectory
	if spillDir == "" {
		spillDir, err = os.MkdirTemp(filepath.Join(stateRoot, "runs"), "scan-")
		if err != nil {
			return AggregationProgress{}, err
		}
		defer func() { _ = os.RemoveAll(spillDir) }()
	}
	engine := newAggEngine(groupSchema, groupIndexes, aggs, job.Request.MaxInMemoryGroups, spillDir)
	engine.ctx = ctx
	if checkpoint.StatePath != "" {
		path := filepath.Join(stateRoot, filepath.FromSlash(checkpoint.StatePath))
		if _, err := openRunReader(path, checkpoint.StateFrames); err != nil {
			return AggregationProgress{}, fmt.Errorf("confirmed aggregate state is corrupted: %w", err)
		}
		engine.runs = append(engine.runs, spilledRun{path: path, frames: checkpoint.StateFrames})
	}
	hasNewSegments := false
	for _, segment := range current.Segments {
		if _, ok := confirmed[segment.ID]; !ok {
			hasNewSegments = true
			break
		}
	}
	if hasNewSegments && checkpoint.Complete {
		checkpoint.Complete = false
		checkpoint.ResultPath = ""
		checkpoint.ResultRows = 0
		checkpoint.ResultCRC32 = 0
		checkpoint.ResultSize = 0
		progress.Checkpoint = checkpoint
	}
	for _, segment := range current.Segments {
		if _, ok := confirmed[segment.ID]; ok {
			continue
		}
		if err := ctx.Err(); err != nil {
			return progress, err
		}
		if !job.Request.Reference && segmentCanSkip(current.Columns, segment, predicates) {
			next := checkpoint
			next.Version = aggregationCheckpointVersion
			next.DatasetRoot = job.DatasetRoot
			next.Request = job.Request
			next.ConfirmedSegments = append(append([]string(nil), checkpoint.ConfirmedSegments...), segment.ID)
			if err := writeAggregationCheckpoint(job.checkpointPath(), next); err != nil {
				return progress, err
			}
			checkpoint = next
			continue
		}
		columns, err := r.readSegmentColumns(current, segment, readIndexes)
		if err != nil {
			return progress, fmt.Errorf("segment %s: %w", segment.ID, err)
		}
		for rowIndex := uint64(0); rowIndex < segment.Rows; rowIndex++ {
			rowColumns := make([]any, len(current.Columns))
			for columnIndex, values := range columns {
				rowColumns[columnIndex] = values[rowIndex]
			}
			if err := engine.addRow(rowColumns, predicates); err != nil {
				return progress, err
			}
		}
		stateRelative, stateFrames, err := engine.snapshotTo(stateRoot, len(checkpoint.ConfirmedSegments)+1)
		if err != nil {
			return progress, err
		}
		next := checkpoint
		next.Version = aggregationCheckpointVersion
		next.DatasetRoot = job.DatasetRoot
		next.Request = job.Request
		next.ConfirmedSegments = append(append([]string(nil), checkpoint.ConfirmedSegments...), segment.ID)
		next.StatePath = stateRelative
		next.StateFrames = stateFrames
		if err := writeAggregationCheckpoint(job.checkpointPath(), next); err != nil {
			return progress, err
		}
		pruneStaleStateFiles(stateRoot, next.StatePath)
		checkpoint = next
		confirmed[segment.ID] = struct{}{}
	}
	if len(checkpoint.ConfirmedSegments) != len(current.Segments) {
		if err := ctx.Err(); err != nil {
			return progress, err
		}
		return progress, context.Canceled
	}
	if checkpoint.Complete && len(checkpoint.ConfirmedSegments) == len(current.Segments) {
		result, err := readPublishedAggregate(filepath.Join(stateRoot, filepath.FromSlash(checkpoint.ResultPath)), checkpoint.ResultSize, checkpoint.ResultCRC32)
		if err != nil {
			return AggregationProgress{}, err
		}
		return AggregationProgress{Checkpoint: checkpoint, Result: result, Complete: true}, nil
	}
	stateRelative, stateFrames, err := engine.snapshotTo(stateRoot, len(current.Segments))
	if err != nil {
		return progress, err
	}
	resultRelative, err := nextResultRelative(stateRoot, checkpoint.ResultPath)
	if err != nil {
		return progress, err
	}
	resultPath := filepath.Join(stateRoot, filepath.FromSlash(resultRelative))
	writer, err := newAggregateResultWriter(resultPath, outputSchema)
	if err != nil {
		return progress, err
	}
	_, err = engine.finishRows(writer.writeRow)
	if err != nil {
		writer.close()
		return progress, err
	}
	size, checksum, rows, err := writer.finish()
	if err != nil {
		return progress, err
	}
	published := checkpoint
	published.Version = aggregationCheckpointVersion
	published.DatasetRoot = job.DatasetRoot
	published.Request = job.Request
	published.ConfirmedSegments = segmentIDs(current)
	published.Complete = true
	published.StatePath = stateRelative
	published.StateFrames = stateFrames
	published.ResultPath = resultRelative
	published.ResultRows = rows
	published.ResultCRC32 = checksum
	published.ResultSize = size
	if err := writeAggregationCheckpoint(filepath.Join(stateRoot, "checkpoint.next.json"), published); err != nil {
		return progress, err
	}
	if err := os.Rename(filepath.Join(stateRoot, "checkpoint.next.json"), job.checkpointPath()); err != nil {
		return progress, err
	}
	pruneStaleStateFiles(stateRoot, published.StatePath)
	result, err := readPublishedAggregate(resultPath, size, checksum)
	if err != nil {
		return progress, err
	}
	return AggregationProgress{Checkpoint: published, Result: result, Complete: true}, nil
}

func nextResultRelative(stateRoot string, previous string) (string, error) {
	version := 1
	if previous != "" {
		base := filepath.Base(filepath.FromSlash(previous))
		if len(base) == len("result-000001.agr") && strings.HasPrefix(base, "result-") && strings.HasSuffix(base, ".agr") {
			if parsed, err := strconv.Atoi(base[len("result-") : len(base)-len(".agr")]); err == nil && parsed > 0 {
				version = parsed + 1
			}
		}
	}
	entries, err := os.ReadDir(filepath.Join(stateRoot, "results"))
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	for _, entry := range entries {
		base := entry.Name()
		if len(base) != len("result-000001.agr") || !strings.HasPrefix(base, "result-") || !strings.HasSuffix(base, ".agr") {
			continue
		}
		if parsed, parseErr := strconv.Atoi(base[len("result-") : len(base)-len(".agr")]); parseErr == nil && parsed >= version {
			version = parsed + 1
		}
	}
	return filepath.ToSlash(filepath.Join("results", fmt.Sprintf("result-%06d.agr", version))), nil
}

func segmentIDs(current manifest) []string {
	ids := make([]string, 0, len(current.Segments))
	for _, segment := range current.Segments {
		ids = append(ids, segment.ID)
	}
	return ids
}

// pruneStaleStateFiles removes confirmed snapshot files other than the one the
// current checkpoint points at. Only files matching the confirmed name pattern
// are considered, so active spill run directories are never deleted.
func pruneStaleStateFiles(stateRoot string, keepRelative string) {
	runsDir := filepath.Join(stateRoot, "runs")
	entries, err := os.ReadDir(runsDir)
	if err != nil {
		return
	}
	keep := filepath.Base(filepath.FromSlash(keepRelative))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || name == keep {
			continue
		}
		if len(name) == len("state-000001.spill") && strings.HasPrefix(name, "state-") && strings.HasSuffix(name, ".spill") {
			if _, parseErr := strconv.Atoi(name[len("state-") : len(name)-len(".spill")]); parseErr == nil {
				_ = os.Remove(filepath.Join(runsDir, name))
			}
		}
	}
}

func readAggregationCheckpoint(path string) (AggregationCheckpoint, bool, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return AggregationCheckpoint{}, false, nil
	}
	if err != nil {
		return AggregationCheckpoint{}, false, err
	}
	var checkpoint AggregationCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return AggregationCheckpoint{}, false, fmt.Errorf("invalid aggregation checkpoint: %w", err)
	}
	if checkpoint.Version != aggregationCheckpointVersion {
		return AggregationCheckpoint{}, false, fmt.Errorf("unsupported aggregation checkpoint version %d", checkpoint.Version)
	}
	return checkpoint, true, nil
}

func writeAggregationCheckpoint(path string, checkpoint AggregationCheckpoint) error {
	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return atomicWriteFile(path, data, true)
}
