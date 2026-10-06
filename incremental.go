package columnar

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
)

// IncrementalAggregationJob is a versioned, previewable incremental
// publication of an aggregation result. Each input dataset revision produces
// at most one release; stable segment identities drive append/replace/withdraw
// handling and confirmed segments are never accumulated twice.
type IncrementalAggregationJob struct {
	DatasetRoot string
	StateRoot   string
	Request     AggregateRequest
}

type SegmentChangeKind string

const (
	SegmentAppended  SegmentChangeKind = "appended"
	SegmentReplaced  SegmentChangeKind = "replaced"
	SegmentWithdrawn SegmentChangeKind = "withdrawn"
)

// SegmentChange describes one stable segment identity involved in a round.
type SegmentChange struct {
	ID          string            `json:"id"`
	Kind        SegmentChangeKind `json:"kind"`
	OldRevision uint64            `json:"old_revision,omitempty"`
	NewRevision uint64            `json:"new_revision,omitempty"`
	Rows        uint64            `json:"rows,omitempty"`
	Skipped     bool              `json:"skipped,omitempty"`
}

// GroupChange describes one result group affected by the new dataset revision.
type GroupChange struct {
	Key    []any  `json:"-"`
	Before []any  `json:"-"`
	After  []any  `json:"-"`
	Status string `json:"status"`
}

type IncrementalPreview struct {
	DatasetRevision uint64
	BaseRelease     int
	Release         int
	Changes         []SegmentChange
	AffectedGroups  []GroupChange
	Result          PublishedAggregate
	SpilledRuns     int
	Spilled         bool
}

type IncrementalReleaseRecord struct {
	Number          int              `json:"number"`
	DatasetRevision uint64           `json:"dataset_revision"`
	Request         AggregateRequest `json:"request"`
	ResultPath      string           `json:"result_path"`
	ResultRows      uint64           `json:"result_rows"`
	ResultCRC32     uint32           `json:"result_crc32"`
	ResultSize      int64            `json:"result_size"`
	Segments        []releaseSegment `json:"segments"`
	Changes         []SegmentChange  `json:"changes"`
}

type IncrementalRelease struct {
	Record IncrementalReleaseRecord
	Result PublishedAggregate
}

type releaseSegment struct {
	ID       string `json:"id"`
	Revision uint64 `json:"revision"`
	State    string `json:"state"`
	Frames   uint32 `json:"frames"`
	Skipped  bool   `json:"skipped,omitempty"`
}

type incrementalIndex struct {
	Version  int                        `json:"version"`
	Current  int                        `json:"current_release"`
	Releases []IncrementalReleaseRecord `json:"releases"`
}

type incrementalStage struct {
	Version         int              `json:"version"`
	DatasetRevision uint64           `json:"dataset_revision"`
	BaseRelease     int              `json:"base_release"`
	Release         int              `json:"release"`
	Request         AggregateRequest `json:"request"`
	Changes         []SegmentChange  `json:"changes"`
	Segments        []releaseSegment `json:"segments"`
	ResultPath      string           `json:"result_path"`
	ResultRows      uint64           `json:"result_rows"`
	ResultCRC32     uint32           `json:"result_crc32"`
	ResultSize      int64            `json:"result_size"`
	SpilledRuns     int              `json:"spilled_runs"`
}

const incrementalVersion = 1

func (job IncrementalAggregationJob) stateRoot() string {
	if job.StateRoot != "" {
		return filepath.Clean(job.StateRoot)
	}
	return filepath.Join(filepath.Clean(job.DatasetRoot), "aggregate-state", "incremental")
}

func (job IncrementalAggregationJob) releasesDir() string {
	return filepath.Join(job.stateRoot(), "releases")
}

func (job IncrementalAggregationJob) stageDir() string {
	return filepath.Join(job.stateRoot(), "stage")
}

func (job IncrementalAggregationJob) indexPath() string {
	return filepath.Join(job.stateRoot(), "index.json")
}

func (r *Reader) DatasetRevision() (uint64, error) {
	current, err := r.store.readManifest()
	if err != nil {
		return 0, err
	}
	return current.Revision, nil
}

func readIncrementalIndex(path string) (incrementalIndex, bool, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return incrementalIndex{Version: incrementalVersion}, false, nil
	}
	if err != nil {
		return incrementalIndex{}, false, err
	}
	var index incrementalIndex
	if err := json.Unmarshal(data, &index); err != nil {
		return incrementalIndex{}, false, fmt.Errorf("invalid incremental index: %w", err)
	}
	if index.Version != 0 && index.Version != incrementalVersion {
		return incrementalIndex{}, false, fmt.Errorf("unsupported incremental index version %d", index.Version)
	}
	index.Version = incrementalVersion
	if index.Releases == nil {
		index.Releases = []IncrementalReleaseRecord{}
	}
	return index, true, nil
}

func writeIncrementalIndex(path string, index incrementalIndex) error {
	index.Version = incrementalVersion
	data, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return atomicWriteFile(path, data, true)
}

func readReleaseRecord(dir string, number int) (IncrementalReleaseRecord, error) {
	path := filepath.Join(dir, fmt.Sprintf("release-%06d.json", number))
	data, err := os.ReadFile(path)
	if err != nil {
		return IncrementalReleaseRecord{}, err
	}
	var record IncrementalReleaseRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return IncrementalReleaseRecord{}, fmt.Errorf("invalid incremental release %d: %w", number, err)
	}
	return record, nil
}

func writeReleaseRecord(dir string, record IncrementalReleaseRecord) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	path := filepath.Join(dir, fmt.Sprintf("release-%06d.json", record.Number))
	return atomicWriteFile(path, data, true)
}

func readStage(path string) (incrementalStage, bool, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return incrementalStage{}, false, nil
	}
	if err != nil {
		return incrementalStage{}, false, err
	}
	var stage incrementalStage
	if err := json.Unmarshal(data, &stage); err != nil {
		return incrementalStage{}, false, fmt.Errorf("invalid incremental stage: %w", err)
	}
	return stage, true, nil
}

func writeStage(path string, stage incrementalStage) error {
	stage.Version = incrementalVersion
	data, err := json.MarshalIndent(stage, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return atomicWriteFile(path, data, true)
}

func sameAggregateRequest(left, right AggregateRequest) bool {
	left.MaxInMemoryGroups = 0
	right.MaxInMemoryGroups = 0
	left.SpillDirectory = ""
	right.SpillDirectory = ""
	left.Reference = false
	right.Reference = false
	leftData, err := json.Marshal(left)
	if err != nil {
		return false
	}
	rightData, err := json.Marshal(right)
	if err != nil {
		return false
	}
	return bytes.Equal(leftData, rightData)
}

func (r *Reader) prepareIncremental(ctx context.Context, job IncrementalAggregationJob) (manifest, incrementalIndex, []namedAggregate, []int, []boundPredicate, Schema, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if job.DatasetRoot == "" {
		job.DatasetRoot = r.store.root
	}
	if err := os.MkdirAll(filepath.Join(job.stateRoot(), "segments"), 0o755); err != nil {
		return manifest{}, incrementalIndex{}, nil, nil, nil, Schema{}, err
	}
	if err := os.MkdirAll(job.releasesDir(), 0o755); err != nil {
		return manifest{}, incrementalIndex{}, nil, nil, nil, Schema{}, err
	}
	current, err := r.store.readManifest()
	if err != nil {
		return manifest{}, incrementalIndex{}, nil, nil, nil, Schema{}, err
	}
	index, _, err := readIncrementalIndex(job.indexPath())
	if err != nil {
		return manifest{}, incrementalIndex{}, nil, nil, nil, Schema{}, err
	}
	if index.Current > 0 {
		record, err := readReleaseRecord(job.releasesDir(), index.Current)
		if err != nil {
			return manifest{}, incrementalIndex{}, nil, nil, nil, Schema{}, err
		}
		if !sameAggregateRequest(record.Request, job.Request) {
			return manifest{}, incrementalIndex{}, nil, nil, nil, Schema{}, fmt.Errorf("incremental job request does not match release %d", index.Current)
		}
	}
	groupSchema, groupIndexes, aggs, predicates, outputSchema, err := bindAggregateRequest(r.schema, job.Request)
	if err != nil {
		return manifest{}, incrementalIndex{}, nil, nil, nil, Schema{}, err
	}
	_ = groupSchema
	return current, index, aggs, groupIndexes, predicates, outputSchema, nil
}

func (r *Reader) PreviewIncrementalAggregation(ctx context.Context, job IncrementalAggregationJob) (IncrementalPreview, error) {
	current, index, aggs, groupIndexes, predicates, outputSchema, err := r.prepareIncremental(ctx, job)
	if err != nil {
		return IncrementalPreview{}, err
	}
	var baseRecord *IncrementalReleaseRecord
	if index.Current > 0 {
		record, readErr := readReleaseRecord(job.releasesDir(), index.Current)
		if readErr != nil {
			return IncrementalPreview{}, readErr
		}
		baseRecord = &record
	}
	changes := diffSegments(baseRecord, current)
	stagePath := filepath.Join(job.stageDir(), "stage.json")
	stage, stageExists, err := readStage(stagePath)
	if err != nil {
		return IncrementalPreview{}, err
	}
	if stageExists && (stage.DatasetRevision != current.Revision || !sameAggregateRequest(stage.Request, job.Request) || !sameChanges(stage.Changes, changes)) {
		_ = os.RemoveAll(job.stageDir())
		stageExists = false
	}
	if stageExists {
		if record, exists := releaseForRevision(index, current.Revision); exists {
			result, err := readPublishedAggregate(filepath.Join(job.stateRoot(), filepath.FromSlash(record.ResultPath)), record.ResultSize, record.ResultCRC32)
			if err != nil {
				return IncrementalPreview{}, err
			}
			return IncrementalPreview{
				DatasetRevision: current.Revision,
				BaseRelease:     index.Current,
				Release:         record.Number,
				Changes:         record.Changes,
				Result:          result,
				SpilledRuns:     0,
			}, nil
		}
		return r.loadStagePreview(job, stage)
	}

	if err := os.MkdirAll(filepath.Join(job.stageDir(), "segments"), 0o755); err != nil {
		return IncrementalPreview{}, err
	}
	segments := make([]releaseSegment, 0, len(current.Segments))
	changed := make(map[string]SegmentChange, len(changes))
	for _, change := range changes {
		changed[change.ID] = change
	}
	spillCount := 0
	readIndexes := requiredColumns(groupIndexes, aggs, predicates)
	if job.Request.Reference {
		readIndexes = make([]int, len(current.Columns))
		for i := range readIndexes {
			readIndexes[i] = i
		}
	}
	for _, segment := range current.Segments {
		change, isChanged := changed[segment.ID]
		if !isChanged && baseRecord != nil {
			if existing, ok := releaseSegmentByID(baseRecord.Segments, segment.ID); ok && existing.Revision == segment.Rev {
				segments = append(segments, existing)
				continue
			}
		}
		if !isChanged {
			isChanged = true
			change = SegmentChange{ID: segment.ID, Kind: SegmentAppended, NewRevision: segment.Rev, Rows: segment.Rows}
			changes = append(changes, change)
			changed[segment.ID] = change
		}
		stateRelative := filepath.ToSlash(filepath.Join("stage", "segments", segmentStateName(segment.ID, segment.Rev)))
		statePath := filepath.Join(job.stateRoot(), filepath.FromSlash(stateRelative))
		if !job.Request.Reference && segmentCanSkip(current.Columns, segment, predicates) {
			for index := range changes {
				if changes[index].ID == segment.ID {
					changes[index].Skipped = true
				}
			}
			segments = append(segments, releaseSegment{ID: segment.ID, Revision: segment.Rev, Skipped: true})
			_ = os.Remove(statePath)
			continue
		}
		for _, fileInfo := range segment.Files {
			if err := r.store.validateFileMetadata(fileInfo.Path, fileInfo.Size); err != nil {
				return IncrementalPreview{}, fmt.Errorf("segment %s: %w", segment.ID, err)
			}
		}
		columns, err := r.readSegmentColumns(current, segment, readIndexes)
		if err != nil {
			return IncrementalPreview{}, fmt.Errorf("segment %s: %w", segment.ID, err)
		}
		scanSpill := job.Request.SpillDirectory
		if scanSpill == "" {
			tempDir, err := os.MkdirTemp(job.stageDir(), "scan-")
			if err != nil {
				return IncrementalPreview{}, err
			}
			defer func() { _ = os.RemoveAll(tempDir) }()
			scanSpill = tempDir
		}
		engine := newAggEngine(groupSchemaFromRequest(r.schema, groupIndexes), groupIndexes, aggs, job.Request.MaxInMemoryGroups, scanSpill)
		engine.ctx = ctx
		for rowIndex := uint64(0); rowIndex < segment.Rows; rowIndex++ {
			if err := ctx.Err(); err != nil {
				return IncrementalPreview{}, err
			}
			rowColumns := make([]any, len(current.Columns))
			for columnIndex, values := range columns {
				rowColumns[columnIndex] = values[rowIndex]
			}
			if err := engine.addRow(rowColumns, predicates); err != nil {
				return IncrementalPreview{}, err
			}
		}
		frames, err := engine.snapshotToPath(statePath)
		if err != nil {
			return IncrementalPreview{}, err
		}
		spillCount += engine.spillRunCount()
		segments = append(segments, releaseSegment{ID: segment.ID, Revision: segment.Rev, State: stateRelative, Frames: frames})
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].ID < changes[j].ID })
	sort.Slice(segments, func(i, j int) bool { return segments[i].ID < segments[j].ID })

	mergeEngine := newAggEngine(groupSchemaFromRequest(r.schema, groupIndexes), groupIndexes, aggs, job.Request.MaxInMemoryGroups, filepath.Join(job.stageDir(), "merge"))
	mergeEngine.ctx = ctx
	for _, segment := range segments {
		if segment.Skipped {
			continue
		}
		path := filepath.Join(job.stateRoot(), filepath.FromSlash(segment.State))
		if _, err := openRunReader(path, segment.Frames); err != nil {
			return IncrementalPreview{}, fmt.Errorf("segment state for %q is corrupted: %w", segment.ID, err)
		}
		mergeEngine.runs = append(mergeEngine.runs, spilledRun{path: path, frames: segment.Frames})
	}
	newRows := make([][]any, 0)
	mergeRuns, err := mergeEngine.finishRows(func(row []any) error {
		newRows = append(newRows, row)
		return nil
	})
	if err != nil {
		return IncrementalPreview{}, err
	}

	resultRelative := filepath.ToSlash(filepath.Join("stage", "result.agr"))
	resultPath := filepath.Join(job.stateRoot(), filepath.FromSlash(resultRelative))
	writer, err := newAggregateResultWriter(resultPath, outputSchema)
	if err != nil {
		return IncrementalPreview{}, err
	}
	for _, row := range newRows {
		if err := writer.writeRow(row); err != nil {
			writer.close()
			return IncrementalPreview{}, err
		}
	}
	size, checksum, rowCount, err := writer.finish()
	if err != nil {
		return IncrementalPreview{}, err
	}

	affected, err := r.diffGroupResults(job, baseRecord, newRows, len(groupIndexes))
	if err != nil {
		return IncrementalPreview{}, err
	}
	nextRelease := index.Current + 1
	if len(index.Releases) >= nextRelease {
		nextRelease = len(index.Releases) + 1
	}
	stage = incrementalStage{
		DatasetRevision: current.Revision,
		BaseRelease:     index.Current,
		Release:         nextRelease,
		Request:         job.Request,
		Changes:         changes,
		Segments:        segments,
		ResultPath:      resultRelative,
		ResultRows:      rowCount,
		ResultCRC32:     checksum,
		ResultSize:      size,
		SpilledRuns:     spillCount + mergeRuns,
	}
	if err := writeStage(stagePath, stage); err != nil {
		return IncrementalPreview{}, err
	}
	return IncrementalPreview{
		DatasetRevision: current.Revision,
		BaseRelease:     index.Current,
		Release:         nextRelease,
		Changes:         changes,
		AffectedGroups:  affected,
		Result:          PublishedAggregate{Schema: outputSchema, Rows: newRows},
		SpilledRuns:     spillCount + mergeRuns,
		Spilled:         spillCount+mergeRuns > 0,
	}, nil
}

func groupSchemaFromRequest(schema Schema, indexes []int) Schema {
	columns := make([]Column, len(indexes))
	for i, index := range indexes {
		columns[i] = schema.Columns[index]
	}
	groupSchema, _ := NewSchema(columns)
	return groupSchema
}

func segmentStateName(id string, revision uint64) string {
	return fmt.Sprintf("%s-%06d.spill", id, revision)
}

func releaseSegmentByID(segments []releaseSegment, id string) (releaseSegment, bool) {
	for _, segment := range segments {
		if segment.ID == id {
			return segment, true
		}
	}
	return releaseSegment{}, false
}

func releaseForRevision(index incrementalIndex, revision uint64) (IncrementalReleaseRecord, bool) {
	for _, record := range index.Releases {
		if record.DatasetRevision == revision {
			return record, true
		}
	}
	return IncrementalReleaseRecord{}, false
}

func sameChanges(left, right []SegmentChange) bool {
	leftData, _ := json.Marshal(left)
	rightData, _ := json.Marshal(right)
	return bytes.Equal(leftData, rightData)
}

// diffSegments compares the current manifest against the previous release using
// stable segment ids and per-identity revisions.
func diffSegments(base *IncrementalReleaseRecord, current manifest) []SegmentChange {
	currentByID := make(map[string]segmentInfo, len(current.Segments))
	for _, segment := range current.Segments {
		currentByID[segment.ID] = segment
	}
	baseByID := make(map[string]segmentInfo)
	if base != nil {
		for _, segment := range base.Segments {
			currentInfo, ok := currentByID[segment.ID]
			_ = currentInfo
			_ = ok
			baseByID[segment.ID] = segmentInfo{ID: segment.ID, Rev: segment.Revision, Rows: 0}
		}
	}
	changes := make([]SegmentChange, 0)
	if base == nil {
		for _, segment := range current.Segments {
			changes = append(changes, SegmentChange{ID: segment.ID, Kind: SegmentAppended, NewRevision: segment.Rev, Rows: segment.Rows})
		}
	} else {
		for _, segment := range base.Segments {
			currentInfo, ok := currentByID[segment.ID]
			if !ok {
				changes = append(changes, SegmentChange{ID: segment.ID, Kind: SegmentWithdrawn, OldRevision: segment.Revision})
				continue
			}
			if currentInfo.Rev != segment.Revision {
				changes = append(changes, SegmentChange{ID: segment.ID, Kind: SegmentReplaced, OldRevision: segment.Revision, NewRevision: currentInfo.Rev, Rows: currentInfo.Rows})
			}
		}
		for _, segment := range current.Segments {
			if _, ok := baseByID[segment.ID]; !ok {
				changes = append(changes, SegmentChange{ID: segment.ID, Kind: SegmentAppended, NewRevision: segment.Rev, Rows: segment.Rows})
			}
		}
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].ID < changes[j].ID })
	return changes
}

func (r *Reader) diffGroupResults(job IncrementalAggregationJob, base *IncrementalReleaseRecord, newRows [][]any, groupColumns int) ([]GroupChange, error) {
	changes := make([]GroupChange, 0)
	if base == nil {
		for _, row := range newRows {
			changes = append(changes, GroupChange{Key: append([]any(nil), row...), After: append([]any(nil), row...), Status: "appeared"})
		}
		return changes, nil
	}
	oldCursor, err := openResultCursor(filepath.Join(job.stateRoot(), filepath.FromSlash(base.ResultPath)))
	if err != nil {
		return nil, err
	}
	defer func() { _ = oldCursor.close(base.ResultRows, base.ResultCRC32) }()
	newIndex := 0
	oldRow, hasOld, err := oldCursor.next()
	if err != nil {
		return nil, err
	}
	for hasOld || newIndex < len(newRows) {
		var newRow []any
		if newIndex < len(newRows) {
			newRow = newRows[newIndex]
		}
		if !hasOld {
			changes = append(changes, GroupChange{Key: append([]any(nil), newRow...), After: append([]any(nil), newRow...), Status: "appeared"})
			newIndex++
			continue
		}
		if newRow == nil {
			changes = append(changes, GroupChange{Key: append([]any(nil), oldRow...), Before: append([]any(nil), oldRow...), Status: "disappeared"})
			hasOld, err = advanceCursor(oldCursor, &oldRow)
			if err != nil {
				return nil, err
			}
			continue
		}
		comparison := 0
		if groupColumns > 0 {
			oldKey := groupRowKey(oldRow, groupColumns)
			newKey := groupRowKey(newRow, groupColumns)
			switch {
			case oldKey < newKey:
				comparison = -1
			case oldKey > newKey:
				comparison = 1
			}
		}
		switch comparison {
		case -1:
			changes = append(changes, GroupChange{Key: append([]any(nil), oldRow...), Before: append([]any(nil), oldRow...), Status: "disappeared"})
			hasOld, err = advanceCursor(oldCursor, &oldRow)
			if err != nil {
				return nil, err
			}
		case 1:
			changes = append(changes, GroupChange{Key: append([]any(nil), newRow...), After: append([]any(nil), newRow...), Status: "appeared"})
			newIndex++
		default:
			if !rowsEqual(oldRow, newRow) {
				changes = append(changes, GroupChange{Key: append([]any(nil), newRow...), Before: append([]any(nil), oldRow...), After: append([]any(nil), newRow...), Status: "updated"})
			}
			newIndex++
			hasOld, err = advanceCursor(oldCursor, &oldRow)
			if err != nil {
				return nil, err
			}
		}
	}
	for {
		_, hasMore, err := oldCursor.next()
		if err != nil {
			return nil, err
		}
		if !hasMore {
			break
		}
	}
	return changes, nil
}

func advanceCursor(cursor *resultCursor, row *[]any) (bool, error) {
	next, has, err := cursor.next()
	if err != nil || !has {
		return false, err
	}
	*row = next
	return true, nil
}

func groupRowKey(row []any, groupColumns int) string {
	parts := make([]byte, 0, 16)
	for i := 0; i < groupColumns; i++ {
		parts = appendGroupKeyPart(parts, row[i])
	}
	return string(parts)
}

func rowsEqual(left, right []any) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !valueEqual(left[i], right[i]) {
			return false
		}
	}
	return true
}

func valueEqual(left, right any) bool {
	switch leftValue := left.(type) {
	case nil:
		return right == nil
	case float64:
		rightValue, ok := right.(float64)
		return ok && math.IsNaN(leftValue) == math.IsNaN(rightValue) && (math.IsNaN(leftValue) || leftValue == rightValue)
	default:
		return left == right
	}
}

func (r *Reader) loadStagePreview(job IncrementalAggregationJob, stage incrementalStage) (IncrementalPreview, error) {
	result, err := readPublishedAggregate(filepath.Join(job.stateRoot(), filepath.FromSlash(stage.ResultPath)), stage.ResultSize, stage.ResultCRC32)
	if err != nil {
		return IncrementalPreview{}, err
	}
	var base *IncrementalReleaseRecord
	if stage.BaseRelease > 0 {
		record, err := readReleaseRecord(job.releasesDir(), stage.BaseRelease)
		if err != nil {
			return IncrementalPreview{}, err
		}
		base = &record
	}
	affected, err := r.diffGroupResults(job, base, result.Rows, len(stage.Request.GroupBy))
	if err != nil {
		return IncrementalPreview{}, err
	}
	return IncrementalPreview{
		DatasetRevision: stage.DatasetRevision,
		BaseRelease:     stage.BaseRelease,
		Release:         stage.Release,
		Changes:         stage.Changes,
		AffectedGroups:  affected,
		Result:          result,
		SpilledRuns:     stage.SpilledRuns,
		Spilled:         stage.SpilledRuns > 0,
	}, nil
}

// resultCursor streams sorted rows from a published aggregate result file.
type resultCursor struct {
	file     *os.File
	schema   Schema
	hash     hash.Hash32
	position int64
	rowEnd   int64
	rows     uint64
}

func openResultCursor(path string) (*resultCursor, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if info.Size() < int64(len(aggregateResultMagic)+4+8+4+len(aggregateResultEnd)) {
		file.Close()
		return nil, fmt.Errorf("aggregate result %s is truncated", path)
	}
	magic := make([]byte, len(aggregateResultMagic))
	if _, err := io.ReadFull(file, magic); err != nil || string(magic) != aggregateResultMagic {
		file.Close()
		return nil, fmt.Errorf("aggregate result %s has an invalid header", path)
	}
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(file, lengthBytes); err != nil {
		file.Close()
		return nil, err
	}
	schemaLength := int(binary.LittleEndian.Uint32(lengthBytes))
	schemaData := make([]byte, schemaLength)
	if _, err := io.ReadFull(file, schemaData); err != nil {
		file.Close()
		return nil, err
	}
	var columns []Column
	if err := json.Unmarshal(schemaData, &columns); err != nil {
		file.Close()
		return nil, err
	}
	schema, err := NewSchema(columns)
	if err != nil {
		file.Close()
		return nil, err
	}
	hash := crc32.NewIEEE()
	hash.Write(magic)
	hash.Write(lengthBytes)
	hash.Write(schemaData)
	position, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		return nil, err
	}
	position = int64(len(aggregateResultMagic)) + 4 + int64(schemaLength)
	if _, err := file.Seek(position, io.SeekStart); err != nil {
		file.Close()
		return nil, err
	}
	return &resultCursor{
		file: file, schema: schema, hash: hash, position: position,
		rowEnd: info.Size() - int64(8+4+len(aggregateResultEnd)),
	}, nil
}

func (cursor *resultCursor) next() ([]any, bool, error) {
	if cursor.position >= cursor.rowEnd {
		return nil, false, nil
	}
	row := make([]any, len(cursor.schema.Columns))
	for i, column := range cursor.schema.Columns {
		if _, err := cursor.file.Seek(cursor.position, io.SeekStart); err != nil {
			return nil, false, err
		}
		value, raw, err := cursor.readValue(column)
		if err != nil {
			return nil, false, err
		}
		cursor.hash.Write(raw)
		cursor.position += int64(len(raw))
		row[i] = value
	}
	cursor.rows++
	return row, true, nil
}

func (cursor *resultCursor) close(expectedRows uint64, expectedCRC uint32) error {
	defer cursor.file.Close()
	tail := make([]byte, 12+len(aggregateResultEnd))
	if _, err := cursor.file.ReadAt(tail, cursor.rowEnd); err != nil {
		return err
	}
	storedRows := binary.LittleEndian.Uint64(tail[:8])
	storedCRC := binary.LittleEndian.Uint32(tail[8:12])
	if string(tail[12:]) != aggregateResultEnd {
		return fmt.Errorf("aggregate result has an invalid trailer")
	}
	if cursor.position != cursor.rowEnd || storedRows != expectedRows || storedRows != cursor.rows {
		return fmt.Errorf("aggregate result row count is corrupted")
	}
	if storedCRC != expectedCRC || cursor.hash.Sum32() != expectedCRC {
		return fmt.Errorf("aggregate result checksum is corrupted")
	}
	return nil
}

func (cursor *resultCursor) readValue(column Column) (any, []byte, error) {
	tag := make([]byte, 1)
	if _, err := io.ReadFull(cursor.file, tag); err != nil {
		return nil, nil, err
	}
	if tag[0] == keyNull {
		return nil, tag, nil
	}
	readFixed := func(size int) ([]byte, error) {
		raw := make([]byte, size)
		if _, err := io.ReadFull(cursor.file, raw); err != nil {
			return nil, err
		}
		return raw, nil
	}
	switch column.Type {
	case TypeInt64:
		if tag[0] != keyInt64 {
			return nil, nil, fmt.Errorf("aggregate result value is corrupted")
		}
		raw, err := readFixed(8)
		if err != nil {
			return nil, nil, err
		}
		return int64(binary.LittleEndian.Uint64(raw)), append(tag, raw...), nil
	case TypeFloat64:
		if tag[0] != keyFloat {
			return nil, nil, fmt.Errorf("aggregate result value is corrupted")
		}
		raw, err := readFixed(8)
		if err != nil {
			return nil, nil, err
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(raw)), append(tag, raw...), nil
	case TypeBool:
		if tag[0] != keyBool {
			return nil, nil, fmt.Errorf("aggregate result value is corrupted")
		}
		raw, err := readFixed(1)
		if err != nil {
			return nil, nil, err
		}
		if raw[0] > 1 {
			return nil, nil, fmt.Errorf("aggregate result bool is corrupted")
		}
		return raw[0] == 1, append(tag, raw...), nil
	case TypeString:
		if tag[0] != keyString {
			return nil, nil, fmt.Errorf("aggregate result value is corrupted")
		}
		rawLength, err := readFixed(4)
		if err != nil {
			return nil, nil, err
		}
		length := int(binary.LittleEndian.Uint32(rawLength))
		raw, err := readFixed(length)
		if err != nil {
			return nil, nil, err
		}
		rawValue := string(raw)
		full := append(append(tag, rawLength...), raw...)
		return rawValue, full, nil
	default:
		return nil, nil, fmt.Errorf("unsupported result type %s", column.Type)
	}
}

func (r *Reader) PublishIncrementalAggregation(ctx context.Context, job IncrementalAggregationJob) (IncrementalRelease, error) {
	current, index, _, _, _, _, err := r.prepareIncremental(ctx, job)
	if err != nil {
		return IncrementalRelease{}, err
	}
	if record, exists := releaseForRevision(index, current.Revision); exists {
		return r.loadIncrementalRelease(job, record)
	}
	stagePath := filepath.Join(job.stageDir(), "stage.json")
	stage, exists, err := readStage(stagePath)
	if err != nil {
		return IncrementalRelease{}, err
	}
	if !exists {
		if _, err := r.PreviewIncrementalAggregation(ctx, job); err != nil {
			return IncrementalRelease{}, err
		}
		stage, exists, err = readStage(stagePath)
		if err != nil {
			return IncrementalRelease{}, err
		}
	}
	if !exists || stage.DatasetRevision != current.Revision || !sameAggregateRequest(stage.Request, job.Request) {
		return IncrementalRelease{}, fmt.Errorf("incremental stage is stale or missing")
	}
	number := stage.Release
	releaseDir := filepath.Join(job.releasesDir(), fmt.Sprintf("release-%06d", number))
	if err := os.MkdirAll(filepath.Join(releaseDir, "segments"), 0o755); err != nil {
		return IncrementalRelease{}, err
	}
	publishedSegments := make([]releaseSegment, 0, len(stage.Segments))
	for _, segment := range stage.Segments {
		if segment.Skipped {
			publishedSegments = append(publishedSegments, segment)
			continue
		}
		name := filepath.Base(filepath.FromSlash(segment.State))
		targetRelative := filepath.ToSlash(filepath.Join("releases", fmt.Sprintf("release-%06d", number), "segments", name))
		source := filepath.Join(job.stateRoot(), filepath.FromSlash(segment.State))
		target := filepath.Join(job.stateRoot(), filepath.FromSlash(targetRelative))
		data, err := os.ReadFile(source)
		if err != nil {
			return IncrementalRelease{}, err
		}
		if err := atomicWriteFile(target, data, true); err != nil {
			return IncrementalRelease{}, err
		}
		segment.State = targetRelative
		publishedSegments = append(publishedSegments, segment)
	}
	resultRelative := filepath.ToSlash(filepath.Join("releases", fmt.Sprintf("release-%06d", number), "result.agr"))
	targetResult := filepath.Join(job.stateRoot(), filepath.FromSlash(resultRelative))
	resultData, err := os.ReadFile(filepath.Join(job.stateRoot(), filepath.FromSlash(stage.ResultPath)))
	if err != nil {
		return IncrementalRelease{}, err
	}
	if err := atomicWriteFile(targetResult, resultData, true); err != nil {
		return IncrementalRelease{}, err
	}
	record := IncrementalReleaseRecord{
		Number:          number,
		DatasetRevision: stage.DatasetRevision,
		Request:         stage.Request,
		ResultPath:      resultRelative,
		ResultRows:      stage.ResultRows,
		ResultCRC32:     stage.ResultCRC32,
		ResultSize:      stage.ResultSize,
		Segments:        publishedSegments,
		Changes:         stage.Changes,
	}
	if err := writeReleaseRecord(job.releasesDir(), record); err != nil {
		return IncrementalRelease{}, err
	}
	index.Releases = append(index.Releases, record)
	index.Current = number
	if err := writeIncrementalIndex(filepath.Join(job.stateRoot(), "index.next.json"), index); err != nil {
		return IncrementalRelease{}, err
	}
	if err := os.Rename(filepath.Join(job.stateRoot(), "index.next.json"), job.indexPath()); err != nil {
		return IncrementalRelease{}, err
	}
	if err := syncDir(job.stateRoot()); err != nil {
		return IncrementalRelease{}, err
	}
	_ = os.RemoveAll(job.stageDir())
	return r.loadIncrementalRelease(job, record)
}

func (r *Reader) CancelIncrementalAggregation(job IncrementalAggregationJob) error {
	if err := os.RemoveAll(job.stageDir()); err != nil {
		return err
	}
	return syncDir(job.stateRoot())
}

func (r *Reader) loadIncrementalRelease(job IncrementalAggregationJob, record IncrementalReleaseRecord) (IncrementalRelease, error) {
	result, err := readPublishedAggregate(filepath.Join(job.stateRoot(), filepath.FromSlash(record.ResultPath)), record.ResultSize, record.ResultCRC32)
	if err != nil {
		return IncrementalRelease{}, err
	}
	return IncrementalRelease{Record: record, Result: result}, nil
}

func (r *Reader) CurrentIncrementalRelease(job IncrementalAggregationJob) (IncrementalRelease, bool, error) {
	index, exists, err := readIncrementalIndex(job.indexPath())
	if err != nil || !exists || index.Current == 0 {
		return IncrementalRelease{}, false, err
	}
	record, err := readReleaseRecord(job.releasesDir(), index.Current)
	if err != nil {
		return IncrementalRelease{}, false, err
	}
	release, err := r.loadIncrementalRelease(job, record)
	return release, true, err
}

func (r *Reader) IncrementalReleaseByNumber(job IncrementalAggregationJob, number int) (IncrementalRelease, error) {
	record, err := readReleaseRecord(job.releasesDir(), number)
	if err != nil {
		return IncrementalRelease{}, err
	}
	return r.loadIncrementalRelease(job, record)
}
