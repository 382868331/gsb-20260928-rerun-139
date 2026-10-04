package columnar

import (
	"context"
	"fmt"
	"os"
)

func (r *Reader) Aggregate(request AggregateRequest) (AggregateResult, error) {
	return r.AggregateContext(context.Background(), request)
}

func (r *Reader) AggregateContext(ctx context.Context, request AggregateRequest) (AggregateResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	current, err := r.store.readManifest()
	if err != nil {
		return AggregateResult{}, err
	}
	groupSchema, groupIndexes, aggs, predicates, outputSchema, err := bindAggregateRequest(r.schema, request)
	if err != nil {
		return AggregateResult{}, err
	}
	spillDir := request.SpillDirectory
	if spillDir == "" && request.MaxInMemoryGroups > 0 {
		spillDir, err = os.MkdirTemp("", "columnar-agg-")
		if err != nil {
			return AggregateResult{}, err
		}
		defer func() { _ = os.RemoveAll(spillDir) }()
	}
	engine := newAggEngine(groupSchema, groupIndexes, aggs, request.MaxInMemoryGroups, spillDir)
	engine.ctx = ctx
	readIndexes := requiredColumns(groupIndexes, aggs, predicates)
	if request.Reference {
		readIndexes = make([]int, len(current.Columns))
		for i := range readIndexes {
			readIndexes[i] = i
		}
	}
	readSet := make(map[int]struct{}, len(readIndexes))
	for _, index := range readIndexes {
		readSet[index] = struct{}{}
	}
	segmentIDs := make([]string, 0)
	for _, segment := range current.Segments {
		select {
		case <-ctx.Done():
			return AggregateResult{}, ctx.Err()
		default:
		}
		if !request.Reference && segmentCanSkip(current.Columns, segment, predicates) {
			continue
		}
		for _, fileInfo := range segment.Files {
			if err := r.store.validateFileMetadata(fileInfo.Path, fileInfo.Size); err != nil {
				return AggregateResult{}, fmt.Errorf("segment %s: %w", segment.ID, err)
			}
		}
		columns, err := r.readSegmentColumns(current, segment, readIndexes)
		if err != nil {
			return AggregateResult{}, fmt.Errorf("segment %s: %w", segment.ID, err)
		}
		segmentIDs = append(segmentIDs, segment.ID)
		for rowIndex := uint64(0); rowIndex < segment.Rows; rowIndex++ {
			rowColumns := make([]any, len(current.Columns))
			for columnIndex, values := range columns {
				rowColumns[columnIndex] = values[rowIndex]
			}
			if err := engine.addRow(rowColumns, predicates); err != nil {
				return AggregateResult{}, err
			}
		}
	}
	groups, runCount, err := engine.finish()
	if err != nil {
		return AggregateResult{}, err
	}
	columnsRead := make([]string, 0, len(readSet))
	for index := range readSet {
		columnsRead = append(columnsRead, r.schema.Columns[index].Name)
	}
	sortStrings(columnsRead)
	return AggregateResult{
		Schema: outputSchema, GroupSchema: groupSchema, OutputSchema: outputSchema,
		Rows: groups, Groups: groups, ColumnsRead: columnsRead,
		SegmentsScanned: segmentIDs, SegmentsRead: segmentIDs,
		SpilledRuns: runCount, Spilled: runCount > 0,
	}, nil
}
