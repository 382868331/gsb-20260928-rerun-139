package columnar

import (
	"container/heap"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

type spilledRun struct {
	path   string
	frames uint32
}

type aggEngine struct {
	groupSchema Schema
	groupIdx    []int
	aggs        []namedAggregate
	maxGroups   int
	spillDir    string
	state       *aggState
	runs        []spilledRun
	runSequence int
	ctx         context.Context
}

func newAggEngine(groupSchema Schema, groupIndexes []int, aggs []namedAggregate, maxGroups int, spillDir string) *aggEngine {
	return &aggEngine{
		groupSchema: groupSchema,
		groupIdx:    groupIndexes,
		aggs:        aggs,
		maxGroups:   maxGroups,
		spillDir:    spillDir,
		state:       newAggState(len(aggs)),
	}
}

func (engine *aggEngine) addRow(values []any, predicates []boundPredicate) error {
	ctx := engine.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := aggregateRow(ctx, values, engine.state, engine.groupIdx, engine.aggs, predicates); err != nil {
		return err
	}
	if engine.maxGroups > 0 && engine.state.Len() > engine.maxGroups {
		return engine.flush()
	}
	return nil
}

func (engine *aggEngine) flush() error {
	if engine.state.Len() == 0 {
		return nil
	}
	if engine.spillDir == "" {
		return fmt.Errorf("aggregation exceeded in-memory group limit but no spill directory is configured")
	}
	if err := os.MkdirAll(engine.spillDir, 0o755); err != nil {
		return err
	}
	engine.runSequence++
	name := fmt.Sprintf("run-%06d.tmp", engine.runSequence)
	path := filepath.Join(engine.spillDir, name)
	writer, err := createRunWriter(path)
	if err != nil {
		return err
	}
	order := append([]string(nil), engine.state.order...)
	sort.Strings(order)
	var frames uint32
	for _, key := range order {
		frame := spillFrame{
			key:    key,
			groups: engine.state.keys[key],
			accs:   make([]aggAccumulator, len(engine.aggs)),
		}
		for i := range engine.aggs {
			frame.accs[i] = *engine.state.tables[i][key]
		}
		if err := writer.writeFrame(frame, engine.aggs); err != nil {
			writer.file.Close()
			return err
		}
		frames++
	}
	if err := writer.close(); err != nil {
		return err
	}
	finalName := fmt.Sprintf("run-%06d.spill", engine.runSequence)
	finalPath := filepath.Join(engine.spillDir, finalName)
	if err := os.Rename(path, finalPath); err != nil {
		return err
	}
	engine.runs = append(engine.runs, spilledRun{path: finalPath, frames: frames})
	engine.state = newAggState(len(engine.aggs))
	return nil
}

type mergedGroup struct {
	key    string
	groups []any
	accs   []aggAccumulator
}

func (engine *aggEngine) finishRows(emit func([]any) error) (int, error) {
	runCount := len(engine.runs)
	if runCount == 0 && engine.state.Len() == 0 {
		if len(engine.groupIdx) == 0 {
			row, err := engine.buildRow("", []any{}, make([]aggAccumulator, len(engine.aggs)))
			if err != nil {
				return 0, err
			}
			return 0, emit(row)
		}
		return 0, nil
	}
	if runCount == 0 {
		for _, row := range engine.materializeInMemory() {
			if err := emit(row); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}
	if err := engine.flush(); err != nil {
		return len(engine.runs), err
	}
	runs := engine.runs
	readers := make([]*runReader, len(runs))
	closers := make([]io.Closer, 0, len(runs))
	closeAll := func() {
		for _, closer := range closers {
			_ = closer.Close()
		}
	}
	queue := make(frameHeap, 0, len(runs))
	for i, run := range runs {
		reader, err := openRunReader(run.path, run.frames)
		if err != nil {
			closeAll()
			return len(runs), err
		}
		readers[i] = reader
		closers = append(closers, reader)
		frame, err := reader.next(engine.aggs)
		if err != nil && err != io.EOF {
			closeAll()
			return len(runs), err
		}
		if err == nil {
			queue = append(queue, heapEntry{frame: frame, run: i})
		}
	}
	heap.Init(&queue)
	for queue.Len() > 0 {
		current := heap.Pop(&queue).(heapEntry)
		key := current.frame.key
		groups := current.frame.groups
		accs := current.frame.accs
		advance := func(entry heapEntry) error {
			frame, err := readers[entry.run].next(engine.aggs)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			heap.Push(&queue, heapEntry{frame: frame, run: entry.run})
			return nil
		}
		if err := advance(current); err != nil {
			closeAll()
			return len(runs), err
		}
		for queue.Len() > 0 && queue[0].frame.key == key {
			entry := heap.Pop(&queue).(heapEntry)
			for i := range accs {
				accs[i] = accs[i].merge(entry.frame.accs[i])
			}
			if groups == nil {
				groups = entry.frame.groups
			}
			if err := advance(entry); err != nil {
				closeAll()
				return len(runs), err
			}
		}
		row, err := engine.buildRow(key, groups, accs)
		if err != nil {
			closeAll()
			return len(runs), err
		}
		if err := emit(row); err != nil {
			closeAll()
			return len(runs), err
		}
	}
	closeAll()
	return len(runs), nil
}

func (engine *aggEngine) finishFrames(emit func(spillFrame) error) (int, error) {
	if len(engine.runs) == 0 && engine.state.Len() == 0 {
		if len(engine.groupIdx) == 0 {
			return 0, emit(spillFrame{key: "", groups: []any{}, accs: make([]aggAccumulator, len(engine.aggs))})
		}
		return 0, nil
	}
	if len(engine.runs) == 0 {
		if err := engine.flush(); err != nil {
			return 0, err
		}
	}
	if err := engine.flush(); err != nil {
		return len(engine.runs), err
	}
	runs := engine.runs
	readers := make([]*runReader, len(runs))
	closers := make([]io.Closer, 0, len(runs))
	closeAll := func() {
		for _, closer := range closers {
			_ = closer.Close()
		}
	}
	queue := make(frameHeap, 0, len(runs))
	for i, run := range runs {
		reader, err := openRunReader(run.path, run.frames)
		if err != nil {
			closeAll()
			return len(runs), err
		}
		readers[i] = reader
		closers = append(closers, reader)
		frame, err := reader.next(engine.aggs)
		if err != nil && err != io.EOF {
			closeAll()
			return len(runs), err
		}
		if err == nil {
			queue = append(queue, heapEntry{frame: frame, run: i})
		}
	}
	heap.Init(&queue)
	for queue.Len() > 0 {
		current := heap.Pop(&queue).(heapEntry)
		key := current.frame.key
		groups := current.frame.groups
		accs := current.frame.accs
		advance := func(entry heapEntry) error {
			frame, err := readers[entry.run].next(engine.aggs)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			heap.Push(&queue, heapEntry{frame: frame, run: entry.run})
			return nil
		}
		if err := advance(current); err != nil {
			closeAll()
			return len(runs), err
		}
		for queue.Len() > 0 && queue[0].frame.key == key {
			entry := heap.Pop(&queue).(heapEntry)
			for i := range accs {
				accs[i] = accs[i].merge(entry.frame.accs[i])
			}
			if groups == nil {
				groups = entry.frame.groups
			}
			if err := advance(entry); err != nil {
				closeAll()
				return len(runs), err
			}
		}
		if err := emit(spillFrame{key: key, groups: groups, accs: accs}); err != nil {
			closeAll()
			return len(runs), err
		}
	}
	closeAll()
	return len(runs), nil
}

func (engine *aggEngine) snapshotTo(stateRoot string, sequence int) (string, uint32, error) {
	relative := filepath.ToSlash(filepath.Join("runs", fmt.Sprintf("state-%06d.spill", sequence)))
	finalPath := filepath.Join(stateRoot, filepath.FromSlash(relative))
	frames, err := engine.snapshotToPath(finalPath)
	if err != nil {
		return "", 0, err
	}
	engine.runs = []spilledRun{{path: finalPath, frames: frames}}
	engine.state = newAggState(len(engine.aggs))
	return relative, frames, nil
}

func (engine *aggEngine) snapshotToPath(finalPath string) (uint32, error) {
	tempPath := finalPath + ".tmp"
	writer, err := createRunWriter(tempPath)
	if err != nil {
		return 0, err
	}
	var frames uint32
	runCount, err := engine.finishFrames(func(frame spillFrame) error {
		if err := writer.writeFrame(frame, engine.aggs); err != nil {
			return err
		}
		frames++
		return nil
	})
	if err != nil {
		writer.file.Close()
		_ = os.Remove(tempPath)
		return 0, err
	}
	if err := writer.close(); err != nil {
		_ = os.Remove(tempPath)
		return 0, err
	}
	if err := os.Chmod(tempPath, 0o644); err != nil {
		_ = os.Remove(tempPath)
		return 0, err
	}
	if err := os.Rename(tempPath, finalPath); err != nil {
		_ = os.Remove(tempPath)
		return 0, err
	}
	if err := syncDir(filepath.Dir(finalPath)); err != nil {
		return 0, err
	}
	_ = runCount
	return frames, nil
}

// spillRunCount reports how many spill files the engine produced while
// accumulating the current segment.
func (engine *aggEngine) spillRunCount() int { return engine.runSequence }

func (engine *aggEngine) finish() ([][]any, int, error) {
	rows := make([][]any, 0)
	runCount, err := engine.finishRows(func(row []any) error {
		rows = append(rows, row)
		return nil
	})
	return rows, runCount, err
}

func (engine *aggEngine) buildRow(key string, groups []any, accs []aggAccumulator) ([]any, error) {
	if groups == nil {
		decoded, err := decodeGroupKey(key, engine.groupSchema)
		if err != nil {
			return nil, err
		}
		groups = decoded
	}
	row := make([]any, len(groups)+len(engine.aggs))
	copy(row, groups)
	for i, agg := range engine.aggs {
		row[len(groups)+i] = finishAccumulator(agg.funcName, accs[i], agg.inputType)
	}
	return row, nil
}

func (engine *aggEngine) materializeInMemory() [][]any {
	order := append([]string(nil), engine.state.order...)
	sort.Strings(order)
	rows := make([][]any, 0, len(order))
	for _, key := range order {
		accs := make([]aggAccumulator, len(engine.aggs))
		for i := range engine.aggs {
			accs[i] = *engine.state.tables[i][key]
		}
		row, err := engine.buildRow(key, engine.state.keys[key], accs)
		if err != nil {
			return nil
		}
		rows = append(rows, row)
	}
	return rows
}

type heapEntry struct {
	frame spillFrame
	run   int
}

type frameHeap []heapEntry

func (heap frameHeap) Len() int           { return len(heap) }
func (heap frameHeap) Less(i, j int) bool { return heap[i].frame.key < heap[j].frame.key }
func (heap frameHeap) Swap(i, j int)      { heap[i], heap[j] = heap[j], heap[i] }
func (heap *frameHeap) Push(value any)    { *heap = append(*heap, value.(heapEntry)) }
func (heap *frameHeap) Pop() any {
	old := *heap
	n := len(old)
	entry := old[n-1]
	*heap = old[:n-1]
	return entry
}
