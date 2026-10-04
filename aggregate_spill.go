package columnar

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
)

const aggregatePartitionCount = 64

type hashAggregator struct {
	plan          aggregationPlan
	limit         int
	tempRoot      string
	spillRoot     string
	states        map[string]*groupState
	order         []string
	spilled       bool
	spillFiles    map[uint64]*os.File
	spillEncoders map[uint64]*gob.Encoder
}

type groupState struct {
	Key  groupKey
	Accs []accumulator
}

type persistedGroup struct {
	Key  groupKey
	Accs []accumulator
}

func init() {
	gob.Register(int64(0))
	gob.Register(float64(0))
	gob.Register(true)
	gob.Register("")
}

func newHashAggregator(plan aggregationPlan, limit int, tempDir string) *hashAggregator {
	if limit < 1 {
		limit = 1024
	}
	return &hashAggregator{
		plan:     plan,
		limit:    limit,
		tempRoot: tempDir,
		states:   make(map[string]*groupState),
	}
}

func (a *hashAggregator) addRow(row []any) error {
	key := makeGroupKey(a.plan, row)
	identity := string(identityBytes(key))
	state, ok := a.states[identity]
	if !ok {
		if len(a.plan.groupIndexes) > 0 && len(a.states) >= a.limit {
			if err := a.flush(); err != nil {
				return err
			}
		}
		state = &groupState{Key: key, Accs: make([]accumulator, len(a.plan.specs))}
		a.states[identity] = state
		a.order = append(a.order, identity)
	}
	updateAccumulators(state.Accs, a.plan, row)
	return nil
}

func (a *hashAggregator) rows(ctx context.Context) ([][]any, error) {
	if !a.spilled {
		return a.materialize(a.currentStatesSorted(), true), nil
	}
	if err := a.flush(); err != nil {
		return nil, err
	}
	a.states = map[string]*groupState{}
	a.order = nil
	var rows [][]any
	entries, err := os.ReadDir(a.spillRoot)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry.IsDir() {
			continue
		}
		leafRows, err := a.processSpillFile(filepath.Join(a.spillRoot, entry.Name()), entry.Name(), 0)
		if err != nil {
			return nil, err
		}
		rows = append(rows, leafRows...)
	}
	sortRows(rows)
	return rows, nil
}

func (a *hashAggregator) cleanup() {
	if a.spillRoot != "" {
		for _, file := range a.spillFiles {
			file.Close()
		}
		_ = os.RemoveAll(a.spillRoot)
	}
}

func (a *hashAggregator) flush() error {
	if len(a.states) == 0 {
		return nil
	}
	if err := a.ensureSpillRoot(); err != nil {
		return err
	}
	if a.spillFiles == nil {
		a.spillFiles = make(map[uint64]*os.File)
		a.spillEncoders = make(map[uint64]*gob.Encoder)
	}
	for _, identity := range a.order {
		state := a.states[identity]
		partition := partitionFor(state.Key, 0)
		encoder, ok := a.spillEncoders[partition]
		if !ok {
			path := filepath.Join(a.spillRoot, spillFileName(0, partition))
			file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
			if err != nil {
				return err
			}
			a.spillFiles[partition] = file
			encoder = gob.NewEncoder(file)
			a.spillEncoders[partition] = encoder
		}
		if err := encoder.Encode(persistedGroup{Key: state.Key, Accs: state.Accs}); err != nil {
			return err
		}
	}
	a.states = make(map[string]*groupState)
	a.order = nil
	a.spilled = true
	return nil
}

func (a *hashAggregator) ensureSpillRoot() error {
	if a.spillRoot != "" {
		return nil
	}
	root, err := os.MkdirTemp(a.tempRoot, "columnar-aggregate-*")
	if err != nil {
		return err
	}
	a.spillRoot = root
	return nil
}

func (a *hashAggregator) closeAndSyncFiles() error {
	for _, file := range a.spillFiles {
		if err := file.Sync(); err != nil {
			file.Close()
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	a.spillFiles = nil
	a.spillEncoders = nil
	return nil
}

func (a *hashAggregator) processSpillFile(path string, scope string, level int) ([][]any, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := gob.NewDecoder(file)
	merged := make(map[string]*groupState)
	var order []string
	childWritten := false
	for {
		var group persistedGroup
		if err := decoder.Decode(&group); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("aggregate spill file is corrupted: %w", err)
		}
		identity := string(identityBytes(group.Key))
		state, ok := merged[identity]
		if !ok {
			if len(merged) >= a.limit {
				if err := a.writeChildren(merged, order, filepath.Join(scope, fmt.Sprintf("level-%03d", level+1))); err != nil {
					return nil, err
				}
				childWritten = true
				merged = make(map[string]*groupState)
				order = nil
			}
			state = &groupState{Key: group.Key, Accs: make([]accumulator, len(a.plan.specs))}
			merged[identity] = state
			order = append(order, identity)
		}
		for i := range group.Accs {
			mergeAccumulator(&state.Accs[i], group.Accs[i])
		}
	}
	if childWritten {
		if len(merged) > 0 {
			if err := a.writeChildren(merged, order, filepath.Join(scope, fmt.Sprintf("level-%03d", level+1))); err != nil {
				return nil, err
			}
		}
		childDir := filepath.Join(a.spillRoot, scope, fmt.Sprintf("level-%03d", level+1))
		entries, err := os.ReadDir(childDir)
		if err != nil {
			return nil, err
		}
		var rows [][]any
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			childRows, err := a.processSpillFile(filepath.Join(childDir, entry.Name()), filepath.Join(scope, fmt.Sprintf("level-%03d", level+1), entry.Name()), level+1)
			if err != nil {
				return nil, err
			}
			rows = append(rows, childRows...)
		}
		return rows, nil
	}
	return a.materializeStates(merged, order, false), nil
}

func (a *hashAggregator) writeChildren(states map[string]*groupState, order []string, scope string) error {
	childDir := filepath.Join(a.spillRoot, scope)
	if err := os.MkdirAll(childDir, 0o700); err != nil {
		return err
	}
	nonce, err := nextSpillNonce(childDir)
	if err != nil {
		return err
	}
	var syncFiles []*os.File
	defer func() {
		for _, file := range syncFiles {
			file.Close()
		}
	}()
	for _, identity := range order {
		state := states[identity]
		partition := partitionForWithScope(state.Key, scope)
		path := filepath.Join(childDir, fmt.Sprintf("p-%03d-%03d.gob", partition, nonce))
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return err
		}
		syncFiles = append(syncFiles, file)
		encoder := gob.NewEncoder(file)
		if err := encoder.Encode(persistedGroup{Key: state.Key, Accs: state.Accs}); err != nil {
			return err
		}
	}
	for _, file := range syncFiles {
		if err := file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func nextSpillNonce(directory string) (int, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return len(entries), nil
}

func (a *hashAggregator) currentStatesSorted() []*groupState {
	order := append([]string(nil), a.order...)
	sort.Slice(order, func(i, j int) bool {
		return compareGroupKeys(a.states[order[i]].Key, a.states[order[j]].Key) < 0
	})
	states := make([]*groupState, 0, len(order))
	for _, identity := range order {
		states = append(states, a.states[identity])
	}
	return states
}

func (a *hashAggregator) materialize(states []*groupState, globalIfEmpty bool) [][]any {
	if len(states) == 0 && globalIfEmpty && len(a.plan.groupIndexes) == 0 {
		return [][]any{a.materializeOne(&groupState{Accs: make([]accumulator, len(a.plan.specs))})}
	}
	rows := make([][]any, 0, len(states))
	for _, state := range states {
		rows = append(rows, a.materializeOne(state))
	}
	return rows
}

func (a *hashAggregator) materializeStates(states map[string]*groupState, order []string, sortOutput bool) [][]any {
	if sortOutput {
		sort.Slice(order, func(i, j int) bool {
			return compareGroupKeys(states[order[i]].Key, states[order[j]].Key) < 0
		})
	}
	result := make([]*groupState, 0, len(order))
	for _, identity := range order {
		result = append(result, states[identity])
	}
	return a.materialize(result, false)
}

func (a *hashAggregator) materializeOne(state *groupState) []any {
	row := make([]any, 0, len(state.Key.Parts)+len(a.plan.specs))
	for _, part := range state.Key.Parts {
		if part.Null {
			row = append(row, nil)
		} else {
			row = append(row, part.Value)
		}
	}
	for i, resolved := range a.plan.specs {
		row = append(row, accumulatorValue(resolved.spec.Func, state.Accs[i]))
	}
	return row
}

func partitionFor(key groupKey, level uint64) uint64 {
	hash := fnv.New64a()
	var levelBytes [8]byte
	binary.LittleEndian.PutUint64(levelBytes[:], level)
	_, _ = hash.Write(levelBytes[:])
	_, _ = hash.Write(identityBytes(key))
	return hash.Sum64() % aggregatePartitionCount
}

func partitionForWithScope(key groupKey, scope string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(scope))
	_, _ = hash.Write(identityBytes(key))
	return hash.Sum64() % aggregatePartitionCount
}

func spillFileName(level uint64, partition uint64) string {
	return fmt.Sprintf("p-%03d-%03d.gob", level, partition)
}

func identityBytes(key groupKey) []byte {
	buffer := bytes.NewBuffer(nil)
	for _, part := range key.Parts {
		if part.Null {
			buffer.WriteByte(0)
			continue
		}
		buffer.WriteByte(1)
		switch value := part.Value.(type) {
		case int64:
			buffer.WriteByte(byte(TypeInt64))
			var data [8]byte
			binary.LittleEndian.PutUint64(data[:], uint64(value))
			buffer.Write(data[:])
		case float64:
			buffer.WriteByte(byte(TypeFloat64))
			var data [8]byte
			binary.LittleEndian.PutUint64(data[:], math.Float64bits(value))
			buffer.Write(data[:])
		case bool:
			buffer.WriteByte(byte(TypeBool))
			if value {
				buffer.WriteByte(1)
			} else {
				buffer.WriteByte(0)
			}
		case string:
			buffer.WriteByte(byte(TypeString))
			var length [4]byte
			binary.LittleEndian.PutUint32(length[:], uint32(len(value)))
			buffer.Write(length[:])
			buffer.WriteString(value)
		}
	}
	return buffer.Bytes()
}

func sortRows(rows [][]any) {
	sort.Slice(rows, func(i, j int) bool {
		for column := range rows[i] {
			left, right := rows[i][column], rows[j][column]
			if compare := compareOutputValues(left, right); compare != 0 {
				return compare < 0
			}
		}
		return false
	})
}

func compareOutputValues(left any, right any) int {
	if left == nil || right == nil {
		if left == nil && right == nil {
			return 0
		}
		if left == nil {
			return -1
		}
		return 1
	}
	return compareValues(left, right)
}
