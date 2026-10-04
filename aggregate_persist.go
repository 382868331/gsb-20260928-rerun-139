package columnar

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const aggregateResultMagic = "CAGR1\x00"

func mergeAggregationRun(ctx context.Context, plan aggregationPlan, runRoot string, job *aggregateJobManifest) ([][]any, error) {
	if len(plan.groupIndexes) == 0 {
		state := &groupState{Accs: make([]accumulator, len(plan.specs))}
		for _, completed := range job.Completed {
			for _, file := range completed.Files {
				groups, err := readPersistedGroups(filepath.Join(runRoot, filepath.FromSlash(file.Path)))
				if err != nil {
					return nil, err
				}
				for _, group := range groups {
					for i := range group.Accs {
						mergeAccumulator(&state.Accs[i], group.Accs[i])
					}
				}
			}
		}
		return [][]any{materializeAggregateState(plan, state)}, nil
	}
	grouped := make(map[string][]aggregateStateFile)
	for _, completed := range job.Completed {
		for _, file := range completed.Files {
			name := filepath.Base(file.Path)
			grouped[name] = append(grouped[name], file)
		}
	}
	names := make([]string, 0, len(grouped))
	for name := range grouped {
		names = append(names, name)
	}
	sort.Strings(names)
	var rows [][]any
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		mergeRoot, err := os.MkdirTemp(runRoot, "merge-*")
		if err != nil {
			return nil, err
		}
		inputRoot := filepath.Join(mergeRoot, "input")
		if err := os.MkdirAll(inputRoot, 0o700); err != nil {
			return nil, err
		}
		outputFile, err := os.OpenFile(filepath.Join(inputRoot, name), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return nil, err
		}
		outputEncoder := gob.NewEncoder(outputFile)
		for _, file := range grouped[name] {
			data, err := os.ReadFile(filepath.Join(runRoot, filepath.FromSlash(file.Path)))
			if err != nil {
				outputFile.Close()
				return nil, err
			}
			if int64(len(data)) != file.Size || crc32Checksum(data) != file.CRC32 {
				outputFile.Close()
				return nil, fmt.Errorf("aggregate state %s is corrupted", file.Path)
			}
			groups, err := decodeGobGroups(data)
			if err != nil {
				outputFile.Close()
				return nil, err
			}
			for _, group := range groups {
				if err := outputEncoder.Encode(group); err != nil {
					outputFile.Close()
					return nil, err
				}
			}
		}
		if err := outputFile.Close(); err != nil {
			return nil, err
		}
		agg := newHashAggregator(plan, plan.maxGroups, mergeRoot)
		agg.spillRoot = mergeRoot
		partRows, err := agg.processSpillFile(filepath.Join(inputRoot, name), name, 0)
		if err != nil {
			return nil, err
		}
		rows = append(rows, partRows...)
	}
	sortRows(rows)
	return rows, nil
}

func decodeGobGroups(data []byte) ([]persistedGroup, error) {
	decoder := gob.NewDecoder(bytes.NewReader(data))
	var groups []persistedGroup
	for {
		var group persistedGroup
		if err := decoder.Decode(&group); err != nil {
			if err == io.EOF {
				return groups, nil
			}
			return nil, fmt.Errorf("aggregate state is corrupted: %w", err)
		}
		groups = append(groups, group)
	}
}

func readPersistedGroups(path string) ([]persistedGroup, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	return decodeGobGroups(data)
}

func collectStateFiles(root string, relativeRoot string) ([]aggregateStateFile, error) {
	var files []aggregateStateFile
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files = append(files, aggregateStateFile{
			Path:  filepath.ToSlash(filepath.Join(relativeRoot, relative)),
			Size:  int64(len(data)),
			CRC32: crc32Checksum(data),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func validateCompletedSegments(datasetRoot string, runRoot string, completed []completedSegment) error {
	for _, segment := range completed {
		if segment.ID == "" {
			return fmt.Errorf("aggregate job has empty completed segment id")
		}
		for _, file := range segment.Files {
			data, err := os.ReadFile(filepath.Join(runRoot, filepath.FromSlash(file.Path)))
			if err != nil {
				return err
			}
			if int64(len(data)) != file.Size || crc32Checksum(data) != file.CRC32 {
				return fmt.Errorf("aggregate state for segment %s is corrupted", segment.ID)
			}
		}
	}
	return nil
}

func readAggregateJob(path string) (aggregateJobManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return aggregateJobManifest{}, err
	}
	var job aggregateJobManifest
	if err := json.Unmarshal(data, &job); err != nil {
		return aggregateJobManifest{}, fmt.Errorf("invalid aggregate job: %w", err)
	}
	if job.Version != aggregateManifestVersion {
		return aggregateJobManifest{}, fmt.Errorf("unsupported aggregate job version %d", job.Version)
	}
	return job, nil
}

func readAggregatePointer(root string, name string) (aggregatePointer, error) {
	path := filepath.Join(root, "aggregates", name, "current.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return aggregatePointer{}, err
	}
	var pointer aggregatePointer
	if err := json.Unmarshal(data, &pointer); err != nil {
		return aggregatePointer{}, fmt.Errorf("invalid aggregate pointer: %w", err)
	}
	if pointer.Version < 1 || pointer.Result == "" {
		return aggregatePointer{}, fmt.Errorf("aggregate pointer %q is invalid", name)
	}
	return pointer, nil
}

func nextAggregateVersion(root string, name string) int {
	versionsRoot := filepath.Join(root, "aggregates", name, "versions")
	entries, err := os.ReadDir(versionsRoot)
	if err != nil {
		return 1
	}
	max := 0
	for _, entry := range entries {
		var value int
		if _, err := fmt.Sscanf(entry.Name(), "%d", &value); err == nil && value > max {
			max = value
		}
	}
	return max + 1
}

func materializeAggregateState(plan aggregationPlan, state *groupState) []any {
	row := make([]any, 0, len(state.Key.Parts)+len(plan.specs))
	for _, part := range state.Key.Parts {
		if part.Null {
			row = append(row, nil)
		} else {
			row = append(row, part.Value)
		}
	}
	for i, resolved := range plan.specs {
		row = append(row, accumulatorValue(resolved.spec.Func, state.Accs[i]))
	}
	return row
}
