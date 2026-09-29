package columnar

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type fileStore struct {
	root string
	mu   sync.Mutex
}

func newFileStore(root string) (*fileStore, error) {
	clean := filepath.Clean(root)
	if err := os.MkdirAll(clean, 0o755); err != nil {
		return nil, err
	}
	info, err := os.Stat(clean)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, &fs.PathError{Op: "open", Path: clean, Err: errors.New("not a directory")}
	}
	return &fileStore{root: clean}, nil
}

func (s *fileStore) readManifest() (manifest, error) {
	data, err := os.ReadFile(filepath.Join(s.root, "manifest.json"))
	if err != nil {
		return manifest{}, err
	}
	value, err := decodeManifest(data)
	if err != nil {
		return manifest{}, err
	}
	schema, err := NewSchema(value.Columns)
	if err != nil {
		return manifest{}, err
	}
	value.Columns = schema.Columns
	return value, nil
}

func (s *fileStore) writeManifest(value manifest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := marshalManifest(value)
	if err != nil {
		return err
	}
	return atomicWriteFile(filepath.Join(s.root, "manifest.json"), data, true)
}

func (s *fileStore) nextSegmentID(current manifest) (string, error) {
	max := 0
	for _, segment := range current.Segments {
		var value int
		if _, err := fmt.Sscanf(segment.ID, "%d", &value); err == nil && value >= max {
			max = value + 1
		}
	}
	if max == 0 {
		max = len(current.Segments) + 1
	}
	return formatSegmentID(max), nil
}

func formatSegmentID(value int) string { return fmt.Sprintf("%010d", value) }

func (s *fileStore) publishSegment(segment segmentInfo, value manifest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	segmentDir := filepath.Join(s.root, "segments", segment.ID)
	if err := syncDir(segmentDir); err != nil {
		return err
	}
	if err := syncDir(filepath.Join(s.root, "segments")); err != nil {
		return err
	}
	return s.writeManifestLocked(value)
}

func (s *fileStore) writeManifestLocked(value manifest) error {
	data, err := marshalManifest(value)
	if err != nil {
		return err
	}
	return atomicWriteFile(filepath.Join(s.root, "manifest.json"), data, true)
}

func (s *fileStore) readColumnFile(relative string) ([]byte, error) {
	clean := filepath.Clean(filepath.Join(s.root, filepath.FromSlash(relative)))
	rootPrefix := s.root + string(os.PathSeparator)
	if clean != s.root && !isWithin(clean, rootPrefix) {
		return nil, &fs.PathError{Op: "open", Path: relative, Err: fs.ErrInvalid}
	}
	return os.ReadFile(clean)
}

func (s *fileStore) validateFileMetadata(relative string, size int64) error {
	clean := filepath.Clean(filepath.Join(s.root, filepath.FromSlash(relative)))
	rootPrefix := s.root + string(os.PathSeparator)
	if clean != s.root && !isWithin(clean, rootPrefix) {
		return &fs.PathError{Op: "stat", Path: relative, Err: fs.ErrInvalid}
	}
	info, err := os.Stat(clean)
	if err != nil {
		return err
	}
	if info.IsDir() || info.Size() != size {
		return &fs.PathError{Op: "stat", Path: relative, Err: fs.ErrInvalid}
	}
	return nil
}

func isWithin(path string, prefix string) bool {
	if len(path) < len(prefix) {
		return false
	}
	return path[:len(prefix)] == prefix
}

func atomicWriteFile(path string, data []byte, fsyncParent bool) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tempName := file.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tempName)
		}
	}()
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tempName, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tempName, path); err != nil {
		return err
	}
	committed = true
	if fsyncParent {
		return syncDir(dir)
	}
	return nil
}

func syncDir(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func sortedSegmentIDs(segments []segmentInfo) {
	sort.Slice(segments, func(i, j int) bool { return segments[i].ID < segments[j].ID })
}
