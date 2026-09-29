package coldstore

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Store is an on-disk directory containing columnar batches. Data is
// never stored in a database: each batch is a subdirectory holding one
// independently checksummed segment file per column, plus a manifest at
// the root that is the sole publication point.
type Store struct {
	root string
}

// OpenStore opens (or prepares) a local column store at dir. An existing
// directory without a published manifest is reported as an error: an
// interrupted write must never be treated as valid data.
func OpenStore(dir string) (*Store, error) {
	s := &Store{root: dir}
	switch info, err := os.Stat(dir); {
	case err == nil:
		if !info.IsDir() {
			return nil, fmt.Errorf("coldstore: %s is not a directory", dir)
		}
		if _, merr := os.Stat(filepath.Join(dir, manifestName)); errors.Is(merr, fs.ErrNotExist) {
			return nil, fmt.Errorf("coldstore: %s has no %s; dataset is not published (interrupted write?)", dir, manifestName)
		} else if merr != nil {
			return nil, merr
		}
	case errors.Is(err, fs.ErrNotExist):
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	default:
		return nil, err
	}
	return s, nil
}

// CreateStore creates a fresh store at dir, which must not exist yet.
func CreateStore(dir string) (*Store, error) {
	if err := os.Mkdir(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{root: dir}, nil
}

func (s *Store) readManifest() (*manifest, error) {
	raw, err := os.ReadFile(filepath.Join(s.root, manifestName))
	if err != nil {
		return nil, err
	}
	m, err := decodeManifest(raw)
	if err != nil {
		return nil, &CorruptSegmentError{Path: manifestName, Reason: "manifest cannot be decoded: " + err.Error()}
	}
	return m, nil
}

// publishManifest atomically replaces the root manifest, which is the
// moment a batch (or the whole dataset) becomes visible.
func (s *Store) publishManifest(m *manifest) error {
	raw, err := encodeManifest(m)
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.root, "."+manifestName+".tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.root, manifestName)); err != nil {
		os.Remove(tmp)
		return err
	}
	return syncDir(s.root)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
