package columnar

import (
	"encoding/json"
	"errors"
	"fmt"
)

const manifestVersion = 1

type manifest struct {
	Version  int           `json:"version"`
	Revision uint64        `json:"revision"`
	Columns  []Column      `json:"columns"`
	Segments []segmentInfo `json:"segments"`
}

type segmentInfo struct {
	ID    string           `json:"id"`
	Rev   uint64           `json:"rev,omitempty"`
	Rows  uint64           `json:"rows"`
	Files []segmentFile    `json:"files"`
	Stats map[string]stats `json:"stats"`
}

type segmentFile struct {
	Path  string `json:"path"`
	Size  int64  `json:"size"`
	CRC32 uint32 `json:"crc32"`
}

func marshalManifest(value manifest) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func decodeManifest(data []byte) (manifest, error) {
	var value manifest
	if err := json.Unmarshal(data, &value); err != nil {
		return manifest{}, fmt.Errorf("invalid manifest: %w", err)
	}
	if value.Version != manifestVersion {
		return manifest{}, fmt.Errorf("unsupported manifest version %d", value.Version)
	}
	if len(value.Columns) == 0 {
		return manifest{}, errors.New("manifest has no columns")
	}
	seenSegments := make(map[string]struct{}, len(value.Segments))
	for i, segment := range value.Segments {
		if segment.ID == "" {
			return manifest{}, fmt.Errorf("segment %d has no id", i)
		}
		if _, ok := seenSegments[segment.ID]; ok {
			return manifest{}, fmt.Errorf("duplicate segment id %q", segment.ID)
		}
		seenSegments[segment.ID] = struct{}{}
		if segment.Rev == 0 {
			segment.Rev = 1
			value.Segments[i] = segment
		}
		if segment.Rows == 0 {
			return manifest{}, fmt.Errorf("segment %q has zero rows", segment.ID)
		}
		if len(segment.Files) != len(value.Columns) {
			return manifest{}, fmt.Errorf("segment %q lists %d files for %d columns", segment.ID, len(segment.Files), len(value.Columns))
		}
		if segment.Stats == nil {
			return manifest{}, fmt.Errorf("segment %q is missing stats", segment.ID)
		}
		for _, column := range value.Columns {
			if _, ok := segment.Stats[column.Name]; !ok {
				return manifest{}, fmt.Errorf("segment %q is missing stats for %q", segment.ID, column.Name)
			}
		}
		for j, file := range segment.Files {
			if file.Path == "" || file.Size <= 0 || file.CRC32 == 0 {
				return manifest{}, fmt.Errorf("segment %q column %d has invalid file metadata", segment.ID, j)
			}
		}
	}
	return value, nil
}
