package coldstore

import "fmt"

// CorruptSegmentError means a segment file failed integrity checks or
// could not be decoded. Readers always return this error rather than
// silently skipping a damaged segment.
type CorruptSegmentError struct {
	Path   string
	Batch  int
	Column string
	Reason string
}

func (e *CorruptSegmentError) Error() string {
	return fmt.Sprintf("coldstore: corrupt segment %s (batch %d, column %q): %s",
		e.Path, e.Batch, e.Column, e.Reason)
}

// SchemaError means a batch did not match the declared schema.
type SchemaError struct {
	Reason string
}

func (e *SchemaError) Error() string {
	return "coldstore: schema mismatch: " + e.Reason
}
