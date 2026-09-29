package coldstore

import (
	"fmt"
	"time"
)

// Kind identifies the physical type of a column.
type Kind uint8

const (
	KindInt64 Kind = iota + 1
	KindFloat64
	KindBool
	KindString
	KindTimestamp
)

func (k Kind) String() string {
	switch k {
	case KindInt64:
		return "int64"
	case KindFloat64:
		return "float64"
	case KindBool:
		return "bool"
	case KindString:
		return "string"
	case KindTimestamp:
		return "timestamp"
	default:
		return fmt.Sprintf("kind(%d)", int(k))
	}
}

// Schema is the ordered set of columns shared by every batch.
type Schema struct {
	Columns []Column
}

// Column describes one column in the schema.
type Column struct {
	Name string
	Kind Kind
}

func (s Schema) indexOf(name string) (int, bool) {
	for i := range s.Columns {
		if s.Columns[i].Name == name {
			return i, true
		}
	}
	return -1, false
}

// ColumnIndex returns the position of name within the schema.
func (s Schema) ColumnIndex(name string) (int, bool) {
	return s.indexOf(name)
}

// Batch is one horizontal slice of rows stored as independent columns.
// Each column slice has exactly Rows entries. Null marks the matching
// position valid (false) or null (true); a null cell never takes part in
// comparisons even when its backing value happens to be the zero value.
type Batch struct {
	Rows    int
	Columns []ColumnData
}

// ColumnData is the in-memory representation of one column of a batch.
// Exactly one of the value slices is populated, matching the Kind of the
// column; its length equals Batch.Rows.
type ColumnData struct {
	Kind       Kind
	Null       []bool
	Int64s     []int64
	Float64s   []float64
	Bools      []bool
	Strings    []string
	Timestamps []time.Time
}

// Row returns the value of one cell. ok is false when the cell is null.
func (c ColumnData) Row(i int) (any, bool) {
	if i < 0 || i >= c.Len() || (len(c.Null) > i && c.Null[i]) {
		return nil, false
	}
	switch c.Kind {
	case KindInt64:
		return c.Int64s[i], true
	case KindFloat64:
		return c.Float64s[i], true
	case KindBool:
		return c.Bools[i], true
	case KindString:
		return c.Strings[i], true
	case KindTimestamp:
		return c.Timestamps[i], true
	default:
		return nil, false
	}
}

// Len returns the number of cells in the column.
func (c ColumnData) Len() int {
	switch c.Kind {
	case KindInt64:
		return len(c.Int64s)
	case KindFloat64:
		return len(c.Float64s)
	case KindBool:
		return len(c.Bools)
	case KindString:
		return len(c.Strings)
	case KindTimestamp:
		return len(c.Timestamps)
	default:
		return 0
	}
}

// IsNull reports whether position i is null.
func (c ColumnData) IsNull(i int) bool {
	return len(c.Null) > i && c.Null[i]
}
