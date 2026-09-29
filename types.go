package columnar

import (
	"fmt"
	"sort"
)

type Type uint8

const (
	TypeInt64 Type = iota + 1
	TypeFloat64
	TypeBool
	TypeString
)

func (t Type) String() string {
	switch t {
	case TypeInt64:
		return "int64"
	case TypeFloat64:
		return "float64"
	case TypeBool:
		return "bool"
	case TypeString:
		return "string"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(t))
	}
}

type Column struct {
	Name string
	Type Type
}

type Schema struct {
	Columns []Column
	index   map[string]int
}

func NewSchema(columns []Column) (Schema, error) {
	if len(columns) == 0 {
		return Schema{}, fmt.Errorf("schema must contain at least one column")
	}
	seen := make(map[string]int, len(columns))
	copied := make([]Column, len(columns))
	for i, column := range columns {
		if column.Name == "" {
			return Schema{}, fmt.Errorf("column %d has an empty name", i)
		}
		if column.Type < TypeInt64 || column.Type > TypeString {
			return Schema{}, fmt.Errorf("column %q has unsupported type %s", column.Name, column.Type)
		}
		if previous, ok := seen[column.Name]; ok {
			return Schema{}, fmt.Errorf("duplicate column %q at positions %d and %d", column.Name, previous, i)
		}
		seen[column.Name] = i
		copied[i] = column
	}
	return Schema{Columns: copied, index: seen}, nil
}

func (s Schema) ColumnIndex(name string) (int, bool) {
	if s.index == nil {
		for i, column := range s.Columns {
			if column.Name == name {
				return i, true
			}
		}
		return -1, false
	}
	index, ok := s.index[name]
	return index, ok
}

func (s Schema) Project(names []string) (Schema, []int, error) {
	columns := make([]Column, len(names))
	indexes := make([]int, len(names))
	seen := make(map[string]struct{}, len(names))
	for i, name := range names {
		if _, ok := seen[name]; ok {
			return Schema{}, nil, fmt.Errorf("duplicate projected column %q", name)
		}
		seen[name] = struct{}{}
		index, ok := s.ColumnIndex(name)
		if !ok {
			return Schema{}, nil, fmt.Errorf("unknown column %q", name)
		}
		indexes[i] = index
		columns[i] = s.Columns[index]
	}
	projected, _ := NewSchema(columns)
	return projected, indexes, nil
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
