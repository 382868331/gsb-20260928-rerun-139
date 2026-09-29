// Package refrow is a deliberately simple row-oriented reference
// implementation of the same null and typing rules used by coldstore.
// Tests compare coldstore output against it.
package refrow

import (
	"fmt"

	"coldstore"
)

// Table holds all values row by row as tagged cells.
type Table struct {
	Schema coldstore.Schema
	Rows   []Row
}

// Row is one record. Cells[i] is the value and Null[i] the null flag,
// so a null stays distinct from a zero value.
type Row struct {
	Cells []any
	Null  []bool
}

// FromBatches builds the reference table from in-memory batches.
func FromBatches(schema coldstore.Schema, batches []coldstore.Batch) *Table {
	t := &Table{Schema: schema}
	for _, b := range batches {
		for i := 0; i < b.Rows; i++ {
			row := Row{Cells: make([]any, len(schema.Columns)), Null: make([]bool, len(schema.Columns))}
			for c := range schema.Columns {
				v, ok := b.Columns[c].Row(i)
				row.Cells[c] = v
				row.Null[c] = !ok
			}
			t.Rows = append(t.Rows, row)
		}
	}
	return t
}

// Filter evaluates filters using SQL-style three-valued logic: nulls
// never pass, even for !=. It returns surviving row indices.
func (t *Table) Filter(filters []coldstore.Filter) []int {
	var sel []int
rowLoop:
	for ri, row := range t.Rows {
		for _, f := range filters {
			ci, ok := t.Schema.ColumnIndex(f.Column)
			if !ok {
				panic("unknown column " + f.Column)
			}
			if row.Null[ci] || !compare(row.Cells[ci], f) {
				continue rowLoop
			}
		}
		sel = append(sel, ri)
	}
	return sel
}

func compare(cell any, f coldstore.Filter) bool {
	cmp, err := cmpAny(cell, f.Value)
	if err != nil {
		panic(err)
	}
	switch f.Op {
	case coldstore.OpEq:
		return cmp == 0
	case coldstore.OpNe:
		return cmp != 0
	case coldstore.OpLt:
		return cmp < 0
	case coldstore.OpLe:
		return cmp <= 0
	case coldstore.OpGt:
		return cmp > 0
	case coldstore.OpGe:
		return cmp >= 0
	default:
		panic("bad op")
	}
}

func cmpAny(a, b any) (int, error) {
	switch x := a.(type) {
	case int64:
		y, ok := b.(int64)
		if !ok {
			return 0, fmt.Errorf("type mismatch int64 vs %T", b)
		}
		return cmpOrdered(x, y), nil
	case float64:
		y, ok := b.(float64)
		if !ok {
			return 0, fmt.Errorf("type mismatch float64 vs %T", b)
		}
		return cmpOrdered(x, y), nil
	case string:
		y, ok := b.(string)
		if !ok {
			return 0, fmt.Errorf("type mismatch string vs %T", b)
		}
		return cmpOrdered(x, y), nil
	case bool:
		y, ok := b.(bool)
		if !ok {
			return 0, fmt.Errorf("type mismatch bool vs %T", b)
		}
		switch {
		case x == y:
			return 0, nil
		case !x && y:
			return -1, nil
		default:
			return 1, nil
		}
	default:
		return 0, fmt.Errorf("unsupported cell type %T", a)
	}
}

func cmpOrdered[T ~int64 | ~float64 | ~string](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
