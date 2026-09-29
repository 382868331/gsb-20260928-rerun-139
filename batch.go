package columnar

import "fmt"

type Batch struct {
	schema Schema
	rows   [][]any
}

func NewBatch(schema Schema) *Batch {
	return &Batch{schema: schema, rows: make([][]any, 0)}
}

func (b *Batch) Len() int { return len(b.rows) }

func (b *Batch) Schema() Schema { return b.schema }

func (b *Batch) Append(values ...any) error {
	if len(values) != len(b.schema.Columns) {
		return fmt.Errorf("expected %d values, got %d", len(b.schema.Columns), len(values))
	}
	row := make([]any, len(values))
	for i, value := range values {
		normalized, err := normalizeValue(b.schema.Columns[i].Type, value)
		if err != nil {
			return fmt.Errorf("column %q: %w", b.schema.Columns[i].Name, err)
		}
		row[i] = normalized
	}
	b.rows = append(b.rows, row)
	return nil
}

func (b *Batch) Row(index int) []any {
	if index < 0 || index >= len(b.rows) {
		panic(fmt.Sprintf("row index %d out of range [0,%d)", index, len(b.rows)))
	}
	row := make([]any, len(b.rows[index]))
	copy(row, b.rows[index])
	return row
}

func normalizeValue(typ Type, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch typed := value.(type) {
	case int:
		if typ != TypeInt64 {
			return nil, typeError(typ, value)
		}
		return int64(typed), nil
	case int64:
		if typ != TypeInt64 {
			return nil, typeError(typ, value)
		}
		return typed, nil
	case float64:
		if typ != TypeFloat64 {
			return nil, typeError(typ, value)
		}
		return typed, nil
	case float32:
		if typ != TypeFloat64 {
			return nil, typeError(typ, value)
		}
		return float64(typed), nil
	case bool:
		if typ != TypeBool {
			return nil, typeError(typ, value)
		}
		return typed, nil
	case string:
		if typ != TypeString {
			return nil, typeError(typ, value)
		}
		return typed, nil
	default:
		return nil, fmt.Errorf("unsupported Go type %T", value)
	}
}

func typeError(expected Type, value any) error {
	return fmt.Errorf("expected %s, got %T", expected, value)
}
