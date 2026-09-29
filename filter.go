package coldstore

import (
	"fmt"
	"time"
)

// Op is a scalar comparison applied by a filter.
type Op uint8

const (
	OpEq Op = iota + 1
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
)

func (op Op) String() string {
	switch op {
	case OpEq:
		return "=="
	case OpNe:
		return "!="
	case OpLt:
		return "<"
	case OpLe:
		return "<="
	case OpGt:
		return ">"
	case OpGe:
		return ">="
	default:
		return fmt.Sprintf("op(%d)", int(op))
	}
}

// Filter compares one column against a literal. Null cells never match
// any comparison, including !=, mirroring SQL three-valued logic: a
// null is unknown, not a match and not an ordinary zero value.
type Filter struct {
	Column string
	Op     Op
	Value  any
}

// Evaluate returns a per-row selection mask. Null positions and type
// mismatches are false rather than panicking.
func (f Filter) Evaluate(data ColumnData) ([]bool, error) {
	n := data.Len()
	mask := make([]bool, n)
	for i := 0; i < n; i++ {
		if data.IsNull(i) {
			continue
		}
		ok, err := compareCell(data, i, f.Op, f.Value)
		if err != nil {
			return nil, err
		}
		mask[i] = ok
	}
	return mask, nil
}

func compareCell(data ColumnData, i int, op Op, literal any) (bool, error) {
	cmp, err := compareValue(data.Kind, data, i, literal)
	if err != nil {
		return false, err
	}
	switch op {
	case OpEq:
		return cmp == 0, nil
	case OpNe:
		return cmp != 0, nil
	case OpLt:
		return cmp < 0, nil
	case OpLe:
		return cmp <= 0, nil
	case OpGt:
		return cmp > 0, nil
	case OpGe:
		return cmp >= 0, nil
	default:
		return false, fmt.Errorf("unknown comparison op %d", op)
	}
}

// compareValue returns -1/0/1 for cell vs literal. Timestamp literals
// may be time.Time. No implicit cross-kind conversion is performed.
func compareValue(kind Kind, data ColumnData, i int, literal any) (int, error) {
	switch kind {
	case KindInt64:
		v, ok := asInt64(literal)
		if !ok {
			return 0, fmt.Errorf("filter value %v is not int64", literal)
		}
		return compareOrdered(data.Int64s[i], v), nil
	case KindFloat64:
		v, ok := asFloat64(literal)
		if !ok {
			return 0, fmt.Errorf("filter value %v is not float64", literal)
		}
		return compareOrdered(data.Float64s[i], v), nil
	case KindString:
		v, ok := literal.(string)
		if !ok {
			return 0, fmt.Errorf("filter value %v is not string", literal)
		}
		return compareOrdered(data.Strings[i], v), nil
	case KindBool:
		v, ok := literal.(bool)
		if !ok {
			return 0, fmt.Errorf("filter value %v is not bool", literal)
		}
		switch {
		case data.Bools[i] == v:
			return 0, nil
		case !data.Bools[i] && v:
			return -1, nil
		default:
			return 1, nil
		}
	case KindTimestamp:
		v, ok := literal.(time.Time)
		if !ok {
			return 0, fmt.Errorf("filter value %v is not time.Time", literal)
		}
		a := data.Timestamps[i].UnixNano()
		b := v.UnixNano()
		return compareOrdered(a, b), nil
	default:
		return 0, fmt.Errorf("cannot compare kind %s", kind)
	}
}

func compareOrdered[T ~int64 | ~float64 | ~string](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func asInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	default:
		return 0, false
	}
}

func asFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	default:
		return 0, false
	}
}
