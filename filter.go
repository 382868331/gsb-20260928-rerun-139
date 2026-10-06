package columnar

import (
	"encoding/json"
	"fmt"
	"math"
)

type Operator string

const (
	OpEq Operator = "="
	OpNe Operator = "!="
	OpLt Operator = "<"
	OpLe Operator = "<="
	OpGt Operator = ">"
	OpGe Operator = ">="
)

type Predicate struct {
	Column string
	Op     Operator
	Value  any
}

type Filter struct {
	predicates []Predicate
}

func (f *Filter) MarshalJSON() ([]byte, error) {
	if f == nil || len(f.predicates) == 0 {
		return []byte("null"), nil
	}
	return json.Marshal(f.predicates)
}

func (f *Filter) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		f.predicates = nil
		return nil
	}
	return json.Unmarshal(data, &f.predicates)
}

func NewFilter(predicates ...Predicate) (*Filter, error) {
	for i, predicate := range predicates {
		switch predicate.Op {
		case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe:
		default:
			return nil, fmt.Errorf("predicate %d uses unsupported operator %q", i, predicate.Op)
		}
		if predicate.Column == "" {
			return nil, fmt.Errorf("predicate %d has an empty column name", i)
		}
	}
	return &Filter{predicates: append([]Predicate(nil), predicates...)}, nil
}

func (f *Filter) bind(schema Schema) ([]boundPredicate, error) {
	if f == nil || len(f.predicates) == 0 {
		return nil, nil
	}
	bound := make([]boundPredicate, 0, len(f.predicates))
	for _, predicate := range f.predicates {
		index, ok := schema.ColumnIndex(predicate.Column)
		if !ok {
			return nil, fmt.Errorf("unknown filter column %q", predicate.Column)
		}
		column := schema.Columns[index]
		value, err := normalizeValue(column.Type, predicate.Value)
		if err != nil {
			return nil, fmt.Errorf("filter on %q: %w", predicate.Column, err)
		}
		bound = append(bound, boundPredicate{name: column.Name, column: index, op: predicate.Op, value: value})
	}
	return bound, nil
}

type boundPredicate struct {
	name   string
	column int
	op     Operator
	value  any
}

func matches(columns []any, predicates []boundPredicate) bool {
	for _, predicate := range predicates {
		if !matchOne(columns[predicate.column], predicate) {
			return false
		}
	}
	return true
}

func matchOne(value any, predicate boundPredicate) bool {
	if value == nil || predicate.value == nil {
		return false
	}
	compare := compareValues(value, predicate.value)
	if compare == incomparable {
		return false
	}
	switch predicate.op {
	case OpEq:
		return compare == 0
	case OpNe:
		return compare != 0
	case OpLt:
		return compare < 0
	case OpLe:
		return compare <= 0
	case OpGt:
		return compare > 0
	case OpGe:
		return compare >= 0
	default:
		return false
	}
}

const incomparable = math.MinInt8

func compareValues(left any, right any) int {
	switch leftValue := left.(type) {
	case int64:
		rightValue, ok := right.(int64)
		if !ok {
			return incomparable
		}
		if leftValue < rightValue {
			return -1
		}
		if leftValue > rightValue {
			return 1
		}
		return 0
	case float64:
		rightValue, ok := right.(float64)
		if !ok {
			return incomparable
		}
		if math.IsNaN(leftValue) || math.IsNaN(rightValue) {
			return incomparable
		}
		if leftValue < rightValue {
			return -1
		}
		if leftValue > rightValue {
			return 1
		}
		return 0
	case bool:
		rightValue, ok := right.(bool)
		if !ok {
			return incomparable
		}
		if leftValue == rightValue {
			return 0
		}
		if !leftValue {
			return -1
		}
		return 1
	case string:
		rightValue, ok := right.(string)
		if !ok {
			return incomparable
		}
		if leftValue < rightValue {
			return -1
		}
		if leftValue > rightValue {
			return 1
		}
		return 0
	default:
		return incomparable
	}
}
