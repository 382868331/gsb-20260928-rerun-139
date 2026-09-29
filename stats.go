package columnar

import "math"

type stats struct {
	NullCount uint64   `json:"null_count"`
	MinInt64  *int64   `json:"min_int64,omitempty"`
	MaxInt64  *int64   `json:"max_int64,omitempty"`
	MinFloat  *float64 `json:"min_float64,omitempty"`
	MaxFloat  *float64 `json:"max_float64,omitempty"`
	MinBool   *bool    `json:"min_bool,omitempty"`
	MaxBool   *bool    `json:"max_bool,omitempty"`
	MinString *string  `json:"min_string,omitempty"`
	MaxString *string  `json:"max_string,omitempty"`
}

func buildStats(typ Type, values []any) stats {
	result := stats{}
	for _, value := range values {
		if value == nil {
			result.NullCount++
			continue
		}
		if !comparableValue(typ, value) {
			continue
		}
		minimum, maximum := statsRange(typ, result)
		if minimum == nil || compareValues(value, minimum) < 0 {
			setStatsMin(typ, &result, value)
		}
		if maximum == nil || compareValues(value, maximum) > 0 {
			setStatsMax(typ, &result, value)
		}
	}
	return result
}

func statsRange(typ Type, value stats) (any, any) {
	switch typ {
	case TypeInt64:
		return derefInt(value.MinInt64), derefInt(value.MaxInt64)
	case TypeFloat64:
		return derefFloat(value.MinFloat), derefFloat(value.MaxFloat)
	case TypeBool:
		return derefBool(value.MinBool), derefBool(value.MaxBool)
	case TypeString:
		return derefString(value.MinString), derefString(value.MaxString)
	default:
		return nil, nil
	}
}

func derefInt(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func derefFloat(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

func derefBool(value *bool) any {
	if value == nil {
		return nil
	}
	return *value
}

func derefString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func setStatsMin(typ Type, target *stats, value any) {
	switch typ {
	case TypeInt64:
		typed := value.(int64)
		target.MinInt64 = &typed
	case TypeFloat64:
		typed := value.(float64)
		target.MinFloat = &typed
	case TypeBool:
		typed := value.(bool)
		target.MinBool = &typed
	case TypeString:
		typed := value.(string)
		target.MinString = &typed
	}
}

func setStatsMax(typ Type, target *stats, value any) {
	switch typ {
	case TypeInt64:
		typed := value.(int64)
		target.MaxInt64 = &typed
	case TypeFloat64:
		typed := value.(float64)
		target.MaxFloat = &typed
	case TypeBool:
		typed := value.(bool)
		target.MaxBool = &typed
	case TypeString:
		typed := value.(string)
		target.MaxString = &typed
	}
}

func comparableValue(typ Type, value any) bool {
	if typ != TypeFloat64 {
		return true
	}
	floatValue, ok := value.(float64)
	return ok && !math.IsNaN(floatValue)
}

func canSkip(stat stats, predicate boundPredicate) bool {
	minimum, maximum := statsRange(columnTypeFromPredicate(predicate), stat)
	if minimum == nil || maximum == nil {
		return true
	}
	minCompare := compareValues(minimum, predicate.value)
	maxCompare := compareValues(maximum, predicate.value)
	if minCompare == incomparable || maxCompare == incomparable {
		return false
	}
	switch predicate.op {
	case OpEq:
		return maxCompare < 0 || minCompare > 0
	case OpLt:
		return minCompare >= 0
	case OpLe:
		return minCompare > 0
	case OpGt:
		return maxCompare <= 0
	case OpGe:
		return maxCompare < 0
	case OpNe:
		return false
	default:
		return false
	}
}

func columnTypeFromPredicate(predicate boundPredicate) Type {
	switch predicate.value.(type) {
	case int64:
		return TypeInt64
	case float64:
		return TypeFloat64
	case bool:
		return TypeBool
	case string:
		return TypeString
	default:
		return 0
	}
}
