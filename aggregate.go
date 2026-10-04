package columnar

import (
	"context"
	"fmt"
	"math"
)

type AggFunc string

const (
	AggCountStar AggFunc = "count_star"
	AggCount     AggFunc = "count"
	AggSum       AggFunc = "sum"
	AggAvg       AggFunc = "avg"
	AggMin       AggFunc = "min"
	AggMax       AggFunc = "max"
)

type AggregateSpec struct {
	Column string
	Func   AggFunc
	Alias  string
	As     string
}

type AggregateRequest struct {
	GroupBy           []string
	Aggs              []AggregateSpec
	Filter            *Filter
	Reference         bool
	MaxInMemoryGroups int
	SpillDirectory    string
}

type AggregateOptions = AggregateRequest

type AggregateResult struct {
	Schema          Schema
	GroupSchema     Schema
	OutputSchema    Schema
	Rows            [][]any
	Groups          [][]any
	ColumnsRead     []string
	SegmentsScanned []string
	SegmentsRead    []string
	SpilledRuns     int
	Spilled         bool
}

type namedAggregate struct {
	column     int
	name       string
	funcName   AggFunc
	alias      string
	inputType  Type
	outputType Type
}

func bindAggregateRequest(schema Schema, request AggregateRequest) (Schema, []int, []namedAggregate, []boundPredicate, Schema, error) {
	if len(request.Aggs) == 0 {
		return Schema{}, nil, nil, nil, Schema{}, fmt.Errorf("aggregate request needs at least one aggregate")
	}
	groupSchema, groupIndexes, err := schema.Project(request.GroupBy)
	if err != nil {
		return Schema{}, nil, nil, nil, Schema{}, err
	}
	predicates, err := request.Filter.bind(schema)
	if err != nil {
		return Schema{}, nil, nil, nil, Schema{}, err
	}
	aggs := make([]namedAggregate, 0, len(request.Aggs))
	outputColumns := append([]Column(nil), groupSchema.Columns...)
	seenNames := make(map[string]struct{}, len(outputColumns))
	for _, groupColumn := range groupSchema.Columns {
		seenNames[groupColumn.Name] = struct{}{}
	}
	for i, spec := range request.Aggs {
		funcName := spec.Func
		if funcName == "count(*)" || funcName == "countstar" {
			funcName = AggCountStar
		}
		columnName := spec.Column
		if funcName == AggCount && columnName == "" {
			funcName = AggCountStar
		}
		switch funcName {
		case AggCountStar, AggCount, AggSum, AggAvg, AggMin, AggMax:
		default:
			return Schema{}, nil, nil, nil, Schema{}, fmt.Errorf("aggregate %d uses unsupported function %q", i, spec.Func)
		}
		index := -1
		if funcName != AggCountStar {
			var ok bool
			index, ok = schema.ColumnIndex(columnName)
			if !ok {
				return Schema{}, nil, nil, nil, Schema{}, fmt.Errorf("unknown aggregate column %q", columnName)
			}
		}
		if (funcName == AggSum || funcName == AggAvg) && index >= 0 {
			column := schema.Columns[index]
			if column.Type != TypeInt64 && column.Type != TypeFloat64 {
				return Schema{}, nil, nil, nil, Schema{}, fmt.Errorf("%s requires a numeric column, %q is %s", funcName, columnName, column.Type)
			}
		}
		outputName := firstNonEmpty(spec.Alias, spec.As)
		if outputName == "" {
			if funcName == AggCountStar {
				outputName = "count_star"
			} else {
				outputName = fmt.Sprintf("%s_%s", funcName, columnName)
			}
		}
		if _, exists := seenNames[outputName]; exists {
			return Schema{}, nil, nil, nil, Schema{}, fmt.Errorf("duplicate aggregate output name %q", outputName)
		}
		seenNames[outputName] = struct{}{}
		outputType := aggregateOutputType(funcName, schema, index)
		aggs = append(aggs, namedAggregate{
			column: index, name: columnName, funcName: funcName, alias: outputName,
			inputType: inputTypeForAggregate(funcName, schema, index), outputType: outputType,
		})
		outputColumns = append(outputColumns, Column{Name: outputName, Type: outputType})
	}
	outputSchema, err := NewSchema(outputColumns)
	if err != nil {
		return Schema{}, nil, nil, nil, Schema{}, err
	}
	return groupSchema, groupIndexes, aggs, predicates, outputSchema, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func inputTypeForAggregate(fun AggFunc, schema Schema, columnIndex int) Type {
	if fun == AggCountStar || fun == AggCount || columnIndex < 0 {
		return TypeInt64
	}
	return schema.Columns[columnIndex].Type
}

func aggregateOutputType(fun AggFunc, schema Schema, columnIndex int) Type {
	switch fun {
	case AggCountStar, AggCount:
		return TypeInt64
	case AggAvg:
		return TypeFloat64
	default:
		if columnIndex < 0 {
			return TypeInt64
		}
		return schema.Columns[columnIndex].Type
	}
}

func requiredColumns(groupIndexes []int, aggs []namedAggregate, predicates []boundPredicate) []int {
	needed := make(map[int]struct{})
	indexes := make([]int, 0)
	add := func(index int) {
		if index < 0 {
			return
		}
		if _, ok := needed[index]; !ok {
			needed[index] = struct{}{}
			indexes = append(indexes, index)
		}
	}
	for _, index := range groupIndexes {
		add(index)
	}
	for _, agg := range aggs {
		add(agg.column)
	}
	for _, predicate := range predicates {
		add(predicate.column)
	}
	return indexes
}

type aggAccumulator struct {
	countRows    int64
	countNonNull int64
	intSum       int64
	floatSum     float64
	hasFloat     bool
	hasMinMax    bool
	minValue     any
	maxValue     any
}

func (acc *aggAccumulator) accumulateRow(value any) {
	acc.countRows++
}

func (acc *aggAccumulator) accumulateValue(value any) {
	acc.countRows++
	if value == nil {
		return
	}
	acc.countNonNull++
	switch typed := value.(type) {
	case int64:
		if acc.hasFloat {
			acc.floatSum += float64(typed)
		} else {
			acc.intSum += typed
		}
	case float64:
		if !math.IsNaN(typed) {
			if !acc.hasFloat {
				acc.floatSum = float64(acc.intSum)
				acc.hasFloat = true
			}
			acc.floatSum += typed
		}
	}
	if comparableAny(value) {
		if !acc.hasMinMax || compareValues(value, acc.minValue) < 0 {
			acc.minValue = value
		}
		if !acc.hasMinMax || compareValues(value, acc.maxValue) > 0 {
			acc.maxValue = value
		}
		acc.hasMinMax = true
	}
}

func comparableAny(value any) bool {
	if floatValue, ok := value.(float64); ok {
		return !math.IsNaN(floatValue)
	}
	return true
}

func (acc aggAccumulator) merge(other aggAccumulator) aggAccumulator {
	acc.countRows += other.countRows
	acc.countNonNull += other.countNonNull
	switch {
	case acc.hasFloat && other.hasFloat:
		acc.floatSum += other.floatSum
	case acc.hasFloat:
		acc.floatSum += float64(other.intSum)
	case other.hasFloat:
		acc.hasFloat = true
		acc.floatSum = float64(acc.intSum) + other.floatSum
		acc.intSum = 0
	default:
		acc.intSum += other.intSum
	}
	if other.hasMinMax {
		if !acc.hasMinMax || compareValues(other.minValue, acc.minValue) < 0 {
			acc.minValue = other.minValue
		}
		if !acc.hasMinMax || compareValues(other.maxValue, acc.maxValue) > 0 {
			acc.maxValue = other.maxValue
		}
		acc.hasMinMax = true
	}
	return acc
}

type aggState struct {
	order  []string
	keys   map[string][]any
	tables []map[string]*aggAccumulator
}

func newAggState(aggregateCount int) *aggState {
	state := &aggState{order: make([]string, 0), keys: make(map[string][]any), tables: make([]map[string]*aggAccumulator, aggregateCount)}
	for i := range state.tables {
		state.tables[i] = make(map[string]*aggAccumulator)
	}
	return state
}

func (state *aggState) Len() int { return len(state.order) }

func (state *aggState) add(key string, groupValues []any, values []any, aggs []namedAggregate) {
	if _, exists := state.tables[0][key]; !exists {
		state.order = append(state.order, key)
		state.keys[key] = groupValues
	}
	for i, agg := range aggs {
		acc, exists := state.tables[i][key]
		if !exists {
			acc = &aggAccumulator{}
			state.tables[i][key] = acc
		}
		var value any
		if agg.column >= 0 {
			value = values[agg.column]
		}
		if agg.funcName == AggCountStar {
			acc.accumulateRow(value)
		} else {
			acc.accumulateValue(value)
		}
	}
}

func aggregateRow(ctx context.Context, values []any, state *aggState, groupIndexes []int, aggs []namedAggregate, predicates []boundPredicate) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if !matches(values, predicates) {
		return nil
	}
	groupValues := make([]any, len(groupIndexes))
	encodedParts := make([]byte, 0, 16)
	for i, index := range groupIndexes {
		groupValues[i] = values[index]
		encodedParts = appendGroupKeyPart(encodedParts, groupValues[i])
	}
	state.add(string(encodedParts), groupValues, values, aggs)
	return nil
}

func finishAccumulator(fun AggFunc, acc aggAccumulator, columnType Type) any {
	switch fun {
	case AggCountStar:
		return acc.countRows
	case AggCount:
		return acc.countNonNull
	case AggSum:
		if acc.countNonNull == 0 {
			return nil
		}
		if columnType == TypeFloat64 || acc.hasFloat {
			return acc.floatSum
		}
		return acc.intSum
	case AggAvg:
		if acc.countNonNull == 0 {
			return nil
		}
		if columnType == TypeFloat64 || acc.hasFloat {
			return acc.floatSum / float64(acc.countNonNull)
		}
		return float64(acc.intSum) / float64(acc.countNonNull)
	case AggMin:
		if !acc.hasMinMax {
			return nil
		}
		return acc.minValue
	case AggMax:
		if !acc.hasMinMax {
			return nil
		}
		return acc.maxValue
	default:
		return nil
	}
}
