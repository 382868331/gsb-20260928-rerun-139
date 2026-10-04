package columnar

import (
	"context"
	"fmt"
	"sort"
)

type AggregateFunc string

const (
	AggCount AggregateFunc = "count"
	AggSum   AggregateFunc = "sum"
	AggAvg   AggregateFunc = "avg"
	AggMin   AggregateFunc = "min"
	AggMax   AggregateFunc = "max"
)

type AggregationSpec struct {
	Column string
	Func   AggregateFunc
	Name   string
}

type AggregateOptions struct {
	GroupBy    []string
	Aggregates []AggregationSpec
	Filter     *Filter
	MaxGroups  int
	Reference  bool
	TempDir    string
}

type AggregateResult struct {
	Schema          Schema
	Rows            [][]any
	SegmentsScanned []string
	ColumnsRead     []string
	Spilled         bool
}

type groupKey struct {
	Parts []groupKeyPart
}

type groupKeyPart struct {
	Null  bool
	Value any
}

type accumulator struct {
	Count    int64
	HasValue bool
	IntSum   int64
	FloatSum float64
	SumFloat bool
	Minimum  any
	Maximum  any
}

type aggregationPlan struct {
	groupIndexes []int
	groupColumns []Column
	specs        []resolvedAggregation
	predicates   []boundPredicate
	maxGroups    int
	outputSchema Schema
}

type resolvedAggregation struct {
	spec        AggregationSpec
	columnIndex int
	column      Column
	hasColumn   bool
}

func (r *Reader) Aggregate(ctx context.Context, options AggregateOptions) (AggregateResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	current, err := r.store.readManifest()
	if err != nil {
		return AggregateResult{}, err
	}
	plan, needed, err := buildAggregationPlan(r.schema, options)
	if err != nil {
		return AggregateResult{}, err
	}
	agg := newHashAggregator(plan, options.MaxGroups, options.TempDir)
	defer agg.cleanup()
	result := AggregateResult{Schema: plan.outputSchema}
	readIndexes := append([]int(nil), needed...)
	if options.Reference {
		readIndexes = make([]int, len(current.Columns))
		for i := range readIndexes {
			readIndexes[i] = i
		}
	}
	for _, segment := range current.Segments {
		if err := ctx.Err(); err != nil {
			return AggregateResult{}, err
		}
		if !options.Reference && segmentCanSkip(current.Columns, segment, plan.predicates) {
			continue
		}
		columns, err := r.readSegmentColumns(current, segment, readIndexes)
		if err != nil {
			return AggregateResult{}, fmt.Errorf("segment %s: %w", segment.ID, err)
		}
		result.SegmentsScanned = append(result.SegmentsScanned, segment.ID)
		for rowIndex := uint64(0); rowIndex < segment.Rows; rowIndex++ {
			if err := ctx.Err(); err != nil {
				return AggregateResult{}, err
			}
			rowColumns := make([]any, len(current.Columns))
			for columnIndex, values := range columns {
				rowColumns[columnIndex] = values[rowIndex]
			}
			if !matches(rowColumns, plan.predicates) {
				continue
			}
			if err := agg.addRow(rowColumns); err != nil {
				return AggregateResult{}, err
			}
		}
	}
	rows, err := agg.rows(ctx)
	if err != nil {
		return AggregateResult{}, err
	}
	result.Rows = rows
	result.Spilled = agg.spilled
	result.ColumnsRead = make([]string, 0, len(needed))
	for _, index := range needed {
		result.ColumnsRead = append(result.ColumnsRead, r.schema.Columns[index].Name)
	}
	if options.Reference {
		result.ColumnsRead = make([]string, len(r.schema.Columns))
		for i, column := range r.schema.Columns {
			result.ColumnsRead[i] = column.Name
		}
	}
	sort.Strings(result.ColumnsRead)
	return result, nil
}

func buildAggregationPlan(schema Schema, options AggregateOptions) (aggregationPlan, []int, error) {
	if len(options.Aggregates) == 0 {
		return aggregationPlan{}, nil, fmt.Errorf("aggregate requires at least one aggregation")
	}
	plan := aggregationPlan{maxGroups: options.MaxGroups}
	if plan.maxGroups <= 0 {
		plan.maxGroups = 1024
	}
	needed := make(map[int]struct{})
	seenGroups := make(map[string]struct{})
	for _, name := range options.GroupBy {
		if _, ok := seenGroups[name]; ok {
			return aggregationPlan{}, nil, fmt.Errorf("duplicate group column %q", name)
		}
		seenGroups[name] = struct{}{}
		index, ok := schema.ColumnIndex(name)
		if !ok {
			return aggregationPlan{}, nil, fmt.Errorf("unknown group column %q", name)
		}
		plan.groupIndexes = append(plan.groupIndexes, index)
		plan.groupColumns = append(plan.groupColumns, schema.Columns[index])
		needed[index] = struct{}{}
	}
	seenAggs := make(map[string]struct{})
	for _, spec := range options.Aggregates {
		if spec.Name == "" {
			spec.Name = defaultAggregateName(spec)
		}
		if _, ok := seenAggs[spec.Name]; ok {
			return aggregationPlan{}, nil, fmt.Errorf("duplicate aggregate output %q", spec.Name)
		}
		seenAggs[spec.Name] = struct{}{}
		resolved := resolvedAggregation{spec: spec}
		if spec.Column != "" {
			index, ok := schema.ColumnIndex(spec.Column)
			if !ok {
				return aggregationPlan{}, nil, fmt.Errorf("unknown aggregate column %q", spec.Column)
			}
			resolved.columnIndex = index
			resolved.column = schema.Columns[index]
			resolved.hasColumn = true
			needed[index] = struct{}{}
		}
		if err := validateAggregation(spec, resolved); err != nil {
			return aggregationPlan{}, nil, err
		}
		plan.specs = append(plan.specs, resolved)
	}
	predicates, err := options.Filter.bind(schema)
	if err != nil {
		return aggregationPlan{}, nil, err
	}
	plan.predicates = predicates
	for _, predicate := range predicates {
		needed[predicate.column] = struct{}{}
	}
	indexes := make([]int, 0, len(needed))
	for index := range needed {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	plan.outputSchema = aggregateOutputSchema(plan)
	return plan, indexes, nil
}

func validateAggregation(spec AggregationSpec, resolved resolvedAggregation) error {
	switch spec.Func {
	case AggCount:
	case AggSum, AggAvg, AggMin, AggMax:
		if !resolved.hasColumn {
			return fmt.Errorf("aggregate %s requires a column", spec.Func)
		}
		if (spec.Func == AggSum || spec.Func == AggAvg) &&
			resolved.column.Type != TypeInt64 && resolved.column.Type != TypeFloat64 {
			return fmt.Errorf("%s requires numeric column, got %s", spec.Func, resolved.column.Type)
		}
	default:
		return fmt.Errorf("unsupported aggregate %q", spec.Func)
	}
	return nil
}

func defaultAggregateName(spec AggregationSpec) string {
	if spec.Column == "" {
		return string(spec.Func)
	}
	return spec.Column + "_" + string(spec.Func)
}

func aggregateOutputSchema(plan aggregationPlan) Schema {
	columns := make([]Column, 0, len(plan.groupColumns)+len(plan.specs))
	columns = append(columns, plan.groupColumns...)
	for _, resolved := range plan.specs {
		typ := TypeInt64
		switch resolved.spec.Func {
		case AggAvg:
			typ = TypeFloat64
		case AggSum, AggMin, AggMax:
			typ = resolved.column.Type
		}
		columns = append(columns, Column{Name: resolved.spec.Name, Type: typ})
	}
	schema, _ := NewSchema(columns)
	return schema
}

func updateAccumulators(states []accumulator, plan aggregationPlan, row []any) {
	for i, resolved := range plan.specs {
		switch resolved.spec.Func {
		case AggCount:
			if !resolved.hasColumn || row[resolved.columnIndex] != nil {
				states[i].Count++
			}
			continue
		}
		if !resolved.hasColumn || row[resolved.columnIndex] == nil {
			continue
		}
		value := row[resolved.columnIndex]
		states[i].HasValue = true
		states[i].Count++
		switch resolved.spec.Func {
		case AggSum, AggAvg:
			switch typed := value.(type) {
			case int64:
				states[i].IntSum += typed
			case float64:
				states[i].FloatSum += typed
				states[i].SumFloat = true
			}
		case AggMin:
			if states[i].Minimum == nil || compareValues(value, states[i].Minimum) < 0 {
				states[i].Minimum = value
			}
		case AggMax:
			if states[i].Maximum == nil || compareValues(value, states[i].Maximum) > 0 {
				states[i].Maximum = value
			}
		}
	}
}

func mergeAccumulator(dst *accumulator, src accumulator) {
	dst.Count += src.Count
	if src.HasValue {
		dst.HasValue = true
		dst.IntSum += src.IntSum
		dst.FloatSum += src.FloatSum
		dst.SumFloat = dst.SumFloat || src.SumFloat
		if dst.Minimum == nil || (src.Minimum != nil && compareValues(src.Minimum, dst.Minimum) < 0) {
			dst.Minimum = src.Minimum
		}
		if dst.Maximum == nil || (src.Maximum != nil && compareValues(src.Maximum, dst.Maximum) > 0) {
			dst.Maximum = src.Maximum
		}
	}
}

func accumulatorValue(fn AggregateFunc, state accumulator) any {
	switch fn {
	case AggCount:
		return state.Count
	case AggSum:
		if !state.HasValue {
			return nil
		}
		if state.SumFloat {
			return state.FloatSum + float64(state.IntSum)
		}
		return state.IntSum
	case AggAvg:
		if !state.HasValue || state.Count == 0 {
			return nil
		}
		return (state.FloatSum + float64(state.IntSum)) / float64(state.Count)
	case AggMin:
		return state.Minimum
	case AggMax:
		return state.Maximum
	default:
		return nil
	}
}

func makeGroupKey(plan aggregationPlan, row []any) groupKey {
	parts := make([]groupKeyPart, len(plan.groupIndexes))
	for i, index := range plan.groupIndexes {
		value := row[index]
		parts[i] = groupKeyPart{Null: value == nil, Value: value}
	}
	return groupKey{Parts: parts}
}

func compareGroupKeys(left groupKey, right groupKey) int {
	for i := range left.Parts {
		if left.Parts[i].Null || right.Parts[i].Null {
			if left.Parts[i].Null != right.Parts[i].Null {
				if left.Parts[i].Null {
					return -1
				}
				return 1
			}
			continue
		}
		if compare := compareValues(left.Parts[i].Value, right.Parts[i].Value); compare != incomparable && compare != 0 {
			return compare
		}
	}
	return 0
}
