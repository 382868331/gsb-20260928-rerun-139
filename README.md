# columnar

`columnar` is a dependency-free, on-disk columnar processing library for local Go programs. It does not use a database: a dataset is a JSON manifest plus one binary file per column per segment.

## First-round model

- `Schema` defines ordered columns using `TypeInt64`, `TypeFloat64`, `TypeBool`, or `TypeString`.
- `Batch` normalizes input values into the schema's concrete Go types.
- `Writer.Flush` writes rows as an immutable segment: one `*.col` file per column.
- `Reader.Scan` selects columns, applies predicates, skips segments using min/max/null statistics, and reads only projected columns plus filter columns.
- `ScanOptions{Reference: true}` uses a row-style reference scan that reads every column and every segment; tests compare it with the optimized column scan.
- Reader.Aggregate performs grouped count, sum, avg, min, and max aggregation. It reads only group, aggregate, and filter columns and also offers a row-style reference mode.

## Null and zero rules

Nulls are stored explicitly in a null bitmap. They are distinct from `0`, `0.0`, `false`, and `""`. A comparison with a null value, including `!=`, does not match in this first-round query semantics.

Predicates are bound to a column name before reading, and each row evaluates predicates against values from that same column index. A projected column can therefore never be accidentally used as another column's filter value.

The same identity rule applies to grouping: a null group key is a real key, but it is never coalesced with zero, false, or an empty string. Aggregates keep null inputs separate from non-null inputs. count(*) counts every matching row; count(column) counts only non-null values; sum, avg, min, and max ignore nulls and return null for a group containing no non-null input.

## Integrity and publication

Each column file contains a type, row count, null bitmap, non-null values, CRC-32, and end marker. The manifest records the file size and CRC as segment metadata. Corruption, truncation, type mismatches, and invalid row counts return errors identifying the segment and column; valid segments are not used to silently replace damaged data.

Column files and `manifest.json` are written through temporary files and atomic renames. A segment directory from an interrupted write is invisible until all of its column files have been written, fsynced, and the manifest has been atomically published.

## Aggregate state and publication

When a grouped aggregation has more active keys than AggregateOptions.MaxGroups, states are hash-partitioned to temporary gob files. Final merge recursively repartitions an oversized partition. A single hot key therefore remains one bounded state instead of being split or collapsed, while a high-cardinality workload does not retain an unbounded in-memory hash table.

RunAggregation materializes intermediate state per immutable source segment. Each segment directory and its CRC records are confirmed only after its files have been synced and renamed. ResumeAggregation skips confirmed source segments, so cancellation cannot double-count them. Damaged confirmed temporary state causes the pending run to be rebuilt without replacing the existing current.json; result files are versioned and a new pointer is atomically renamed only after the replacement result is durable.

## Example

```go
schema, _ := columnar.NewSchema([]columnar.Column{
	{Name: "id", Type: columnar.TypeInt64},
	{Name: "active", Type: columnar.TypeBool},
})

writer, _ := columnar.Create("data/users", schema)
_ = writer.Append(int64(1), true)
_ = writer.Close()

reader, _ := columnar.OpenReader("data/users")
filter, _ := columnar.NewFilter(columnar.Predicate{
	Column: "active",
	Op: columnar.OpEq,
	Value: true,
})
result, _ := reader.Scan(columnar.ScanOptions{
	Columns: []string{"id"},
	Filter: filter,
})
```
