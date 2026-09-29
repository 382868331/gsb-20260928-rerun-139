# columnar

`columnar` is a dependency-free, on-disk columnar processing library for local Go programs. It does not use a database: a dataset is a JSON manifest plus one binary file per column per segment.

## First-round model

- `Schema` defines ordered columns using `TypeInt64`, `TypeFloat64`, `TypeBool`, or `TypeString`.
- `Batch` normalizes input values into the schema's concrete Go types.
- `Writer.Flush` writes rows as an immutable segment: one `*.col` file per column.
- `Reader.Scan` selects columns, applies predicates, skips segments using min/max/null statistics, and reads only projected columns plus filter columns.
- `ScanOptions{Reference: true}` uses a row-style reference scan that reads every column and every segment; tests compare it with the optimized column scan.

## Null and zero rules

Nulls are stored explicitly in a null bitmap. They are distinct from `0`, `0.0`, `false`, and `""`. A comparison with a null value, including `!=`, does not match in this first-round query semantics.

Predicates are bound to a column name before reading, and each row evaluates predicates against values from that same column index. A projected column can therefore never be accidentally used as another column's filter value.

## Integrity and publication

Each column file contains a type, row count, null bitmap, non-null values, CRC-32, and end marker. The manifest records the file size and CRC as segment metadata. Corruption, truncation, type mismatches, and invalid row counts return errors identifying the segment and column; valid segments are not used to silently replace damaged data.

Column files and `manifest.json` are written through temporary files and atomic renames. A segment directory from an interrupted write is invisible until all of its column files have been written, fsynced, and the manifest has been atomically published.

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
