// Package coldstore is a local, file-based columnar data processing
// library. It intentionally has no database dependency: datasets are
// directories of checksummed per-column segment files published through
// a single manifest.
//
// # Organization
//
// A Schema names ordered columns with a Kind. Data is appended in
// Batches. Each batch is stored as its own directory containing one
// segment file per column; readers therefore open only the segments for
// the columns they need.
//
// Null and zero-value rules (stable across future extensions)
//
// Every segment carries a per-row null bitmap in addition to its value
// payload. Null cells are distinct from ordinary zero values:
//
//   - a null int64 0, float64 0, empty string or false bool is still
//     null, and the null flag round-trips independently;
//   - nulls never satisfy a comparison, including !=, following
//     three-valued logic;
//   - comparisons never convert between kinds implicitly; a mismatched
//     filter literal is an error, not a match.
//
// Future compression and numeric/vector operations must preserve these
// same rules: keep the bitmap authoritative for nullness, and decode
// into the same typed ColumnData before comparing or computing.
//
// # Integrity and publication
//
// Each segment ends with an IEEE CRC-32 covering magic, kind, row count,
// bitmap and values. A segment that cannot be opened, fails its CRC, has
// an invalid payload or disagrees with the manifest is reported as a
// CorruptSegmentError. Damaged segments are never skipped silently.
// Batches pruned by min/max statistics are not opened, so pruning never
// masks corruption of segments a query genuinely needed.
//
// Segments are fsynced and renamed during writing, but a batch is only
// visible after Close atomically replaces manifest.json. A process that
// stops before Close leaves segment files without a manifest; opening
// that directory fails, so unfinished data is never published as valid.
package coldstore
