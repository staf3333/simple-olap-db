# Simple OLAP Database

A column-oriented storage engine built from scratch in Go, inspired by Chapter 3 of *Designing Data-Intensive Applications*.

## Why This Project?

Row-based storage is great for transactional workloads, but terrible for analytical queries that touch a few columns across millions of rows. This project builds the intuition for *why* by implementing both approaches and seeing the difference firsthand.

## Goals

- [x] Implement a row-based storage engine as a baseline
- [x] Implement a column-based storage engine
- [x] Implement query logic (SUM) on both stores
- [ ] Benchmark analytical queries (aggregations, filters) across both
- [ ] Understand I/O patterns — why column storage wins for OLAP

### Stretch Goals

- [ ] Add basic compression (run-length encoding, dictionary encoding)
- [ ] Support a simple SQL-like query interface

## Key Concepts

| Concept | DDIA Reference | Status |
|---------|---------------|--------|
| Column-oriented storage | Ch. 3 — Column-Oriented Storage | ✅ |
| Row vs column tradeoffs | Ch. 3 — Comparing Row and Column Storage | ✅ |
| Compression (RLE, dictionary) | Ch. 3 — Column Compression | 🔲 (stretch) |
| Sort order in column stores | Ch. 3 — Sort Order in Column Storage | 🔲 (stretch) |
| Materialized views / data cubes | Ch. 3 — Aggregation | 🔲 (stretch) |

## What I Learned

### Column stores use position as row identity
There are no explicit references between column files. Entry at index 42 in every column file belongs to the same logical row. Sorting rearranges ALL columns by the same permutation — the positional contract is maintained.

### JSON loses type information for integers
When unmarshaling JSON into `map[string]any`, Go decodes all numbers as `float64`. You need the schema to convert back to `int64` where needed. This was a fun gotcha to debug — the values *looked* identical in test output but `reflect.DeepEqual` caught the type mismatch.

### Column store reads only what you need
For `SUM(price)` on a 6-column table, the column store reads ~1/6th the data. Scale to 100+ columns and you're reading 1% of the data. The win isn't a clever algorithm — it's just data layout matching access patterns.

## Getting Started

```bash
# Run all tests
go test ./... -v

# Run benchmarks (once implemented)
go test ./storage -bench=. -benchmem
```

## Project Structure

```
simple-olap-db/
├── cmd/main.go              # Entry point
├── datagen/generate.go      # Random sales data generator
├── storage/
│   ├── types.go             # Column, Row, Schema, interfaces, helpers
│   ├── row_store.go         # JSON lines row store (Write, ReadAll, SUM)
│   ├── column_store.go      # Text-based column store (Write, ReadColumns, SUM)
│   └── storage_test.go      # Round-trip, selective read, and query tests
```

## Tech

- **Language:** Go
- **Inspired by:** DDIA Chapter 3 — Storage and Retrieval
