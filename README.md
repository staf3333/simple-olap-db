# Simple OLAP Database

A column-oriented storage engine built from scratch in Go, inspired by Chapter 3 of *Designing Data-Intensive Applications*.

## Why This Project?

Row-based storage is great for transactional workloads, but terrible for analytical queries that touch a few columns across millions of rows. This project builds the intuition for *why* by implementing both approaches and seeing the difference firsthand.

## Goals

- [ ] Implement a row-based storage engine as a baseline
- [ ] Implement a column-based storage engine
- [ ] Benchmark analytical queries (aggregations, filters) across both
- [ ] Understand I/O patterns — why column storage wins for OLAP

### Stretch Goals

- [ ] Add basic compression (run-length encoding, dictionary encoding)
- [ ] Support a simple SQL-like query interface

## Key Concepts

| Concept | DDIA Reference | Status |
|---------|---------------|--------|
| Column-oriented storage | Ch. 3 — Column-Oriented Storage | 🔲 |
| Row vs column tradeoffs | Ch. 3 — Comparing Row and Column Storage | 🔲 |
| Compression (RLE, dictionary) | Ch. 3 — Column Compression | 🔲 (stretch) |
| Sort order in column stores | Ch. 3 — Sort Order in Column Storage | 🔲 (stretch) |
| Materialized views / data cubes | Ch. 3 — Aggregation | 🔲 (stretch) |

## What I Learned

> Update this section as you go — capture insights, surprises, and "aha" moments.

<!--
Example entries:

### Why column storage crushes aggregations
Scanning a single column file means sequential I/O on just the data you need.
Row storage forces you to read entire rows even if you only want one field.
The difference was Xms vs Yms on 1M rows — that's not a micro-optimization, it's architectural.

### Dictionary encoding was simpler than I expected
Replaced repeated strings with integer IDs. Cut storage by ~70% on the `category` column.
The lookup table fits in memory, so decoding is basically free.
-->

## Getting Started

```bash
# TODO: fill in once project is scaffolded
go run .
```

## Tech

- **Language:** Go
- **Inspired by:** DDIA Chapter 3 — Storage and Retrieval
