# Scalability & Performance Benchmarks (Phase 5 -- not yet implemented)

This document will hold the measured benchmarks from spec section 35
(1M / 10M / 50M+ record tests, ingestion/Kafka/Spark/query throughput,
compaction time, storage size) once the full pipeline exists to measure.

## Phase 1 notes

The bulk loader uses batched `COPY` (default batch size 25,000 rows, see
`SEED_BATCH_SIZE` in `.env.example`) rather than row-by-row `INSERT`,
specifically so the suggested minimums (1M+ orders, 2M+ order_items) are
practical to load. Actual load-time measurements will be added here once
Phase 5's benchmarking harness (`scripts/benchmark.py`) exists.
