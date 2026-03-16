# OTLP Metric Store

Go backend that receives OTLP metrics via gRPC and stores them in ClickHouse. Supports all five metric types (Gauge, Sum, Histogram, Exponential Histogram, Summary) using a normalized two-table schema that separates metric metadata from data points.

## Design Decisions

### Two-table schema: metadata + slim data points

The original schema is denormalized — every data-point row repeats all metadata (resource attributes, scope info, metric name, etc.). This solution splits it into:

- **`metric_metadata`** — one row per unique metric series, keyed by a `UInt64` fingerprint
- **Slim data-point tables** — just fingerprint, timestamps, value(s), and flags

The fingerprint is computed with `xxhash` over all identity fields (service name, metric name/unit/description, resource attributes, scope info, data-point attributes). Map keys are sorted before hashing for determinism, and fields are null-byte delimited to avoid ambiguous concatenation.

### Why ReplacingMergeTree for metadata

The metadata table uses `ReplacingMergeTree(LastSeen)`. This means the app can blindly insert metadata on every request without checking for existence — duplicates get collapsed during ClickHouse's background merges. Within each batch, an in-memory `map[uint64]struct{}` deduplicates before we even hit ClickHouse.

A `TTL toDateTime(LastSeen) + INTERVAL 30 DAY DELETE` automatically cleans up metadata for series that haven't been seen in 30 days. This handles cardinality growth from pod restarts, label churn, and ephemeral workloads without manual cleanup.

### No full table scans

All data-point tables use `PARTITION BY toDate(TimeUnix)` and `ORDER BY (MetricFingerprint, toUnixTimestamp64Nano(TimeUnix))`. Since data is always queried for a specific time-frame (as stated in the requirements), ClickHouse prunes irrelevant date-partitions entirely. Within a partition, the primary key ordering means queries for a specific series + time window only touch the relevant granules.

I verified this locally by running `EXPLAIN indexes = 1` queries against a ClickHouse instance with data spread across multiple dates and fingerprints. The output confirmed partition pruning and granule-level key pruning were both active. I wrote these as one-off integration tests but chose not to commit them — they're brittle because the EXPLAIN output format and the pruning statistics depend on ClickHouse version, data volume, and part merge state. A more robust approach would be to include these checks in a dedicated integration/smoke test pipeline that runs against a representative dataset on a stable ClickHouse version, rather than asserting on EXPLAIN output in unit-style tests where the data is minimal and the ClickHouse instance is ephemeral.

## Assumptions and Trade-offs

**Separate mappers per metric type.** The `Export` method calls all five mappers on every request, even though a batch typically contains only one or two types. For non-matching types, the mapper iterates the outer OTLP hierarchy but the inner data-point loop returns immediately (nil type assertion, no allocations). The overhead is negligible compared to the ClickHouse round-trip, and the alternative (a single-pass type switch) would couple all five types into one function for microsecond-level savings.

**Sequential inserts.** The six inserts per request (metadata + 5 data-point types) run sequentially. They target independent tables and could be parallelized with `errgroup`. I kept them sequential for simplicity and correctness within the time budget. Moreover, the gRPC server already handles concurrent requests in separate goroutines, so the system is parallel at the request level.

**No cross-request buffering or backpressure.** Each `Export` call flushes to ClickHouse immediately. Under high message volume, this means many small inserts rather than fewer large ones — ClickHouse performs best with larger, less frequent batches. The standard solution is a bounded in-memory queue: the gRPC handler appends rows to the queue and returns immediately, while a background worker drains the queue and flushes to ClickHouse on a size or time threshold (e.g., every 5,000 rows or every second, whichever comes first). The bounded queue also provides natural backpressure — when ClickHouse can't keep up and the queue fills, the handler either blocks or returns a gRPC `RESOURCE_EXHAUSTED` status, signalling the sender to back off. This is the same pattern the OpenTelemetry Collector's batch processor uses. I kept the current synchronous design for simplicity and debuggability within the scope of this exercise.

**At-least-once delivery.** As there's no cross-table transaction in ClickHouse, if for example the sum insert fails after gauge succeeds, the handler returns a gRPC error and the sender (typically an OTel Collector) retries the full request. This may duplicate data points but won't lose them. This is an acceptable tradeoff as orphaned metadata from partial failures is harmless, we support idempotency via `ReplacingMergeTree`, and it is eventually cleaned up by TTL.

**No integration tests for histogram/exp-histogram/summary.** Their insert methods are structurally identical to gauge/sum (same batch pattern, same schema shape). Unit tests verify the mappers extract the correct type-specific fields. Adding integration tests for all five would be repetitive without increasing coverage of interesting behaviour.

**Observability/Debug signals.** Self-instrumentation is limited to three counters: **requests received**, **metadata rows inserted**, **data-point rows inserted**. In a production deployment it would be ideal to have latency histograms and error rate breakdowns by metric type.

**No containerization of the Go app.** A Dockerfile and container-based deployment would be needed for production, but it's straightforward for this application (statically compiled Go binary, no runtime dependencies) and doesn't add meaningful signal to the exercise, so I left it out.

**No schema migrations.** Tables are created on startup via `CREATE TABLE IF NOT EXISTS`. A production system should use versioned migration scripts tracked in source control and validated in CI.

**No secrets management.** Credentials are passed via command-line flags. A production deployment would integrate with a secrets manager (Vault, AWS Secrets Manager, etc.).

## How to Run

Prerequisites: Go 1.26+, Docker.

```shell
make docker-up          # starts ClickHouse on ports 8123/9000
go run .                # gRPC server on localhost:4317
```

Available flags:

| Flag | Default | Description |
|---|---|---|
| `--listenAddr` | `localhost:4317` | gRPC listen address |
| `--clickhouse-addr` | `localhost:9000` | ClickHouse native protocol address |
| `--clickhouse-database` | `default` | Database name |
| `--clickhouse-username` | `default` | Username |
| `--clickhouse-password` | `test` | Password |

The app creates all tables on startup and shuts down gracefully on SIGINT/SIGTERM.

## How to Test

```shell
make test               # unit tests (no external deps)
make test-integration   # integration tests (needs Docker for testcontainers)
make test-all           # both
make bench              # Go benchmarks
```

### Benchmark results (Apple M2)

```
BenchmarkComputeFingerprint-8       2,383,478      498 ns/op      128 B/op     2 allocs/op
BenchmarkMapGaugeRows_10-8            165,475    6,823 ns/op   14,952 B/op    47 allocs/op
BenchmarkMapGaugeRows_100-8            17,010   70,349 ns/op  132,808 B/op   329 allocs/op
BenchmarkMapGaugeRows_1000-8            1,638  734,466 ns/op 1,114,876 B/op 3,046 allocs/op
BenchmarkMapSumRows_100-8              16,771   71,994 ns/op  135,608 B/op   329 allocs/op
```

Mapper throughput scales linearly — 10x data points produces ~10x time with no superlinear blowup. At 1,000 data points, the full mapping (fingerprint + dedup + row construction) completes in 0.7ms, well below a typical ClickHouse insert round-trip. These benchmarks were used to verify that no non-linear behaviour was introduced by the fingerprinting and deduplication logic.

## Not Implemented

- **Query/read API** — the application is ingest-only. A read path would query the metadata table for matching fingerprints and join against the data-point tables for a given time range.
- **Buffered ingest pipeline** — as described in the trade-offs above, a bounded queue between the gRPC handler and ClickHouse would improve throughput (larger batches, fewer round-trips) and add backpressure (reject or block when the queue is full). This is the single highest-impact change for production readiness.
- **Retry with backoff** — currently the receiver fails fast, which is correct since OTLP senders retry on gRPC errors. With a queue-based pipeline, retries would move to the flush worker — a bounded retry (1–2 attempts with exponential backoff) on transient ClickHouse errors before dropping the batch and incrementing an error counter.
- **Load testing** — benchmarks validate the mapper layer in isolation. End-to-end throughput validation (e.g., with k6 generating OTLP traffic at target rates) would be needed to size the queue, tune flush intervals, and validate backpressure behaviour.
