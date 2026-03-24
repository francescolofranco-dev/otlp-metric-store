# CLAUDE.md — Project Reference for OTLP Metric Store

## Project Overview

Go backend that receives OpenTelemetry (OTLP) metric data via gRPC and stores it in ClickHouse using a normalized two-table schema. Built for the Dash0 Staff/Senior Product Engineer take-home exercise.

Supports all five OTLP metric types: Gauge, Sum, Histogram, Exponential Histogram, Summary.

## Architecture

### Two-table schema

Instead of denormalized rows where every data point duplicates all metadata, the schema is split into:

- **`metric_metadata`** (ReplacingMergeTree) — one row per unique metric series, keyed by a deterministic `UInt64` xxhash fingerprint. Bloom filter indexes on Map columns for attribute filtering. 30-day TTL on `LastSeen` for automatic cardinality cleanup.
- **Slim data-point tables** (MergeTree) — one per metric type, containing only fingerprint + timestamps + type-specific values + flags. Partitioned by `toDate(TimeUnix)`, ordered by `(MetricFingerprint, toUnixTimestamp64Nano(TimeUnix))`.

### Fingerprint computation

`computeFingerprint()` in [fingerprint.go](fingerprint.go) hashes nine identity fields (service name, metric name/unit/description, resource/scope/data-point attributes, scope name/version) using `xxhash`. Map keys are sorted before hashing for determinism. Fields are null-byte delimited to prevent ambiguous concatenation.

### Data flow (Export RPC)

1. gRPC request arrives at `Export()` in [metrics_service.go](metrics_service.go)
2. Five type-specific mappers ([metrics_mapper.go](metrics_mapper.go)) extract metadata + slim data-point rows, deduplicating metadata in-memory by fingerprint
3. Metadata from all types is combined and deduplicated across the full batch
4. Metadata inserted first (idempotent via ReplacingMergeTree)
5. Data points inserted per type (only for types that have rows)
6. Structured logging: per-type counts, unique fingerprints, insert duration

### Generic mapper pattern

`mapMetricRows[DP, Row]()` is a generic function that walks the OTLP resource/scope/metric hierarchy. Each metric type provides a `dataPointMapper` implementation that extracts data points and builds rows. This avoids duplicating the OTLP traversal logic five times.

## File Layout

| File | Purpose |
|---|---|
| [server.go](server.go) | `main()`, CLI flags, gRPC server setup, graceful shutdown, OTel counter init |
| [otel.go](otel.go) | OTel SDK initialization (traces, metrics, logs) |
| [metrics_service.go](metrics_service.go) | `Export()` handler — orchestrates mapping and inserts |
| [metrics_mapper.go](metrics_mapper.go) | Generic mapper framework + 5 concrete mappers + helpers |
| [fingerprint.go](fingerprint.go) | `computeFingerprint()` and `writeMapSorted()` |
| [clickhouse_client.go](clickhouse_client.go) | `MetricsStore` interface, `ClickHouseMetricsStore` impl, row structs |
| [clickhouse_schema.go](clickhouse_schema.go) | DDL constants for all 6 tables |

## Code Style

- Standard Go conventions: camelCase unexported, PascalCase exported
- Standard Go error handling: `if err != nil { return fmt.Errorf("context: %w", err) }`
- Import grouping: stdlib, external, internal
- Godoc on exported symbols
- Effective Go and Go Code Review Comments guidelines

## Testing

| Command | What it runs |
|---|---|
| `make test` | Unit tests — fingerprint determinism, all 5 mappers, gRPC round-trip with nil store |
| `make test-integration` | Integration tests with testcontainers ClickHouse — table creation, gauge/sum inserts with JOIN verification, metadata dedup, end-to-end gRPC→ClickHouse |
| `make bench` | Go benchmarks — fingerprint throughput, mapper throughput at 10/100/1000 data points |

Integration tests use build tag `//go:build integration` and require Docker.

## Development

```shell
make docker-up    # ClickHouse on ports 8123/9000
go run .          # gRPC server on localhost:4317
make docker-down  # tear down
```

CLI flags: `--listenAddr`, `--clickhouse-addr`, `--clickhouse-database`, `--clickhouse-username`, `--clickhouse-password`

## Key Design Decisions

See [INTERVIEW.md](INTERVIEW.md) for detailed rationale and trade-offs. Summary:

- **xxhash fingerprint** — fast, deterministic, non-cryptographic (collision resistance not needed for a grouping key)
- **ReplacingMergeTree(LastSeen)** — idempotent metadata inserts, no upsert queries needed
- **30-day TTL** — handles cardinality churn from pod restarts / label changes
- **PARTITION BY date + ORDER BY fingerprint+time** — no full table scans for time-range queries
- **Bloom filter indexes on Map columns** — data-skipping for attribute-based filters, only on metadata table
- **In-memory dedup before insert** — avoids sending duplicate metadata rows to ClickHouse
- **Sequential inserts** — simplicity over marginal latency gain; server is concurrent at the request level
- **No buffered pipeline** — documented as the highest-impact production improvement (bounded queue + flush worker)
