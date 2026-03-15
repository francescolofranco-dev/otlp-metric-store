package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// MetadataRow represents a unique metric series identity for the lookup table.
type MetadataRow struct {
	MetricFingerprint     uint64
	ServiceName           string
	MetricName            string
	MetricDescription     string
	MetricUnit            string
	ResourceAttributes    map[string]string
	ResourceSchemaUrl     string
	ScopeName             string
	ScopeVersion          string
	ScopeAttributes       map[string]string
	ScopeDroppedAttrCount uint32
	ScopeSchemaUrl        string
	Attributes            map[string]string
	LastSeen              time.Time
}

// GaugeRow represents a slim gauge data point referencing metadata by fingerprint.
type GaugeRow struct {
	MetricFingerprint uint64
	StartTimeUnix     time.Time
	TimeUnix          time.Time
	Value             float64
	Flags             uint32
}

// SumRow represents a slim sum data point referencing metadata by fingerprint.
type SumRow struct {
	MetricFingerprint      uint64
	StartTimeUnix          time.Time
	TimeUnix               time.Time
	Value                  float64
	Flags                  uint32
	AggregationTemporality int32
	IsMonotonic            bool
}

// HistogramRow represents a slim histogram data point referencing metadata by fingerprint.
type HistogramRow struct {
	MetricFingerprint      uint64
	StartTimeUnix          time.Time
	TimeUnix               time.Time
	Count                  uint64
	Sum                    float64
	BucketCounts           []uint64
	ExplicitBounds         []float64
	Min                    float64
	Max                    float64
	Flags                  uint32
	AggregationTemporality int32
}

// ExponentialHistogramRow represents a slim exponential histogram data point referencing metadata by fingerprint.
type ExponentialHistogramRow struct {
	MetricFingerprint      uint64
	StartTimeUnix          time.Time
	TimeUnix               time.Time
	Count                  uint64
	Sum                    float64
	Scale                  int32
	ZeroCount              uint64
	PositiveOffset         int32
	PositiveBucketCounts   []uint64
	NegativeOffset         int32
	NegativeBucketCounts   []uint64
	Min                    float64
	Max                    float64
	Flags                  uint32
	AggregationTemporality int32
}

// SummaryRow represents a slim summary data point referencing metadata by fingerprint.
type SummaryRow struct {
	MetricFingerprint       uint64
	StartTimeUnix           time.Time
	TimeUnix                time.Time
	Count                   uint64
	Sum                     float64
	ValueAtQuantileQuantile []float64
	ValueAtQuantileValue    []float64
	Flags                   uint32
}

// MetricsStore defines the interface for storing metrics in ClickHouse.
type MetricsStore interface {
	CreateTables(ctx context.Context) error
	InsertMetadata(ctx context.Context, rows []MetadataRow) error
	InsertGauge(ctx context.Context, rows []GaugeRow) error
	InsertSum(ctx context.Context, rows []SumRow) error
	InsertHistogram(ctx context.Context, rows []HistogramRow) error
	InsertExponentialHistogram(ctx context.Context, rows []ExponentialHistogramRow) error
	InsertSummary(ctx context.Context, rows []SummaryRow) error
	Close() error
}

// ClickHouseMetricsStore implements MetricsStore using a ClickHouse connection.
type ClickHouseMetricsStore struct {
	conn driver.Conn
}

// NewClickHouseMetricsStore creates a new ClickHouseMetricsStore connected to the given address.
func NewClickHouseMetricsStore(ctx context.Context, addr string, database string, username string, password string) (*ClickHouseMetricsStore, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("pinging clickhouse: %w", err)
	}
	return &ClickHouseMetricsStore{conn: conn}, nil
}

// CreateTables executes DDL for the metadata table and all 5 data-point tables.
func (s *ClickHouseMetricsStore) CreateTables(ctx context.Context) error {
	ddls := []string{
		createMetricMetadataTableSQL,
		createGaugeTableSQL,
		createSumTableSQL,
		createHistogramTableSQL,
		createExponentialHistogramTableSQL,
		createSummaryTableSQL,
	}
	for _, ddl := range ddls {
		if err := s.conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("creating table: %w", err)
		}
	}
	return nil
}

// InsertMetadata batch-inserts metadata rows into metric_metadata.
func (s *ClickHouseMetricsStore) InsertMetadata(ctx context.Context, rows []MetadataRow) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO metric_metadata")
	if err != nil {
		return fmt.Errorf("preparing metadata batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.MetricFingerprint,
			r.ServiceName,
			r.MetricName,
			r.MetricDescription,
			r.MetricUnit,
			r.ResourceAttributes,
			r.ResourceSchemaUrl,
			r.ScopeName,
			r.ScopeVersion,
			r.ScopeAttributes,
			r.ScopeDroppedAttrCount,
			r.ScopeSchemaUrl,
			r.Attributes,
			r.LastSeen,
		); err != nil {
			return fmt.Errorf("appending metadata row: %w", err)
		}
	}
	return batch.Send()
}

// InsertGauge batch-inserts gauge rows into otel_metrics_gauge.
func (s *ClickHouseMetricsStore) InsertGauge(ctx context.Context, rows []GaugeRow) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_gauge")
	if err != nil {
		return fmt.Errorf("preparing gauge batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.MetricFingerprint,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Value,
			r.Flags,
		); err != nil {
			return fmt.Errorf("appending gauge row: %w", err)
		}
	}
	return batch.Send()
}

// InsertSum batch-inserts sum rows into otel_metrics_sum.
func (s *ClickHouseMetricsStore) InsertSum(ctx context.Context, rows []SumRow) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_sum")
	if err != nil {
		return fmt.Errorf("preparing sum batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.MetricFingerprint,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Value,
			r.Flags,
			r.AggregationTemporality,
			r.IsMonotonic,
		); err != nil {
			return fmt.Errorf("appending sum row: %w", err)
		}
	}
	return batch.Send()
}

// InsertHistogram batch-inserts histogram rows into otel_metrics_histogram.
func (s *ClickHouseMetricsStore) InsertHistogram(ctx context.Context, rows []HistogramRow) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_histogram")
	if err != nil {
		return fmt.Errorf("preparing histogram batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.MetricFingerprint,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Count,
			r.Sum,
			r.BucketCounts,
			r.ExplicitBounds,
			r.Min,
			r.Max,
			r.Flags,
			r.AggregationTemporality,
		); err != nil {
			return fmt.Errorf("appending histogram row: %w", err)
		}
	}
	return batch.Send()
}

// InsertExponentialHistogram batch-inserts exponential histogram rows into otel_metrics_exponential_histogram.
func (s *ClickHouseMetricsStore) InsertExponentialHistogram(ctx context.Context, rows []ExponentialHistogramRow) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_exponential_histogram")
	if err != nil {
		return fmt.Errorf("preparing exponential histogram batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.MetricFingerprint,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Count,
			r.Sum,
			r.Scale,
			r.ZeroCount,
			r.PositiveOffset,
			r.PositiveBucketCounts,
			r.NegativeOffset,
			r.NegativeBucketCounts,
			r.Min,
			r.Max,
			r.Flags,
			r.AggregationTemporality,
		); err != nil {
			return fmt.Errorf("appending exponential histogram row: %w", err)
		}
	}
	return batch.Send()
}

// InsertSummary batch-inserts summary rows into otel_metrics_summary.
func (s *ClickHouseMetricsStore) InsertSummary(ctx context.Context, rows []SummaryRow) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_summary")
	if err != nil {
		return fmt.Errorf("preparing summary batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.MetricFingerprint,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Count,
			r.Sum,
			r.ValueAtQuantileQuantile,
			r.ValueAtQuantileValue,
			r.Flags,
		); err != nil {
			return fmt.Errorf("appending summary row: %w", err)
		}
	}
	return batch.Send()
}

// Close closes the underlying ClickHouse connection.
func (s *ClickHouseMetricsStore) Close() error {
	return s.conn.Close()
}
