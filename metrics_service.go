package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
)

// insertErr logs the error and returns it wrapped with context for the gRPC caller.
func insertErr(ctx context.Context, table string, err error) error {
	slog.ErrorContext(ctx, "Failed to insert rows", slog.String("table", table), slog.String("error", err.Error()))
	return fmt.Errorf("inserting into %s: %w", table, err)
}

type dash0MetricsServiceServer struct {
	store MetricsStore

	colmetricspb.UnimplementedMetricsServiceServer
}

func newServer(store MetricsStore) colmetricspb.MetricsServiceServer {
	return &dash0MetricsServiceServer{store: store}
}

func (m *dash0MetricsServiceServer) Export(ctx context.Context, request *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	slog.DebugContext(ctx, "Received ExportMetricsServiceRequest")
	metricsReceivedCounter.Add(ctx, 1)

	if m.store != nil {
		// Single-pass mapping: walks the OTLP hierarchy once, dispatching by metric type.
		mapped := MapAllMetricRows(request.GetResourceMetrics())

		start := time.Now()

		// Insert metadata first.
		if len(mapped.Metadata) > 0 {
			if err := m.store.InsertMetadata(ctx, mapped.Metadata); err != nil {
				return nil, insertErr(ctx, "metric_metadata", err)
			}
		}

		// Insert data points for each type that has rows.
		if len(mapped.GaugeRows) > 0 {
			if err := m.store.InsertGauge(ctx, mapped.GaugeRows); err != nil {
				return nil, insertErr(ctx, "otel_metrics_gauge", err)
			}
		}
		if len(mapped.SumRows) > 0 {
			if err := m.store.InsertSum(ctx, mapped.SumRows); err != nil {
				return nil, insertErr(ctx, "otel_metrics_sum", err)
			}
		}
		if len(mapped.HistogramRows) > 0 {
			if err := m.store.InsertHistogram(ctx, mapped.HistogramRows); err != nil {
				return nil, insertErr(ctx, "otel_metrics_histogram", err)
			}
		}
		if len(mapped.ExpHistogramRows) > 0 {
			if err := m.store.InsertExponentialHistogram(ctx, mapped.ExpHistogramRows); err != nil {
				return nil, insertErr(ctx, "otel_metrics_exponential_histogram", err)
			}
		}
		if len(mapped.SummaryRows) > 0 {
			if err := m.store.InsertSummary(ctx, mapped.SummaryRows); err != nil {
				return nil, insertErr(ctx, "otel_metrics_summary", err)
			}
		}

		duration := time.Since(start)
		totalDatapoints := int64(len(mapped.GaugeRows) + len(mapped.SumRows) + len(mapped.HistogramRows) + len(mapped.ExpHistogramRows) + len(mapped.SummaryRows))

		slog.InfoContext(ctx, "Inserted metrics",
			slog.Int("gauge_datapoints", len(mapped.GaugeRows)),
			slog.Int("sum_datapoints", len(mapped.SumRows)),
			slog.Int("histogram_datapoints", len(mapped.HistogramRows)),
			slog.Int("exp_histogram_datapoints", len(mapped.ExpHistogramRows)),
			slog.Int("summary_datapoints", len(mapped.SummaryRows)),
			slog.Int("metadata_rows", len(mapped.Metadata)),
			slog.Duration("duration", duration),
		)

		metadataRowsInsertedCounter.Add(ctx, int64(len(mapped.Metadata)))
		datapointRowsInsertedCounter.Add(ctx, totalDatapoints)
	}

	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}
