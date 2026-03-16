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
		rm := request.GetResourceMetrics()

		// Map all 5 metric types.
		gaugeMetadata, gaugeRows := MapGaugeRows(rm)
		sumMetadata, sumRows := MapSumRows(rm)
		histMetadata, histRows := MapHistogramRows(rm)
		expHistMetadata, expHistRows := MapExponentialHistogramRows(rm)
		summaryMetadata, summaryRows := MapSummaryRows(rm)

		// Deduplicate metadata across all types.
		seen := make(map[uint64]struct{})
		var allMetadata []MetadataRow
		for _, batch := range [][]MetadataRow{gaugeMetadata, sumMetadata, histMetadata, expHistMetadata, summaryMetadata} {
			for _, md := range batch {
				if _, ok := seen[md.MetricFingerprint]; !ok {
					seen[md.MetricFingerprint] = struct{}{}
					allMetadata = append(allMetadata, md)
				}
			}
		}

		start := time.Now()

		// Insert metadata first.
		if len(allMetadata) > 0 {
			if err := m.store.InsertMetadata(ctx, allMetadata); err != nil {
				return nil, insertErr(ctx, "metric_metadata", err)
			}
		}

		// Insert data points for each type that has rows.
		if len(gaugeRows) > 0 {
			if err := m.store.InsertGauge(ctx, gaugeRows); err != nil {
				return nil, insertErr(ctx, "otel_metrics_gauge", err)
			}
		}
		if len(sumRows) > 0 {
			if err := m.store.InsertSum(ctx, sumRows); err != nil {
				return nil, insertErr(ctx, "otel_metrics_sum", err)
			}
		}
		if len(histRows) > 0 {
			if err := m.store.InsertHistogram(ctx, histRows); err != nil {
				return nil, insertErr(ctx, "otel_metrics_histogram", err)
			}
		}
		if len(expHistRows) > 0 {
			if err := m.store.InsertExponentialHistogram(ctx, expHistRows); err != nil {
				return nil, insertErr(ctx, "otel_metrics_exponential_histogram", err)
			}
		}
		if len(summaryRows) > 0 {
			if err := m.store.InsertSummary(ctx, summaryRows); err != nil {
				return nil, insertErr(ctx, "otel_metrics_summary", err)
			}
		}

		duration := time.Since(start)
		totalDatapoints := int64(len(gaugeRows) + len(sumRows) + len(histRows) + len(expHistRows) + len(summaryRows))

		slog.InfoContext(ctx, "Inserted metrics",
			slog.Int("gauge_datapoints", len(gaugeRows)),
			slog.Int("sum_datapoints", len(sumRows)),
			slog.Int("histogram_datapoints", len(histRows)),
			slog.Int("exp_histogram_datapoints", len(expHistRows)),
			slog.Int("summary_datapoints", len(summaryRows)),
			slog.Int("metadata_rows", len(allMetadata)),
			slog.Duration("duration", duration),
		)

		metadataRowsInsertedCounter.Add(ctx, int64(len(allMetadata)))
		datapointRowsInsertedCounter.Add(ctx, totalDatapoints)
	}

	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}
