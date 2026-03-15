package main

import (
	"context"
	"log/slog"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
)

type dash0MetricsServiceServer struct {
	addr  string
	store MetricsStore

	colmetricspb.UnimplementedMetricsServiceServer
}

func newServer(addr string, store MetricsStore) colmetricspb.MetricsServiceServer {
	return &dash0MetricsServiceServer{addr: addr, store: store}
}

func (m *dash0MetricsServiceServer) Export(ctx context.Context, request *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	slog.DebugContext(ctx, "Received ExportMetricsServiceRequest")
	metricsReceivedCounter.Add(ctx, 1)

	if m.store != nil {
		rm := request.GetResourceMetrics()

		gaugeMetadata, gaugeRows := MapGaugeRows(rm)
		if len(gaugeMetadata) > 0 {
			if err := m.store.InsertMetadata(ctx, gaugeMetadata); err != nil {
				return nil, err
			}
		}
		if len(gaugeRows) > 0 {
			if err := m.store.InsertGauge(ctx, gaugeRows); err != nil {
				return nil, err
			}
		}

		sumMetadata, sumRows := MapSumRows(rm)
		if len(sumMetadata) > 0 {
			if err := m.store.InsertMetadata(ctx, sumMetadata); err != nil {
				return nil, err
			}
		}
		if len(sumRows) > 0 {
			if err := m.store.InsertSum(ctx, sumRows); err != nil {
				return nil, err
			}
		}
	}

	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}
