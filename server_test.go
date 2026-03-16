package main

import (
	"context"
	"log"
	"net"
	"testing"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	otelmetrics "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestExport_NilStore_EmptyResourceMetrics_ReturnsEmptyResponse(t *testing.T) {
	client, closer := startTestServer(t)
	defer closer()

	resp, err := client.Export(context.Background(), &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*otelmetrics.ResourceMetrics{{
			ScopeMetrics: []*otelmetrics.ScopeMetrics{},
			SchemaUrl:    "dash0.com/otlp-metrics-processor-backend",
		}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetPartialSuccess().GetRejectedDataPoints() != 0 {
		t.Errorf("expected 0 rejected data points, got %d", resp.GetPartialSuccess().GetRejectedDataPoints())
	}
}

func startTestServer(t *testing.T) (colmetricspb.MetricsServiceClient, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)

	grpcServer := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(grpcServer, newServer(nil))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("error serving server: %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("connecting to test server: %v", err)
	}

	closer := func() {
		conn.Close()
		grpcServer.Stop()
		lis.Close()
	}

	return colmetricspb.NewMetricsServiceClient(conn), closer
}
