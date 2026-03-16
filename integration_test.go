//go:build integration

package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func setupClickHouse(t *testing.T) (*ClickHouseMetricsStore, func()) {
	t.Helper()
	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx, "clickhouse/clickhouse-server:26.2",
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithEnv(map[string]string{
			"CLICKHOUSE_USER":     "default",
			"CLICKHOUSE_PASSWORD": "test",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("9000/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("starting clickhouse container: %v", err)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("getting container host: %v", err)
	}
	mappedPort, err := ctr.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("getting mapped port: %v", err)
	}

	addr := fmt.Sprintf("%s:%s", host, mappedPort.Port())
	store, err := NewClickHouseMetricsStore(ctx, addr, "default", "default", "test")
	if err != nil {
		t.Fatalf("creating clickhouse metrics store: %v", err)
	}

	cleanup := func() {
		store.Close()
		if err := ctr.Terminate(ctx); err != nil {
			t.Logf("terminating clickhouse container: %v", err)
		}
	}

	return store, cleanup
}

// integrationResource builds OTLP ResourceMetrics with the given metric, using consistent test identifiers.
func integrationResource(metric *metricspb.Metric) []*metricspb.ResourceMetrics {
	return []*metricspb.ResourceMetrics{{
		Resource: &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"}}},
				{Key: "host.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-host"}}},
			},
		},
		SchemaUrl: "https://opentelemetry.io/schemas/1.4.0",
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: &commonpb.InstrumentationScope{
				Name:    "test-scope",
				Version: "1.0.0",
			},
			Metrics: []*metricspb.Metric{metric},
		}},
	}}
}

func TestCreateTables_AllSixDDLs_TablesExistInClickHouse(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	expectedTables := []string{
		"metric_metadata",
		"otel_metrics_gauge",
		"otel_metrics_sum",
		"otel_metrics_histogram",
		"otel_metrics_exponential_histogram",
		"otel_metrics_summary",
	}

	for _, table := range expectedTables {
		var count uint64
		err := store.conn.QueryRow(ctx,
			"SELECT count() FROM system.tables WHERE database = 'default' AND name = $1", table,
		).Scan(&count)
		if err != nil {
			t.Fatalf("querying system.tables for %s: %v", table, err)
		}
		if count != 1 {
			t.Errorf("expected table %s to exist, got count=%d", table, count)
		}
	}
}

func TestInsertGauge_OneDataPoint_MetadataAndDataJoinCorrectly(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	now := uint64(time.Now().UnixNano())
	rm := integrationResource(&metricspb.Metric{
		Name:        "cpu.utilization",
		Description: "CPU utilization percentage",
		Unit:        "%",
		Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
			DataPoints: []*metricspb.NumberDataPoint{{
				Attributes:        []*commonpb.KeyValue{{Key: "cpu", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "0"}}}},
				StartTimeUnixNano: now - uint64(time.Minute),
				TimeUnixNano:      now,
				Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: 42.5},
			}},
		}},
	})

	metadata, gaugeRows := MapGaugeRows(rm)
	if err := store.InsertMetadata(ctx, metadata); err != nil {
		t.Fatalf("inserting metadata: %v", err)
	}
	if err := store.InsertGauge(ctx, gaugeRows); err != nil {
		t.Fatalf("inserting gauge rows: %v", err)
	}

	// Verify via JOIN that the two-table model reconstructs the full context.
	var (
		serviceName string
		metricName  string
		value       float64
	)
	err := store.conn.QueryRow(ctx, `
		SELECT m.ServiceName, m.MetricName, g.Value
		FROM otel_metrics_gauge g
		JOIN metric_metadata m ON g.MetricFingerprint = m.MetricFingerprint
		WHERE m.MetricName = 'cpu.utilization'`,
	).Scan(&serviceName, &metricName, &value)
	if err != nil {
		t.Fatalf("querying gauge via join: %v", err)
	}

	if serviceName != "test-service" {
		t.Errorf("ServiceName: got %q, want %q", serviceName, "test-service")
	}
	if metricName != "cpu.utilization" {
		t.Errorf("MetricName: got %q, want %q", metricName, "cpu.utilization")
	}
	if value != 42.5 {
		t.Errorf("Value: got %f, want 42.5", value)
	}
}

func TestInsertSum_CumulativeMonotonic_MetadataAndDataJoinCorrectly(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	now := uint64(time.Now().UnixNano())
	rm := integrationResource(&metricspb.Metric{
		Name:        "http.requests.total",
		Description: "Total HTTP requests",
		Unit:        "{request}",
		Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			IsMonotonic:            true,
			DataPoints: []*metricspb.NumberDataPoint{{
				Attributes: []*commonpb.KeyValue{
					{Key: "method", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "GET"}}},
				},
				StartTimeUnixNano: now - uint64(time.Minute),
				TimeUnixNano:      now,
				Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: 1234},
			}},
		}},
	})

	metadata, sumRows := MapSumRows(rm)
	if err := store.InsertMetadata(ctx, metadata); err != nil {
		t.Fatalf("inserting metadata: %v", err)
	}
	if err := store.InsertSum(ctx, sumRows); err != nil {
		t.Fatalf("inserting sum rows: %v", err)
	}

	var (
		serviceName            string
		value                  float64
		aggregationTemporality int32
		isMonotonic            bool
	)
	err := store.conn.QueryRow(ctx, `
		SELECT m.ServiceName, s.Value, s.AggregationTemporality, s.IsMonotonic
		FROM otel_metrics_sum s
		JOIN metric_metadata m ON s.MetricFingerprint = m.MetricFingerprint
		WHERE m.MetricName = 'http.requests.total'`,
	).Scan(&serviceName, &value, &aggregationTemporality, &isMonotonic)
	if err != nil {
		t.Fatalf("querying sum via join: %v", err)
	}

	if serviceName != "test-service" {
		t.Errorf("ServiceName: got %q, want %q", serviceName, "test-service")
	}
	if value != 1234 {
		t.Errorf("Value: got %f, want 1234", value)
	}
	if aggregationTemporality != 2 {
		t.Errorf("AggregationTemporality: got %d, want 2", aggregationTemporality)
	}
	if !isMonotonic {
		t.Error("IsMonotonic: got false, want true")
	}
}

func TestInsertMetadata_DuplicateFingerprint_DeduplicatesAfterOptimize(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	rm := integrationResource(&metricspb.Metric{
		Name: "dedup.gauge",
		Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
			DataPoints: []*metricspb.NumberDataPoint{{
				TimeUnixNano: uint64(time.Now().UnixNano()),
				Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 1},
			}},
		}},
	})

	// Insert the same metadata twice.
	metadata, _ := MapGaugeRows(rm)
	if err := store.InsertMetadata(ctx, metadata); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if err := store.InsertMetadata(ctx, metadata); err != nil {
		t.Fatalf("second insert: %v", err)
	}

	// Force ReplacingMergeTree deduplication.
	if err := store.conn.Exec(ctx, "OPTIMIZE TABLE metric_metadata FINAL"); err != nil {
		t.Fatalf("optimizing table: %v", err)
	}

	var count uint64
	err := store.conn.QueryRow(ctx,
		"SELECT count() FROM metric_metadata WHERE MetricFingerprint = $1", metadata[0].MetricFingerprint,
	).Scan(&count)
	if err != nil {
		t.Fatalf("querying metadata count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row after dedup, got %d", count)
	}
}

func TestGRPCExport_OneGaugeDataPoint_LandsInClickHouseViaEndToEnd(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	// Start gRPC server wired to the ClickHouse store.
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(grpcServer, newServer("bufconn", store))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("error serving server: %v", err)
		}
	}()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("connecting to grpc server: %v", err)
	}
	defer conn.Close()

	client := colmetricspb.NewMetricsServiceClient(conn)

	now := uint64(time.Now().UnixNano())
	_, err = client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "e2e-service"}}},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Scope: &commonpb.InstrumentationScope{Name: "e2e-scope"},
				Metrics: []*metricspb.Metric{{
					Name: "e2e.gauge",
					Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
						DataPoints: []*metricspb.NumberDataPoint{{
							TimeUnixNano: now,
							Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 99.9},
						}},
					}},
				}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("exporting metrics via grpc: %v", err)
	}

	// Verify via JOIN that both tables were populated.
	var (
		svcName string
		value   float64
	)
	err = store.conn.QueryRow(ctx, `
		SELECT m.ServiceName, g.Value
		FROM otel_metrics_gauge g
		JOIN metric_metadata m ON g.MetricFingerprint = m.MetricFingerprint
		WHERE m.MetricName = 'e2e.gauge'`,
	).Scan(&svcName, &value)
	if err != nil {
		t.Fatalf("querying clickhouse via join: %v", err)
	}
	if svcName != "e2e-service" {
		t.Errorf("ServiceName: got %q, want %q", svcName, "e2e-service")
	}
	if value != 99.9 {
		t.Errorf("Value: got %f, want 99.9", value)
	}
}
