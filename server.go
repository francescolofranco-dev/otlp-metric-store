package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	listenAddr            = flag.String("listenAddr", "localhost:4317", "The listen address")
	maxReceiveMessageSize = flag.Int("maxReceiveMessageSize", 16777216, "The max message size in bytes the server can receive")
	clickhouseAddr        = flag.String("clickhouse-addr", "localhost:9000", "ClickHouse native protocol address")
	clickhouseDatabase    = flag.String("clickhouse-database", "default", "ClickHouse database name")
	clickhouseUsername    = flag.String("clickhouse-username", "default", "ClickHouse username")
	clickhousePassword    = flag.String("clickhouse-password", "test", "ClickHouse password")
)

const name = "dash0.com/otlp-metrics-processor-backend"

var (
	meter                        = otel.Meter(name)
	logger                       = otelslog.NewLogger(name)
	metricsReceivedCounter       metric.Int64Counter
	metadataRowsInsertedCounter  metric.Int64Counter
	datapointRowsInsertedCounter metric.Int64Counter
)

func init() {
	var err error
	metricsReceivedCounter, err = meter.Int64Counter("com.dash0.homeexercise.metrics.received",
		metric.WithDescription("The number of metrics received by otlp-metrics-processor-backend"),
		metric.WithUnit("{metric}"))
	if err != nil {
		panic(err)
	}
	metadataRowsInsertedCounter, err = meter.Int64Counter("com.dash0.homeexercise.metadata.rows.inserted",
		metric.WithDescription("The number of metadata rows inserted into ClickHouse"),
		metric.WithUnit("{row}"))
	if err != nil {
		panic(err)
	}
	datapointRowsInsertedCounter, err = meter.Int64Counter("com.dash0.homeexercise.datapoint.rows.inserted",
		metric.WithDescription("The number of data point rows inserted into ClickHouse"),
		metric.WithUnit("{row}"))
	if err != nil {
		panic(err)
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	slog.SetDefault(logger)
	logger.Info("Starting application")

	// Set up OpenTelemetry.
	otelShutdown, err := setupOTelSDK(context.Background())
	if err != nil {
		return
	}

	// Handle shutdown properly so nothing leaks.
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	flag.Parse()

	// Listen for SIGINT/SIGTERM for graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Connect to ClickHouse and create tables.
	store, err := NewClickHouseMetricsStore(ctx, *clickhouseAddr, *clickhouseDatabase, *clickhouseUsername, *clickhousePassword)
	if err != nil {
		return fmt.Errorf("connecting to clickhouse: %w", err)
	}
	defer store.Close()

	if err := store.CreateTables(ctx); err != nil {
		return fmt.Errorf("creating tables: %w", err)
	}

	slog.Info("Connected to ClickHouse", slog.String("addr", *clickhouseAddr))

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", *listenAddr, err)
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(*maxReceiveMessageSize),
		grpc.Creds(insecure.NewCredentials()),
	)
	colmetricspb.RegisterMetricsServiceServer(grpcServer, newServer(store))

	// Graceful shutdown in a separate goroutine with a hard deadline.
	go func() {
		<-ctx.Done()
		slog.Info("Shutting down gRPC server")
		done := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			slog.Warn("Graceful shutdown timed out, forcing stop")
			grpcServer.Stop()
		}
	}()

	slog.Info("Starting gRPC server", slog.String("listenAddr", *listenAddr))

	return grpcServer.Serve(listener)
}
