package main

import (
	"fmt"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

func syntheticResource() *resourcepb.Resource {
	return &resourcepb.Resource{
		Attributes: []*commonpb.KeyValue{
			strAttr("service.name", "bench-service"),
			strAttr("host.name", "bench-host-01"),
			strAttr("k8s.pod.name", "bench-pod-abc123"),
			strAttr("k8s.namespace", "production"),
			strAttr("deployment.environment", "prod"),
		},
	}
}

func syntheticScope() *commonpb.InstrumentationScope {
	return &commonpb.InstrumentationScope{
		Name:    "bench-scope",
		Version: "1.0.0",
		Attributes: []*commonpb.KeyValue{
			strAttr("library", "otel-go"),
			strAttr("telemetry.sdk.language", "go"),
		},
	}
}

func buildSyntheticGaugeResourceMetrics(n int) []*metricspb.ResourceMetrics {
	dps := make([]*metricspb.NumberDataPoint, n)
	for i := range dps {
		dps[i] = &metricspb.NumberDataPoint{
			Attributes:   []*commonpb.KeyValue{strAttr("cpu", fmt.Sprintf("%d", i))},
			TimeUnixNano: uint64(1_000_000_000 + i),
			Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: float64(i) * 1.5},
		}
	}
	return []*metricspb.ResourceMetrics{{
		Resource:  syntheticResource(),
		SchemaUrl: "https://opentelemetry.io/schemas/1.4.0",
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: syntheticScope(),
			Metrics: []*metricspb.Metric{{
				Name:        "system.cpu.utilization",
				Description: "CPU utilization per core",
				Unit:        "1",
				Data:        &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: dps}},
			}},
		}},
	}}
}

func buildSyntheticSumResourceMetrics(n int) []*metricspb.ResourceMetrics {
	dps := make([]*metricspb.NumberDataPoint, n)
	for i := range dps {
		dps[i] = &metricspb.NumberDataPoint{
			Attributes:   []*commonpb.KeyValue{strAttr("endpoint", fmt.Sprintf("/api/%d", i))},
			TimeUnixNano: uint64(1_000_000_000 + i),
			Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: float64(i)},
		}
	}
	return []*metricspb.ResourceMetrics{{
		Resource:  syntheticResource(),
		SchemaUrl: "https://opentelemetry.io/schemas/1.4.0",
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: syntheticScope(),
			Metrics: []*metricspb.Metric{{
				Name:        "http.server.request.count",
				Description: "Total HTTP requests",
				Unit:        "{request}",
				Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
					AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
					IsMonotonic:            true,
					DataPoints:             dps,
				}},
			}},
		}},
	}}
}

func BenchmarkComputeFingerprint(b *testing.B) {
	resAttrs := map[string]string{
		"service.name":             "bench-service",
		"host.name":               "bench-host-01",
		"k8s.pod.name":            "bench-pod-abc123",
		"k8s.namespace":           "production",
		"deployment.environment":  "prod",
	}
	scopeAttrs := map[string]string{
		"library":                  "otel-go",
		"telemetry.sdk.language":  "go",
	}
	dpAttrs := map[string]string{
		"cpu":    "0",
		"core":   "physical",
		"socket": "0",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		computeFingerprint(
			"bench-service", "system.cpu.utilization", "1", "CPU utilization per core",
			resAttrs, scopeAttrs, dpAttrs,
			"bench-scope", "1.0.0",
		)
	}
}

func BenchmarkMapGaugeRows_10(b *testing.B) {
	rm := buildSyntheticGaugeResourceMetrics(10)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MapGaugeRows(rm)
	}
}

func BenchmarkMapGaugeRows_100(b *testing.B) {
	rm := buildSyntheticGaugeResourceMetrics(100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MapGaugeRows(rm)
	}
}

func BenchmarkMapGaugeRows_1000(b *testing.B) {
	rm := buildSyntheticGaugeResourceMetrics(1000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MapGaugeRows(rm)
	}
}

func BenchmarkMapSumRows_100(b *testing.B) {
	rm := buildSyntheticSumResourceMetrics(100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MapSumRows(rm)
	}
}
