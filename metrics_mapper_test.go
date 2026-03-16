package main

import (
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// --- Test helpers ---

func testResource() *resourcepb.Resource {
	return &resourcepb.Resource{
		Attributes: []*commonpb.KeyValue{
			strAttr("service.name", "test-service"),
			strAttr("host.name", "test-host"),
		},
	}
}

func testScope() *commonpb.InstrumentationScope {
	return &commonpb.InstrumentationScope{
		Name:    "test-scope",
		Version: "1.0.0",
	}
}

func strAttr(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
	}
}

func float64Ptr(v float64) *float64 { return &v }

// buildResourceMetrics wraps metrics in the standard test resource/scope hierarchy.
func buildResourceMetrics(metrics ...*metricspb.Metric) []*metricspb.ResourceMetrics {
	return []*metricspb.ResourceMetrics{{
		Resource:  testResource(),
		SchemaUrl: "https://opentelemetry.io/schemas/1.4.0",
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope:     testScope(),
			SchemaUrl: "https://opentelemetry.io/schemas/scope/1.0.0",
			Metrics:   metrics,
		}},
	}}
}

// gaugeMetric creates a Gauge metric with the given data points.
func gaugeMetric(name string, dps ...*metricspb.NumberDataPoint) *metricspb.Metric {
	return &metricspb.Metric{
		Name:        name,
		Description: name + " description",
		Unit:        "1",
		Data:        &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: dps}},
	}
}

// sumMetric creates a Sum metric with the given config and data points.
func sumMetric(name string, temporality metricspb.AggregationTemporality, monotonic bool, dps ...*metricspb.NumberDataPoint) *metricspb.Metric {
	return &metricspb.Metric{
		Name: name,
		Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			AggregationTemporality: temporality,
			IsMonotonic:            monotonic,
			DataPoints:             dps,
		}},
	}
}

// numberDP creates a NumberDataPoint with the given value and attributes.
func numberDP(value float64, attrs ...*commonpb.KeyValue) *metricspb.NumberDataPoint {
	return &metricspb.NumberDataPoint{
		Attributes:   attrs,
		TimeUnixNano: 1_000_000_000,
		Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: value},
	}
}

// --- Common mapping behavior (tested via Gauge as the simplest type) ---

func TestMapGaugeRows_OneDataPoint_ProducesOneMetadataAndOneGaugeRow(t *testing.T) {
	rm := buildResourceMetrics(gaugeMetric("cpu", numberDP(42.5, strAttr("env", "prod"))))

	metadata, rows := MapGaugeRows(rm)

	if len(metadata) != 1 {
		t.Fatalf("expected 1 metadata row, got %d", len(metadata))
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 gauge row, got %d", len(rows))
	}
	if metadata[0].MetricFingerprint == 0 {
		t.Error("expected non-zero fingerprint")
	}
	if rows[0].MetricFingerprint != metadata[0].MetricFingerprint {
		t.Error("gauge row fingerprint should match metadata fingerprint")
	}
	if rows[0].Value != 42.5 {
		t.Errorf("expected Value=42.5, got %f", rows[0].Value)
	}
}

func TestMapGaugeRows_TwoDataPointsSameAttrs_DeduplicatesMetadataToOneRow(t *testing.T) {
	env := strAttr("env", "prod")
	rm := buildResourceMetrics(gaugeMetric("cpu",
		numberDP(40.0, env),
		numberDP(42.5, env),
	))

	metadata, rows := MapGaugeRows(rm)

	if len(metadata) != 1 {
		t.Fatalf("expected 1 deduplicated metadata row, got %d", len(metadata))
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 gauge rows, got %d", len(rows))
	}
	if rows[0].MetricFingerprint != rows[1].MetricFingerprint {
		t.Error("both gauge rows should share the same fingerprint")
	}
}

func TestMapGaugeRows_TwoDataPointsDifferentAttrs_ProducesTwoDistinctFingerprints(t *testing.T) {
	rm := buildResourceMetrics(gaugeMetric("cpu",
		numberDP(40.0, strAttr("env", "prod")),
		numberDP(42.5, strAttr("env", "staging")),
	))

	metadata, rows := MapGaugeRows(rm)

	if len(metadata) != 2 {
		t.Fatalf("expected 2 metadata rows for different attributes, got %d", len(metadata))
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 gauge rows, got %d", len(rows))
	}
	if rows[0].MetricFingerprint == rows[1].MetricFingerprint {
		t.Error("different attributes should produce different fingerprints")
	}
}

func TestMapGaugeRows_NilOrEmptyInput_ReturnsEmptySlices(t *testing.T) {
	metadata, rows := MapGaugeRows(nil)
	if len(metadata) != 0 || len(rows) != 0 {
		t.Errorf("expected empty results for nil input, got %d metadata + %d rows", len(metadata), len(rows))
	}

	metadata, rows = MapGaugeRows([]*metricspb.ResourceMetrics{})
	if len(metadata) != 0 || len(rows) != 0 {
		t.Errorf("expected empty results for empty input, got %d metadata + %d rows", len(metadata), len(rows))
	}
}

func TestMapGaugeRows_SumMetricInput_ReturnsEmptySlices(t *testing.T) {
	rm := buildResourceMetrics(sumMetric("requests",
		metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE, true,
		numberDP(100),
	))

	metadata, rows := MapGaugeRows(rm)

	if len(metadata) != 0 || len(rows) != 0 {
		t.Errorf("expected no rows for non-gauge metric, got %d metadata + %d rows", len(metadata), len(rows))
	}
}

func TestMapGaugeRows_OneDataPoint_PopulatesAllMetadataFields(t *testing.T) {
	rm := buildResourceMetrics(gaugeMetric("cpu", numberDP(42.5, strAttr("env", "prod"))))

	metadata, _ := MapGaugeRows(rm)
	if len(metadata) != 1 {
		t.Fatalf("expected 1 metadata row, got %d", len(metadata))
	}

	md := metadata[0]
	checks := []struct {
		field string
		got   string
		want  string
	}{
		{"ServiceName", md.ServiceName, "test-service"},
		{"MetricName", md.MetricName, "cpu"},
		{"MetricDescription", md.MetricDescription, "cpu description"},
		{"MetricUnit", md.MetricUnit, "1"},
		{"ResourceSchemaUrl", md.ResourceSchemaUrl, "https://opentelemetry.io/schemas/1.4.0"},
		{"ScopeName", md.ScopeName, "test-scope"},
		{"ScopeVersion", md.ScopeVersion, "1.0.0"},
		{"ScopeSchemaUrl", md.ScopeSchemaUrl, "https://opentelemetry.io/schemas/scope/1.0.0"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: got %q, want %q", c.field, c.got, c.want)
		}
	}
	if v, ok := md.ResourceAttributes["host.name"]; !ok || v != "test-host" {
		t.Errorf("ResourceAttributes[host.name]: got %q", v)
	}
	if v, ok := md.Attributes["env"]; !ok || v != "prod" {
		t.Errorf("Attributes[env]: got %q", v)
	}
	if md.LastSeen.IsZero() {
		t.Error("LastSeen should not be zero")
	}
}

// --- Type-specific field extraction ---

func TestMapSumRows_OneDataPoint_ProducesOneMetadataAndOneSumRow(t *testing.T) {
	rm := buildResourceMetrics(sumMetric("requests",
		metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE, true,
		numberDP(1234, strAttr("method", "GET")),
	))

	metadata, rows := MapSumRows(rm)

	if len(metadata) != 1 || len(rows) != 1 {
		t.Fatalf("expected 1+1, got %d+%d", len(metadata), len(rows))
	}
	if rows[0].MetricFingerprint != metadata[0].MetricFingerprint {
		t.Error("fingerprint mismatch")
	}
	if rows[0].Value != 1234 {
		t.Errorf("Value: got %f, want 1234", rows[0].Value)
	}
}

func TestMapSumRows_CumulativeMonotonic_PreservesTemporalityAndMonotonicity(t *testing.T) {
	rm := buildResourceMetrics(sumMetric("requests",
		metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE, true,
		numberDP(100),
	))

	_, rows := MapSumRows(rm)

	if len(rows) != 1 {
		t.Fatalf("expected 1 sum row, got %d", len(rows))
	}
	if rows[0].AggregationTemporality != int32(metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE) {
		t.Errorf("AggregationTemporality: got %d", rows[0].AggregationTemporality)
	}
	if !rows[0].IsMonotonic {
		t.Error("expected IsMonotonic=true")
	}
}

func TestMapHistogramRows_OneDataPoint_ExtractsBucketCountsSumMinMax(t *testing.T) {
	rm := buildResourceMetrics(&metricspb.Metric{
		Name: "duration",
		Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
			AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			DataPoints: []*metricspb.HistogramDataPoint{{
				Attributes:     []*commonpb.KeyValue{strAttr("env", "prod")},
				TimeUnixNano:   1_000_000_000,
				Count:          10,
				Sum:            float64Ptr(150.0),
				BucketCounts:   []uint64{2, 3, 5},
				ExplicitBounds: []float64{10, 50},
				Min:            float64Ptr(1.0),
				Max:            float64Ptr(100.0),
			}},
		}},
	})

	metadata, rows := MapHistogramRows(rm)

	if len(metadata) != 1 || len(rows) != 1 {
		t.Fatalf("expected 1+1, got %d+%d", len(metadata), len(rows))
	}
	hr := rows[0]
	if hr.MetricFingerprint != metadata[0].MetricFingerprint {
		t.Error("fingerprint mismatch")
	}
	if hr.Count != 10 {
		t.Errorf("Count: got %d, want 10", hr.Count)
	}
	if hr.Sum != 150.0 {
		t.Errorf("Sum: got %f, want 150", hr.Sum)
	}
	if len(hr.BucketCounts) != 3 || len(hr.ExplicitBounds) != 2 {
		t.Errorf("BucketCounts=%d ExplicitBounds=%d, want 3 and 2", len(hr.BucketCounts), len(hr.ExplicitBounds))
	}
	if hr.Min != 1.0 || hr.Max != 100.0 {
		t.Errorf("Min/Max: got %f/%f, want 1/100", hr.Min, hr.Max)
	}
	if hr.AggregationTemporality != int32(metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE) {
		t.Errorf("AggregationTemporality: got %d", hr.AggregationTemporality)
	}
}

func TestMapExponentialHistogramRows_OneDataPoint_ExtractsScaleBucketsMinMax(t *testing.T) {
	rm := buildResourceMetrics(&metricspb.Metric{
		Name: "duration.exp",
		Data: &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
			AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
			DataPoints: []*metricspb.ExponentialHistogramDataPoint{{
				Attributes:   []*commonpb.KeyValue{strAttr("env", "prod")},
				TimeUnixNano: 1_000_000_000,
				Count:        20,
				Sum:          float64Ptr(300.0),
				Scale:        3,
				ZeroCount:    1,
				Positive: &metricspb.ExponentialHistogramDataPoint_Buckets{
					Offset: 0, BucketCounts: []uint64{1, 2, 3},
				},
				Negative: &metricspb.ExponentialHistogramDataPoint_Buckets{
					Offset: -1, BucketCounts: []uint64{4, 5},
				},
				Min: float64Ptr(0.5),
				Max: float64Ptr(200.0),
			}},
		}},
	})

	metadata, rows := MapExponentialHistogramRows(rm)

	if len(metadata) != 1 || len(rows) != 1 {
		t.Fatalf("expected 1+1, got %d+%d", len(metadata), len(rows))
	}
	r := rows[0]
	if r.MetricFingerprint != metadata[0].MetricFingerprint {
		t.Error("fingerprint mismatch")
	}
	if r.Count != 20 || r.Sum != 300.0 || r.Scale != 3 || r.ZeroCount != 1 {
		t.Errorf("basic fields: Count=%d Sum=%f Scale=%d ZeroCount=%d", r.Count, r.Sum, r.Scale, r.ZeroCount)
	}
	if r.PositiveOffset != 0 || len(r.PositiveBucketCounts) != 3 {
		t.Errorf("positive buckets: offset=%d len=%d", r.PositiveOffset, len(r.PositiveBucketCounts))
	}
	if r.NegativeOffset != -1 || len(r.NegativeBucketCounts) != 2 {
		t.Errorf("negative buckets: offset=%d len=%d", r.NegativeOffset, len(r.NegativeBucketCounts))
	}
	if r.Min != 0.5 || r.Max != 200.0 {
		t.Errorf("Min/Max: got %f/%f", r.Min, r.Max)
	}
	if r.AggregationTemporality != int32(metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA) {
		t.Errorf("AggregationTemporality: got %d", r.AggregationTemporality)
	}
}

func TestMapSummaryRows_ThreeQuantiles_ExtractsCountSumAndQuantilePairs(t *testing.T) {
	rm := buildResourceMetrics(&metricspb.Metric{
		Name: "duration.summary",
		Data: &metricspb.Metric_Summary{Summary: &metricspb.Summary{
			DataPoints: []*metricspb.SummaryDataPoint{{
				Attributes:   []*commonpb.KeyValue{strAttr("env", "prod")},
				TimeUnixNano: 1_000_000_000,
				Count:        100,
				Sum:          5000.0,
				QuantileValues: []*metricspb.SummaryDataPoint_ValueAtQuantile{
					{Quantile: 0.5, Value: 45.0},
					{Quantile: 0.9, Value: 85.0},
					{Quantile: 0.99, Value: 98.0},
				},
			}},
		}},
	})

	metadata, rows := MapSummaryRows(rm)

	if len(metadata) != 1 || len(rows) != 1 {
		t.Fatalf("expected 1+1, got %d+%d", len(metadata), len(rows))
	}
	r := rows[0]
	if r.MetricFingerprint != metadata[0].MetricFingerprint {
		t.Error("fingerprint mismatch")
	}
	if r.Count != 100 || r.Sum != 5000.0 {
		t.Errorf("Count=%d Sum=%f", r.Count, r.Sum)
	}
	wantQ := []float64{0.5, 0.9, 0.99}
	wantV := []float64{45.0, 85.0, 98.0}
	if len(r.ValueAtQuantileQuantile) != len(wantQ) {
		t.Fatalf("expected %d quantiles, got %d", len(wantQ), len(r.ValueAtQuantileQuantile))
	}
	for i := range wantQ {
		if r.ValueAtQuantileQuantile[i] != wantQ[i] || r.ValueAtQuantileValue[i] != wantV[i] {
			t.Errorf("quantile[%d]: got q=%f v=%f, want q=%f v=%f",
				i, r.ValueAtQuantileQuantile[i], r.ValueAtQuantileValue[i], wantQ[i], wantV[i])
		}
	}
}

// --- Helper function unit tests ---

func TestAnyValueToString(t *testing.T) {
	tests := []struct {
		name  string
		input *commonpb.AnyValue
		want  string
	}{
		{"nil", nil, ""},
		{"string", &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello"}}, "hello"},
		{"int", &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 42}}, "42"},
		{"double", &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 3.14}}, "3.14"},
		{"bool", &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}, "true"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := anyValueToString(tt.input); got != tt.want {
				t.Errorf("anyValueToString() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNumberDataPointValue(t *testing.T) {
	tests := []struct {
		name  string
		input *metricspb.NumberDataPoint
		want  float64
	}{
		{"double", &metricspb.NumberDataPoint{Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 3.14}}, 3.14},
		{"int", &metricspb.NumberDataPoint{Value: &metricspb.NumberDataPoint_AsInt{AsInt: 42}}, 42.0},
		{"nil value", &metricspb.NumberDataPoint{}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := numberDataPointValue(tt.input); got != tt.want {
				t.Errorf("numberDataPointValue() = %f, want %f", got, tt.want)
			}
		})
	}
}
