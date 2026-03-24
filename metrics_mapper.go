package main

import (
	"fmt"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// serviceName extracts the service.name from resource attributes, returning "" if not found.
func serviceName(resource *resourcepb.Resource) string {
	if resource == nil {
		return ""
	}
	for _, attr := range resource.GetAttributes() {
		if attr.GetKey() == "service.name" {
			return attr.GetValue().GetStringValue()
		}
	}
	return ""
}

// kvToMap converts a slice of OTLP KeyValue pairs to a Go map.
func kvToMap(attrs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, kv := range attrs {
		m[kv.GetKey()] = anyValueToString(kv.GetValue())
	}
	return m
}

// anyValueToString converts an OTLP AnyValue to its string representation.
func anyValueToString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return v.GetStringValue()
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", v.GetIntValue())
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", v.GetDoubleValue())
	case *commonpb.AnyValue_BoolValue:
		return fmt.Sprintf("%t", v.GetBoolValue())
	default:
		return fmt.Sprintf("%v", v)
	}
}

// nanosToTime converts a uint64 nanoseconds-since-epoch to time.Time.
func nanosToTime(nanos uint64) time.Time {
	return time.Unix(0, int64(nanos))
}

// numberDataPointValue extracts the float64 value from a NumberDataPoint.
func numberDataPointValue(dp *metricspb.NumberDataPoint) float64 {
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	default:
		return 0
	}
}

// dataPointMapper defines how to extract data points from a metric and build typed rows.
type dataPointMapper[DP any, Row any] struct {
	getDataPoints func(*metricspb.Metric) []DP
	getAttributes func(DP) []*commonpb.KeyValue
	buildRow      func(uint64, DP, *metricspb.Metric) Row
}

// MappedMetrics holds the results of a single-pass mapping over all metric types.
type MappedMetrics struct {
	Metadata          []MetadataRow
	GaugeRows         []GaugeRow
	SumRows           []SumRow
	HistogramRows     []HistogramRow
	ExpHistogramRows  []ExponentialHistogramRow
	SummaryRows       []SummaryRow
}

// mapDataPoints extracts data-point rows from a single metric using the given mapper callbacks,
// reusing pre-computed resource/scope context. It appends metadata and data rows to the provided slices.
func mapDataPoints[DP any, Row any](
	metric *metricspb.Metric,
	m dataPointMapper[DP, Row],
	svcName string, resAttrs map[string]string, resSchemaUrl string,
	scope *commonpb.InstrumentationScope, scopeAttrs map[string]string, scopeSchemaUrl string,
	now time.Time,
	seen map[uint64]struct{},
	metadataRows *[]MetadataRow,
	dataRows *[]Row,
) {
	for _, dp := range m.getDataPoints(metric) {
		dpAttrs := kvToMap(m.getAttributes(dp))
		fp := computeFingerprint(
			svcName, metric.GetName(), metric.GetUnit(), metric.GetDescription(),
			resAttrs, scopeAttrs, dpAttrs,
			scope.GetName(), scope.GetVersion(),
		)
		if _, ok := seen[fp]; !ok {
			seen[fp] = struct{}{}
			*metadataRows = append(*metadataRows, MetadataRow{
				MetricFingerprint:     fp,
				ServiceName:           svcName,
				MetricName:            metric.GetName(),
				MetricDescription:     metric.GetDescription(),
				MetricUnit:            metric.GetUnit(),
				ResourceAttributes:    resAttrs,
				ResourceSchemaUrl:     resSchemaUrl,
				ScopeName:             scope.GetName(),
				ScopeVersion:          scope.GetVersion(),
				ScopeAttributes:       scopeAttrs,
				ScopeDroppedAttrCount: scope.GetDroppedAttributesCount(),
				ScopeSchemaUrl:        scopeSchemaUrl,
				Attributes:            dpAttrs,
				LastSeen:              now,
			})
		}
		*dataRows = append(*dataRows, m.buildRow(fp, dp, metric))
	}
}

// MapAllMetricRows walks the resource/scope/metric hierarchy once and dispatches
// each metric to the appropriate type-specific mapper based on which Data oneof is set.
func MapAllMetricRows(resourceMetrics []*metricspb.ResourceMetrics) MappedMetrics {
	now := time.Now()
	seen := make(map[uint64]struct{})
	var result MappedMetrics

	for _, rm := range resourceMetrics {
		svcName := serviceName(rm.GetResource())
		resAttrs := kvToMap(rm.GetResource().GetAttributes())
		resSchemaUrl := rm.GetSchemaUrl()

		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope()
			scopeAttrs := kvToMap(scope.GetAttributes())
			scopeSchemaUrl := sm.GetSchemaUrl()

			for _, metric := range sm.GetMetrics() {
				switch {
				case metric.GetGauge() != nil:
					mapDataPoints(metric, gaugeMapper, svcName, resAttrs, resSchemaUrl, scope, scopeAttrs, scopeSchemaUrl, now, seen, &result.Metadata, &result.GaugeRows)
				case metric.GetSum() != nil:
					mapDataPoints(metric, sumMapper, svcName, resAttrs, resSchemaUrl, scope, scopeAttrs, scopeSchemaUrl, now, seen, &result.Metadata, &result.SumRows)
				case metric.GetHistogram() != nil:
					mapDataPoints(metric, histogramMapper, svcName, resAttrs, resSchemaUrl, scope, scopeAttrs, scopeSchemaUrl, now, seen, &result.Metadata, &result.HistogramRows)
				case metric.GetExponentialHistogram() != nil:
					mapDataPoints(metric, expHistogramMapper, svcName, resAttrs, resSchemaUrl, scope, scopeAttrs, scopeSchemaUrl, now, seen, &result.Metadata, &result.ExpHistogramRows)
				case metric.GetSummary() != nil:
					mapDataPoints(metric, summaryMapper, svcName, resAttrs, resSchemaUrl, scope, scopeAttrs, scopeSchemaUrl, now, seen, &result.Metadata, &result.SummaryRows)
				}
			}
		}
	}
	return result
}

// --- Gauge ---

var gaugeMapper = dataPointMapper[*metricspb.NumberDataPoint, GaugeRow]{
	getDataPoints: func(m *metricspb.Metric) []*metricspb.NumberDataPoint {
		if g := m.GetGauge(); g != nil {
			return g.GetDataPoints()
		}
		return nil
	},
	getAttributes: func(dp *metricspb.NumberDataPoint) []*commonpb.KeyValue {
		return dp.GetAttributes()
	},
	buildRow: func(fp uint64, dp *metricspb.NumberDataPoint, _ *metricspb.Metric) GaugeRow {
		return GaugeRow{
			MetricFingerprint: fp,
			StartTimeUnix:     nanosToTime(dp.GetStartTimeUnixNano()),
			TimeUnix:          nanosToTime(dp.GetTimeUnixNano()),
			Value:             numberDataPointValue(dp),
			Flags:             dp.GetFlags(),
		}
	},
}

// MapGaugeRows converts ResourceMetrics into deduplicated MetadataRows and slim GaugeRows.
func MapGaugeRows(rm []*metricspb.ResourceMetrics) ([]MetadataRow, []GaugeRow) {
	m := MapAllMetricRows(rm)
	return m.Metadata, m.GaugeRows
}

// --- Sum ---

var sumMapper = dataPointMapper[*metricspb.NumberDataPoint, SumRow]{
	getDataPoints: func(m *metricspb.Metric) []*metricspb.NumberDataPoint {
		if s := m.GetSum(); s != nil {
			return s.GetDataPoints()
		}
		return nil
	},
	getAttributes: func(dp *metricspb.NumberDataPoint) []*commonpb.KeyValue {
		return dp.GetAttributes()
	},
	buildRow: func(fp uint64, dp *metricspb.NumberDataPoint, metric *metricspb.Metric) SumRow {
		return SumRow{
			MetricFingerprint:      fp,
			StartTimeUnix:          nanosToTime(dp.GetStartTimeUnixNano()),
			TimeUnix:               nanosToTime(dp.GetTimeUnixNano()),
			Value:                  numberDataPointValue(dp),
			Flags:                  dp.GetFlags(),
			AggregationTemporality: int32(metric.GetSum().GetAggregationTemporality()),
			IsMonotonic:            metric.GetSum().GetIsMonotonic(),
		}
	},
}

// MapSumRows converts ResourceMetrics into deduplicated MetadataRows and slim SumRows.
func MapSumRows(rm []*metricspb.ResourceMetrics) ([]MetadataRow, []SumRow) {
	m := MapAllMetricRows(rm)
	return m.Metadata, m.SumRows
}

// --- Histogram ---

var histogramMapper = dataPointMapper[*metricspb.HistogramDataPoint, HistogramRow]{
	getDataPoints: func(m *metricspb.Metric) []*metricspb.HistogramDataPoint {
		if h := m.GetHistogram(); h != nil {
			return h.GetDataPoints()
		}
		return nil
	},
	getAttributes: func(dp *metricspb.HistogramDataPoint) []*commonpb.KeyValue {
		return dp.GetAttributes()
	},
	buildRow: func(fp uint64, dp *metricspb.HistogramDataPoint, metric *metricspb.Metric) HistogramRow {
		return HistogramRow{
			MetricFingerprint:      fp,
			StartTimeUnix:          nanosToTime(dp.GetStartTimeUnixNano()),
			TimeUnix:               nanosToTime(dp.GetTimeUnixNano()),
			Count:                  dp.GetCount(),
			Sum:                    dp.GetSum(),
			BucketCounts:           dp.GetBucketCounts(),
			ExplicitBounds:         dp.GetExplicitBounds(),
			Min:                    dp.GetMin(),
			Max:                    dp.GetMax(),
			Flags:                  dp.GetFlags(),
			AggregationTemporality: int32(metric.GetHistogram().GetAggregationTemporality()),
		}
	},
}

// MapHistogramRows converts ResourceMetrics into deduplicated MetadataRows and slim HistogramRows.
func MapHistogramRows(rm []*metricspb.ResourceMetrics) ([]MetadataRow, []HistogramRow) {
	m := MapAllMetricRows(rm)
	return m.Metadata, m.HistogramRows
}

// --- Exponential Histogram ---

var expHistogramMapper = dataPointMapper[*metricspb.ExponentialHistogramDataPoint, ExponentialHistogramRow]{
	getDataPoints: func(m *metricspb.Metric) []*metricspb.ExponentialHistogramDataPoint {
		if eh := m.GetExponentialHistogram(); eh != nil {
			return eh.GetDataPoints()
		}
		return nil
	},
	getAttributes: func(dp *metricspb.ExponentialHistogramDataPoint) []*commonpb.KeyValue {
		return dp.GetAttributes()
	},
	buildRow: func(fp uint64, dp *metricspb.ExponentialHistogramDataPoint, metric *metricspb.Metric) ExponentialHistogramRow {
		positive := dp.GetPositive()
		negative := dp.GetNegative()
		return ExponentialHistogramRow{
			MetricFingerprint:      fp,
			StartTimeUnix:          nanosToTime(dp.GetStartTimeUnixNano()),
			TimeUnix:               nanosToTime(dp.GetTimeUnixNano()),
			Count:                  dp.GetCount(),
			Sum:                    dp.GetSum(),
			Scale:                  dp.GetScale(),
			ZeroCount:              dp.GetZeroCount(),
			PositiveOffset:         positive.GetOffset(),
			PositiveBucketCounts:   positive.GetBucketCounts(),
			NegativeOffset:         negative.GetOffset(),
			NegativeBucketCounts:   negative.GetBucketCounts(),
			Min:                    dp.GetMin(),
			Max:                    dp.GetMax(),
			Flags:                  dp.GetFlags(),
			AggregationTemporality: int32(metric.GetExponentialHistogram().GetAggregationTemporality()),
		}
	},
}

// MapExponentialHistogramRows converts ResourceMetrics into deduplicated MetadataRows and slim ExponentialHistogramRows.
func MapExponentialHistogramRows(rm []*metricspb.ResourceMetrics) ([]MetadataRow, []ExponentialHistogramRow) {
	m := MapAllMetricRows(rm)
	return m.Metadata, m.ExpHistogramRows
}

// --- Summary ---

var summaryMapper = dataPointMapper[*metricspb.SummaryDataPoint, SummaryRow]{
	getDataPoints: func(m *metricspb.Metric) []*metricspb.SummaryDataPoint {
		if s := m.GetSummary(); s != nil {
			return s.GetDataPoints()
		}
		return nil
	},
	getAttributes: func(dp *metricspb.SummaryDataPoint) []*commonpb.KeyValue {
		return dp.GetAttributes()
	},
	buildRow: func(fp uint64, dp *metricspb.SummaryDataPoint, _ *metricspb.Metric) SummaryRow {
		quantiles := dp.GetQuantileValues()
		quantileSlice := make([]float64, len(quantiles))
		valueSlice := make([]float64, len(quantiles))
		for i, qv := range quantiles {
			quantileSlice[i] = qv.GetQuantile()
			valueSlice[i] = qv.GetValue()
		}
		return SummaryRow{
			MetricFingerprint:       fp,
			StartTimeUnix:           nanosToTime(dp.GetStartTimeUnixNano()),
			TimeUnix:                nanosToTime(dp.GetTimeUnixNano()),
			Count:                   dp.GetCount(),
			Sum:                     dp.GetSum(),
			ValueAtQuantileQuantile: quantileSlice,
			ValueAtQuantileValue:    valueSlice,
			Flags:                   dp.GetFlags(),
		}
	},
}

// MapSummaryRows converts ResourceMetrics into deduplicated MetadataRows and slim SummaryRows.
func MapSummaryRows(rm []*metricspb.ResourceMetrics) ([]MetadataRow, []SummaryRow) {
	m := MapAllMetricRows(rm)
	return m.Metadata, m.SummaryRows
}
