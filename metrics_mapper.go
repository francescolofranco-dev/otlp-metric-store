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

// MapGaugeRows converts ResourceMetrics into MetadataRows and slim GaugeRows.
// TODO: metadata extraction will be implemented in a follow-up change.
func MapGaugeRows(resourceMetrics []*metricspb.ResourceMetrics) ([]MetadataRow, []GaugeRow) {
	var metadataRows []MetadataRow
	var gaugeRows []GaugeRow
	for _, rm := range resourceMetrics {
		for _, sm := range rm.GetScopeMetrics() {
			for _, metric := range sm.GetMetrics() {
				gauge := metric.GetGauge()
				if gauge == nil {
					continue
				}
				for _, dp := range gauge.GetDataPoints() {
					gaugeRows = append(gaugeRows, GaugeRow{
						StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
						TimeUnix:      nanosToTime(dp.GetTimeUnixNano()),
						Value:         numberDataPointValue(dp),
						Flags:         dp.GetFlags(),
					})
				}
			}
		}
	}
	return metadataRows, gaugeRows
}

// MapSumRows converts ResourceMetrics into MetadataRows and slim SumRows.
// TODO: metadata extraction will be implemented in a follow-up change.
func MapSumRows(resourceMetrics []*metricspb.ResourceMetrics) ([]MetadataRow, []SumRow) {
	var metadataRows []MetadataRow
	var sumRows []SumRow
	for _, rm := range resourceMetrics {
		for _, sm := range rm.GetScopeMetrics() {
			for _, metric := range sm.GetMetrics() {
				sum := metric.GetSum()
				if sum == nil {
					continue
				}
				for _, dp := range sum.GetDataPoints() {
					sumRows = append(sumRows, SumRow{
						StartTimeUnix:          nanosToTime(dp.GetStartTimeUnixNano()),
						TimeUnix:               nanosToTime(dp.GetTimeUnixNano()),
						Value:                  numberDataPointValue(dp),
						Flags:                  dp.GetFlags(),
						AggregationTemporality: int32(sum.GetAggregationTemporality()),
						IsMonotonic:            sum.GetIsMonotonic(),
					})
				}
			}
		}
	}
	return metadataRows, sumRows
}
