package main

import "testing"

func TestComputeFingerprint_Deterministic(t *testing.T) {
	attrs := map[string]string{"host": "web-1", "region": "us-east-1"}
	scopeAttrs := map[string]string{"library": "otel-go"}

	fp1 := computeFingerprint("svc", "cpu.usage", "1", "CPU usage percent", attrs, scopeAttrs, nil, "scope", "v1")
	fp2 := computeFingerprint("svc", "cpu.usage", "1", "CPU usage percent", attrs, scopeAttrs, nil, "scope", "v1")

	if fp1 != fp2 {
		t.Fatalf("expected identical fingerprints, got %d and %d", fp1, fp2)
	}
}

func TestComputeFingerprint_MapKeyOrderIndependent(t *testing.T) {
	// Build two maps with the same entries added in different order.
	m1 := map[string]string{"a": "1", "b": "2", "c": "3"}
	m2 := map[string]string{"c": "3", "a": "1", "b": "2"}

	fp1 := computeFingerprint("svc", "metric", "", "", m1, nil, nil, "", "")
	fp2 := computeFingerprint("svc", "metric", "", "", m2, nil, nil, "", "")

	if fp1 != fp2 {
		t.Fatalf("fingerprints should be identical regardless of map insertion order, got %d and %d", fp1, fp2)
	}
}

func TestComputeFingerprint_DifferentInputsDiffer(t *testing.T) {
	base := func() uint64 {
		return computeFingerprint(
			"svc", "metric", "unit", "desc",
			map[string]string{"k": "v"},
			map[string]string{"sk": "sv"},
			map[string]string{"dk": "dv"},
			"scope", "v1",
		)
	}
	baseFP := base()

	tests := []struct {
		name string
		fp   uint64
	}{
		{"serviceName", computeFingerprint("other", "metric", "unit", "desc", map[string]string{"k": "v"}, map[string]string{"sk": "sv"}, map[string]string{"dk": "dv"}, "scope", "v1")},
		{"metricName", computeFingerprint("svc", "other", "unit", "desc", map[string]string{"k": "v"}, map[string]string{"sk": "sv"}, map[string]string{"dk": "dv"}, "scope", "v1")},
		{"metricUnit", computeFingerprint("svc", "metric", "other", "desc", map[string]string{"k": "v"}, map[string]string{"sk": "sv"}, map[string]string{"dk": "dv"}, "scope", "v1")},
		{"metricDescription", computeFingerprint("svc", "metric", "unit", "other", map[string]string{"k": "v"}, map[string]string{"sk": "sv"}, map[string]string{"dk": "dv"}, "scope", "v1")},
		{"resourceAttrs", computeFingerprint("svc", "metric", "unit", "desc", map[string]string{"k": "other"}, map[string]string{"sk": "sv"}, map[string]string{"dk": "dv"}, "scope", "v1")},
		{"scopeAttrs", computeFingerprint("svc", "metric", "unit", "desc", map[string]string{"k": "v"}, map[string]string{"sk": "other"}, map[string]string{"dk": "dv"}, "scope", "v1")},
		{"dpAttrs", computeFingerprint("svc", "metric", "unit", "desc", map[string]string{"k": "v"}, map[string]string{"sk": "sv"}, map[string]string{"dk": "other"}, "scope", "v1")},
		{"scopeName", computeFingerprint("svc", "metric", "unit", "desc", map[string]string{"k": "v"}, map[string]string{"sk": "sv"}, map[string]string{"dk": "dv"}, "other", "v1")},
		{"scopeVersion", computeFingerprint("svc", "metric", "unit", "desc", map[string]string{"k": "v"}, map[string]string{"sk": "sv"}, map[string]string{"dk": "dv"}, "scope", "other")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.fp == baseFP {
				t.Errorf("changing %s should produce a different fingerprint", tt.name)
			}
		})
	}
}

func TestComputeFingerprint_EmptyFields(t *testing.T) {
	fp := computeFingerprint("", "", "", "", nil, nil, nil, "", "")
	if fp == 0 {
		t.Fatal("fingerprint of all-empty inputs should be non-zero")
	}
}
