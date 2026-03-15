package main

import (
	"sort"

	"github.com/cespare/xxhash/v2"
)

// computeFingerprint produces a deterministic UInt64 fingerprint for a unique
// metric series identity by hashing all metadata fields with xxhash. Map keys
// are sorted before hashing to guarantee determinism regardless of Go map
// iteration order.
func computeFingerprint(
	serviceName, metricName, metricUnit, metricDescription string,
	resourceAttrs, scopeAttrs, dpAttrs map[string]string,
	scopeName, scopeVersion string,
) uint64 {
	d := xxhash.New()

	// Write each field separated by a null byte delimiter.
	writeField := func(s string) {
		d.WriteString(s)
		d.Write([]byte{0})
	}

	writeField(serviceName)
	writeField(metricName)
	writeField(metricUnit)
	writeField(metricDescription)

	writeMapSorted(d, resourceAttrs)
	writeMapSorted(d, scopeAttrs)

	writeField(scopeName)
	writeField(scopeVersion)

	writeMapSorted(d, dpAttrs)

	return d.Sum64()
}

// writeMapSorted writes map entries into the hash digest in sorted key order,
// each key and value followed by a null byte delimiter.
func writeMapSorted(d *xxhash.Digest, m map[string]string) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		d.WriteString(k)
		d.Write([]byte{0})
		d.WriteString(m[k])
		d.Write([]byte{0})
	}
}
