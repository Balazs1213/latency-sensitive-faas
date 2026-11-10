package core

import "testing"

// classifyTraffic must not mistake an overloaded app for an idle one: under a heavy overload no request completes
// in the metric window, but there are requests (spans) in it
func TestClassifyTraffic(t *testing.T) {
	tests := []struct {
		name       string
		measured   bool
		traceCount int
		want       trafficState
	}{
		{"measured latency", true, 40, trafficMeasured},
		{"measured latency, few traces", true, 1, trafficMeasured},
		{"requests but none completed", false, 323, trafficStalled},
		{"a single request in flight", false, 1, trafficStalled},
		{"no requests", false, 0, trafficIdle},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyTraffic(tt.measured, tt.traceCount); got != tt.want {
				t.Errorf("classifyTraffic(%v, %d) = %v, want %v", tt.measured, tt.traceCount, got, tt.want)
			}
		})
	}
}
