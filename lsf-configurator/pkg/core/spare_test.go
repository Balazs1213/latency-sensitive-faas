package core

import "testing"

// Only the compositions planned for a single pod get spare pods in the single mode; all of them in the all mode
func TestSparePods(t *testing.T) {
	single := CompositionInfo{RequiredReplicas: 1}
	several := CompositionInfo{RequiredReplicas: 9}
	tests := []struct {
		name  string
		mode  string
		count int
		ci    CompositionInfo
		want  int
	}{
		{"off, single pod", SparePodsOff, 1, single, 0},
		{"single mode, single pod", SparePodsSingle, 1, single, 1},
		{"single mode, several pods", SparePodsSingle, 1, several, 0},
		{"all mode, several pods", SparePodsAll, 2, several, 2},
		{"single mode, two spares", SparePodsSingle, 2, single, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sparePods(tt.mode, tt.count, tt.ci); got != tt.want {
				t.Errorf("sparePods(%q, %d, %d replicas) = %d, want %d", tt.mode, tt.count, tt.ci.RequiredReplicas, got, tt.want)
			}
		})
	}
}
