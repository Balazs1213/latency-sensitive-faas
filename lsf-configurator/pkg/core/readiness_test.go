package core

import "testing"

// A layout can serve only if each of its compositions has a ready pod: a lost node leaves one without any
func TestAllCompositionsReady(t *testing.T) {
	layout := Layout{"node-a": fusedComposition(), "node-b": detectComposition()}
	ready := map[string]int{"node-a": 1, "node-b": 0}
	pods := func(node string, _ CompositionInfo) int { return ready[node] }

	if allCompositionsReady(layout, pods) {
		t.Error("a composition without a ready pod: want false")
	}
	ready["node-b"] = 3
	if !allCompositionsReady(layout, pods) {
		t.Error("every composition has ready pods: want true")
	}
	if allCompositionsReady(Layout{}, pods) {
		t.Error("empty layout: want false")
	}
}

// The failover target is another layout that can serve; the active layout itself is never chosen
func TestFailoverTarget(t *testing.T) {
	app := &FunctionApp{ActiveLayoutKey: "large", LayoutCandidates: map[string]Layout{"small": {}, "large": {}}}
	tests := []struct {
		name    string
		serving map[string]bool
		want    string
	}{
		{"no layout can serve", map[string]bool{}, ""},
		{"only the active layout can serve", map[string]bool{"large": true}, ""},
		{"another layout can serve", map[string]bool{"small": true}, "small"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := failoverTarget(app, func(key string) bool { return tt.serving[key] })
			if got != tt.want {
				t.Errorf("failoverTarget = %q, want %q", got, tt.want)
			}
		})
	}
}
