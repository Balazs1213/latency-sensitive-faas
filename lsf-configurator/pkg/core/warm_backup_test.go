package core

import "testing"

// Every composition of a layout has a warm backup target: the free node its backup layout would move it to
func TestWarmBackupTargets(t *testing.T) {
	detect := CompositionInfo{RequiredReplicas: 9}
	fusion := CompositionInfo{RequiredReplicas: 1}
	layout := Layout{"n1": detect, "n2": fusion}
	available := map[string]bool{"n1": true, "n2": true, "n3": true}

	targets := warmBackupTargets(layout, available)
	if len(targets) != 2 {
		t.Fatalf("got %d targets, want 2: %v", len(targets), targets)
	}
	for i, want := range []CompositionInfo{detect, fusion} {
		if targets[i].node != "n3" || targets[i].ci.RequiredReplicas != want.RequiredReplicas {
			t.Errorf("target %d = %s with %d replicas, want n3 with %d", i, targets[i].node,
				targets[i].ci.RequiredReplicas, want.RequiredReplicas)
		}
	}
}

// Without a free node there is no backup, so no warm backup either
func TestWarmBackupTargetsWithoutFreeNode(t *testing.T) {
	layout := Layout{"n1": CompositionInfo{RequiredReplicas: 9}, "n2": CompositionInfo{RequiredReplicas: 1}}
	available := map[string]bool{"n1": true, "n2": true}
	if targets := warmBackupTargets(layout, available); len(targets) != 0 {
		t.Errorf("got %v, want no target", targets)
	}
}

// The warm backups cover every layout candidate, so they are ready whichever layout is active, but not a backup
func TestAllWarmTargets(t *testing.T) {
	merged := CompositionInfo{RequiredReplicas: 2}
	app := &FunctionApp{LayoutCandidates: map[string]Layout{
		"min_rate":        {"n1": merged},
		"max_rate":        {"n1": CompositionInfo{RequiredReplicas: 9}, "n2": CompositionInfo{RequiredReplicas: 1}},
		"max_rate@backup": {"n1": CompositionInfo{RequiredReplicas: 9}, "n3": CompositionInfo{RequiredReplicas: 1}},
	}}
	available := map[string]bool{"n1": true, "n2": true, "n3": true}
	if targets := allWarmTargets(app, available); len(targets) != 3 {
		t.Errorf("got %d targets, want 3 (2 of max_rate, 1 of min_rate): %v", len(targets), targets)
	}
}
