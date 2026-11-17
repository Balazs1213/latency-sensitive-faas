package core

import "testing"

func TestBackupKey(t *testing.T) {
	key := backupKey("large")
	if original, ok := backedUpLayout(key); !ok || original != "large" {
		t.Errorf("backedUpLayout(%q) = %q, %v, want \"large\", true", key, original, ok)
	}
	if _, ok := backedUpLayout("large"); ok {
		t.Error("a layout candidate is not a backup layout")
	}
}

func TestUsesUnavailableNode(t *testing.T) {
	layout := Layout{"node-a": fusedComposition(), "node-b": detectComposition()}
	if usesUnavailableNode(layout, map[string]bool{"node-a": true, "node-b": true}) {
		t.Error("every node is available: want false")
	}
	if !usesUnavailableNode(layout, map[string]bool{"node-a": true, "node-b": false}) {
		t.Error("node-b is not available: want true")
	}
	if !usesUnavailableNode(layout, map[string]bool{"node-a": true}) {
		t.Error("node-b is not reported by the cluster: want true")
	}
}

// The composition of the unavailable node moves to the first free available node; the others stay in place
func TestRelocateLayout(t *testing.T) {
	layout := Layout{"node-a": detectComposition(), "node-b": fusedComposition()}
	available := map[string]bool{"node-a": true, "node-b": false, "node-c": true, "node-d": true}

	backup := relocateLayout(layout, available)
	if len(backup) != 2 {
		t.Fatalf("backup has %d compositions, want 2: %v", len(backup), backup)
	}
	if compositionKey(backup["node-a"]) != compositionKey(layout["node-a"]) {
		t.Error("the composition on the available node-a must stay")
	}
	if compositionKey(backup["node-c"]) != compositionKey(layout["node-b"]) {
		t.Error("the composition of node-b must move to node-c, the first free available node")
	}
	if _, ok := backup["node-b"]; ok {
		t.Error("the backup must not use the unavailable node-b")
	}
}

// Without a free available node there is no backup
func TestRelocateLayoutWithoutFreeNode(t *testing.T) {
	layout := Layout{"node-a": detectComposition(), "node-b": fusedComposition()}
	if backup := relocateLayout(layout, map[string]bool{"node-a": true, "node-b": false}); backup != nil {
		t.Errorf("want no backup, got %v", backup)
	}
}
