package core

import (
	"sort"
	"strings"
)

// backupSuffix marks the layout candidate that stands in for another one whose nodes are not available
const backupSuffix = "@backup"

// backupKey returns the candidate key of the backup of a layout candidate
func backupKey(layoutKey string) string {
	return layoutKey + backupSuffix
}

// backedUpLayout returns the key of the layout candidate a backup layout stands in for, and whether the key is a
// backup layout at all
func backedUpLayout(layoutKey string) (string, bool) {
	return strings.CutSuffix(layoutKey, backupSuffix)
}

// usesUnavailableNode reports whether a layout places a composition on a node that is not available (a node the
// cluster does not report counts as not available)
func usesUnavailableNode(layout Layout, available map[string]bool) bool {
	for node := range layout {
		if !available[node] {
			return true
		}
	}
	return false
}

// relocateLayout returns the backup of a layout: the compositions on unavailable nodes move to available nodes
// that the layout does not use yet (one composition per node, as in every layout), in name order; the others stay.
// The compositions keep their replicas, so the backup has the capacity of the layout. It returns nil when there
// are not enough such nodes.
func relocateLayout(layout Layout, available map[string]bool) Layout {
	var free []string
	for node, ok := range available {
		if _, used := layout[node]; ok && !used {
			free = append(free, node)
		}
	}
	sort.Strings(free)

	backup := make(Layout, len(layout))
	for _, node := range sortedNodes(layout) {
		if available[node] {
			backup[node] = layout[node]
			continue
		}
		if len(free) == 0 {
			return nil
		}
		backup[free[0]] = layout[node]
		free = free[1:]
	}
	return backup
}

// sortedNodes returns the nodes of a layout in name order
func sortedNodes(layout Layout) []string {
	nodes := make([]string, 0, len(layout))
	for node := range layout {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	return nodes
}
