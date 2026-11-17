package core

import (
	"log"
	"sort"
	"strings"
	"time"
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

// followNodeAvailability moves the apps off unavailable nodes to a backup layout and back once the nodes are
// available again, once per metric window; only with a backup layout mode
func (c *latencyController) followNodeAvailability(apps []*FunctionApp) {
	now := time.Now()
	if c.backupLayout == "off" || now.Sub(c.lastNodeCheck) < c.metricWindow {
		return
	}
	c.lastNodeCheck = now
	available, err := c.composer.NodeAvailability()
	if err != nil {
		log.Printf("Failed to read the node availability: %v", err)
		return
	}
	for _, a := range apps {
		app := c.controlledApp(a.Id)
		if app == nil || c.inCooldown(app.Id) {
			continue
		}
		if original, isBackup := backedUpLayout(app.ActiveLayoutKey); isBackup {
			c.returnFromBackup(app, original, available)
		} else if usesUnavailableNode(app.LayoutCandidates[app.ActiveLayoutKey], available) {
			c.switchToBackup(app, available)
		}
	}
}

// switchToBackup adds the backup of the active layout as a layout candidate and switches to it: its deployments
// on the new nodes are created now (cold), and the traffic moves once they are ready
func (c *latencyController) switchToBackup(app *FunctionApp, available map[string]bool) {
	backup := relocateLayout(app.LayoutCandidates[app.ActiveLayoutKey], available)
	if backup == nil {
		log.Printf("Active layout %s of app %s uses an unavailable node, but no free node is available for a backup",
			app.ActiveLayoutKey, app.Id)
		return
	}
	key := backupKey(app.ActiveLayoutKey)
	app.LayoutCandidates[key] = backup
	logPhase(app.Id, "backup", key, "", 0)
	log.Printf("Active layout %s of app %s uses an unavailable node, switching to the backup layout %v",
		app.ActiveLayoutKey, app.Id, sortedNodes(backup))
	c.switchTo(app, key)
}

// returnFromBackup switches back to the layout a backup stands in for once its nodes are available again; the
// backup leaves the candidates, so the cleanup after the switch removes its deployments
func (c *latencyController) returnFromBackup(app *FunctionApp, original string, available map[string]bool) {
	if usesUnavailableNode(app.LayoutCandidates[original], available) {
		return
	}
	delete(app.LayoutCandidates, app.ActiveLayoutKey)
	logPhase(app.Id, "backup_return", original, "", 0)
	log.Printf("Nodes of layout %s of app %s are available again, switching back from the backup", original, app.Id)
	c.switchTo(app, original)
}

// switchTo switches an app to a given layout candidate with all required replicas
func (c *latencyController) switchTo(app *FunctionApp, layoutKey string) {
	c.applyReconfig(app, func(a *FunctionApp) (string, error) {
		return c.handleLayoutChange(a, map[string]string{a.ActiveLayoutKey: layoutKey}, true)
	})
}
