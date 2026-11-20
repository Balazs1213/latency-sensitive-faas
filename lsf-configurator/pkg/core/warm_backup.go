package core

import (
	"log"
)

// Backup layout modes: what the controller does when a node of the active layout becomes unavailable
const (
	// BackupOff keeps the layout: the compositions on the node wait for it
	BackupOff = "off"
	// BackupCold deploys the backup layout when a node becomes unavailable: its moved compositions start cold
	BackupCold = "cold"
	// BackupWarm also runs one warm pod of every composition of every layout candidate on the node its backup
	// would move it to, so that the backup layout serves at once and only scales up to the required pods
	BackupWarm = "warm"
)

// backupLayoutMode returns a known backup layout mode: an unknown one falls back to off
func backupLayoutMode(mode string) string {
	switch mode {
	case BackupCold, BackupWarm:
		return mode
	default:
		return BackupOff
	}
}

// warmTarget is a composition of the active layout and the node its backup layout would move it to
type warmTarget struct {
	node string
	ci   CompositionInfo
}

// warmBackups are the warm backup deployments of an app ("fcId@node") and the ones being created
type warmBackups struct {
	keys    map[string]bool
	pending map[string]bool
}

// warmBackupTargets returns, for every node of a layout, the node its compositions would move to if that node
// became unavailable: the backup layout of that failure. A composition whose failure leaves no free node has
// no target.
func warmBackupTargets(layout Layout, available map[string]bool) []warmTarget {
	var targets []warmTarget
	for _, lost := range sortedNodes(layout) {
		without := make(map[string]bool, len(available))
		for node, ok := range available {
			without[node] = ok && node != lost
		}
		backup := relocateLayout(layout, without)
		for _, node := range sortedNodes(backup) {
			if _, inLayout := layout[node]; !inLayout {
				targets = append(targets, warmTarget{node: node, ci: backup[node]})
			}
		}
	}
	return targets
}

// warmState returns the warm backups of an app; the caller holds warmMu
func (c *latencyController) warmState(appID string) *warmBackups {
	st, ok := c.warm[appID]
	if !ok {
		st = &warmBackups{keys: make(map[string]bool), pending: make(map[string]bool)}
		c.warm[appID] = st
	}
	return st
}

// forgetWarmBackups forgets the warm backups of an app, so that the next check sets the existing ones back to one
// pod (after the backup layout served, they run the pods of the active layout)
func (c *latencyController) forgetWarmBackups(appID string) {
	c.warmMu.Lock()
	defer c.warmMu.Unlock()
	delete(c.warm, appID)
}

// warmKeepKeys returns the warm backup deployments of an app ("fcId@node"), which the cleanup after a switch keeps
func (c *latencyController) warmKeepKeys(appID string) map[string]bool {
	c.warmMu.Lock()
	defer c.warmMu.Unlock()
	keys := make(map[string]bool)
	if st, ok := c.warm[appID]; ok {
		for key := range st.keys {
			keys[key] = true
		}
	}
	return keys
}

// ensureWarmBackups keeps one warm pod of every composition of every layout candidate on the node its backup
// layout would move it to (warm mode only), so that the backups are warm whichever layout is active when a node
// fails: it creates the missing ones, sets a deployment that served (after a return from the backup) back to one
// pod, and removes the warm backups of candidates that are gone
func (c *latencyController) ensureWarmBackups(app *FunctionApp, available map[string]bool) {
	if c.backupLayout != BackupWarm {
		return
	}
	fcByKey := compositionsByKey(app)
	existing, _ := indexDeployments(app)
	wanted := make(map[string]bool)

	c.warmMu.Lock()
	st := c.warmState(app.Id)
	for _, t := range allWarmTargets(app, available) {
		fc, ok := fcByKey[compositionKey(t.ci)]
		if !ok {
			continue
		}
		key := fc.Id + "@" + t.node
		wanted[key] = true
		if st.keys[key] || st.pending[key] {
			continue
		}
		if dep, ok := existing[key]; ok {
			st.keys[key] = true
			go c.setWarmMinScale(app.Id, dep)
			continue
		}
		st.pending[key] = true
		go c.createWarmBackup(app.Id, t.node, fc, t.ci, key)
	}
	var stale []string
	for key := range st.keys {
		if !wanted[key] {
			stale = append(stale, key)
			delete(st.keys, key)
		}
	}
	c.warmMu.Unlock()

	if len(stale) > 0 {
		go c.removeStaleWarmBackups(app, stale, existing)
	}
}

// allWarmTargets returns the warm backup targets of every layout candidate (a backup layout has none of its own)
func allWarmTargets(app *FunctionApp, available map[string]bool) []warmTarget {
	var targets []warmTarget
	for _, key := range sortedLayoutKeys(app) {
		if _, isBackup := backedUpLayout(key); !isBackup {
			targets = append(targets, warmBackupTargets(app.LayoutCandidates[key], available)...)
		}
	}
	return targets
}

// createWarmBackup creates a warm backup deployment with one pod; its routing table is set when its backup
// layout becomes active
func (c *latencyController) createWarmBackup(appID, node string, fc *FunctionComposition, ci CompositionInfo, key string) {
	scale := Scale{MinReplicas: 1, MaxReplicas: max(ci.RequiredReplicas, 1), TargetConcurrency: ci.TargetConcurrency}
	resources := Resources{Memory: ci.Memory, CPU: ci.MCPU}
	dep, depChan, err := c.composer.CreateFcDeployment(fc.Id, c.deployNamespace, node, make(RoutingTable), scale, resources)
	if err == nil {
		if r := <-depChan; r.Err != nil {
			err = r.Err
		}
	}

	c.warmMu.Lock()
	defer c.warmMu.Unlock()
	st := c.warmState(appID)
	delete(st.pending, key)
	if err != nil {
		log.Printf("Failed to create the warm backup of fc %s on node %s: %v", fc.Id, node, err)
		return
	}
	st.keys[key] = true
	logPhase(appID, "warm_backup_ready", "", dep.Id, 0)
	log.Printf("Warm backup %s of fc %s ready on node %s", dep.Id, fc.Id, node)
}

// setWarmMinScale sets a deployment that serves as a warm backup to one pod
func (c *latencyController) setWarmMinScale(appID string, dep *Deployment) {
	if err := c.composer.SetDeploymentMinScale(dep, 1); err != nil {
		log.Printf("Failed to set the warm backup %s of app %s to one pod: %v", dep.Id, appID, err)
	}
}

// removeWarmLeftovers deletes at start-up, when the backup layout is not warm, the deployments that no layout
// candidate uses: the warm backups of an earlier warm mode (with prewarm off, removeInactiveDeployments does it)
func (c *latencyController) removeWarmLeftovers(appID string) {
	if c.backupLayout == BackupWarm {
		return
	}
	// Reload the app: compositions and their deployments are only loaded by GetFunctionApp
	app, err := c.composer.GetFunctionApp(appID)
	if err != nil || app == nil {
		return
	}
	keep := c.layoutKeepKeys(app, compositionsByKey(app), true)
	for _, fc := range app.Compositions {
		for _, d := range fc.Deployments {
			if !keep[fc.Id+"@"+d.Node] {
				c.deleteDeployment(app.Id, fc, d)
			}
		}
	}
}

// removeStaleWarmBackups deletes the warm backups of candidates that are gone, unless a layout still uses them
func (c *latencyController) removeStaleWarmBackups(app *FunctionApp, stale []string, existing map[string]*Deployment) {
	inUse := c.layoutKeepKeys(app, compositionsByKey(app), true)
	for _, key := range stale {
		dep, ok := existing[key]
		if !ok || inUse[key] {
			continue
		}
		if fc := getFunctionComposition(app, dep.FunctionCompositionId); fc != nil {
			c.deleteDeployment(app.Id, fc, dep)
		}
	}
}
