package core

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
)

type ReconfigEvent struct {
	EventType  string  `json:"event_type"`
	AppID      string  `json:"app_id"`
	EventTime  int64   `json:"event_time"` // Unix timestamp in milliseconds
	DurationMs float64 `json:"duration_ms"`
}

func (c *latencyController) handleLatencyViolation(app *FunctionApp) (string, error) {
	//log.Printf("App %s exceeds latency threshold (%d ms). Triggering reconfiguration.", app.Id, app.LatencyLimit)
	return c.handleLayoutChange(app, layoutUpgradePath, true)
}

func (c *latencyController) handleLayoutDowngrade(app *FunctionApp) (string, error) {
	//log.Printf("App %s is below latency threshold. Considering layout downgrade.", app.Id)
	return c.handleLayoutChange(app, layoutDowngradePath, false)
}

func (c *latencyController) handleLayoutChange(app *FunctionApp, path map[string]string, isUpgrade bool) (string, error) {
	nextLayoutKey := path[app.ActiveLayoutKey]
	if nextLayoutKey == "" {
		// No further layout candidates available
		return "", nil
	}

	nextLayout, ok := app.LayoutCandidates[nextLayoutKey]
	if !ok {
		return "", fmt.Errorf("no layout candidate found for key %s in app %s", nextLayoutKey, app.Id)
	}

	app.ActiveLayoutKey = nextLayoutKey
	if err := c.composer.functionAppRepo.Save(app); err != nil {
		return "", fmt.Errorf("failed to update active layout key for app %s: %w", app.Id, err)
	}

	// Phase marker: decision taken; the switch duration is measured from here
	c.lastReconfigsMu.Lock()
	c.reconfigStartTimes[app.Id] = time.Now()
	c.lastReconfigsMu.Unlock()
	logPhase(app.Id, "decision", nextLayoutKey, "", 0)

	// Prewarm: an upgrade consumes the boost; after a downgrade the layout just left still runs all
	// required replicas, so it goes back to the standby level (adaptive: after the quiet period)
	switch {
	case c.prewarmMode == "adaptive" && isUpgrade:
		delete(c.boostedSince, app.Id)
	case c.prewarmMode == "adaptive":
		c.boostedSince[app.Id] = time.Now()
	case c.prewarmMode == "one" && !isUpgrade:
		go c.setLayoutMinScale(app, layoutUpgradePath[nextLayoutKey], false)
	}

	go func() {
		if err := c.deployLayout(app.Id, nextLayout, isUpgrade, reuseDeployments, false); err != nil {
			log.Printf("Failed to deploy new layout for app %s: %v", app.Id, err)
		}
	}()

	log.Printf("App %s successfully transitioned to layout %s", app.Id, nextLayoutKey)
	return nextLayoutKey, nil
}

func (c *latencyController) deployLayout(appId string, layout Layout, isUpgrade bool, reuseFunctions bool, standby bool) error {
	log.Printf("Deploying layout for app %s: %v", appId, layout)

	app, err := c.composer.GetFunctionApp(appId)
	if err != nil {
		return err
	}
	fcByKey := compositionsByKey(app)
	oldDepsByKey, oldCompToDepID := indexDeployments(app)

	deps, err := c.deployCompositions(app, layout, fcByKey, oldDepsByKey, isUpgrade, reuseFunctions, standby)
	if err != nil {
		return err
	}
	referencedDepIDs, err := c.applyRoutingTables(app, layout, fcByKey, deps, oldCompToDepID)
	if err != nil {
		return err
	}

	// Standby layout: deployments and routing tables are ready, but the entry point is not switched
	if standby {
		logPhase(appId, "standby_ready", "", "", 0)
		return nil
	}
	if err := c.switchEntryPoint(app, deps.compToDepID); err != nil {
		return err
	}
	c.logReconfigComplete(appId)

	// Cleanup: remove the deployments that are neither part of the new layout, nor routed to, nor kept warm
	keepKeys := c.prewarmKeepKeys(app, fcByKey)
	go c.removeUnusedDeployments(app, func(fc *FunctionComposition, d *Deployment) bool {
		return deps.ids[d.Id] || referencedDepIDs[d.Id] || keepKeys[fc.Id+"@"+d.Node]
	})

	return nil
}

// layoutDeployments are the deployments that serve a layout
type layoutDeployments struct {
	byKey       map[string]*Deployment // fcId@node -> deployment
	ids         map[string]bool        // ids of the deployments
	compToDepID map[string]string      // component -> id of the deployment that serves it
}

// compositionsByKey indexes the function compositions of the app by their components key
func compositionsByKey(app *FunctionApp) map[string]*FunctionComposition {
	fcByKey := make(map[string]*FunctionComposition)
	for _, fc := range app.Compositions {
		fcByKey[componentsKey(fc.Components)] = fc
	}
	return fcByKey
}

// indexDeployments indexes the current deployments of the app by fcId@node and maps every component to a
// deployment that serves it: the fallback route for a component that the new layout does not serve
func indexDeployments(app *FunctionApp) (map[string]*Deployment, map[string]string) {
	depsByKey := make(map[string]*Deployment)
	compToDepID := make(map[string]string)
	for _, fc := range app.Compositions {
		for _, d := range fc.Deployments {
			depsByKey[fc.Id+"@"+d.Node] = d
			for _, comp := range fc.Components {
				if _, ok := compToDepID[comp]; !ok {
					compToDepID[comp] = d.Id
				}
			}
		}
	}
	return depsByKey, compToDepID
}

// deployCompositions reuses or creates the deployment of every composition of the layout, the compositions in
// parallel, and collects the deployments
func (c *latencyController) deployCompositions(app *FunctionApp, layout Layout, fcByKey map[string]*FunctionComposition,
	oldDepsByKey map[string]*Deployment, isUpgrade, reuseFunctions, standby bool) (layoutDeployments, error) {
	type depResult struct {
		key string
		dep *Deployment
		err error
	}
	resultChan := make(chan depResult, len(layout))
	for node, compositionInfo := range layout {
		go func(node string, compositionInfo CompositionInfo) {
			key, dep, err := c.ensureDeployment(app.Id, node, compositionInfo, fcByKey, oldDepsByKey, isUpgrade, reuseFunctions, standby)
			resultChan <- depResult{key, dep, err}
		}(node, compositionInfo)
	}

	deps := layoutDeployments{
		byKey:       make(map[string]*Deployment),
		ids:         make(map[string]bool),
		compToDepID: make(map[string]string),
	}
	for i := 0; i < len(layout); i++ {
		res := <-resultChan
		if res.err != nil {
			return deps, res.err
		}
		deps.byKey[res.key] = res.dep
		deps.ids[res.dep.Id] = true
		fc := getFunctionComposition(app, res.dep.FunctionCompositionId)
		if fc == nil {
			panic(fmt.Sprintf("unexpected error: missing function composition %s for deployment %s", res.dep.FunctionCompositionId, res.dep.Id))
		}
		for _, comp := range fc.Components {
			deps.compToDepID[comp] = res.dep.Id
		}
	}
	return deps, nil
}

// applyRoutingTables sets the routing table of every deployment of the layout and returns the deployments
// that the tables route to
func (c *latencyController) applyRoutingTables(app *FunctionApp, layout Layout, fcByKey map[string]*FunctionComposition,
	deps layoutDeployments, oldCompToDepID map[string]string) (map[string]bool, error) {
	referenced := make(map[string]bool)
	for node, compositionInfo := range layout {
		componentNames := profileNames(compositionInfo)
		matchedFc := fcByKey[componentsKey(componentNames)]
		depKey := matchedFc.Id + "@" + node
		dep := deps.byKey[depKey]
		if dep == nil {
			panic(fmt.Sprintf("unexpected error: missing deployment for key %s", depKey))
		}

		rt := routingTable(app, dep.Id, componentNames, deps.compToDepID, oldCompToDepID, referenced)
		if err := c.composer.SetRoutingTable(dep.Id, rt); err != nil {
			return nil, fmt.Errorf("failed to set routing table for deployment %s: %w", dep.Id, err)
		}
		log.Printf("Set routing table for deployment %s: %v", dep.Id, rt)
	}
	return referenced, nil
}

// switchEntryPoint points the entry point of the app (its DNS record) to the deployment of its first component
func (c *latencyController) switchEntryPoint(app *FunctionApp, compToDepID map[string]string) error {
	//TODO: this list is not necessarily ordered, but its fine for now
	firstComponent := app.Components[0].Name
	firstDepID, ok := compToDepID[firstComponent]
	if !ok {
		return fmt.Errorf("no deployment found for first component %s", firstComponent)
	}
	if err := c.composer.UpdateDNSRecord(app.Id, c.deployNamespace, firstDepID); err != nil {
		return fmt.Errorf("failed to update DNS record for app %s: %w", app.Id, err)
	}
	log.Printf("Updated DNS record for app %s to deployment %s (first component: %s)", app.Id, firstDepID, firstComponent)
	logPhase(app.Id, "dns_updated", "", firstDepID, 0)
	return nil
}

// ensureDeployment returns the deployment (and its fcId@node key) that serves a layout composition on a node:
// the existing one when deployments are reused, otherwise a newly created one
func (c *latencyController) ensureDeployment(appId, node string, ci CompositionInfo, fcByKey map[string]*FunctionComposition,
	activeDepsByKey map[string]*Deployment, isUpgrade, reuseFunctions, standby bool) (string, *Deployment, error) {
	matchedFc, ok := fcByKey[compositionKey(ci)]
	if !ok {
		return "", nil, fmt.Errorf("no matching function composition for components: %v", ci.ComponentProfiles)
	}
	depKey := matchedFc.Id + "@" + node
	if reuseFunctions {
		if dep, ok := activeDepsByKey[depKey]; ok {
			c.reuseDeployment(appId, node, dep, ci, isUpgrade, standby)
			return depKey, dep, nil
		}
	}
	dep, err := c.createDeployment(appId, node, matchedFc, ci, isUpgrade, standby)
	if err != nil {
		return "", nil, err
	}
	return depKey, dep, nil
}

// reuseDeployment reuses the existing deployment of a composition on a node. A reused deployment keeps its old
// min-scale: it is set to the required replicas when it becomes active (upgrade, or any switch with prewarm) and
// to the standby level for standby layouts. In follow mode an activated layout keeps its pods: they already
// follow the rate.
func (c *latencyController) reuseDeployment(appId, node string, dep *Deployment, ci CompositionInfo, isUpgrade, standby bool) {
	log.Printf("Reusing existing deployment %s for node %s", dep.Id, node)
	logPhase(appId, "deployment_reused", "", dep.Id, 0)
	followsRate := c.prewarmMode == "follow" && !standby
	if followsRate || (!isUpgrade && !standby && c.prewarmMode == "off") {
		return
	}
	minScale := ci.RequiredReplicas
	if standby && c.prewarmMode != "full" {
		minScale = 1
	}
	// Run in the background: the min-scale does not affect routing, so it must not delay the switch
	go func() {
		if err := c.composer.SetDeploymentMinScale(dep, minScale); err != nil {
			log.Printf("Failed to set min-scale of reused deployment %s: %v", dep.Id, err)
		}
	}()
}

// createDeployment creates a new deployment of a composition on a node and waits until it is ready. Its routing
// table is set later, once the ids of all deployments of the layout are known.
func (c *latencyController) createDeployment(appId, node string, fc *FunctionComposition, ci CompositionInfo, isUpgrade, standby bool) (*Deployment, error) {
	log.Printf("No existing deployment found for node %s, creating new", node)
	scale := Scale{
		MinReplicas:       c.initialMinReplicas(ci, isUpgrade, standby),
		MaxReplicas:       ci.RequiredReplicas,
		TargetConcurrency: ci.TargetConcurrency,
	}
	resources := Resources{
		Memory: ci.Memory,
		CPU:    ci.MCPU,
	}
	newDep, depChan, err := c.composer.CreateFcDeployment(fc.Id, c.deployNamespace, node, make(RoutingTable), scale, resources)
	if err != nil {
		return nil, fmt.Errorf("failed to create deployment for fc %s on node %s: %w", fc.Id, node, err)
	}
	logPhase(appId, "deployment_created", "", newDep.Id, 0)

	if r := <-depChan; r.Err != nil {
		return nil, fmt.Errorf("deployment task failed for fc %s on node %s: %w", fc.Id, node, r.Err)
	}
	logPhase(appId, "deployment_ready", "", newDep.Id, 0)

	fc.Deployments = append(fc.Deployments, newDep) // add to fc's deployments
	log.Printf("Created new deployment %s for fc %s on node %s", newDep.Id, fc.Id, node)
	return newDep, nil
}

// initialMinReplicas is the min-scale of a new deployment: all required replicas warm on upgrades, for "full"
// standby layouts and (with prewarm) for the active layout, so that it runs at the capacity the model planned
// for; one pod otherwise
func (c *latencyController) initialMinReplicas(ci CompositionInfo, isUpgrade, standby bool) int {
	if isUpgrade || (standby && c.prewarmMode == "full") || (!standby && c.prewarmMode != "off") {
		return ci.RequiredReplicas
	}
	return 1
}

// routingTable builds the routing table of a deployment: for every link of its components the deployment that
// serves the link's target ("local" when it is the deployment itself). The deployments routed to are added to
// referenced, so that the cleanup keeps them.
func routingTable(app *FunctionApp, depID string, componentNames []string, compToDepID, oldCompToDepID map[string]string, referenced map[string]bool) RoutingTable {
	rt := make(RoutingTable)
	for _, comp := range componentNames {
		var routes []Route
		for _, link := range app.Links {
			if link.From != comp {
				continue
			}
			targetDepID := routeTarget(link.To, compToDepID, oldCompToDepID)
			functionField := targetDepID
			if depID == targetDepID {
				functionField = "local"
			}
			routes = append(routes, Route{To: link.To, Function: functionField})
			referenced[targetDepID] = true
		}
		rt[comp] = routes
	}
	return rt
}

// routeTarget returns the deployment that serves a component: its deployment in the new layout or, as a
// fallback, its deployment in the old layout
func routeTarget(comp string, compToDepID, oldCompToDepID map[string]string) string {
	if depID, ok := compToDepID[comp]; ok {
		return depID
	}
	if oldDepID, ok := oldCompToDepID[comp]; ok {
		log.Printf("Warning: using fallback routing for component %s to old deployment %s", comp, oldDepID)
		return oldDepID
	}
	panic(fmt.Sprintf("no new or fallback deployment available for component %s", comp))
}

// logReconfigComplete logs the end-to-end duration of the app's switch, measured from the decision
func (c *latencyController) logReconfigComplete(appId string) {
	c.lastReconfigsMu.Lock()
	defer c.lastReconfigsMu.Unlock()
	startTime, ok := c.reconfigStartTimes[appId]
	if !ok {
		log.Printf("Warning: Missing start time for app %s in reconfigStartTime map.", appId)
		return
	}
	event := ReconfigEvent{
		EventType:  "RECONFIG_COMPLETE",
		AppID:      appId,
		EventTime:  startTime.UnixMilli(),
		DurationMs: float64(time.Since(startTime)) / float64(time.Millisecond),
	}
	jsonBytes, err := json.Marshal(event)
	if err != nil {
		log.Printf("Error marshalling reconfig event: %v", err)
	} else {
		log.Printf("[RECONFIG_EVENT] %s", string(jsonBytes))
	}
	delete(c.reconfigStartTimes, appId)
}

// prewarmKeepKeys returns the deployments (fcId@node) to keep after a switch: with prewarm every deployment
// of any layout candidate, so that the previously active layout stays warm for the next switch
func (c *latencyController) prewarmKeepKeys(app *FunctionApp, fcByKey map[string]*FunctionComposition) map[string]bool {
	keepKeys := make(map[string]bool)
	if c.prewarmMode == "off" {
		return keepKeys
	}
	for _, candidate := range app.LayoutCandidates {
		for node, ci := range candidate {
			if fc, ok := fcByKey[compositionKey(ci)]; ok {
				keepKeys[fc.Id+"@"+node] = true
			}
		}
	}
	return keepKeys
}

// removeUnusedDeployments deletes, after a grace period, every deployment of the app that keep does not keep
func (c *latencyController) removeUnusedDeployments(app *FunctionApp, keep func(*FunctionComposition, *Deployment) bool) {
	time.Sleep(10 * time.Second)
	for _, fc := range app.Compositions {
		var kept []*Deployment
		for _, d := range fc.Deployments {
			if keep(fc, d) || !c.deleteDeployment(app.Id, fc, d) {
				kept = append(kept, d)
			}
		}
		fc.Deployments = kept
	}
}

// deleteDeployment deletes an old deployment of a composition and reports whether it is gone
func (c *latencyController) deleteDeployment(appId string, fc *FunctionComposition, d *Deployment) bool {
	log.Printf("Deleting old deployment %s (fc %s on node %s)", d.Id, fc.Id, d.Node)
	delChan, err := c.composer.DeleteFcDeployment(d.Id)
	if err != nil {
		log.Printf("Failed to delete deployment %s: %v", d.Id, err)
		return false
	}
	if r := <-delChan; r.Err != nil {
		log.Printf("Failed to delete deployment %s: %v", d.Id, r.Err)
		return false
	}
	log.Printf("Deleted deployment %s", d.Id)
	logPhase(appId, "old_deleted", "", d.Id, 0)
	return true
}

func getFunctionComposition(app *FunctionApp, fcId string) *FunctionComposition {
	for _, fc := range app.Compositions {
		if fc.Id == fcId {
			return fc
		}
	}
	return nil
}

func componentsKey(comps []string) string {
	if len(comps) == 0 {
		return ""
	}
	cp := append([]string(nil), comps...)
	sort.Strings(cp)
	return strings.Join(cp, ",")
}

// logPhase writes one machine-readable line per reconfiguration phase.
// The lines are used to split a layout switch into detection and execution time.
func logPhase(appId, phase, layout, deploymentId string, value float64) {
	event := map[string]interface{}{
		"app_id":        appId,
		"phase":         phase,
		"t_ms":          time.Now().UnixMilli(),
		"layout":        layout,
		"deployment_id": deploymentId,
		"value":         value,
	}
	if b, err := json.Marshal(event); err == nil {
		log.Printf("[RECONFIG_PHASE] %s", string(b))
	}
}

// removeInactiveDeployments deletes every deployment of the app that is not part of its active layout
func (c *latencyController) removeInactiveDeployments(appId string) {
	// Reload the app: compositions and their deployments are only loaded by GetFunctionApp
	app, err := c.composer.GetFunctionApp(appId)
	if err != nil || app == nil {
		return
	}

	// Keys of the active layout's deployments: "components@node", the same key the reuse logic uses
	keep := make(map[string]bool)
	for node, ci := range app.LayoutCandidates[app.ActiveLayoutKey] {
		names := make([]string, len(ci.ComponentProfiles))
		for i, cp := range ci.ComponentProfiles {
			names[i] = cp.Name
		}
		keep[componentsKey(names)+"@"+node] = true
	}

	for _, fc := range app.Compositions {
		for _, d := range fc.Deployments {
			if keep[componentsKey(fc.Components)+"@"+d.Node] {
				continue
			}
			// Delete through the framework: routing table, database record and Knative service together
			delChan, err := c.composer.DeleteFcDeployment(d.Id)
			if err != nil {
				log.Printf("Failed to delete inactive deployment %s: %v", d.Id, err)
				continue
			}
			if r := <-delChan; r.Err != nil {
				log.Printf("Failed to delete inactive deployment %s: %v", d.Id, r.Err)
				continue
			}
			log.Printf("Deleted inactive deployment %s (prewarm off)", d.Id)
			logPhase(app.Id, "old_deleted", "", d.Id, 0)
		}
	}
}

// compositionKey is the components key of a layout composition, the same key the deployments are matched by
func compositionKey(ci CompositionInfo) string {
	return componentsKey(profileNames(ci))
}

// profileNames returns the component names of a layout composition
func profileNames(ci CompositionInfo) []string {
	names := make([]string, len(ci.ComponentProfiles))
	for i, cp := range ci.ComponentProfiles {
		names[i] = cp.Name
	}
	return names
}
