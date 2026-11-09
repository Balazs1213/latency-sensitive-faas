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

	// Build a fast lookup: compositionKey -> FunctionComposition
	fcByKey := make(map[string]*FunctionComposition)
	for _, fc := range app.Compositions {
		fcByKey[componentsKey(fc.Components)] = fc
	}

	// Build a map of currently active deployments keyed by fcId@node
	// We are going to modify this map as we reuse/create deployments
	activeDepsByKey := make(map[string]*Deployment)
	oldCompToDepID := make(map[string]string) // component -> deployment id mapping from old layout, used as fallback
	for _, fc := range app.Compositions {
		for _, d := range fc.Deployments {
			k := fc.Id + "@" + d.Node
			activeDepsByKey[k] = d
			for _, comp := range fc.Components {
				// Snapshot old component -> deployment id mapping (fallback)
				if _, ok := oldCompToDepID[comp]; !ok {
					oldCompToDepID[comp] = d.Id
				}
			}
		}
	}

	// single-pass creation/reuse and build comp -> dep mapping
	activeDepIDs := make(map[string]bool)  // set of active dep ids
	compToDepID := make(map[string]string) // component -> deployment id

	type depResult struct {
		key string
		dep *Deployment
		err error
	}

	resultChan := make(chan depResult, len(layout))

	for node, compositionInfo := range layout {
		// process each node+components in parallel
		go func(node string, compositionInfo CompositionInfo) {
			components := compositionInfo.ComponentProfiles

			componentNames := make([]string, len(components))
			for i, cp := range components {
				componentNames[i] = cp.Name
			}
			fcKey := componentsKey(componentNames)
			matchedFc, ok := fcByKey[fcKey]
			if !ok {
				resultChan <- depResult{"", nil, fmt.Errorf("no matching function composition for components: %v", components)}
				return
			}

			depKey := matchedFc.Id + "@" + node
			if reuseFunctions {
				if dep, ok := activeDepsByKey[depKey]; ok {
					// if a deployment already exists for this fc+node, reuse it
					log.Printf("Reusing existing deployment %s for node %s", dep.Id, node)
					logPhase(appId, "deployment_reused", "", dep.Id, 0)
					// A reused deployment keeps its old min-scale: set it to the required replicas when it becomes
					// active (upgrade, or any switch with prewarm) and to the standby level for standby layouts.
					// In follow mode an activated layout keeps its pods: they already follow the rate.
					followsRate := c.prewarmMode == "follow" && !standby
					if !followsRate && (isUpgrade || standby || c.prewarmMode != "off") {
						minScale := compositionInfo.RequiredReplicas
						if standby && c.prewarmMode != "full" {
							minScale = 1
						}
						// Run in the background: the min-scale does not affect routing, so it must not delay the switch
						go func(d *Deployment, n int) {
							if err := c.composer.SetDeploymentMinScale(d, n); err != nil {
								log.Printf("Failed to set min-scale of reused deployment %s: %v", d.Id, err)
							}
						}(dep, minScale)
					}
					resultChan <- depResult{depKey, dep, nil}
					return
				}
			}

			// otherwise, create a new deployment, routing table will be set later once all deployments ids are known
			log.Printf("No existing deployment found for node %s, creating new", node)

			emptyRT := make(RoutingTable)
			minReplicas := 1
			// All required replicas warm: on upgrades, for "full" standby layouts, and (with prewarm)
			// for the active layout, so it runs at the capacity the model planned for
			if isUpgrade || (standby && c.prewarmMode == "full") || (!standby && c.prewarmMode != "off") {
				minReplicas = compositionInfo.RequiredReplicas
			}

			scale := Scale{
				MinReplicas:       minReplicas,
				MaxReplicas:       compositionInfo.RequiredReplicas,
				TargetConcurrency: compositionInfo.TargetConcurrency,
			}
			resources := Resources{
				Memory: compositionInfo.Memory,
				CPU:    compositionInfo.MCPU,
			}
			newDep, depChan, err := c.composer.CreateFcDeployment(matchedFc.Id, c.deployNamespace, node, emptyRT, scale, resources)
			if err != nil {
				resultChan <- depResult{"", nil, fmt.Errorf("failed to create deployment for fc %s on node %s: %w", matchedFc.Id, node, err)}
				return
			}

			logPhase(appId, "deployment_created", "", newDep.Id, 0)

			r := <-depChan
			if r.Err != nil {
				resultChan <- depResult{"", nil, fmt.Errorf("deployment task failed for fc %s on node %s: %w", matchedFc.Id, node, r.Err)}
				return
			}

			logPhase(appId, "deployment_ready", "", newDep.Id, 0)

			matchedFc.Deployments = append(matchedFc.Deployments, newDep) // add to fc's deployments
			log.Printf("Created new deployment %s for fc %s on node %s", newDep.Id, matchedFc.Id, node)
			resultChan <- depResult{depKey, newDep, nil}
		}(node, compositionInfo)
	}

	// collect results and update the active deployment mapping
	for i := 0; i < len(layout); i++ {
		res := <-resultChan
		if res.err != nil {
			return res.err
		}
		activeDepsByKey[res.key] = res.dep
		activeDepIDs[res.dep.Id] = true
		fc := getFunctionComposition(app, res.dep.FunctionCompositionId)
		if fc == nil {
			panic(fmt.Sprintf("unexpected error: missing function composition %s for deployment %s", res.dep.FunctionCompositionId, res.dep.Id))
		}
		for _, comp := range fc.Components {
			compToDepID[comp] = res.dep.Id
		}
	}

	// build and apply routing tables
	referencedDepIDs := make(map[string]bool)
	for node, compositionInfo := range layout {
		comps := compositionInfo.ComponentProfiles

		componentNames := make([]string, len(comps))
		for i, cp := range comps {
			componentNames[i] = cp.Name
		}

		fcKey := componentsKey(componentNames)
		matchedFc := fcByKey[fcKey]
		depKey := matchedFc.Id + "@" + node
		dep := activeDepsByKey[depKey]

		if dep == nil {
			panic(fmt.Sprintf("unexpected error: missing deployment for key %s", depKey))
		}

		rt := make(RoutingTable)
		for _, comp := range componentNames {
			var routes []Route
			for _, link := range app.Links {
				if link.From != comp {
					continue
				}
				if targetDepID, ok := compToDepID[link.To]; ok {
					// If the target deployment is the same as the current deployment, set Function to "local"
					functionField := targetDepID
					if dep.Id == targetDepID {
						functionField = "local"
					}
					routes = append(routes, Route{To: link.To, Function: functionField})
					referencedDepIDs[targetDepID] = true
					continue
				}
				// fallback: use old deployment if available
				if oldDepID, ok := oldCompToDepID[link.To]; ok {
					functionField := oldDepID
					if dep.Id == oldDepID {
						functionField = "local"
					}
					routes = append(routes, Route{To: link.To, Function: functionField})
					referencedDepIDs[oldDepID] = true
					log.Printf("Warning: using fallback routing for component %s to old deployment %s", link.To, oldDepID)
					continue
				}
				panic(fmt.Sprintf("no new or fallback deployment available for component %s", link.To))
			}
			rt[comp] = routes
		}

		err = c.composer.SetRoutingTable(dep.Id, rt)
		if err != nil {
			return fmt.Errorf("failed to set routing table for deployment %s: %w", dep.Id, err)
		}
		log.Printf("Set routing table for deployment %s: %v", dep.Id, rt)
	}

	// Standby layout: deployments and routing tables are ready, but the entry point is not switched
	if standby {
		logPhase(appId, "standby_ready", "", "", 0)
		return nil
	}

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
	logPhase(appId, "dns_updated", "", firstDepID, 0)

	c.logReconfigComplete(appId)

	// Cleanup: remove the deployments that are neither part of the new layout, nor routed to, nor kept warm
	keepKeys := c.prewarmKeepKeys(app, fcByKey)
	go c.removeUnusedDeployments(app, func(fc *FunctionComposition, d *Deployment) bool {
		return activeDepIDs[d.Id] || referencedDepIDs[d.Id] || keepKeys[fc.Id+"@"+d.Node]
	})

	return nil
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
	names := make([]string, len(ci.ComponentProfiles))
	for i, cp := range ci.ComponentProfiles {
		names[i] = cp.Name
	}
	return componentsKey(names)
}
