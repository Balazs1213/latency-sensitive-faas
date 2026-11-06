package core

import (
	"log"
	"time"
)

// ensureStandbyLayouts deploys every inactive layout candidate of the app without traffic,
// so that a later switch only needs rerouting (no pod startup)
func (c *latencyController) ensureStandbyLayouts(app *FunctionApp) {
	if c.prewarmMode == "off" {
		return
	}
	// The active layout runs with all required replicas (it may have been deployed with one pod)
	c.setLayoutMinScale(app, app.ActiveLayoutKey, true)
	for key, layout := range app.LayoutCandidates {
		if key == app.ActiveLayoutKey {
			continue
		}
		logPhase(app.Id, "standby_deploy", key, "", 0)
		if err := c.deployLayout(app.Id, layout, false, true, true); err != nil {
			log.Printf("Failed to deploy standby layout %s for app %s: %v", key, app.Id, err)
		}
	}
}

// setLayoutMinScale sets the min-scale of every deployment of a layout candidate:
// all required replicas when boosted, one warm pod otherwise
func (c *latencyController) setLayoutMinScale(app *FunctionApp, layoutKey string, boost bool) {
	// Reload the app: compositions and their deployments are only loaded by GetFunctionApp
	fresh, err := c.composer.GetFunctionApp(app.Id)
	if err != nil || fresh == nil {
		log.Printf("Failed to load app %s for min-scale change: %v", app.Id, err)
		return
	}
	app = fresh
	layout, ok := app.LayoutCandidates[layoutKey]
	if !ok {
		return
	}
	fcByKey := make(map[string]*FunctionComposition)
	for _, fc := range app.Compositions {
		fcByKey[componentsKey(fc.Components)] = fc
	}
	for node, ci := range layout {
		names := make([]string, len(ci.ComponentProfiles))
		for i, cp := range ci.ComponentProfiles {
			names[i] = cp.Name
		}
		fc, ok := fcByKey[componentsKey(names)]
		if !ok {
			continue
		}
		minScale := 1
		if boost {
			minScale = ci.RequiredReplicas
		}
		for _, d := range fc.Deployments {
			if d.Node != node {
				continue
			}
			if err := c.composer.SetDeploymentMinScale(d, minScale); err != nil {
				log.Printf("Failed to set min-scale of deployment %s: %v", d.Id, err)
				continue
			}
			log.Printf("Set min-scale of deployment %s (layout %s) to %d", d.Id, layoutKey, minScale)
		}
	}
}

// adaptStandby boosts the standby of the next larger layout when the arrival rate gets close to the
// capacity of the active layout, and scales it back to one warm pod after a quiet period
func (c *latencyController) adaptStandby(app *FunctionApp, traceCount int) {
	next := layoutUpgradePath[app.ActiveLayoutKey]
	if next == "" {
		return
	}
	rate := float64(traceCount) / c.metricWindow.Seconds()
	threshold := c.boostRateFactor * c.layoutCapacity(app.Id, app.LayoutCandidates[app.ActiveLayoutKey])
	now := time.Now()

	if rate > threshold {
		c.lastAboveBoost[app.Id] = now
		if _, boosted := c.boostedSince[app.Id]; !boosted {
			c.boostedSince[app.Id] = now
			logPhase(app.Id, "standby_boost", next, "", rate)
			go c.setLayoutMinScale(app, next, true)
		}
		return
	}

	since, boosted := c.boostedSince[app.Id]
	if !boosted {
		return
	}
	last := c.lastAboveBoost[app.Id]
	if last.Before(since) {
		last = since
	}
	if now.Sub(last) >= unboostQuietPeriod {
		delete(c.boostedSince, app.Id)
		logPhase(app.Id, "standby_unboost", next, "", rate)
		go c.setLayoutMinScale(app, next, false)
	}
}
