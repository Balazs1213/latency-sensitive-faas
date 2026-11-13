package core

import (
	"log"
	"time"
)

// compositionReadyPods returns how many pods of a layout composition's deployments on a node are ready
func (c *latencyController) compositionReadyPods(app *FunctionApp, ci CompositionInfo, node string) int {
	key := compositionKey(ci)
	ready := 0
	for _, fc := range app.Compositions {
		if componentsKey(fc.Components) != key {
			continue
		}
		for _, d := range fc.Deployments {
			if d.Node != node {
				continue
			}
			n, err := c.composer.DeploymentReadyPods(d)
			if err != nil {
				log.Printf("Failed to read the ready pods of deployment %s: %v", d.Id, err)
				continue
			}
			ready += n
		}
	}
	return ready
}

// upgradeReady reports whether the next larger layout can take the current rate right away. In follow mode its
// standby pods follow the rate, so each of its compositions must have at least the pods the rate needs ready;
// the other modes keep their standby at a fixed level and are not gated.
func (c *latencyController) upgradeReady(app *FunctionApp, rate float64) bool {
	if c.prewarmMode != "follow" {
		return true
	}
	next := layoutUpgradePath[app.ActiveLayoutKey]
	for node, ci := range app.LayoutCandidates[next] {
		need := c.podsForRate(app.Id, ci, rate)
		ready := c.compositionReadyPods(app, ci, node)
		if ready < need {
			key := compositionKey(ci) + "@" + node
			log.Printf("Switch to %s waits for %s: %d of %d pods ready", next, key, ready, need)
			logPhase(app.Id, "switch_wait", next, key, float64(ready))
			return false
		}
	}
	return true
}

// startupWatch is a raised min-scale whose new pods are not ready yet
type startupWatch struct {
	since  time.Time
	target int
	ci     CompositionInfo
	node   string
}

// watchStartup starts a pod startup measurement when a min-scale is raised above the pods ready now
func (c *latencyController) watchStartup(app *FunctionApp, key string, ci CompositionInfo, node string, target int, now time.Time) {
	if len(app.Compositions) == 0 {
		return // deployments not loaded: the ready pods cannot be read
	}
	if c.compositionReadyPods(app, ci, node) >= target {
		return // the pods already run (e.g. started by Knative itself): nothing to measure
	}
	if c.startupPending[app.Id] == nil {
		c.startupPending[app.Id] = make(map[string]startupWatch)
	}
	c.startupPending[app.Id][key] = startupWatch{since: now, target: target, ci: ci, node: node}
}

// observeStartups completes the pending startup measurements whose pods became ready and keeps the mean pod
// startup time; without loaded deployments the pending measurements are dropped, as they could not be timed
func (c *latencyController) observeStartups(app *FunctionApp, now time.Time) {
	pending := c.startupPending[app.Id]
	if len(pending) == 0 {
		return
	}
	if len(app.Compositions) == 0 {
		delete(c.startupPending, app.Id)
		return
	}
	for key, w := range pending {
		if c.compositionReadyPods(app, w.ci, w.node) < w.target {
			continue
		}
		d := now.Sub(w.since)
		c.startupMean = (c.startupMean*time.Duration(c.startupSamples) + d) / time.Duration(c.startupSamples+1)
		c.startupSamples++
		delete(pending, key)
		log.Printf("Pod startup of %s: %.1f s to %d ready pods (mean %.1f s over %d)",
			key, d.Seconds(), w.target, c.startupMean.Seconds(), c.startupSamples)
		logPhase(app.Id, "pod_startup", app.ActiveLayoutKey, key, d.Seconds())
	}
}

// scaleDownHold is how long a pod the rate no longer needs is kept. By the ski rental rule it is kept as long as
// starting it again takes, the measured mean pod startup time; until that is measured, the fixed fallback.
func (c *latencyController) scaleDownHold() time.Duration {
	if c.startupSamples > 0 {
		return c.startupMean
	}
	return rateScaleDownHold
}

// allCompositionsReady reports whether every composition of a layout has at least one ready pod on its node. An
// empty (unknown) layout cannot serve, so it is not ready.
func allCompositionsReady(layout Layout, readyPods func(node string, ci CompositionInfo) int) bool {
	if len(layout) == 0 {
		return false
	}
	for node, ci := range layout {
		if readyPods(node, ci) == 0 {
			return false
		}
	}
	return true
}

// failoverTarget picks the layout an app switches to when its active layout cannot serve: the first other layout
// candidate (in key order) that can serve, or "" when there is none
func failoverTarget(app *FunctionApp, serving func(key string) bool) string {
	for _, key := range sortedLayoutKeys(app) {
		if key != app.ActiveLayoutKey && serving(key) {
			return key
		}
	}
	return ""
}
