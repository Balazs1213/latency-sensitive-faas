package core

import (
	"log"
	"time"
)

// rateFollow is the rate-following state of one app
type rateFollow struct {
	activeKey  string               // the active layout at the last update
	pods       map[string]int       // "components@node" -> min-scale last set by the controller
	lastNeeded map[string]time.Time // "components@node" -> last tick the rate needed at least that many pods
}

// followRate keeps the min-scale of the followed layouts' compositions at the pods the measured rate needs
// (the replica formula with the measured rate): raised at once, lowered only after the scale-down hold.
// Follow mode only: the active layout and the next larger layout follow the rate, every other layout
// waits with one pod.
func (c *latencyController) followRate(app *FunctionApp, rate float64) {
	// Only the follow mode sizes the pods by the rate; the other modes keep the active layout at its planned
	// replicas (all warm), so they remain a clean point of comparison
	if c.prewarmMode != "follow" {
		return
	}
	now := time.Now()

	// Pod startup measurement: check whether the pods of earlier raised min-scales became ready
	c.observeStartups(app, now)

	// Rate forecast: size the pods for the rate expected when a pod requested now becomes ready
	sizingRate := c.forecastRate(app.Id, rate, now)
	targets := c.rateTargets(app, sizingRate)

	st := c.rateFollowState(app.Id)
	if st.activeKey != app.ActiveLayoutKey {
		st.activate(app, now)
	}
	for key, t := range targets {
		c.followTarget(app, st, key, t, rate, sizingRate, now)
	}
}

// rateTarget is the number of pods a layout composition on a node needs
type rateTarget struct {
	ci   CompositionInfo
	node string
	need int
}

// rateTargets returns the pods every layout composition of the app needs ("components@node" -> target): the
// active layout and the next larger layout follow the rate, every other layout waits with one pod. A composition
// shared by two layouts gets the larger target.
func (c *latencyController) rateTargets(app *FunctionApp, sizingRate float64) map[string]*rateTarget {
	targets := make(map[string]*rateTarget)
	for layoutKey, layout := range app.LayoutCandidates {
		followed := layoutKey == app.ActiveLayoutKey || layoutKey == layoutUpgradePath[app.ActiveLayoutKey]
		for node, ci := range layout {
			need := 1
			if followed {
				need = c.podsForRate(app.Id, ci, sizingRate)
			}
			key := compositionKey(ci) + "@" + node
			if t, ok := targets[key]; !ok || need > t.need {
				targets[key] = &rateTarget{ci: ci, node: node, need: need}
			}
		}
	}
	return targets
}

// rateFollowState returns the rate-following state of the app, created on first use
func (c *latencyController) rateFollowState(appID string) *rateFollow {
	st := c.rateFollows[appID]
	if st == nil {
		st = &rateFollow{pods: make(map[string]int), lastNeeded: make(map[string]time.Time)}
		c.rateFollows[appID] = st
	}
	return st
}

// activate records a newly active layout: a composition not followed yet starts with all its planned replicas
// warm, the others keep their current pods, which already follow the rate
func (st *rateFollow) activate(app *FunctionApp, now time.Time) {
	for node, ci := range app.LayoutCandidates[app.ActiveLayoutKey] {
		key := compositionKey(ci) + "@" + node
		if _, seen := st.pods[key]; !seen {
			st.pods[key] = ci.RequiredReplicas
			st.lastNeeded[key] = now
		}
	}
	st.activeKey = app.ActiveLayoutKey
}

// followTarget moves the min-scale of one layout composition to its target: raised at once, lowered only after
// the rate has needed fewer pods for the scale-down hold
func (c *latencyController) followTarget(app *FunctionApp, st *rateFollow, key string, t *rateTarget, rate, sizingRate float64, now time.Time) {
	current, seen := st.pods[key]
	if !seen {
		// A standby composition not seen yet: deployed with one warm pod
		current = 1
		st.pods[key] = current
		st.lastNeeded[key] = now
	}
	if t.need >= current {
		st.lastNeeded[key] = now
	}
	raise := t.need > current
	lower := t.need < current && now.Sub(st.lastNeeded[key]) >= c.scaleDownHold()
	if !raise && !lower {
		return
	}

	st.pods[key] = t.need
	log.Printf("Rate following %s: rate %.2f req/s (sized for %.2f), pod capacity %.2f req/s -> %d pods",
		key, rate, sizingRate, c.podCapacity(app.Id, t.ci), t.need)
	logPhase(app.Id, "rate_scale", app.ActiveLayoutKey, key, float64(t.need))
	// Pod startup measurement: from raising the min-scale until its pods are ready
	if raise {
		c.watchStartup(app, key, t.ci, t.node, t.need, now)
	} else {
		delete(c.startupPending[app.Id], key)
	}
	// Run in the background: patching the cluster must not delay the controller tick
	go c.setCompositionMinScale(app.Id, t.ci, t.node, t.need)
}

// setCompositionMinScale sets the min-scale of a layout composition's deployments on one node
func (c *latencyController) setCompositionMinScale(appID string, ci CompositionInfo, node string, minScale int) {
	// Reload the app: compositions and their deployments are only loaded by GetFunctionApp
	app, err := c.composer.GetFunctionApp(appID)
	if err != nil || app == nil {
		log.Printf("Failed to load app %s for min-scale change: %v", appID, err)
		return
	}
	key := compositionKey(ci)
	for _, fc := range app.Compositions {
		if componentsKey(fc.Components) != key {
			continue
		}
		for _, d := range fc.Deployments {
			if d.Node != node {
				continue
			}
			if err := c.composer.SetDeploymentMinScale(d, minScale); err != nil {
				log.Printf("Failed to set min-scale of deployment %s: %v", d.Id, err)
				continue
			}
			log.Printf("Set min-scale of deployment %s to %d (rate following)", d.Id, minScale)
		}
	}
}

// ratePoint is one measured arrival rate
type ratePoint struct {
	t    time.Time
	rate float64
}

// forecastRate is the arrival rate the pods are sized for. The measured rate is the mean of the last metric
// window, so it describes the moment half a window ago; with the forecast on and a growing rate, it is
// extrapolated by its growth over the last window to half a window plus one pod startup time ahead.
// A falling rate is not extrapolated: scaling down is left to the hold time.
func (c *latencyController) forecastRate(appID string, rate float64, now time.Time) float64 {
	// Keep the measured rates of the last metric window
	history := append(c.rateHistory[appID], ratePoint{t: now, rate: rate})
	for len(history) > 1 && now.Sub(history[0].t) > c.metricWindow {
		history = history[1:]
	}
	c.rateHistory[appID] = history

	if !c.rateForecast || len(history) < 2 {
		return rate
	}
	span := now.Sub(history[0].t).Seconds()
	if span <= 0 {
		return rate
	}
	growth := (rate - history[0].rate) / span // req/s per second
	if growth <= 0 {
		return rate
	}
	ahead := c.metricWindow.Seconds()/2 + c.startupMean.Seconds()
	return rate + growth*ahead
}

// rateDowngradeDue reports whether a follow mode app can switch back to the smaller layout: the arrival rate has
// fitted the smaller layout's capacity for the scale-down hold. By the ski rental rule the larger layout is kept
// as long as starting a pod takes; the smaller layout waits warm, so the switch back and a later switch up are fast.
func (c *latencyController) rateDowngradeDue(app *FunctionApp, rate float64) bool {
	smaller := layoutDowngradePath[app.ActiveLayoutKey]
	if smaller == "" || c.rateAboveCapacity(app, smaller, rate) {
		delete(c.downgradeSince, app.Id)
		return false
	}
	since, ok := c.downgradeSince[app.Id]
	if !ok {
		c.downgradeSince[app.Id] = time.Now()
		return false
	}
	if time.Since(since) < c.scaleDownHold() {
		return false
	}
	delete(c.downgradeSince, app.Id)
	logPhase(app.Id, "rate_downgrade", app.ActiveLayoutKey, "", rate)
	return true
}
