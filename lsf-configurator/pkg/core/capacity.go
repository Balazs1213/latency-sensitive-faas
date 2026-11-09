package core

import "math"

// layoutCapacity is the arrival rate (req/s) a layout is sized for at the model's target utilization:
// the minimum over its compositions of the planned replicas times the capacity of one pod
func (c *latencyController) layoutCapacity(appID string, layout Layout) float64 {
	capacity := math.Inf(1)
	for _, ci := range layout {
		podCapacity := c.podCapacity(appID, ci)
		if podCapacity <= 0 {
			continue
		}
		if compCapacity := float64(ci.RequiredReplicas) * podCapacity; compCapacity < capacity {
			capacity = compCapacity
		}
	}
	return capacity
}

// componentRuntime is the runtime (ms) of a component in the capacity model: with measured capacity and
// enough spans in the last metric window the measured average, otherwise the profiled runtime
func (c *latencyController) componentRuntime(appID string, cp ComponentProfile) float64 {
	if c.measuredCapacity {
		if m, ok := c.componentRuntimes[appID][cp.Name]; ok && m.Count >= minimalTraceCount {
			return m.AvgMs
		}
	}
	return float64(cp.Runtime)
}

// podCapacity is the arrival rate one pod of a composition serves at the model's target utilization:
// (1000 / t) * C * U, the denominator of the framework's replica formula
func (c *latencyController) podCapacity(appID string, ci CompositionInfo) float64 {
	runtime := 0.0
	for _, cp := range ci.ComponentProfiles {
		runtime += c.componentRuntime(appID, cp)
	}
	if runtime <= 0 {
		return 0
	}
	concurrency := ci.TargetConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	return 1000 / runtime * float64(concurrency) * c.targetUtilization
}

// podsForRate applies the replica formula R = ceil(rate / podCapacity) to the measured arrival rate:
// the pods a composition needs right now, at least one and at most its planned replicas
func (c *latencyController) podsForRate(appID string, ci CompositionInfo, rate float64) int {
	capacity := c.podCapacity(appID, ci)
	if capacity <= 0 {
		return ci.RequiredReplicas // no runtime profile: keep the planned replicas
	}
	pods := int(math.Ceil(rate / capacity))
	if pods < 1 {
		pods = 1
	}
	if pods > ci.RequiredReplicas {
		pods = ci.RequiredReplicas
	}
	return pods
}

// rateAboveCapacity reports whether proactive switching is on and the arrival rate exceeds the configured
// share of the layout's capacity (the rate the layout was sized for at the model's target utilization)
func (c *latencyController) rateAboveCapacity(app *FunctionApp, layoutKey string, rate float64) bool {
	if c.switchRateFactor <= 0 {
		return false
	}
	layout, ok := app.LayoutCandidates[layoutKey]
	if !ok {
		return false
	}
	return rate > c.switchRateFactor*c.layoutCapacity(app.Id, layout)
}
