package core

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

// set target concurrency globally to 1 for now, but this can be different per function composition depending on how CPU-bound they are
// this should be measured and set accordingly for each function composition)
const (
	minimalTraceCount       = 10
	logInterval             = 1 * time.Minute
	minConsecutiveDowngrade = 300
	reuseDeployments        = true
	unboostQuietPeriod      = 60 * time.Second // adaptive prewarm: scale the standby back after this long below the threshold
	rateScaleDownHold       = 60 * time.Second // rate following: scale-down hold until the pod startup time is measured
	idleDowngradeStep       = 20               // downgrade counter step of a tick without traffic (a low-latency tick counts 1)
)

type MetricType string

const (
	MetricTypeP95     MetricType = "P95"
	MetricTypeAverage MetricType = "AVG"
)

type MetricQueryFunc func(timeRangeGte string) (map[string]float64, map[string]int, error)

type latencyController struct {
	composer                     *Composer
	metrics                      MetricsReader
	scenarioManager              ScenarioManager
	delay                        time.Duration
	deployNamespace              string
	lastReconfigs                map[string]time.Time
	cooldownPeriod               time.Duration
	availableNodeMemoryGb        int // same for all nodes for now, in GB
	lastReconfigsMu              sync.Mutex
	latencyDowngradeFactor       float64
	aggMetricType                MetricType
	metricQueryFunc              MetricQueryFunc
	metricQueryTimeRange         string
	reconfigStartTimes           map[string]time.Time
	lastLogTime                  time.Time
	consecutiveDowngradeEligible map[string]int                         // appId -> count of consecutive eligible downgrade intervals
	prewarmMode                  string                                 // "off", "one", "full", "adaptive" or "follow": warm standby level of the inactive layouts
	targetUtilization            float64                                // utilization target of the layout model, used for the layout capacity
	boostRateFactor              float64                                // adaptive prewarm: boost threshold as a share of the active layout's capacity
	switchRateFactor             float64                                // proactive switching: upgrade at this share of the active layout's capacity (0 = off)
	metricWindow                 time.Duration                          // length of the metric query window (arrival rate = trace count / window)
	boostedSince                 map[string]time.Time                   // adaptive prewarm: appId -> when the standby layout was boosted
	lastAboveBoost               map[string]time.Time                   // adaptive prewarm: appId -> last tick with the rate above the threshold
	rateFollows                  map[string]*rateFollow                 // appId -> rate-following state of the active layout
	measuredCapacity             bool                                   // capacity model uses the measured component runtimes
	componentRuntimes            map[string]map[string]ComponentRuntime // appId -> component -> measured runtime in the last metric window
	startupPending               map[string]map[string]startupWatch     // appId -> "components@node" -> raised min-scale waiting for its pods
	startupMean                  time.Duration                          // mean measured pod startup time (0 = not measured yet)
	startupSamples               int                                    // number of pod startup measurements in the mean
	rateForecast                 bool                                   // rate following sizes the pods for the forecast rate
	rateHistory                  map[string][]ratePoint                 // appId -> measured rates of the last metric window
	downgradeSince               map[string]time.Time                   // appId -> since when the rate has fitted the smaller layout (follow mode)

}

func NewController(composer *Composer, metrics MetricsReader, scenarioManager ScenarioManager,
	delay time.Duration, deployNamespace string, availableNodeMemoryGb int, aggMetricType MetricType,
	metricQueryTimeRange string, latencyDowngradeFactor float64, prewarmMode string, targetUtilization float64, boostRateFactor float64, switchRateFactor float64, measuredCapacity bool, rateForecast bool) Controller {

	if prewarmMode != "one" && prewarmMode != "full" && prewarmMode != "adaptive" && prewarmMode != "follow" {
		prewarmMode = "off"
	}
	log.Printf("Layout prewarm mode: %s (boost at %.2f of the layout capacity, utilization %.2f)", prewarmMode, boostRateFactor, targetUtilization)
	log.Printf("Proactive switching: factor %.2f of the layout capacity (0 = off)", switchRateFactor)
	log.Printf("Capacity model: measured component runtimes %t (false = profiled runtimes)", measuredCapacity)
	log.Printf("Rate forecast: %t (pods sized for the rate expected after a pod startup)", rateForecast)

	// Length of the metric window, e.g. "now-15s" -> 15s; the arrival rate is the trace count divided by it
	metricWindow, err := time.ParseDuration(strings.TrimPrefix(metricQueryTimeRange, "now-"))
	if err != nil || metricWindow <= 0 {
		log.Printf("Warning: cannot parse the metric window from '%s', assuming 15s", metricQueryTimeRange)
		metricWindow = 15 * time.Second
	}

	if aggMetricType != MetricTypeP95 && aggMetricType != MetricTypeAverage {
		log.Printf("Warning: Invalid metric type '%s' provided. Defaulting to P95.", aggMetricType)
		aggMetricType = MetricTypeP95
	}

	var queryFunc MetricQueryFunc
	switch aggMetricType {
	case MetricTypeP95:
		queryFunc = metrics.Query95thPercentileAppRuntimes
	case MetricTypeAverage:
		queryFunc = metrics.QueryAverageAppRuntimes
	}

	return &latencyController{
		composer:                     composer,
		metrics:                      metrics,
		scenarioManager:              scenarioManager,
		delay:                        delay,
		deployNamespace:              deployNamespace,
		lastReconfigs:                make(map[string]time.Time),
		cooldownPeriod:               120 * time.Second,
		availableNodeMemoryGb:        availableNodeMemoryGb,
		lastReconfigsMu:              sync.Mutex{},
		latencyDowngradeFactor:       latencyDowngradeFactor,
		aggMetricType:                aggMetricType,
		metricQueryFunc:              queryFunc,
		metricQueryTimeRange:         metricQueryTimeRange,
		reconfigStartTimes:           make(map[string]time.Time),
		lastLogTime:                  time.Now(),
		consecutiveDowngradeEligible: make(map[string]int),
		prewarmMode:                  prewarmMode,
		targetUtilization:            targetUtilization,
		boostRateFactor:              boostRateFactor,
		switchRateFactor:             switchRateFactor,
		metricWindow:                 metricWindow,
		boostedSince:                 make(map[string]time.Time),
		lastAboveBoost:               make(map[string]time.Time),
		rateFollows:                  make(map[string]*rateFollow),
		measuredCapacity:             measuredCapacity,
		componentRuntimes:            make(map[string]map[string]ComponentRuntime),
		startupPending:               make(map[string]map[string]startupWatch),
		rateForecast:                 rateForecast,
		rateHistory:                  make(map[string][]ratePoint),
		downgradeSince:               make(map[string]time.Time),
	}
}

func (c *latencyController) Start(ctx context.Context) error {
	log.Println("Latency Controller started")
	c.initLayouts()
	ticker := time.NewTicker(c.delay)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Println("Latency Controller received cancellation signal")
			return nil
		case <-ticker.C:
			c.tick()
		}
	}
}

// tick runs one control interval: it queries the app latencies and trace counts of the metric window,
// then decides on a reconfiguration for every app with traffic and for every app without traffic
func (c *latencyController) tick() {
	runtimes, traceCounts, err := c.metricQueryFunc(c.metricQueryTimeRange)
	if err != nil {
		log.Printf("Error querying app runtime metrics: %v", err)
		return
	}
	c.refreshComponentRuntimes()
	c.logRuntimes(runtimes)

	handledApps := make(map[string]bool)
	for appId, runtime := range runtimes {
		handledApps[appId] = true
		c.controlApp(appId, runtime, traceCounts[appId])
	}

	// Handle apps with no reported runtimes (possible downgrade)
	apps, err := c.composer.functionAppRepo.GetAll()
	if err != nil {
		log.Printf("Error retrieving all function apps: %v", err)
		return
	}
	for _, app := range apps {
		if !handledApps[app.Id] {
			c.controlIdleApp(app, traceCounts[app.Id])
		}
	}
}

// controlApp decides on the reconfiguration of an app with traffic in the metric window
func (c *latencyController) controlApp(appId string, runtime float64, traceCount int) {
	app, err := c.composer.GetFunctionApp(appId)
	if err != nil {
		log.Printf("Error retrieving function app %s: %v", appId, err)
		return
	}
	if app == nil {
		log.Printf("Found traces for app %s, but app is not registered in the database. Skipping", appId)
		return
	}
	if app.LatencyLimit <= 0 {
		return
	}

	// Phase marker: the controller sees the latency above the limit (logged on every such tick)
	if runtime > float64(app.LatencyLimit) {
		logPhase(app.Id, "violation_seen", app.ActiveLayoutKey, "", runtime)
	}
	// Adaptive prewarm: scale up the next layout's standby before the latency limit is reached
	if c.prewarmMode == "adaptive" {
		c.adaptStandby(app, traceCount)
	}
	// Rate following: keep the active layout's warm pods at what the measured rate needs
	rate := c.arrivalRate(traceCount)
	c.followRate(app, rate)

	if c.inCooldown(app.Id) {
		return
	}
	handler := c.chooseAction(app, runtime, traceCount, rate)
	if handler == nil {
		return
	}
	c.applyReconfig(app, handler)
}

// chooseAction picks the reconfiguration of an app with traffic, or returns nil when the app stays on its layout.
// In order: a proactive upgrade when the rate reaches the active layout's capacity, an upgrade on a latency above
// the limit, then a downgrade, either on the rate (follow mode) or after enough consecutive ticks with a low latency.
func (c *latencyController) chooseAction(app *FunctionApp, runtime float64, traceCount int, rate float64) func(*FunctionApp) (string, error) {
	rateUpgrade := layoutUpgradePath[app.ActiveLayoutKey] != "" && c.rateAboveCapacity(app, app.ActiveLayoutKey, rate)
	if rateUpgrade && c.upgradeReady(app, rate) {
		// Proactive upgrade: the active layout reached the rate it was sized for
		logPhase(app.Id, "rate_trigger", app.ActiveLayoutKey, "", rate)
		c.resetDowngradeCount(app.Id)
		return c.handleLatencyViolation
	}
	if runtime > float64(app.LatencyLimit) {
		// Upgrade on the latency, but only with enough traces, to avoid reconfigurations on insufficient data
		if traceCount < minimalTraceCount {
			return nil
		}
		c.resetDowngradeCount(app.Id)
		return c.handleLatencyViolation
	}
	if c.prewarmMode == "follow" && c.switchRateFactor > 0 {
		// Follow mode: switch back on the arrival rate after the ski rental hold, not on a low latency
		if rateUpgrade || !c.rateDowngradeDue(app, rate) {
			return nil
		}
		return c.handleLayoutDowngrade
	}
	if !rateUpgrade && runtime < float64(app.LatencyLimit)*c.latencyDowngradeFactor &&
		!c.rateAboveCapacity(app, layoutDowngradePath[app.ActiveLayoutKey], rate) {
		// With proactive switching, never downgrade to a layout that the current rate would overload.
		// Oscillation defense: downgrade only after enough consecutive eligible ticks
		if c.addDowngradeCount(app.Id, 1) < minConsecutiveDowngrade {
			return nil
		}
		return c.handleLayoutDowngrade
	}
	// Not eligible for a downgrade: reset the counter
	c.resetDowngradeCount(app.Id)
	return nil
}

// applyReconfig runs the chosen reconfiguration of the app and starts its cooldown period when the layout changed
func (c *latencyController) applyReconfig(app *FunctionApp, handler func(*FunctionApp) (string, error)) {
	nextLayoutKey, err := handler(app)
	if err != nil {
		log.Printf("Error handling reconfiguration for app %s: %v", app.Id, err)
		return
	}
	if nextLayoutKey == "" {
		// No further layout candidate in this direction
		return
	}
	log.Printf("App %s transitioned to layout %s. Deploying...", app.Id, nextLayoutKey)
	c.markReconfigured(app.Id)
	log.Printf("Reconfiguration in progress for app %s, applying cooldown period", app.Id)
}

// controlIdleApp handles an app without traffic in the metric window: its standby and warm pods follow the zero
// rate, and after enough ticks without traffic it is downgraded by one layout
func (c *latencyController) controlIdleApp(app *FunctionApp, traceCount int) {
	if app.LatencyLimit <= 0 {
		return
	}
	// Adaptive prewarm: without traffic the rate is low, so a boosted standby is scaled back after the quiet period
	if c.prewarmMode == "adaptive" {
		c.adaptStandby(app, traceCount)
	}
	// Rate following: without traffic the rate is zero, so the warm pods go down to one after the hold time
	c.followRate(app, c.arrivalRate(traceCount))

	if app.ActiveLayoutKey == LayoutKeyMin || c.inCooldown(app.Id) {
		return
	}
	// A tick without traffic raises the downgrade counter faster than a low-latency tick
	count := c.addDowngradeCount(app.Id, idleDowngradeStep)
	if count < minConsecutiveDowngrade {
		return
	}
	c.resetDowngradeCount(app.Id)

	log.Printf("No runtime reported for app %s for %d consecutive intervals, downgrading to minimal layout.", app.Id, count)
	nextLayoutKey, err := c.handleLayoutChange(app, layoutDowngradePath, false)
	if err != nil {
		log.Printf("Error downgrading app %s to minimal layout: %v", app.Id, err)
		return
	}
	if nextLayoutKey != "" {
		log.Printf("App %s transitioned to layout %s due to missing runtimes. Deploying...", app.Id, nextLayoutKey)
		c.markReconfigured(app.Id)
	}
}

// refreshComponentRuntimes updates the measured component runtimes of the capacity model from the
// same metric window (measured capacity only)
func (c *latencyController) refreshComponentRuntimes() {
	if !c.measuredCapacity {
		return
	}
	measured, err := c.metrics.QueryComponentRuntimes(c.metricQueryTimeRange)
	if err != nil {
		log.Printf("Error querying component runtimes: %v", err)
		return
	}
	c.componentRuntimes = measured
}

// logRuntimes logs the latencies of the apps once every logInterval
func (c *latencyController) logRuntimes(runtimes map[string]float64) {
	now := time.Now()
	if now.Sub(c.lastLogTime) < logInterval {
		return
	}
	if len(runtimes) > 0 {
		var runtimeStrings []string
		for appID, rt := range runtimes {
			runtimeStrings = append(runtimeStrings, fmt.Sprintf("%s: %.0fms", appID, rt))
		}
		sort.Strings(runtimeStrings)
		log.Printf("Current app runtimes (%s): [%s]", c.aggMetricType, strings.Join(runtimeStrings, ", "))
	} else {
		log.Printf("No app runtimes reported in this interval.")
	}
	c.lastLogTime = now
}

// initLayouts prepares the layouts of the already registered apps at start-up: with prewarming it brings up
// their inactive layouts, with prewarm off it removes the standby deployments left over from an earlier
// prewarm mode, so that a switch starts from cold pods as in the original framework
func (c *latencyController) initLayouts() {
	apps, err := c.composer.functionAppRepo.GetAll()
	if err != nil {
		log.Printf("Error retrieving function apps for the layout preparation: %v", err)
		return
	}
	for _, app := range apps {
		if c.prewarmMode == "off" {
			go c.removeInactiveDeployments(app.Id)
		} else if app.LatencyLimit > 0 && len(app.LayoutCandidates) > 1 {
			go c.ensureStandbyLayouts(app)
		}
	}
}

// inCooldown reports whether the app was reconfigured less than the cooldown period ago
func (c *latencyController) inCooldown(appId string) bool {
	c.lastReconfigsMu.Lock()
	defer c.lastReconfigsMu.Unlock()
	last, ok := c.lastReconfigs[appId]
	return ok && time.Since(last) < c.cooldownPeriod
}

// markReconfigured starts the cooldown period of the app
func (c *latencyController) markReconfigured(appId string) {
	c.lastReconfigsMu.Lock()
	defer c.lastReconfigsMu.Unlock()
	c.lastReconfigs[appId] = time.Now()
}

// resetDowngradeCount clears the app's count of consecutive downgrade-eligible ticks
func (c *latencyController) resetDowngradeCount(appId string) {
	c.lastReconfigsMu.Lock()
	defer c.lastReconfigsMu.Unlock()
	c.consecutiveDowngradeEligible[appId] = 0
}

// addDowngradeCount raises the app's count of consecutive downgrade-eligible ticks by step
// and returns the new count
func (c *latencyController) addDowngradeCount(appId string, step int) int {
	c.lastReconfigsMu.Lock()
	defer c.lastReconfigsMu.Unlock()
	c.consecutiveDowngradeEligible[appId] += step
	return c.consecutiveDowngradeEligible[appId]
}

// arrivalRate converts a trace count of the metric window to an arrival rate (req/s)
func (c *latencyController) arrivalRate(traceCount int) float64 {
	return float64(traceCount) / c.metricWindow.Seconds()
}

func (c *latencyController) RegisterFunctionApp(creationData FunctionAppCreationData) (*FunctionApp, error) {
	app, err := c.composer.CreateFunctionApp(creationData)
	if err != nil {
		log.Printf("Error creating function app: %v", err)
		return nil, err
	}

	candidates, err := c.scenarioManager.GenerateLayoutCandidates(
		app.Components,
		app.Links,
		app.LatencyLimit,
		c.availableNodeMemoryGb*1024)
	if err != nil {
		log.Printf("Error generating layout candidates for app %s: %v", app.Id, err)
		return nil, err
	}
	log.Printf("Generated layout candidates for app: %s: %v", app.Id, candidates)
	app.LayoutCandidates = candidates
	// Default to the minimal layout initially
	app.ActiveLayoutKey = LayoutKeyMin
	err = c.composer.functionAppRepo.Save(app)
	if err != nil {
		log.Printf("Error saving function app %s: %v", app.Id, err)
		return nil, err
	}

	if err := c.createCompositions(app); err != nil {
		return nil, err
	}
	go c.deployInitialLayout(app.Id, app.LayoutCandidates[app.ActiveLayoutKey])

	return app, nil
}

// createCompositions creates the function composition of every layout candidate once, starting with the
// active layout's, so that its build starts first
func (c *latencyController) createCompositions(app *FunctionApp) error {
	created := make(map[string]bool)
	for _, key := range sortedLayoutKeys(app) {
		for _, compositionInfo := range app.LayoutCandidates[key] {
			componentNames := make([]string, len(compositionInfo.ComponentProfiles))
			for i, cp := range compositionInfo.ComponentProfiles {
				componentNames[i] = cp.Name
			}
			fcKey := componentsKey(componentNames)
			if created[fcKey] {
				continue
			}
			if _, err := c.composer.AddFunctionComposition(app.Id, componentNames, ""); err != nil {
				log.Printf("Error adding function composition for app %s: %v", app.Id, err)
				return err
			}
			created[fcKey] = true
		}
	}
	return nil
}

// sortedLayoutKeys returns the layout candidate keys of the app, the active layout first, the rest in key order
func sortedLayoutKeys(app *FunctionApp) []string {
	var keys []string
	for k := range app.LayoutCandidates {
		keys = append(keys, k)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		if keys[i] == app.ActiveLayoutKey {
			return true
		}
		if keys[j] == app.ActiveLayoutKey {
			return false
		}
		return keys[i] < keys[j]
	})
	return keys
}

// deployInitialLayout deploys the active layout of a newly registered app, then its standby layouts (prewarm)
func (c *latencyController) deployInitialLayout(appId string, layout Layout) {
	if err := c.deployLayout(appId, layout, false, reuseDeployments, false); err != nil {
		log.Printf("Error deploying layout for app %s: %v", appId, err)
		return
	}
	log.Printf("Successfully deployed function app with layout %s: %v", appId, layout)
	// Prewarm: deploy the other layout candidates once the initial layout is up
	if fresh, err := c.composer.GetFunctionApp(appId); err == nil && fresh != nil {
		c.ensureStandbyLayouts(fresh)
	}
}
