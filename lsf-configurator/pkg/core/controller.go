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
			runtimes, traceCounts, err := c.metricQueryFunc(c.metricQueryTimeRange)
			if err != nil {
				log.Printf("Error querying app runtime metrics: %v", err)
				continue
			}
			// Measured capacity: the components' average runtimes in the same metric window
			if c.measuredCapacity {
				if measured, err := c.metrics.QueryComponentRuntimes(c.metricQueryTimeRange); err != nil {
					log.Printf("Error querying component runtimes: %v", err)
				} else {
					c.componentRuntimes = measured
				}
			}
			// Log runtimes at defined intervals
			now := time.Now()
			if now.Sub(c.lastLogTime) >= logInterval {
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

			handledApps := make(map[string]bool)
			for appId, runtime := range runtimes {
				handledApps[appId] = true
				// log.Printf("App %s metrics with runtime agg func %s: %.0f ms", appId, c.aggMetricType, runtime)
				app, err := c.composer.GetFunctionApp(appId)
				if err != nil {
					log.Printf("Error retrieving function app %s: %v", appId, err)
					continue
				}
				if app == nil {
					log.Printf("Found traces for app %s, but app is not registered in the database. Skipping", appId)
					continue
				}

				if app.LatencyLimit <= 0 {
					continue
				}

				// Phase marker: the controller sees the latency above the limit (logged on every such tick)
				if runtime > float64(app.LatencyLimit) {
					logPhase(app.Id, "violation_seen", app.ActiveLayoutKey, "", runtime)
				}

				// Adaptive prewarm: scale up the next layout's standby before the latency limit is reached
				if c.prewarmMode == "adaptive" {
					c.adaptStandby(app, traceCounts[appId])
				}

				// Rate following: keep the active layout's warm pods at what the measured rate needs
				c.followRate(app, c.arrivalRate(traceCounts[appId]))

				if c.inCooldown(app.Id) {
					continue
				}

				// Determine action: proactively on the arrival rate, otherwise on the latency
				var handler func(*FunctionApp) (string, error)
				rate := c.arrivalRate(traceCounts[appId])
				rateUpgrade := layoutUpgradePath[app.ActiveLayoutKey] != "" && c.rateAboveCapacity(app, app.ActiveLayoutKey, rate)
				if rateUpgrade && c.upgradeReady(app, rate) {
					// Proactive upgrade: the active layout reached the rate it was sized for
					logPhase(app.Id, "rate_trigger", app.ActiveLayoutKey, "", rate)
					handler = c.handleLatencyViolation
					c.resetDowngradeCount(app.Id)
				} else if runtime > float64(app.LatencyLimit) {
					// Check trace count for upgrades to avoid reconfigurations based on insufficient data
					count, ok := traceCounts[appId]
					if !ok || count < minimalTraceCount {
						// log.Printf("Skipping app %s reconfiguration due to insufficient trace count (%d < %d)", appId, count, minimalTraceCount)
						continue
					}
					handler = c.handleLatencyViolation
					// Reset downgrade counter on upgrade
					c.resetDowngradeCount(app.Id)
				} else if c.prewarmMode == "follow" && c.switchRateFactor > 0 {
					// Follow mode: switch back on the arrival rate after the ski rental hold, not on a low latency
					if rateUpgrade || !c.rateDowngradeDue(app, rate) {
						continue
					}
					handler = c.handleLayoutDowngrade
				} else if !rateUpgrade && runtime < float64(app.LatencyLimit)*c.latencyDowngradeFactor &&
					!c.rateAboveCapacity(app, layoutDowngradePath[app.ActiveLayoutKey], rate) {
					// With proactive switching, never downgrade to a layout that the current rate would overload
					// Oscillation defense for downgrade: track consecutive intervals
					count := c.addDowngradeCount(app.Id, 1)
					if count < minConsecutiveDowngrade {
						// log.Printf("App %s eligible for downgrade (%d/%d consecutive intervals)", app.Id, count, minConsecutiveDowngrade)
						continue
					}
					handler = c.handleLayoutDowngrade
				} else {
					// Reset counter if not eligible for downgrade
					c.resetDowngradeCount(app.Id)
					continue
				}

				nextLayoutKey, err := handler(app)
				if err != nil {
					log.Printf("Error handling reconfiguration for app %s: %v", app.Id, err)
					continue
				}
				if nextLayoutKey == "" {
					//log.Printf("No further layout candidates available for app %s. Skipping reconfiguration.", app.Id)
					continue
				} else {
					log.Printf("App %s transitioned to layout %s. Deploying...", app.Id, nextLayoutKey)
				}
				c.markReconfigured(app.Id)

				log.Printf("Reconfiguration in progress for app %s, applying cooldown period", app.Id)
			}

			// Handle apps with no reported runtimes (possible downgrade)
			apps, err := c.composer.functionAppRepo.GetAll()
			if err != nil {
				log.Printf("Error retrieving all function apps: %v", err)
				continue
			}
			for _, app := range apps {
				if handledApps[app.Id] {
					continue
				}
				if app.LatencyLimit <= 0 {
					continue
				}
				// Adaptive prewarm: without traffic the rate is low, so a boosted standby is scaled back after the quiet period
				if c.prewarmMode == "adaptive" {
					c.adaptStandby(app, traceCounts[app.Id])
				}

				// Rate following: without traffic the rate is zero, so the warm pods go down to one after the hold time
				c.arrivalRate(traceCounts[app.Id])

				if app.ActiveLayoutKey == LayoutKeyMin {
					continue
				}
				if c.inCooldown(app.Id) {
					continue
				}

				// Increment downgrade counter for apps with no runtime, use a higher increment to trigger faster downgrade
				count := c.addDowngradeCount(app.Id, 20)

				if count < minConsecutiveDowngrade {
					// log.Printf("App %s has no runtime (%d/%d consecutive intervals)", app.Id, count, minConsecutiveDowngrade)
					continue
				}

				c.resetDowngradeCount(app.Id)

				log.Printf("No runtime reported for app %s for %d consecutive intervals, downgrading to minimal layout.", app.Id, count)
				nextLayoutKey, err := c.handleLayoutChange(app, layoutDowngradePath, false)
				if err != nil {
					log.Printf("Error downgrading app %s to minimal layout: %v", app.Id, err)
					continue
				}
				if nextLayoutKey != "" {
					log.Printf("App %s transitioned to layout %s due to missing runtimes. Deploying...", app.Id, nextLayoutKey)
					c.markReconfigured(app.Id)
				}
			}
		}
	}
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

	createdCompositionKeys := make(map[string]bool)

	// Sort layout candidate keys, putting LayoutKeyMin first, so it gets built first
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

	for _, key := range keys {
		layout := app.LayoutCandidates[key]
		for _, compositionInfo := range layout {
			components := compositionInfo.ComponentProfiles

			componentNames := make([]string, len(components))
			for i, cp := range components {
				componentNames[i] = cp.Name
			}
			fcKey := componentsKey(componentNames)
			if createdCompositionKeys[fcKey] {
				continue
			}
			_, err := c.composer.AddFunctionComposition(app.Id, componentNames, "")
			if err != nil {
				log.Printf("Error adding function composition for app %s: %v", app.Id, err)
				return nil, err
			}
			createdCompositionKeys[fcKey] = true
		}
	}

	go func(appId string, layout Layout) {
		err = c.deployLayout(appId, layout, false, reuseDeployments, false)
		if err != nil {
			log.Printf("Error deploying layout for app %s: %v", appId, err)
			return
		}
		log.Printf("Successfully deployed function app with layout %s: %v", appId, layout)
		// Prewarm: deploy the other layout candidates once the initial layout is up
		if fresh, err := c.composer.GetFunctionApp(appId); err == nil && fresh != nil {
			c.ensureStandbyLayouts(fresh)
		}
	}(app.Id, app.LayoutCandidates[app.ActiveLayoutKey])

	return app, nil
}
