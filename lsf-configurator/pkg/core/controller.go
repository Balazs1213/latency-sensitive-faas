package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
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
)

type MetricType string

const (
	MetricTypeP95     MetricType = "P95"
	MetricTypeAverage MetricType = "AVG"
)

type MetricQueryFunc func(timeRangeGte string) (map[string]float64, map[string]int, error)

type ReconfigEvent struct {
	EventType  string  `json:"event_type"`
	AppID      string  `json:"app_id"`
	EventTime  int64   `json:"event_time"` // Unix timestamp in milliseconds
	DurationMs float64 `json:"duration_ms"`
}

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
	consecutiveDowngradeEligible map[string]int // appId -> count of consecutive eligible downgrade intervals
	prewarmMode                  string               // "off", "one", "full" or "adaptive": warm standby level of the inactive layouts
	targetUtilization            float64              // utilization target of the layout model, used for the layout capacity
	boostRateFactor              float64              // adaptive prewarm: boost threshold as a share of the active layout's capacity
	metricWindow                 time.Duration        // length of the metric query window (arrival rate = trace count / window)
	boostedSince                 map[string]time.Time // adaptive prewarm: appId -> when the standby layout was boosted
	lastAboveBoost               map[string]time.Time // adaptive prewarm: appId -> last tick with the rate above the threshold
}

func NewController(composer *Composer, metrics MetricsReader, scenarioManager ScenarioManager,
	delay time.Duration, deployNamespace string, availableNodeMemoryGb int, aggMetricType MetricType,
	metricQueryTimeRange string, latencyDowngradeFactor float64, prewarmMode string, targetUtilization float64, boostRateFactor float64) Controller {

	if prewarmMode != "one" && prewarmMode != "full" && prewarmMode != "adaptive" {
		prewarmMode = "off"
	}
	log.Printf("Layout prewarm mode: %s (boost at %.2f of the layout capacity, utilization %.2f)", prewarmMode, boostRateFactor, targetUtilization)

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
		metricWindow:                 metricWindow,
		boostedSince:                 make(map[string]time.Time),
		lastAboveBoost:               make(map[string]time.Time),
	}
}

func (c *latencyController) Start(ctx context.Context) error {
	log.Println("Latency Controller started")
	// Prewarm: bring up the inactive layouts of the already registered apps
	if c.prewarmMode != "off" {
		if apps, err := c.composer.functionAppRepo.GetAll(); err == nil {
			for _, app := range apps {
				if app.LatencyLimit > 0 && len(app.LayoutCandidates) > 1 {
					go c.ensureStandbyLayouts(app)
				}
			}
		}
	}
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

				c.lastReconfigsMu.Lock()
				last, ok := c.lastReconfigs[app.Id]
				if ok && time.Since(last) < c.cooldownPeriod {
					// log.Printf("Skipping reconfiguration for app %s due to cooldown (last at %v)", app.Id, last)
					c.lastReconfigsMu.Unlock()
					continue
				}
				c.lastReconfigsMu.Unlock()

				// Determine action based on runtime
				var handler func(*FunctionApp) (string, error)
				if runtime > float64(app.LatencyLimit) {
					// Check trace count for upgrades to avoid reconfigurations based on insufficient data
					count, ok := traceCounts[appId]
					if !ok || count < minimalTraceCount {
						// log.Printf("Skipping app %s reconfiguration due to insufficient trace count (%d < %d)", appId, count, minimalTraceCount)
						continue
					}
					handler = c.handleLatencyViolation
					// Reset downgrade counter on upgrade
					c.lastReconfigsMu.Lock()
					c.consecutiveDowngradeEligible[app.Id] = 0
					c.lastReconfigsMu.Unlock()
				} else if runtime < float64(app.LatencyLimit)*c.latencyDowngradeFactor {
					// Oscillation defense for downgrade: track consecutive intervals
					c.lastReconfigsMu.Lock()
					c.consecutiveDowngradeEligible[app.Id]++
					count := c.consecutiveDowngradeEligible[app.Id]
					c.lastReconfigsMu.Unlock()
					if count < minConsecutiveDowngrade {
						// log.Printf("App %s eligible for downgrade (%d/%d consecutive intervals)", app.Id, count, minConsecutiveDowngrade)
						continue
					}
					handler = c.handleLayoutDowngrade
				} else {
					// Reset counter if not eligible for downgrade
					c.lastReconfigsMu.Lock()
					c.consecutiveDowngradeEligible[app.Id] = 0
					c.lastReconfigsMu.Unlock()
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
				c.lastReconfigsMu.Lock()
				c.lastReconfigs[app.Id] = time.Now()
				c.lastReconfigsMu.Unlock()

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
				if app.ActiveLayoutKey == LayoutKeyMin {
					continue
				}
				c.lastReconfigsMu.Lock()
				last, ok := c.lastReconfigs[app.Id]
				if ok && time.Since(last) < c.cooldownPeriod {
					c.lastReconfigsMu.Unlock()
					continue
				}
				c.lastReconfigsMu.Unlock()

				// Increment downgrade counter for apps with no runtime, use a higher increment to trigger faster downgrade
				c.lastReconfigsMu.Lock()
				c.consecutiveDowngradeEligible[app.Id] += 20
				count := c.consecutiveDowngradeEligible[app.Id]
				c.lastReconfigsMu.Unlock()

				if count < minConsecutiveDowngrade {
					// log.Printf("App %s has no runtime (%d/%d consecutive intervals)", app.Id, count, minConsecutiveDowngrade)
					continue
				}

				c.lastReconfigsMu.Lock()
				c.consecutiveDowngradeEligible[app.Id] = 0
				c.lastReconfigsMu.Unlock()

				log.Printf("No runtime reported for app %s for %d consecutive intervals, downgrading to minimal layout.", app.Id, count)
				nextLayoutKey, err := c.handleLayoutChange(app, layoutDowngradePath, false)
				if err != nil {
					log.Printf("Error downgrading app %s to minimal layout: %v", app.Id, err)
					continue
				}
				if nextLayoutKey != "" {
					log.Printf("App %s transitioned to layout %s due to missing runtimes. Deploying...", app.Id, nextLayoutKey)
					c.lastReconfigsMu.Lock()
					c.lastReconfigs[app.Id] = time.Now()
					c.lastReconfigsMu.Unlock()
				}
			}
		}
	}
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

	compRuntimeMap := make(map[string]int) // component -> runtime in ms
	for _, comp := range app.Components {
		compRuntimeMap[comp.Name] = comp.Runtime
	}
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
					// A reused deployment keeps its old min-scale: raise it on upgrades and set standby deployments to the standby level
					if isUpgrade || standby {
						minScale := compositionInfo.RequiredReplicas
						if standby && c.prewarmMode != "full" {
							minScale = 1
						}
						if err := c.composer.SetDeploymentMinScale(dep, minScale); err != nil {
							log.Printf("Failed to set min-scale of reused deployment %s: %v", dep.Id, err)
						}
					}
					resultChan <- depResult{depKey, dep, nil}
					return
				}
			}

			// otherwise, create a new deployment, routing table will be set later once all deployments ids are known
			log.Printf("No existing deployment found for node %s, creating new", node)

			emptyRT := make(RoutingTable)
			minReplicas := 1
			// Upgrades and "full" standby layouts start with all required replicas warm
			if isUpgrade || (standby && c.prewarmMode == "full") {
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

	// Measure and log reconfiguration end-to-end latency
	c.lastReconfigsMu.Lock()
	startTime, ok := c.reconfigStartTimes[appId]
	if ok {
		duration := time.Since(startTime)
		durationMs := float64(duration) / float64(time.Millisecond)
		event := ReconfigEvent{
			EventType:  "RECONFIG_COMPLETE",
			AppID:      appId,
			EventTime:  startTime.UnixMilli(),
			DurationMs: durationMs,
		}

		jsonBytes, err := json.Marshal(event)
		if err != nil {
			log.Printf("Error marshalling reconfig event: %v", err)
		} else {
			log.Printf("[RECONFIG_EVENT] %s", string(jsonBytes))
		}

		delete(c.reconfigStartTimes, appId)
	} else {
		log.Printf("Warning: Missing start time for app %s in reconfigStartTime map.", appId)
	}
	c.lastReconfigsMu.Unlock()

	// With prewarm, keep every deployment that belongs to any layout candidate (fc@node), so the
	// previously active layout stays warm for the next switch
	keepKeys := make(map[string]bool)
	if c.prewarmMode != "off" {
		for _, candidate := range app.LayoutCandidates {
			for node, ci := range candidate {
				names := make([]string, len(ci.ComponentProfiles))
				for i, cp := range ci.ComponentProfiles {
					names[i] = cp.Name
				}
				if fc, ok := fcByKey[componentsKey(names)]; ok {
					keepKeys[fc.Id+"@"+node] = true
				}
			}
		}
	}

	// cleanup: remove unused deployments asynchronously
	go func() {
		time.Sleep(10 * time.Second)
		for _, fc := range app.Compositions {
			var kept []*Deployment
			for _, d := range fc.Deployments {
				if activeDepIDs[d.Id] || referencedDepIDs[d.Id] || keepKeys[fc.Id+"@"+d.Node] {
					kept = append(kept, d)
					continue
				}
				log.Printf("Deleting old deployment %s (fc %s on node %s)", d.Id, fc.Id, d.Node)
				delChan, err := c.composer.DeleteFcDeployment(d.Id)
				if err != nil {
					log.Printf("Failed to delete deployment %s: %v", d.Id, err)
					kept = append(kept, d)
					continue
				}
				r := <-delChan
				if r.Err != nil {
					log.Printf("Failed to delete deployment %s: %v", d.Id, r.Err)
					kept = append(kept, d)
				} else {
					log.Printf("Deleted deployment %s", d.Id)
					logPhase(appId, "old_deleted", "", d.Id, 0)
				}
			}
			fc.Deployments = kept
		}
	}()

	return nil
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

// ensureStandbyLayouts deploys every inactive layout candidate of the app without traffic,
// so that a later switch only needs rerouting (no pod startup)
func (c *latencyController) ensureStandbyLayouts(app *FunctionApp) {
	if c.prewarmMode == "off" {
		return
	}
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


// layoutCapacity is the arrival rate (req/s) a layout is sized for at the model's target utilization:
// the minimum over its compositions of replicas * concurrency * utilization * 1000 / runtime of the composition
func (c *latencyController) layoutCapacity(layout Layout) float64 {
	capacity := math.Inf(1)
	for _, ci := range layout {
		runtime := 0
		for _, cp := range ci.ComponentProfiles {
			runtime += cp.Runtime
		}
		if runtime <= 0 {
			continue
		}
		concurrency := ci.TargetConcurrency
		if concurrency < 1 {
			concurrency = 1
		}
		compCapacity := float64(ci.RequiredReplicas*concurrency) * c.targetUtilization * 1000 / float64(runtime)
		if compCapacity < capacity {
			capacity = compCapacity
		}
	}
	return capacity
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
	threshold := c.boostRateFactor * c.layoutCapacity(app.LayoutCandidates[app.ActiveLayoutKey])
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