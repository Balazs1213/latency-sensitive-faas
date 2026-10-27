package core

import (
	"context"
	"encoding/json"
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
}

func NewController(composer *Composer, metrics MetricsReader, scenarioManager ScenarioManager,
	delay time.Duration, deployNamespace string, availableNodeMemoryGb int, aggMetricType MetricType,
	metricQueryTimeRange string, latencyDowngradeFactor float64) Controller {

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
	}
}

func (c *latencyController) Start(ctx context.Context) error {
	log.Println("Latency Controller started")
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
		err = c.deployLayout(appId, layout, false, reuseDeployments)
		if err != nil {
			log.Printf("Error deploying layout for app %s: %v", appId, err)
			return
		}
		log.Printf("Successfully deployed function app with layout %s: %v", appId, layout)
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

	go func() {
		if err := c.deployLayout(app.Id, nextLayout, isUpgrade, reuseDeployments); err != nil {
			log.Printf("Failed to deploy new layout for app %s: %v", app.Id, err)
		}
	}()

	log.Printf("App %s successfully transitioned to layout %s", app.Id, nextLayoutKey)
	return nextLayoutKey, nil
}

func (c *latencyController) deployLayout(appId string, layout Layout, isUpgrade bool, reuseFunctions bool) error {
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
					resultChan <- depResult{depKey, dep, nil}
					return
				}
			}

			// otherwise, create a new deployment, routing table will be set later once all deployments ids are known
			log.Printf("No existing deployment found for node %s, creating new", node)

			emptyRT := make(RoutingTable)
			minReplicas := 1
			// For upgrades, set minReplicas to requiredReplicas/2 to reduce cold starts
			if isUpgrade {
				// minReplicas = int(math.Ceil(float64(compositionInfo.RequiredReplicas) / 2))
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

	// cleanup: remove unused deployments asynchronously
	go func() {
		time.Sleep(10 * time.Second)
		for _, fc := range app.Compositions {
			var kept []*Deployment
			for _, d := range fc.Deployments {
				if activeDepIDs[d.Id] || referencedDepIDs[d.Id] {
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
