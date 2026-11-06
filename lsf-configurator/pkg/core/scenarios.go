package core

import (
	"fmt"
	"sort"
)

const (
	LayoutKeyMin = "min_rate"
	// LayoutKeyAvg = "avg_rate"
	LayoutKeyMax = "max_rate"
)

var layoutUpgradePath = map[string]string{
	// LayoutKeyMin: LayoutKeyAvg,
	// LayoutKeyAvg: LayoutKeyMax,
	LayoutKeyMin: LayoutKeyMax,
	LayoutKeyMax: "",
}

var layoutDowngradePath = map[string]string{
	// LayoutKeyMax: LayoutKeyAvg,
	// LayoutKeyAvg: LayoutKeyMin,
	LayoutKeyMax: LayoutKeyMin,
	LayoutKeyMin: "",
}

type scenarioManager struct {
	calculator                  LayoutCalculator
	targetConcurrency           int
	invocationSharedMemoryRatio float64
	componentMCPUAllocation     int
	overheadMCPUAllocation      int
	targetUtilization           float64
	memorySafetyBufferRatio     float64
}

func NewScenarioManager(calculator LayoutCalculator, targetConcurrency int, invocationSharedMemoryRatio float64,
	componentMCPUAllocation int, overheadMCPUAllocation int, targetUtilization float64, memorySafetyBufferRatio float64) ScenarioManager {
	return &scenarioManager{
		calculator:                  calculator,
		targetConcurrency:           targetConcurrency,
		invocationSharedMemoryRatio: invocationSharedMemoryRatio,
		componentMCPUAllocation:     componentMCPUAllocation,
		overheadMCPUAllocation:      overheadMCPUAllocation,
		targetUtilization:           targetUtilization,
		memorySafetyBufferRatio:     memorySafetyBufferRatio,
	}
}

func (sm *scenarioManager) GenerateLayoutCandidates(
	components []Component,
	links []ComponentLink,
	appLatencyReq int,
	memoryAvailable int) (map[string]Layout, error) {
	rates := []struct {
		Name     string
		Key      string
		RateFunc func(min, max float64) float64
	}{
		{
			Name:     "Minimum Rate Layout",
			Key:      LayoutKeyMin,
			RateFunc: func(min, max float64) float64 { return min },
		},
		// {
		// 	Name:     "Average Rate Layout",
		// 	Key:      LayoutKeyAvg,
		// 	RateFunc: func(min, max float64) float64 { return (min + max) * 0.5 },
		// },
		{
			Name:     "Maximum Rate Layout",
			Key:      LayoutKeyMax,
			RateFunc: func(min, max float64) float64 { return max },
		},
	}

	candidates := make(map[string]Layout, len(rates))

	compMap := make(map[string]Component)
	for _, c := range components {
		compMap[c.Name] = c
	}

	for _, r := range rates {
		layoutScenario := sm.buildLayoutScenario(compMap, links, r.RateFunc)
		layoutScenario.LatencyRequirement = appLatencyReq
		layoutScenario.AvailableNodeMemory = memoryAvailable
		layoutScenario.TargetConcurrency = sm.targetConcurrency
		layoutScenario.InvocationSharedMemoryRatio = sm.invocationSharedMemoryRatio
		layoutScenario.ComponentMCPUAllocation = sm.componentMCPUAllocation
		layoutScenario.OverheadMCPUAllocation = sm.overheadMCPUAllocation
		layoutScenario.TargetUtilization = sm.targetUtilization
		layoutScenario.MemorySafetyBufferRatio = sm.memorySafetyBufferRatio

		layout, err := sm.calculator.CalculateLayout(*layoutScenario)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate layout for %s: %w", r.Name, err)
		}
		candidates[r.Key] = layout
	}
	return candidates, nil
}

func (sm *scenarioManager) buildLayoutScenario(
	compMap map[string]Component,
	links []ComponentLink,
	rateFunc func(min, max float64) float64) *LayoutScenario {

	sortedLinks := sortLinksByCallGraphOrder(links)

	scenarioLinks := make([]ScenarioLink, 0, len(sortedLinks))
	for _, link := range sortedLinks {
		rate := rateFunc(link.InvocationRate.Min, link.InvocationRate.Max)
		scenarioLinks = append(scenarioLinks, ScenarioLink{
			From:           link.From,
			To:             link.To,
			InvocationRate: rate,
			DataDelay:      link.DataDelay,
		})
	}

	sortedComponents := sortComponentsByCallGraphOrder(compMap, sortedLinks)
	profiles := make([]ComponentProfile, 0, len(compMap))
	for _, comp := range sortedComponents {
		comp := compMap[comp]
		profiles = append(profiles, ComponentProfile{
			Name:    comp.Name,
			Runtime: comp.Runtime,
			Memory:  comp.Memory,
			// start with 1 replica for each component, this will be adjusted by the layout calculator
			RequiredReplicas: 1,
		})
	}

	return &LayoutScenario{
		Profiles: profiles,
		Links:    scenarioLinks,
	}
}

func sortLinksByCallGraphOrder(links []ComponentLink) []ComponentLink {
	if len(links) == 0 {
		return nil
	}

	sorted := make([]ComponentLink, 0, len(links))

	outgoing := make(map[string][]int)    // from -> indices into links
	incomingCount := make(map[string]int) // node -> incoming count
	nodes := make(map[string]struct{})    // all nodes seen

	for i, l := range links {
		outgoing[l.From] = append(outgoing[l.From], i)
		incomingCount[l.To]++
		nodes[l.From] = struct{}{}
		nodes[l.To] = struct{}{}
	}

	// Sort outgoing lists deterministically by target name
	for from, idxs := range outgoing {
		sort.Slice(idxs, func(i, j int) bool {
			return links[idxs[i]].To < links[idxs[j]].To
		})
		outgoing[from] = idxs
	}

	// Collect source nodes (in-degree == 0)
	sources := make([]string, 0)
	for node := range nodes {
		if incomingCount[node] == 0 {
			sources = append(sources, node)
		}
	}
	sort.Strings(sources) // deterministic order for sources

	visitedLink := make([]bool, len(links))

	// Traverse from every source; BFS-style per source to preserve chain order
	queue := make([]string, 0, len(sources))
	queue = append(queue, sources...)

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		for _, idx := range outgoing[node] {
			if visitedLink[idx] {
				continue
			}
			// append this link in traversal order
			sorted = append(sorted, links[idx])
			visitedLink[idx] = true
			// enqueue target so its outgoing links are processed next
			queue = append(queue, links[idx].To)
		}
	}

	// If some links remain (cycles or otherwise), append them deterministically
	if len(sorted) < len(links) {
		remaining := make([]ComponentLink, 0, len(links)-len(sorted))
		for i, l := range links {
			if !visitedLink[i] {
				remaining = append(remaining, l)
			}
		}
		sort.Slice(remaining, func(i, j int) bool {
			if remaining[i].From == remaining[j].From {
				return remaining[i].To < remaining[j].To
			}
			return remaining[i].From < remaining[j].From
		})
		sorted = append(sorted, remaining...)
	}

	return sorted
}

func sortComponentsByCallGraphOrder(compMap map[string]Component, sortedLinks []ComponentLink) []string {
	componentOrder := make([]string, 0, len(compMap))
	seen := make(map[string]bool)

	// Add components in the order they appear in sorted links
	for _, link := range sortedLinks {
		if !seen[link.From] {
			componentOrder = append(componentOrder, link.From)
			seen[link.From] = true
		}
		if !seen[link.To] {
			componentOrder = append(componentOrder, link.To)
			seen[link.To] = true
		}
	}

	// Add any remaining components that don't appear in links (isolated components)
	remainingComponents := make([]string, 0)
	for name := range compMap {
		if !seen[name] {
			remainingComponents = append(remainingComponents, name)
		}
	}

	// Sort isolated components alphabetically for consistency
	sort.Strings(remainingComponents)
	componentOrder = append(componentOrder, remainingComponents...)

	return componentOrder
}
