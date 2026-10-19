package layout

import (
	"bytes"
	"encoding/json"
	"fmt"
	"lsf-configurator/pkg/core"
	"os/exec"
)

type slambucCalculator struct {
	pythonCmd     string
	script        string
	platformNodes []string
	platformDelay int
	maxIterations int
}

func NewLayoutCalculator(pythonCmd, script string, platformNodes []string, platformDelay int) core.LayoutCalculator {
	return &slambucCalculator{
		pythonCmd:     pythonCmd,
		script:        script,
		platformNodes: platformNodes,
		platformDelay: platformDelay,
		maxIterations: 10,
	}
}

func (c *slambucCalculator) CalculateLayout(scenario core.LayoutScenario) (core.Layout, error) {
	prevLayoutKey := ""
	// tracks max replicas seen per component to avoid oscillations
	maxReplicasSeen := initializeMaxReplicas(scenario.Profiles)

	for iter := 0; iter < c.maxIterations; iter++ {
		layout, optCost, latency, err := c.runSLAMBUC(scenario)
		if err != nil {
			return nil, fmt.Errorf("SLAMBUC iteration %d failed: %v", iter, err)
		}
		if latency < 0 {
			return nil, fmt.Errorf("no valid layout found within latency requirement")
		}

		layoutKey := fmt.Sprintf("%d-%d", len(layout), int(optCost))
		if layoutKey == prevLayoutKey {
			// Convergence reached: recalc final replicas for accuracy, because calculation always uses max seen replicas,
			// real count may be lower
			finalProfiles := c.estimateReplicasPerGroup(layout, scenario)
			scenario.Profiles = finalProfiles
			finalLayout := c.buildFinalLayout(layout, scenario)
			return finalLayout, nil
		}

		// Estimate replicas per composition group
		updatedProfiles := c.estimateReplicasPerGroup(layout, scenario)
		prevLayoutKey = layoutKey
		scenario.Profiles = updatedProfiles

		// Anti oscillation: always run next iteration with max replicas seen so far, so it is monotonous
		for i, cp := range updatedProfiles {
			if cp.RequiredReplicas > maxReplicasSeen[cp.Name] {
				maxReplicasSeen[cp.Name] = cp.RequiredReplicas
			}
			cp.RequiredReplicas = maxReplicasSeen[cp.Name]
			updatedProfiles[i] = cp
		}
	}

	return nil, fmt.Errorf("failed to converge layout after %d iterations", c.maxIterations)
}

func (c *slambucCalculator) runSLAMBUC(scenario core.LayoutScenario) (map[string][]core.ComponentProfile, float64, int, error) {
	idMap := make(map[string]int)
	profileMap := make(map[int]core.ComponentProfile)
	nodes := []map[string]interface{}{}

	// Add dummy root node with id 1
	nodes = append(nodes, map[string]interface{}{
		"id":      "P",
		"mem":     0,
		"runtime": 0,
	})

	for i, p := range scenario.Profiles {
		id := i + 1
		idMap[p.Name] = id
		profileMap[id] = p
		nodes = append(nodes, map[string]interface{}{
			"id": id,
			"mem": p.EffectiveMemory(
				scenario.InvocationSharedMemoryRatio,
				scenario.TargetConcurrency,
				scenario.MemorySafetyBufferRatio,
			) * p.RequiredReplicas,
			"runtime": p.Runtime,
		})
	}

	edges := []map[string]interface{}{}

	// Add edge from dummy root to first function profile
	edges = append(edges, map[string]interface{}{
		"from": "P",
		"to":   1,
		"attr": map[string]interface{}{
			"rate": scenario.Links[0].InvocationRate,
			"data": scenario.Links[0].DataDelay,
		},
	})

	for _, l := range scenario.Links {
		fromId, ok := idMap[l.From]
		if !ok {
			continue
		}
		toId, ok := idMap[l.To]
		if !ok {
			continue
		}
		edges = append(edges, map[string]interface{}{
			"from": fromId,
			"to":   toId,
			"attr": map[string]interface{}{
				"rate": l.InvocationRate,
				"data": l.DataDelay,
			},
		})
	}
	input := map[string]interface{}{
		"params": map[string]interface{}{
			"root":   1,
			"M":      scenario.AvailableNodeMemory,
			"L":      scenario.LatencyRequirement,
			"cp_end": len(scenario.Profiles),
			"delay":  c.platformDelay,
		},
		"nodes": nodes,
		"edges": edges,
	}

	jsonInput, err := json.Marshal(input)
	//log.Default().Printf("JSON Input: %s", string(jsonInput))
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to marshal JSON: %w", err)
	}
	// Run Python script
	cmd := exec.Command(c.pythonCmd, c.script)
	cmd.Stdin = bytes.NewReader(jsonInput)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, 0, 0, fmt.Errorf("python script failed: %v, stderr: %s", err, stderr.String())
	}

	// Parse JSON output
	var pyOutput struct {
		Layout  [][]int `json:"layout"`
		OptCost float64 `json:"opt_cost"`
		Latency int     `json:"latency"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &pyOutput); err != nil {
		return nil, 0, 0, fmt.Errorf("failed to parse python output: %w, stdout: %s", err, stdout.String())
	}

	if pyOutput.Latency < 0 {
		return nil, 0, 0, fmt.Errorf("no valid layout found within latency requirement")
	}

	if len(pyOutput.Layout) > len(c.platformNodes) {
		return nil, 0, 0, fmt.Errorf("insufficient memory: layout has more groups (%d) than platform nodes (%d)", len(pyOutput.Layout), len(c.platformNodes))
	}

	layout := make(map[string][]core.ComponentProfile)
	for i, group := range pyOutput.Layout {
		var groupProfiles []core.ComponentProfile
		for _, id := range group {
			if prof, ok := profileMap[id]; ok {
				groupProfiles = append(groupProfiles, prof)
			}
		}
		layout[c.platformNodes[i]] = groupProfiles
	}
	//log.Default().Printf("SLAMBUC layout result: %+v, cost: %f, latency: %d", layout, pyOutput.OptCost, pyOutput.Latency)
	return layout, pyOutput.OptCost, pyOutput.Latency, nil
}

func (c *slambucCalculator) estimateReplicasPerGroup(layout map[string][]core.ComponentProfile, scenario core.LayoutScenario) []core.ComponentProfile {
	updatedProfiles := make([]core.ComponentProfile, 0, len(scenario.Profiles))
	compMap := make(map[string]core.ComponentProfile)
	for _, cp := range scenario.Profiles {
		compMap[cp.Name] = cp
	}

	for _, node := range c.platformNodes {
		group := layout[node]
		if len(group) == 0 {
			continue
		}

		totalRuntime := 0
		totalMemory := 0
		for _, comp := range group {
			totalRuntime += comp.Runtime
			totalMemory += comp.Memory
		}
				// The entry component (first profile) receives the external input rate (first link's rate)
		entryName, entryRate := "", 0.0
		if len(scenario.Profiles) > 0 && len(scenario.Links) > 0 {
			entryName = scenario.Profiles[0].Name
			entryRate = scenario.Links[0].InvocationRate
		}
		arrivalRate := calculateTotalArrivalRate(group, scenario.Links, entryName, entryRate)
		replicas := calculateRequiredReplicas(totalRuntime, scenario.TargetConcurrency, arrivalRate, scenario.TargetUtilization)

		// assign updated replicas to each component in this composition
		for _, comp := range group {
			cp := compMap[comp.Name]
			cp.RequiredReplicas = replicas
			updatedProfiles = append(updatedProfiles, cp)
		}
	}

	return updatedProfiles
}

func (c *slambucCalculator) buildFinalLayout(
	layout map[string][]core.ComponentProfile,
	scenario core.LayoutScenario,
) core.Layout {
	finalLayout := make(core.Layout)

	profileMap := make(map[string]core.ComponentProfile)
	for _, p := range scenario.Profiles {
		profileMap[p.Name] = p
	}

	for node, comps := range layout {
		memory := 0
		var groupReplicas int

		finalComps := make([]core.ComponentProfile, 0, len(comps))
		for _, cp := range comps {
			fp := profileMap[cp.Name]
			finalComps = append(finalComps, fp)
			memory += fp.EffectiveMemory(scenario.InvocationSharedMemoryRatio, scenario.TargetConcurrency, scenario.MemorySafetyBufferRatio)
			if fp.RequiredReplicas > groupReplicas {
				groupReplicas = fp.RequiredReplicas
			}
		}

		finalLayout[node] = core.CompositionInfo{
			ComponentProfiles: finalComps,
			RequiredReplicas:  groupReplicas,
			Memory:            memory,
			MCPU:              (scenario.ComponentMCPUAllocation + scenario.OverheadMCPUAllocation) * scenario.TargetConcurrency,
			TargetConcurrency: scenario.TargetConcurrency,
		}
	}

	return finalLayout
}

func initializeMaxReplicas(profiles []core.ComponentProfile) map[string]int {
	maxMap := make(map[string]int)
	for _, cp := range profiles {
		maxMap[cp.Name] = cp.RequiredReplicas
	}
	return maxMap
}

func MBToGB(mb int) float64 {
	return float64(mb) / 1024
}
