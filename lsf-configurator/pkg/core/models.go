package core

import (
	"encoding/json"
	"mime/multipart"
)

type Component struct {
	Name    string   `json:"name"`
	Memory  int      `json:"memory"`  // in MB
	Runtime int      `json:"runtime"` // The execution time of the component in milliseconds
	Files   []string `json:"files"`   // List of files required by the component
}

type ComponentLink struct {
	From           string         `json:"from"`
	To             string         `json:"to"`
	InvocationRate InvocationRate `json:"invocation_rate"`
	DataDelay      int            `json:"data_delay"` // Delay caused by data transfer in milliseconds
}

type InvocationRate struct {
	Min float64 `json:"min"` // in requests per second
	Max float64 `json:"max"`
}

type FunctionApp struct {
	Id               string                 `json:"id"`
	Name             string                 `json:"name"`
	Runtime          string                 `json:"runtime"`
	Components       []Component            `json:"components"`
	Links            []ComponentLink        `json:"links"`
	Files            []string               `json:"files"`
	Compositions     []*FunctionComposition `json:"compositions"`
	SourcePath       string                 `json:"source_path"`
	LatencyLimit     int                    `json:"latency_limit"`     // in milliseconds
	LayoutCandidates map[string]Layout      `json:"layout_candidates"` // Key: LayoutKey, Value: Layout
	ActiveLayoutKey  string                 `json:"active_layout_key"`
}

type BuildStatus string

const (
	BuildStatusPending BuildStatus = "pending"
	BuildStatusBuilt   BuildStatus = "built"
	BuildStatusError   BuildStatus = "error"
)

type FunctionComposition struct {
	Id            string      `json:"id"`
	FunctionAppId string      `json:"function_app_id"`
	Components    []string    `json:"components"`
	Files         []string    `json:"files"`
	Status        BuildStatus `json:"status"`
	Build         `json:"build"`
	Deployments   []*Deployment `json:"deployments"`
}

type DeploymentStatus string

const (
	DeploymentStatusWaitingForBuild DeploymentStatus = "waiting_for_build"
	DeploymentStatusPending         DeploymentStatus = "pending"
	DeploymentStatusDeployed        DeploymentStatus = "deployed"
	DeploymentStatusError           DeploymentStatus = "error"
)

type Deployment struct {
	Id                    string           `json:"id"`
	FunctionCompositionId string           `json:"function_composition_id"`
	Node                  string           `json:"node"`
	Namespace             string           `json:"namespace"`
	RoutingTable          RoutingTable     `json:"routing_table"`
	Status                DeploymentStatus `json:"status"`
	Scale                 Scale            `json:"scale"`
	Resources             Resources        `json:"resources"`
}

type Scale struct {
	MinReplicas       int `json:"min_replicas"`
	MaxReplicas       int `json:"max_replicas"`
	TargetConcurrency int `json:"target_concurrency"`
}

type Resources struct {
	Memory int `json:"memory"` // in MB
	CPU    int `json:"cpu"`    // in millicores
}

type Route struct {
	To       string `json:"to"`       // Component name
	Function string `json:"function"` // Deployment of a function composition
}

type RoutingTable map[string][]Route // Key: Component name

type Build struct {
	Image     string `json:"image"`
	Timestamp string `json:"timestamp"`
}

type FunctionAppCreationData struct {
	Components   []Component
	Links        []ComponentLink
	UploadDir    string
	Files        []*multipart.FileHeader
	AppName      string
	Runtime      string
	LatencyLimit int
}

type LayoutScenario struct {
	LatencyRequirement          int
	AvailableNodeMemory         int
	Profiles                    []ComponentProfile
	Links                       []ScenarioLink
	ComponentMCPUAllocation     int
	OverheadMCPUAllocation      int
	TargetConcurrency           int
	InvocationSharedMemoryRatio float64
	TargetUtilization           float64
	MemorySafetyBufferRatio     float64
}

type ComponentProfile struct {
	Name             string `json:"name"`
	Runtime          int    `json:"runtime"`
	Memory           int    `json:"memory"`
	RequiredReplicas int    `json:"required_replicas"`
}

func (cp *ComponentProfile) EffectiveMemory(invocationSharedMemoryRatio float64, targetConcurrency int, memorySafetyBufferRatio float64) int {
	sharedPart := invocationSharedMemoryRatio
	perRequestPart := 1.0 - sharedPart

	// total = shared portion + per-request portion * concurrency
	effectiveMemory := float64(cp.Memory) * (sharedPart + perRequestPart*float64(targetConcurrency))

	// Add safety buffer as a percentage of the effective memory
	effectiveMemory = effectiveMemory * (1.0 + memorySafetyBufferRatio)

	return int(effectiveMemory)
}

type ScenarioLink struct {
	From           string
	To             string
	InvocationRate float64
	DataDelay      int
}

type Layout = map[string]CompositionInfo // Key: Node name, Value: CompositionInfo assigned to that node

type CompositionInfo struct {
	ComponentProfiles []ComponentProfile
	RequiredReplicas  int
	Memory            int
	MCPU              int
	TargetConcurrency int
}

func (c CompositionInfo) TotalMemory() int {
	return c.Memory * c.RequiredReplicas
}

type AppResult struct {
	Timestamp string          `json:"timestamp"`
	Event     json.RawMessage `json:"event"`
}
