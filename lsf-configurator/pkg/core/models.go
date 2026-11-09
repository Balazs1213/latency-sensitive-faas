package core

import (
	"encoding/json"
	"mime/multipart"
)

// Component is a component of a function app: a handler with its profiled memory and runtime and the files it
// needs.
type Component struct {
	Name    string   `json:"name"`
	Memory  int      `json:"memory"`  // in MB
	Runtime int      `json:"runtime"` // The execution time of the component in milliseconds
	Files   []string `json:"files"`   // List of files required by the component
}

// ComponentLink is a call from one component to another in the call graph of an app, with the expected range of
// its invocation rate and its data transfer delay.
type ComponentLink struct {
	From           string         `json:"from"`
	To             string         `json:"to"`
	InvocationRate InvocationRate `json:"invocation_rate"`
	DataDelay      int            `json:"data_delay"` // Delay caused by data transfer in milliseconds
}

// InvocationRate is the expected range of the invocation rate of a link.
type InvocationRate struct {
	Min float64 `json:"min"` // in requests per second
	Max float64 `json:"max"`
}

// FunctionApp is an application described by its components and their call graph, with its function
// compositions, its latency limit, its layout candidates and the key of the active layout.
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

// BuildStatus is the state of the image build of a function composition.
type BuildStatus string

// Build states of a function composition.
const (
	BuildStatusPending BuildStatus = "pending"
	BuildStatusBuilt   BuildStatus = "built"
	BuildStatusError   BuildStatus = "error"
)

// FunctionComposition is a group of components of an app that are built into one image and deployed together.
type FunctionComposition struct {
	Id            string      `json:"id"`
	FunctionAppId string      `json:"function_app_id"`
	Components    []string    `json:"components"`
	Files         []string    `json:"files"`
	Status        BuildStatus `json:"status"`
	Build         `json:"build"`
	Deployments   []*Deployment `json:"deployments"`
}

// DeploymentStatus is the state of a deployment.
type DeploymentStatus string

// Deployment states: waiting for the build of the composition, being deployed, deployed, or failed.
const (
	DeploymentStatusWaitingForBuild DeploymentStatus = "waiting_for_build"
	DeploymentStatusPending         DeploymentStatus = "pending"
	DeploymentStatusDeployed        DeploymentStatus = "deployed"
	DeploymentStatusError           DeploymentStatus = "error"
)

// Deployment is a function composition deployed as a Knative service on a node, with its routing table, scale and
// resources. Its ID is also the name of the Knative service and the key of its routing table.
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

// Scale holds the autoscaling bounds and the target concurrency of a deployment.
type Scale struct {
	MinReplicas       int `json:"min_replicas"`
	MaxReplicas       int `json:"max_replicas"`
	TargetConcurrency int `json:"target_concurrency"`
}

// Resources holds the memory and CPU of a deployment.
type Resources struct {
	Memory int `json:"memory"` // in MB
	CPU    int `json:"cpu"`    // in millicores
}

// Route sends the output of a component to the next component, either in the same composition ("local") or in
// another deployment.
type Route struct {
	To       string `json:"to"`       // Component name
	Function string `json:"function"` // Deployment of a function composition
}

// RoutingTable holds the routes of the outputs of each component of a deployment.
type RoutingTable map[string][]Route // Key: Component name

// Build holds the image of a function composition and the time it was built.
type Build struct {
	Image     string `json:"image"`
	Timestamp string `json:"timestamp"`
}

// FunctionAppCreationData is the input for creating a function app: its description and its uploaded files.
type FunctionAppCreationData struct {
	Components   []Component
	Links        []ComponentLink
	UploadDir    string
	Files        []*multipart.FileHeader
	AppName      string
	Runtime      string
	LatencyLimit int
}

// LayoutScenario is the input of a layout calculation: the component profiles and links of an app at one
// invocation rate, its latency requirement, the available node memory and the parameters of the resource model.
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

// ComponentProfile is a component as seen by the layout calculation: its runtime, memory and required replicas.
type ComponentProfile struct {
	Name             string `json:"name"`
	Runtime          int    `json:"runtime"`
	Memory           int    `json:"memory"`
	RequiredReplicas int    `json:"required_replicas"`
}

// EffectiveMemory returns the memory that one replica of the component needs at the given concurrency: the shared
// part once, the per-request part for each concurrent request, plus the safety buffer.
func (cp *ComponentProfile) EffectiveMemory(invocationSharedMemoryRatio float64, targetConcurrency int, memorySafetyBufferRatio float64) int {
	sharedPart := invocationSharedMemoryRatio
	perRequestPart := 1.0 - sharedPart

	// total = shared portion + per-request portion * concurrency
	effectiveMemory := float64(cp.Memory) * (sharedPart + perRequestPart*float64(targetConcurrency))

	// Add safety buffer as a percentage of the effective memory
	effectiveMemory = effectiveMemory * (1.0 + memorySafetyBufferRatio)

	return int(effectiveMemory)
}

// ScenarioLink is a component link in a LayoutScenario, with one invocation rate chosen from the link's range.
type ScenarioLink struct {
	From           string
	To             string
	InvocationRate float64
	DataDelay      int
}

// Layout assigns a composition to each node: the components that run together there, with their replicas and
// resources.
type Layout = map[string]CompositionInfo // Key: Node name, Value: CompositionInfo assigned to that node

// CompositionInfo is the composition placed on one node by a layout, with its replica count and the resources of
// one replica.
type CompositionInfo struct {
	ComponentProfiles []ComponentProfile
	RequiredReplicas  int
	Memory            int
	MCPU              int
	TargetConcurrency int
}

// TotalMemory returns the memory of all replicas of the composition.
func (c CompositionInfo) TotalMemory() int {
	return c.Memory * c.RequiredReplicas
}

// AppResult is a final result of an app read from the result store, with its timestamp.
type AppResult struct {
	Timestamp string          `json:"timestamp"`
	Event     json.RawMessage `json:"event"`
}
