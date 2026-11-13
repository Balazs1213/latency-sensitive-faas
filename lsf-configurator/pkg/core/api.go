//TODO introduce sqlite
// https://chatgpt.com/share/688a4a14-4744-8009-aaad-be0ba6e82700
// add method which allows updating the fc configurations in a function app
// in this case, newly added function compositions should be added to the existing ones
// if more function compositions use the same components, they should be merged/or the image should be reused
// the deployment state should always be tracked, to know which ones are currently in use

package core

import (
	"context"
)

// KnClient prepares the build directory of a function composition and deploys and deletes deployments as Knative
// services.
type KnClient interface {
	Init(ctx context.Context, fc FunctionComposition, runtime, sourcePath string) (string, error)
	Deploy(ctx context.Context, deployment Deployment, image, appID string) error
	Delete(ctx context.Context, deployment Deployment) error
}

// RoutingClient stores and deletes the routing tables of deployments in the routing-table store.
type RoutingClient interface {
	SetRoutingTable(deployment Deployment) error
	DeleteRoutingTable(deploymentID string) error
}

// Builder builds the image of a function composition; NotifyBuildFinished is called when a build has finished.
type Builder interface {
	Build(ctx context.Context, fc FunctionComposition, buildDir string) error
	NotifyBuildFinished()
}

// AlertClient manages the alert connector and the latency alert rules of services in the alerting backend.
type AlertClient interface {
	EnsureAlertConnector(ctx context.Context, connectorName, indexName string) (string, error)
	CreateOrUpdateRule(ctx context.Context, serviceName string, latencyThresholdMs int) (string, error)
}

// ComponentRuntime is the measured processing time of one component: the average duration of its spans
type ComponentRuntime struct {
	AvgMs float64 // average span duration in milliseconds
	Count int     // number of spans the average is computed from
}

// MetricsReader reads node metrics and the measured latencies of apps and components from the metrics backend.
type MetricsReader interface {
	QueryNodeMetrics() ([]NodeMetrics, error)
	Query95thPercentileAppRuntimes(timeRangeGte string) (map[string]float64, map[string]int, error)
	QueryAverageAppRuntimes(timeRangeGte string) (map[string]float64, map[string]int, error)
	QueryComponentRuntimes(timeRangeGte string) (map[string]map[string]ComponentRuntime, error)
	EnsureIndex(ctx context.Context, indexName string) error
}

// Controller is the latency controller: it registers platform-managed function apps and, once started, keeps
// their latency under the limit by switching between their layout candidates.
type Controller interface {
	Start(ctx context.Context) error
	RegisterFunctionApp(creationData FunctionAppCreationData) (*FunctionApp, error)
}

// LayoutCalculator calculates the layout of a scenario: which components are composed together on which node,
// with how many replicas and what resources.
type LayoutCalculator interface {
	CalculateLayout(scenario LayoutScenario) (Layout, error)
}

// ScenarioManager generates the layout candidates of an app, keyed by layout key.
type ScenarioManager interface {
	GenerateLayoutCandidates(
		components []Component,
		links []ComponentLink,
		appLatencyReq int,
		memoryAvailable int) (map[string]Layout, error)
}

// ResultsClient reads the latest results of an app from the result store.
type ResultsClient interface {
	GetAppResults(appID string, count int) ([]AppResult, error)
}

// DNSClient manages the DNS record of an app's entry point, which points to the deployment receiving the app's
// requests.
type DNSClient interface {
	EnsureDNSRecord(ctx context.Context, namespace, appName, targetServiceName string) error
	DeleteDNSRecord(ctx context.Context, namespace, appName string) error
}

// ScaleClient sets the min-scale of Knative services, reads how many of their pods are ready and replaces their
// broken revisions.
type ScaleClient interface {
	SetMinScale(ctx context.Context, namespace, serviceName string, minScale int) error
	ReadyPods(ctx context.Context, namespace, serviceName string) (int, error)
	RepairRevision(ctx context.Context, namespace, serviceName string) (bool, error)
}
