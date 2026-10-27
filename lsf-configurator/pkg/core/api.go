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

type KnClient interface {
	Init(ctx context.Context, fc FunctionComposition, runtime, sourcePath string) (string, error)
	Deploy(ctx context.Context, deployment Deployment, image, appId string) error
	Delete(ctx context.Context, deployment Deployment) error
}

type RoutingClient interface {
	SetRoutingTable(deployment Deployment) error
	DeleteRoutingTable(deploymentId string) error
}

type Builder interface {
	Build(ctx context.Context, fc FunctionComposition, buildDir string) error
	NotifyBuildFinished()
}

type AlertClient interface {
	EnsureAlertConnector(ctx context.Context, connectorName, indexName string) (string, error)
	CreateOrUpdateRule(ctx context.Context, serviceName string, latencyThresholdMs int) (string, error)
}

type MetricsReader interface {
	QueryNodeMetrics() ([]NodeMetrics, error)
	Query95thPercentileAppRuntimes(timeRangeGte string) (map[string]float64, map[string]int, error)
	QueryAverageAppRuntimes(timeRangeGte string) (map[string]float64, map[string]int, error)
	EnsureIndex(ctx context.Context, indexName string) error
}

type Controller interface {
	Start(ctx context.Context) error
	RegisterFunctionApp(creationData FunctionAppCreationData) (*FunctionApp, error)
}

type LayoutCalculator interface {
	CalculateLayout(scenario LayoutScenario) (Layout, error)
}

type ScenarioManager interface {
	GenerateLayoutCandidates(
		components []Component,
		links []ComponentLink,
		appLatencyReq int,
		memoryAvailable int) (map[string]Layout, error)
}

type ResultsClient interface {
	GetAppResults(appId string, count int) ([]AppResult, error)
}

type DNSClient interface {
	EnsureDNSRecord(ctx context.Context, namespace, appName, targetServiceName string) error
	DeleteDNSRecord(ctx context.Context, namespace, appName string) error
}

type ScaleClient interface {
	SetMinScale(ctx context.Context, namespace, serviceName string, minScale int) error
}
