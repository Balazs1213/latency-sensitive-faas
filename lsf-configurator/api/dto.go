package api

import "lsf-configurator/pkg/core"

// FunctionAppCreateDto is the JSON part of a function app creation request. A platform-managed app is registered
// with the latency controller instead of being created directly by the composer.
type FunctionAppCreateDto struct {
	Name            string               `json:"name"`
	Runtime         string               `json:"runtime"`
	Components      []core.Component     `json:"components"`
	Links           []core.ComponentLink `json:"links"`
	LatencyLimit    int                  `json:"latency_limit"`
	PlatformManaged bool                 `json:"platform_managed"`
}

// FunctionCompositionCreateDto is the request body for adding a function composition to an app. A non-empty Image
// is used as the composition's image, and no build is started.
type FunctionCompositionCreateDto struct {
	FunctionAppId string   `json:"function_app_id"`
	Components    []string `json:"components"`
	Image         string   `json:"image"`
}

// DeploymentCreateDto is the request body for creating a deployment of a function composition.
type DeploymentCreateDto struct {
	FunctionCompositionId string            `json:"function_composition_id"`
	Node                  string            `json:"node"`
	Namespace             string            `json:"namespace"`
	RoutingTable          core.RoutingTable `json:"routing_table"`
}

// BulkCreateRequest is the JSON part of a bulk creation request: an app with its function compositions and
// deployments, which refer to each other by temporary IDs.
type BulkCreateRequest struct {
	FunctionApp          FunctionAppCreateDto               `json:"function_app"`
	FunctionCompositions []FunctionCompositionBulkCreateDto `json:"function_compositions"`
	Deployments          []DeploymentBulkCreateDto          `json:"deployments"`
}

// FunctionCompositionBulkCreateDto is a function composition in a BulkCreateRequest; deployments refer to it by
// its TempId.
type FunctionCompositionBulkCreateDto struct {
	TempId     string   `json:"id"`
	Components []string `json:"components"`
	Files      []string `json:"files"`
	Image      string   `json:"image"`
}

// DeploymentBulkCreateDto is a deployment in a BulkCreateRequest. It refers to its composition by temporary ID, and
// the non-local routes of its routing table refer to other deployments by their temporary IDs.
type DeploymentBulkCreateDto struct {
	TempId                    string            `json:"id"`
	TempFunctionCompositionId string            `json:"function_composition_id"`
	Node                      string            `json:"node"`
	Namespace                 string            `json:"namespace"`
	RoutingTable              core.RoutingTable `json:"routing_table"`
}

// UpdateLatencyLimitRequest is the request body for changing the latency limit of an app, in milliseconds.
type UpdateLatencyLimitRequest struct {
	LatencyLimit int `json:"latency_limit"`
}
