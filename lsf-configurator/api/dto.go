package api

import "lsf-configurator/pkg/core"

type FunctionAppCreateDto struct {
	Name            string               `json:"name"`
	Runtime         string               `json:"runtime"`
	Components      []core.Component     `json:"components"`
	Links           []core.ComponentLink `json:"links"`
	LatencyLimit    int                  `json:"latency_limit"`
	PlatformManaged bool                 `json:"platform_managed"`
}

type FunctionCompositionCreateDto struct {
	FunctionAppId string   `json:"function_app_id"`
	Components    []string `json:"components"`
	Image         string   `json:"image"`
}

type DeploymentCreateDto struct {
	FunctionCompositionId string            `json:"function_composition_id"`
	Node                  string            `json:"node"`
	Namespace             string            `json:"namespace"`
	RoutingTable          core.RoutingTable `json:"routing_table"`
}

type BulkCreateRequest struct {
	FunctionApp          FunctionAppCreateDto               `json:"function_app"`
	FunctionCompositions []FunctionCompositionBulkCreateDto `json:"function_compositions"`
	Deployments          []DeploymentBulkCreateDto          `json:"deployments"`
}

type FunctionCompositionBulkCreateDto struct {
	TempId     string   `json:"id"`
	Components []string `json:"components"`
	Files      []string `json:"files"`
	Image      string   `json:"image"`
}

type DeploymentBulkCreateDto struct {
	TempId                    string            `json:"id"`
	TempFunctionCompositionId string            `json:"function_composition_id"`
	Node                      string            `json:"node"`
	Namespace                 string            `json:"namespace"`
	RoutingTable              core.RoutingTable `json:"routing_table"`
}

type UpdateLatencyLimitRequest struct {
	LatencyLimit int `json:"latency_limit"`
}
