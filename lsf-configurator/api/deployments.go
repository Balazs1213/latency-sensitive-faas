package api

import (
	"encoding/json"
	"lsf-configurator/pkg/config"
	"lsf-configurator/pkg/core"
	"net/http"
)

const (
	DeploymentsPath = "/deployments"
)

type HandlerDeployments struct {
	composer *core.Composer
	conf     config.Configuration
	mux      *http.ServeMux
}

func NewHandlerDeployments(composer *core.Composer, conf config.Configuration) *HandlerDeployments {
	h := &HandlerDeployments{
		composer: composer,
		conf:     conf,
		mux:      http.NewServeMux(),
	}

	h.mux.HandleFunc("POST /", h.create)
	h.mux.HandleFunc("PUT /{deployment_id}/routing_table", h.putRoutingTable)

	return h
}

func (h *HandlerDeployments) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	LoggingMiddleware(h.mux).ServeHTTP(w, r)
}

func (h *HandlerDeployments) create(w http.ResponseWriter, r *http.Request) {
	var req DeploymentCreateDto

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	deployment, _, err := h.composer.CreateFcDeployment(req.FunctionCompositionId, req.Node,
		req.Namespace, req.RoutingTable, core.Scale{MinReplicas: 0, MaxReplicas: 0}, core.Resources{Memory: 512, CPU: 1000})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	// Best-effort response write: a failure usually means the client has gone away.
	_ = json.NewEncoder(w).Encode(deployment)
}

func (h *HandlerDeployments) putRoutingTable(w http.ResponseWriter, r *http.Request) {
	deploymentId := r.PathValue("deployment_id")

	var rt core.RoutingTable
	if err := json.NewDecoder(r.Body).Decode(&rt); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	err := h.composer.SetRoutingTable(deploymentId, rt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}
