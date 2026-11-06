package api

import (
	"encoding/json"
	"lsf-configurator/pkg/config"
	"lsf-configurator/pkg/core"
	"net/http"
)

const (
	AppsPath = "/function_apps"
)

type HandlerApps struct {
	composer   *core.Composer
	controller core.Controller
	conf       config.Configuration
	mux        *http.ServeMux
}

func NewHandlerApps(composer *core.Composer, controller core.Controller, conf config.Configuration) *HandlerApps {
	h := &HandlerApps{
		composer:   composer,
		controller: controller,
		conf:       conf,
		mux:        http.NewServeMux(),
	}

	h.mux.HandleFunc("GET /", h.list)
	h.mux.HandleFunc("GET /{id}", h.get)
	h.mux.HandleFunc("POST /", h.create)
	h.mux.HandleFunc("POST /bulk", h.bulkCreate)
	h.mux.HandleFunc("DELETE /{id}", h.delete)
	h.mux.HandleFunc("PATCH /{id}/latency_limit", h.updateLatencyLimit)

	return h
}

func (h *HandlerApps) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	LoggingMiddleware(h.mux).ServeHTTP(w, r)
}

func (h *HandlerApps) list(w http.ResponseWriter, r *http.Request) {
	apps, err := h.composer.ListFunctionApps()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(apps)
}

func (h *HandlerApps) get(w http.ResponseWriter, r *http.Request) {
	appId := r.PathValue("id")
	app, err := h.composer.GetFunctionApp(appId)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(app)
}

func (h *HandlerApps) create(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(10 << 20) // 10MB limit
	if err != nil {
		http.Error(w, "Error parsing multipart form", http.StatusBadRequest)
		return
	}

	jsonStr := r.FormValue("json")
	var payload FunctionAppCreateDto
	if err := json.Unmarshal([]byte(jsonStr), &payload); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	files := r.MultipartForm.File["files"]

	if len(files) == 0 {
		http.Error(w, "No files were uploaded", http.StatusBadRequest)
		return
	}

	data := core.FunctionAppCreationData{
		Components:   payload.Components,
		Links:        payload.Links,
		UploadDir:    h.conf.UploadDir,
		Files:        files,
		AppName:      payload.Name,
		Runtime:      payload.Runtime,
		LatencyLimit: payload.LatencyLimit,
	}

	if payload.PlatformManaged {
		_, err = h.controller.RegisterFunctionApp(data)
	} else {
		_, err = h.composer.CreateFunctionApp(data)
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *HandlerApps) bulkCreate(w http.ResponseWriter, r *http.Request) {
	r.ParseMultipartForm(10 << 20) // 10MB limit

	jsonStr := r.FormValue("json")
	var payload BulkCreateRequest
	if err := json.Unmarshal([]byte(jsonStr), &payload); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	files := r.MultipartForm.File["files"]
	if len(files) == 0 {
		http.Error(w, "No files were uploaded", http.StatusBadRequest)
		return
	}

	// Track created resources
	var createdApp *core.FunctionApp
	var createdCompositions []*core.FunctionComposition
	var createdDeployments []*core.Deployment

	// Create the function app
	data := core.FunctionAppCreationData{
		Components:   payload.FunctionApp.Components,
		Links:        payload.FunctionApp.Links,
		UploadDir:    h.conf.UploadDir,
		Files:        files,
		AppName:      payload.FunctionApp.Name,
		Runtime:      payload.FunctionApp.Runtime,
		LatencyLimit: payload.FunctionApp.LatencyLimit,
	}

	var app *core.FunctionApp
	var err error
	if payload.FunctionApp.PlatformManaged {
		app, err = h.controller.RegisterFunctionApp(data)
	} else {
		app, err = h.composer.CreateFunctionApp(data)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	createdApp = app

	// Map temporary IDs to real IDs for function compositions
	compositionIdMap := make(map[string]string)

	for _, composition := range payload.FunctionCompositions {
		fc, err := h.composer.AddFunctionComposition(app.Id, composition.Components, composition.Image)
		if err != nil {
			h.composer.RollbackBulk(createdApp, createdCompositions, createdDeployments)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		compositionIdMap[composition.TempId] = fc.Id
		createdCompositions = append(createdCompositions, fc)
	}

	// Map temporary deployment IDs to real deployment IDs
	deploymentIdMap := make(map[string]string)

	for _, deployment := range payload.Deployments {
		realCompositionId, exists := compositionIdMap[deployment.TempFunctionCompositionId]
		if !exists {
			h.composer.RollbackBulk(createdApp, createdCompositions, createdDeployments)
			http.Error(w, "Invalid FunctionCompositionId in deployment", http.StatusBadRequest)
			return
		}

		// Create deployment with an empty routing table
		dep, _, err := h.composer.CreateFcDeployment(realCompositionId, deployment.Namespace,
			deployment.Node, core.RoutingTable{}, core.Scale{MinReplicas: 0, MaxReplicas: 0}, core.Resources{Memory: 512, CPU: 1000})
		if err != nil {
			h.composer.RollbackBulk(createdApp, createdCompositions, createdDeployments)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		deploymentIdMap[deployment.TempId] = dep.Id
		createdDeployments = append(createdDeployments, dep)
	}

	// Translate routing tables and set them
	for _, deployment := range payload.Deployments {
		realDeploymentId, exists := deploymentIdMap[deployment.TempId]
		if !exists {
			h.composer.RollbackBulk(createdApp, createdCompositions, createdDeployments)
			http.Error(w, "Invalid deployment ID", http.StatusBadRequest)
			return
		}

		translatedRoutingTable := core.RoutingTable{}
		for component, routes := range deployment.RoutingTable {
			var translatedRoutes []core.Route
			for _, route := range routes {
				if route.Function == "local" {
					// Leave "local" unchanged
					translatedRoutes = append(translatedRoutes, core.Route{
						To:       route.To,
						Function: "local",
					})
					continue
				}

				translatedDeploymentId, exists := deploymentIdMap[route.Function]
				if !exists {
					h.composer.RollbackBulk(createdApp, createdCompositions, createdDeployments)
					http.Error(w, "Invalid function reference in routing table", http.StatusBadRequest)
					return
				}
				translatedRoutes = append(translatedRoutes, core.Route{
					To:       route.To,
					Function: translatedDeploymentId,
				})
			}
			translatedRoutingTable[component] = translatedRoutes
		}

		if err := h.composer.SetRoutingTable(realDeploymentId, translatedRoutingTable); err != nil {
			h.composer.RollbackBulk(createdApp, createdCompositions, createdDeployments)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (h *HandlerApps) delete(w http.ResponseWriter, r *http.Request) {
	appId := r.PathValue("id")
	err := h.composer.DeleteFunctionApp(appId)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *HandlerApps) updateLatencyLimit(w http.ResponseWriter, r *http.Request) {
	appId := r.PathValue("id")
	var req UpdateLatencyLimitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if req.LatencyLimit <= 0 {
		http.Error(w, "Latency limit must be positive", http.StatusBadRequest)
		return
	}

	app, err := h.composer.GetFunctionApp(appId)
	if err != nil || app == nil {
		http.Error(w, "App not found", http.StatusNotFound)
		return
	}

	app.LatencyLimit = req.LatencyLimit
	if err := h.composer.UpdateFunctionApp(app); err != nil {
		http.Error(w, "Failed to update latency limit", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
