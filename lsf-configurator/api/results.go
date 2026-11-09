package api

import (
	"encoding/json"
	"lsf-configurator/pkg/core"
	"net/http"
	"strconv"
)

// ResultsPath is the URL prefix of the results endpoint.
const ResultsPath = "/results"

// HandlerResults serves the latest results of an app from the result store.
type HandlerResults struct {
	resultsClient core.ResultsClient
	mux           *http.ServeMux
}

// NewHandlerResults creates a HandlerResults with its route registered on an internal mux.
func NewHandlerResults(resultsClient core.ResultsClient) *HandlerResults {
	h := &HandlerResults{
		resultsClient: resultsClient,
		mux:           http.NewServeMux(),
	}

	h.mux.HandleFunc("GET /{id}", h.get)

	return h
}

func (h *HandlerResults) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	LoggingMiddleware(h.mux).ServeHTTP(w, r)
}

func (h *HandlerResults) get(w http.ResponseWriter, r *http.Request) {
	appId := r.PathValue("id")
	count := r.URL.Query().Get("count")
	if count == "" {
		count = "10"
	}

	countInt, err := strconv.Atoi(count)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	results, err := h.resultsClient.GetAppResults(appId, countInt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	// Best-effort response write: a failure usually means the client has gone away.
	_ = json.NewEncoder(w).Encode(results)
}
