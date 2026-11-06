package api

import (
	"encoding/json"
	"log"
	"lsf-configurator/pkg/config"
	"lsf-configurator/pkg/core"
	"net/http"
)

const (
	MetricsPath = "/metrics"
)

type HandlerMetrics struct {
	metricsClient core.MetricsReader
	conf          config.Configuration
	mux           *http.ServeMux
}

func NewHandlerMetrics(metricsClient core.MetricsReader, conf config.Configuration) *HandlerMetrics {
	h := &HandlerMetrics{
		metricsClient: metricsClient,
		conf:          conf,
		mux:           http.NewServeMux(),
	}

	h.mux.HandleFunc("GET /apps", h.getAllAppMetrics)

	return h
}

func (h *HandlerMetrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	LoggingMiddleware(h.mux).ServeHTTP(w, r)
}

func (h *HandlerMetrics) getAllAppMetrics(w http.ResponseWriter, r *http.Request) {
	metrics, _, err := h.metricsClient.Query95thPercentileAppRuntimes("now-5m")
	if err != nil {
		log.Printf("Error querying metrics: %v", err)
		http.Error(w, "Failed to query metrics", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(metrics); err != nil {
		log.Printf("Error writing metrics response: %v", err)
	}
}
