package api

import "net/http"

// HealthzPath is the URL path of the health check endpoint.
const (
	HealthzPath = "/healthz"
)

// HealthCheckHandler answers every request with 200 OK.
func HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	// Best-effort response write: a failure usually means the client has gone away.
	_, _ = w.Write([]byte("Ok"))
}
