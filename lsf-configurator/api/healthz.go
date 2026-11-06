package api

import "net/http"

const (
	HealthzPath = "/healthz"
)

func HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	// Best-effort response write: a failure usually means the client has gone away.
	_, _ = w.Write([]byte("Ok"))
}
