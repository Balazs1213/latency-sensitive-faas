package api

import "net/http"

const (
	HealthzPath = "/healthz"
)

func HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ok"))
}
