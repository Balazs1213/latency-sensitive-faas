package api

import (
	"net/http"
	"os"
)

func SpaHandler(publicDir string) http.Handler {
	fs := http.FileServer(http.Dir(publicDir))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Try serving the requested file
		path := publicDir + r.URL.Path
		_, err := os.Stat(path)
		if os.IsNotExist(err) {
			// If file does not exist, serve index.html
			http.ServeFile(w, r, publicDir+"/index.html")
			return
		}
		fs.ServeHTTP(w, r)
	})
}
