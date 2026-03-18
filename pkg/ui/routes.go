package ui

import (
	"net/http"
	"strings"
)

func New(path string) http.Handler {
	vue := NewUIHandler(&Options{
		Path: func() string {
			if path == "" {
				return defaultPath
			}
			return path
		},
	})

	router := http.NewServeMux()

	router.Handle("/{$}", http.RedirectHandler("/dashboard/", http.StatusFound))
	router.Handle("/dashboard", http.RedirectHandler("/dashboard/", http.StatusFound))
	router.Handle("/dashboard/{$}", vue.IndexFile())
	router.Handle("/dashboard/", vue.IndexFileOnNotFound())
	router.Handle("/favicon.png", vue.ServeFaviconDashboard())
	router.Handle("/favicon.ico", vue.ServeFaviconDashboard())
	router.HandleFunc("/k8s/clusters/local/", func(rw http.ResponseWriter, req *http.Request) {
		url := strings.TrimPrefix(req.URL.Path, "/k8s/clusters/local")
		if url == "" {
			url = "/"
		}
		http.Redirect(rw, req, url, http.StatusFound)
	})

	return router
}
