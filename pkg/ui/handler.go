package ui

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rancher/apiserver/pkg/middleware"
	"github.com/sirupsen/logrus"
)

const (
	defaultPath = "./ui"
)

type StringSetting func() string
type BoolSetting func() bool

type Handler struct {
	pathSetting         func() string
	indexSetting        func() string
	releaseSetting      func() bool
	offlineSetting      func() string
	apiUIVersionSetting func() string
	middleware          func(http.Handler) http.Handler
	indexMiddleware     func(http.Handler) http.Handler

	downloadOnce    sync.Once
	downloadSuccess bool
}

type Options struct {
	// The location on disk of the UI files
	Path StringSetting
	// The HTTP URL of the index file to download
	Index StringSetting
	// Whether or not to run the UI offline, should return true/false/dynamic
	Offline StringSetting
	// Whether or not is it release, if true UI will run offline if set to dynamic
	ReleaseSetting BoolSetting
	// The version of API UI to use
	APIUIVersionSetting StringSetting
}

func NewUIHandler(opts *Options) *Handler {
	if opts == nil {
		opts = &Options{}
	}

	h := &Handler{
		indexSetting:        opts.Index,
		offlineSetting:      opts.Offline,
		pathSetting:         opts.Path,
		releaseSetting:      opts.ReleaseSetting,
		apiUIVersionSetting: opts.APIUIVersionSetting,
		middleware: middleware.Chain{
			middleware.Gzip,
			middleware.FrameOptions,
			middleware.CacheMiddleware("json", "js", "css"),
		}.Handler,
		indexMiddleware: middleware.Chain{
			middleware.Gzip,
			middleware.NoCache,
			middleware.FrameOptions,
			middleware.ContentType,
		}.Handler,
	}

	if h.indexSetting == nil {
		h.indexSetting = func() string {
			return "https://releases.rancher.com/dashboard/latest/index.html"
		}
	}

	if h.offlineSetting == nil {
		h.offlineSetting = func() string {
			return "dynamic"
		}
	}

	if h.pathSetting == nil {
		h.pathSetting = func() string {
			return defaultPath
		}
	}

	if h.releaseSetting == nil {
		h.releaseSetting = func() bool {
			return false
		}
	}

	return h
}

func (u *Handler) canDownload(url string) bool {
	u.downloadOnce.Do(func() {
		if err := serveRemote(io.Discard, url); err == nil {
			u.downloadSuccess = true
		} else {
			logrus.Errorf("failed to download %s: %v, falling back to packaged UI", url, err)
		}
	})
	return u.downloadSuccess
}

func (u *Handler) indexPath() (path string, isURL bool) {
	switch u.offlineSetting() {
	case "dynamic":
		if u.releaseSetting() {
			return u.pathSetting(), false
		}
		if u.canDownload(u.indexSetting()) {
			return u.indexSetting(), true
		}
		return u.pathSetting(), false
	case "true":
		return u.pathSetting(), false
	default:
		return u.indexSetting(), true
	}
}

func (u *Handler) apiUIPath() (string, bool) {
	version := u.apiUIVersionSetting()
	remotePath := "https://releases.rancher.com/api-ui/"

	switch u.offlineSetting() {
	case "dynamic":
		if u.releaseSetting() {
			return filepath.Join(u.pathSetting(), "api-ui"), false
		}
		if u.canDownload(fmt.Sprintf("%s/%s/%s", remotePath, version, "ui.min.js")) {
			return remotePath, true
		}
		return filepath.Join(u.pathSetting(), "api-ui"), false
	case "true":
		return filepath.Join(u.pathSetting(), "api-ui"), false
	default:
		return remotePath, true
	}
}

func (u *Handler) ServeAPIUI() http.Handler {
	return u.middleware(http.StripPrefix("/api-ui/",
		http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			base, isURL := u.apiUIPath()
			if isURL {
				remoteURL := base + req.URL.Path
				if err := serveRemote(rw, remoteURL); err != nil {
					logrus.Errorf("failed to fetch asset from %s: %v", remoteURL, err)
				}
			} else {
				parts := strings.SplitN(req.URL.Path, "/", 2)
				if len(parts) == 2 {
					http.ServeFile(rw, req, filepath.Join(base, parts[1]))
				} else {
					http.NotFound(rw, req)
				}
			}
		})))
}

func (u *Handler) ServeAsset() http.Handler {
	return u.middleware(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		http.FileServer(http.Dir(u.pathSetting())).ServeHTTP(rw, req)
	}))
}

func (u *Handler) ServeFaviconDashboard() http.Handler {
	return u.middleware(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		http.FileServer(http.Dir(filepath.Join(u.pathSetting(), "dashboard"))).ServeHTTP(rw, req)
	}))
}

func (u *Handler) IndexFileOnNotFound() http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// we ignore directories here because we want those to come from the CDN when running in that mode
		if stat, err := os.Stat(filepath.Join(u.pathSetting(), req.URL.Path)); err == nil && !stat.IsDir() {
			u.ServeAsset().ServeHTTP(rw, req)
		} else {
			u.IndexFile().ServeHTTP(rw, req)
		}
	})
}

func (u *Handler) IndexFile() http.Handler {
	return u.indexMiddleware(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if path, isURL := u.indexPath(); isURL {
			err := serveRemote(rw, path)
			if err != nil {
				logrus.Errorf("failed to download %s: %v", path, err)
			}
		} else {
			http.ServeFile(rw, req, filepath.Join(path, "index.html"))
		}
	}))
}

func serveRemote(resp io.Writer, url string) error {
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
		},
	}
	r, err := client.Get(url)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	if w, ok := resp.(http.ResponseWriter); ok {
		for k, v := range r.Header {
			for _, vv := range v {
				w.Header().Add(k, vv)
			}
		}
		w.WriteHeader(r.StatusCode)
	}

	_, err = io.Copy(resp, r.Body)
	return err
}
