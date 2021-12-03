package ui

import (
	"crypto/tls"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rancher/apiserver/pkg/middleware"
	"github.com/sirupsen/logrus"
)

var (
	insecureClient = &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
)

const (
	defaultPath = "./ui"
)

type StringSetting func() string
type BoolSetting func() bool

type Handler struct {
	pathSetting     func() string
	indexSetting    func() string
	releaseSetting  func() bool
	offlineSetting  func() string
	middleware      func(http.Handler) http.Handler
	indexMiddleware func(http.Handler) http.Handler

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
}

func NewUIHandler(opts *Options) *Handler {
	if opts == nil {
		opts = &Options{}
	}

	h := &Handler{
		indexSetting:   opts.Index,
		offlineSetting: opts.Offline,
		pathSetting:    opts.Path,
		releaseSetting: opts.ReleaseSetting,
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
		if err := serveIndex(ioutil.Discard, url); err == nil {
			u.downloadSuccess = true
		} else {
			logrus.Errorf("Failed to download %s, falling back to packaged UI", url)
		}
	})
	return u.downloadSuccess
}

func (u *Handler) path() (path string, isURL bool) {
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

func (u *Handler) ServeAsset() http.Handler {
	return u.middleware(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		//http.FileServer(http.Dir(u.pathSetting())).ServeHTTP(rw, req)
		http.FileServer(neuteredFileSystem{http.Dir(u.pathSetting())}).ServeHTTP(rw, req)
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
		if path, isURL := u.path(); isURL {
			_ = serveIndex(rw, path)
		} else {
			http.ServeFile(rw, req, filepath.Join(path, "index.html"))
		}
	}))
}

func serveIndex(resp io.Writer, url string) error {
	r, err := insecureClient.Get(url)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	_, err = io.Copy(resp, r.Body)
	return err
}

// PANDARIA
type neuteredFileSystem struct {
	fs http.FileSystem
}

// PANDARIA
func (nfs neuteredFileSystem) Open(path string) (http.File, error) {
	f, err := nfs.fs.Open(path)
	if err != nil {
		return nil, err
	}

	s, err := f.Stat()
	if err != nil {
		return nil, err
	}

	if s.IsDir() {
		index := strings.TrimSuffix(path, "/") + "/index.html"
		if _, err := nfs.fs.Open(index); err != nil {
			return nil, err
		}
	}

	return f, nil
}
