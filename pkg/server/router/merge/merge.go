package merge

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kube-openapi/pkg/handler3"
)

const (
	headerContentType          = "Content-Type"
	contentTypeApplicationJSON = "application/json"
	contentTypeTextPlain       = "text/plain; charset=utf-8"

	headerContentEncoding = "Content-Encoding"
	contentEncodingGzip   = "gzip"
)

type recorder struct {
	headers http.Header
	body    io.ReadWriter
	code    int
}

func newRecorder() *recorder {
	return &recorder{
		headers: http.Header{},
		body:    bytes.NewBuffer(nil),
	}
}

func (r *recorder) Header() http.Header {
	return r.headers
}

func (r *recorder) Write(b []byte) (int, error) {
	return r.body.Write(b)
}

func (r *recorder) WriteHeader(statusCode int) {
	r.code = statusCode
}

func (r *recorder) isOk() bool {
	return 200 <= r.code && r.code < 400
}

func (r *recorder) getBodyData() ([]byte, error) {
	switch contentType := r.Header().Get(headerContentType); contentType {
	case contentTypeApplicationJSON, contentTypeTextPlain:
	default:
		return nil, fmt.Errorf("unsupported content type '%s'", contentType)
	}

	var body []byte
	var err error

	switch contentType := r.Header().Get(headerContentEncoding); contentType {
	case contentEncodingGzip:
		r, err := gzip.NewReader(r.body)
		if err != nil {
			return nil, fmt.Errorf("failed to decode gzip body: %w", err)
		}

		if body, err = io.ReadAll(r); err != nil {
			return nil, fmt.Errorf("failed to decode gzip body: %w", err)
		}
	default:
		if body, err = io.ReadAll(r.body); err != nil {
			return nil, fmt.Errorf("failed to read body: %w", err)
		}
	}

	return body, nil
}

type mergeFunc func([]byte, []byte) ([]byte, error)

func typedMergeFunc[T any](f func(T, T) (T, error)) mergeFunc {
	return func(primaryData []byte, secondaryData []byte) ([]byte, error) {
		var primary T
		var secondary T

		if err := json.Unmarshal(primaryData, &primary); err != nil {
			return nil, fmt.Errorf("failed unmarshalling primary data: %w", err)
		}

		if err := json.Unmarshal(secondaryData, &secondary); err != nil {
			return nil, fmt.Errorf("failed unmarshalling secondary data: %w", err)
		}

		merged, err := f(primary, secondary)
		if err != nil {
			return nil, err
		}

		data, err := json.Marshal(merged)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal merged object: %w", err)
		}

		return data, nil
	}
}

var APIGropuListMerger = typedMergeFunc(func(lhs metav1.APIGroupList, rhs metav1.APIGroupList) (metav1.APIGroupList, error) {
	lhs.Groups = append(lhs.Groups, slices.DeleteFunc(rhs.Groups, func(target metav1.APIGroup) bool {
		return slices.ContainsFunc(lhs.Groups, func(g metav1.APIGroup) bool {
			return g.Name == target.Name
		})
	})...)

	return lhs, nil
})

var OpenAPIV3Merger = typedMergeFunc(func(lhs handler3.OpenAPIV3Discovery, rhs handler3.OpenAPIV3Discovery) (handler3.OpenAPIV3Discovery, error) {
	maps.Copy(lhs.Paths, rhs.Paths)
	return lhs, nil
})

type merger struct {
	primary   http.Handler
	secondary http.Handler

	merger mergeFunc
}

// merge provides a handler which reconciles the responses between two handlers. If either handler returns with an
// error state, only the response of the primary handler will be used (ie. if the primary handler succeeds and the
// secondary fails only the primary response will be used). If either handler is nil the non-nil handler is returned,
// or nil if both handlers are nil.
func Merge(primary http.Handler, secondary http.Handler, f mergeFunc) http.Handler {
	if primary != nil && secondary != nil {
		return &merger{
			primary:   primary,
			secondary: secondary,
			merger:    f,
		}
	}

	if primary == nil {
		return secondary
	}

	if secondary == nil {
		return primary
	}

	return nil
}

func (m *merger) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	primaryRecorder := newRecorder()
	m.primary.ServeHTTP(primaryRecorder, r)

	secondaryRecorder := newRecorder()
	m.secondary.ServeHTTP(secondaryRecorder, r)

	w.Header().Set(headerContentType, contentTypeApplicationJSON)

	if primaryRecorder.isOk() && !secondaryRecorder.isOk() {
		b, err := io.ReadAll(primaryRecorder.body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if _, err := w.Write(b); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		h := w.Header()
		for k, v := range primaryRecorder.Header() {
			h.Set(k, v[0])
		}

		w.WriteHeader(primaryRecorder.code)

		return
	}

	primaryData, err := primaryRecorder.getBodyData()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	secondaryData, err := secondaryRecorder.getBodyData()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	merged, err := m.merger(primaryData, secondaryData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if _, err := w.Write(merged); err != nil {
		http.Error(w, fmt.Sprintf("failed to write response: %s", err), http.StatusInternalServerError)
		return
	}

	// todo: write status
	// todo: write headers
}
