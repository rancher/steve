package router

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"slices"

	openapi_v2 "github.com/google/gnostic-models/openapiv2"
	openapi_v3 "github.com/google/gnostic-models/openapiv3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	headerContentType          = "Content-Type"
	contentTypeApplicationJson = "application/json"
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

func mergeOpenApiV2Document(lhs, rhs *openapi_v2.Document) (*openapi_v2.Document, error) {
	return nil, fmt.Errorf("not yet implemented")
}

func mergeOpenApiV3Document(lhs, rhs *openapi_v3.Document) (*openapi_v2.Document, error) {
	return nil, fmt.Errorf("not yet implemented")
}

func mergeResponse(lhs any, rhs any) (any, error) {
	if reflect.TypeOf(lhs) != reflect.TypeOf(rhs) {
		return nil, fmt.Errorf("found mismatched types '%T' != '%T'", lhs, rhs)
	}

	switch lhs := lhs.(type) {
	case *metav1.APIGroupList:
		rhs := rhs.(*metav1.APIGroupList)

		lhs.Groups = append(lhs.Groups, slices.DeleteFunc(rhs.Groups, func(target metav1.APIGroup) bool {
			return slices.ContainsFunc(lhs.Groups, func(g metav1.APIGroup) bool {
				return g.Name == target.Name
			})
		})...)

		return lhs, nil
	case *openapi_v2.Document:
		return mergeOpenApiV2Document(lhs, rhs.(*openapi_v2.Document))
	case *openapi_v3.Document:
		return mergeOpenApiV3Document(lhs, rhs.(*openapi_v3.Document))
	default:
		return nil, fmt.Errorf("unsupported type '%T'", lhs)
	}
}

type merger[T any] struct {
	primary   http.Handler
	secondary http.Handler
}

// merge provides a handler which reconciles the responses between two handlers. If either handler returns with an
// error state, only the response of the primary handler will be used (ie. if the primary handler succeeds and the
// secondary fails only the primary response will be used). If either handler is nil the non-nil handler is returned,
// or nil if both handlers are nil.
//
// todo: see if we can get a more specific generic type argument
func merge[T any](primary http.Handler, secondary http.Handler) http.Handler {
	if primary != nil && secondary != nil {
		return &merger[T]{
			primary:   primary,
			secondary: secondary,
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

func (m *merger[T]) getBodyObject(r *recorder) (T, error) {
	var obj T

	switch contentType := r.Header().Get(headerContentType); contentType {
	case contentTypeApplicationJson, contentTypeTextPlain:
	default:
		return obj, fmt.Errorf("unsupported content type '%s'", contentType)
	}

	var body []byte
	var err error

	switch contentType := r.Header().Get(headerContentEncoding); contentType {
	case contentEncodingGzip:
		r, err := gzip.NewReader(r.body)
		if err != nil {
			return obj, fmt.Errorf("failed to decode gzip body: %w", err)
		}

		if body, err = io.ReadAll(r); err != nil {
			return obj, fmt.Errorf("failed to decode gzip body: %w", err)
		}
	default:
		if body, err = io.ReadAll(r.body); err != nil {
			return obj, fmt.Errorf("failed to read body: %w", err)
		}
	}

	if err := json.Unmarshal(body, &obj); err != nil {
		return obj, fmt.Errorf("failed to marshal body data: %w", err)
	}

	return obj, nil
}

func (m *merger[T]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	primaryRecorder := newRecorder()
	m.primary.ServeHTTP(primaryRecorder, r)

	secondaryRecorder := newRecorder()
	m.secondary.ServeHTTP(secondaryRecorder, r)

	w.Header().Set(headerContentType, contentTypeApplicationJson)

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

	primary, err := m.getBodyObject(primaryRecorder)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	secondary, err := m.getBodyObject(secondaryRecorder)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	merged, err := mergeResponse(&primary, &secondary)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// todo: we might want to use the kubectl serializer
	data, err := json.Marshal(merged)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to marshaled merged response: %s", err), http.StatusInternalServerError)
		return
	}

	if _, err := w.Write(data); err != nil {
		http.Error(w, fmt.Sprintf("failed to write response: %s", err), http.StatusInternalServerError)
		return
	}

	// todo: write status
	// todo: write headers
}
