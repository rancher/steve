package ui

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSanitizeCSPPolicy(t *testing.T) {
	tests := []struct {
		name   string
		policy string
		want   string
	}{
		{
			name:   "policy is kept as is",
			policy: DefaultCSPPolicy,
			want:   DefaultCSPPolicy,
		},
		{
			// Without this a policy could append headers of its own.
			name:   "carriage returns and newlines are dropped",
			policy: "base-uri 'self'\r\nX-Injected: evil",
			want:   "base-uri 'self'X-Injected: evil",
		},
		{
			name:   "control characters are dropped",
			policy: "base-uri\x00 'self'\x7f",
			want:   "base-uri 'self'",
		},
		{
			name:   "non-ascii is dropped",
			policy: "base-uri 'sélf'",
			want:   "base-uri 'slf'",
		},
		{
			name:   "surrounding whitespace is trimmed",
			policy: "  base-uri 'self'\t",
			want:   "base-uri 'self'",
		},
		{
			name:   "whitespace only policy disables the header",
			policy: " \r\n\t ",
			want:   "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, sanitizeCSPPolicy(test.policy))
		})
	}
}

func TestCSP(t *testing.T) {
	tests := []struct {
		name   string
		policy string
		want   string
	}{
		{name: "configured policy is served", policy: "object-src 'none'", want: "object-src 'none'"},
		{name: "empty policy disables the header", policy: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := CSP(setting(test.policy))(http.NotFoundHandler())

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dashboard/", nil))

			assert.Equal(t, test.want, recorder.Header().Get(cspHeader))
		})
	}
}

func TestHandlerCSPHeader(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "index.html"), []byte("<html></html>"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "robots.txt"), []byte("User-agent: *"), 0600))

	tests := []struct {
		name    string
		options *Options
		want    string
	}{
		{
			name:    "defaults to DefaultCSPPolicy",
			options: &Options{Path: setting(dir), Offline: setting("true")},
			want:    DefaultCSPPolicy,
		},
		{
			name: "configured policy overrides the default",
			options: &Options{
				Path:      setting(dir),
				Offline:   setting("true"),
				CSPPolicy: setting("default-src 'self'"),
			},
			want: "default-src 'self'",
		},
		{
			name: "empty policy disables the header",
			options: &Options{
				Path:      setting(dir),
				Offline:   setting("true"),
				CSPPolicy: setting(""),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := NewUIHandler(test.options)

			routes := []struct {
				name    string
				handler http.Handler
				path    string
			}{
				{name: "index", handler: handler.IndexFile(), path: "/dashboard/"},
				{name: "asset", handler: handler.ServeAsset(), path: "/robots.txt"},
			}

			for _, route := range routes {
				t.Run(route.name, func(t *testing.T) {
					recorder := httptest.NewRecorder()
					route.handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, route.path, nil))

					assert.Equal(t, test.want, recorder.Header().Get(cspHeader))
				})
			}
		})
	}
}

func setting(value string) StringSetting {
	return func() string {
		return value
	}
}
