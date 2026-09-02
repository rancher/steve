package ui

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolvePolicy(t *testing.T) {
	tests := []struct {
		name       string
		policy     string
		mode       string
		wantHeader string
		wantValue  string
		wantNonce  bool
	}{
		{
			name:   "empty policy is disabled",
			policy: "",
			mode:   CSPModeEnforce,
		},
		{
			name:   "whitespace only policy is disabled",
			policy: "   ",
			mode:   CSPModeEnforce,
		},
		{
			name:   "off mode is disabled",
			policy: "default-src 'self'",
			mode:   CSPModeOff,
		},
		{
			name:   "unrecognized mode is disabled",
			policy: "default-src 'self'",
			mode:   "enfroce",
		},
		{
			name:       "report-only mode",
			policy:     "default-src 'self'",
			mode:       CSPModeReportOnly,
			wantHeader: cspReportOnlyHeader,
			wantValue:  "default-src 'self'",
		},
		{
			name:       "enforce mode",
			policy:     "default-src 'self'",
			mode:       CSPModeEnforce,
			wantHeader: cspHeader,
			wantValue:  "default-src 'self'",
		},
		{
			name:       "nonce is substituted",
			policy:     "script-src 'nonce-" + NoncePlaceholder + "' 'strict-dynamic'",
			mode:       CSPModeEnforce,
			wantHeader: cspHeader,
			wantNonce:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header, value, nonce := resolvePolicy(test.policy, test.mode)

			assert.Equal(t, test.wantHeader, header)
			if !test.wantNonce {
				assert.Equal(t, test.wantValue, value)
				assert.Empty(t, nonce)
				return
			}

			assert.NotEmpty(t, nonce)
			assert.NotContains(t, value, NoncePlaceholder)
			assert.Contains(t, value, "'nonce-"+nonce+"'")
		})
	}
}

func TestResolvePolicyNonceIsPerCall(t *testing.T) {
	policy := "script-src 'nonce-" + NoncePlaceholder + "'"

	_, _, first := resolvePolicy(policy, CSPModeEnforce)
	_, _, second := resolvePolicy(policy, CSPModeEnforce)

	require.NotEmpty(t, first)
	assert.NotEqual(t, first, second)
}

func TestCSPStaticSkipsNoncePolicies(t *testing.T) {
	tests := []struct {
		name       string
		policy     string
		wantHeader string
	}{
		{
			name:       "static policy is served",
			policy:     "default-src 'self'",
			wantHeader: "default-src 'self'",
		},
		{
			// A cacheable response must not pin a nonce that is only valid for
			// the request that generated it.
			name:   "nonce policy is skipped",
			policy: "script-src 'nonce-" + NoncePlaceholder + "'",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := cspStatic(setting(test.policy), setting(CSPModeEnforce))(http.NotFoundHandler())

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/app.js", nil))

			assert.Equal(t, test.wantHeader, recorder.Header().Get(cspHeader))
		})
	}
}

func TestInjectNonce(t *testing.T) {
	const index = `<!DOCTYPE html>
<html>
<head><script src="/app.js"></script></head>
<body>
<script>
	console.log('theme');
</script>
<style>
	body { color: red; }
</style>
<p>not a script tag</p>
</body>
</html>`

	handler := csp(
		setting("script-src 'nonce-"+NoncePlaceholder+"'"),
		setting(CSPModeEnforce),
	)(injectNonce(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.Header().Set("Content-Length", "1")
		_, _ = rw.Write([]byte(index))
	})))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dashboard/", nil))

	nonce := regexp.MustCompile(`'nonce-([^']+)'`).FindStringSubmatch(recorder.Header().Get(cspHeader))
	require.Len(t, nonce, 2)

	body := recorder.Body.String()
	assert.Equal(t, 3, len(regexp.MustCompile(`nonce="`+nonce[1]+`"`).FindAllString(body, -1)))
	assert.Contains(t, body, `<script nonce="`+nonce[1]+`" src="/app.js">`)
	assert.Contains(t, body, `<style nonce="`+nonce[1]+`">`)
	assert.Contains(t, body, "<p>not a script tag</p>")

	// serveRemote copies the upstream Content-Length, which no longer matches.
	assert.Empty(t, recorder.Header().Get("Content-Length"))
}

func TestInjectNonceWithoutPolicyLeavesBodyAlone(t *testing.T) {
	const index = `<html><script>alert(1)</script></html>`

	handler := csp(setting(""), setting(CSPModeEnforce))(
		injectNonce(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
			_, _ = rw.Write([]byte(index))
		})))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dashboard/", nil))

	assert.Equal(t, index, recorder.Body.String())
	assert.Empty(t, recorder.Header().Get(cspHeader))
}

func TestHandlerCSPHeader(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "index.html"), []byte("<html></html>"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "robots.txt"), []byte("User-agent: *"), 0600))

	handler := NewUIHandler(&Options{
		Path:      setting(dir),
		Offline:   setting("true"),
		CSPPolicy: setting("default-src 'self'"),
		CSPMode:   setting(CSPModeReportOnly),
	})

	tests := []struct {
		name    string
		handler http.Handler
		path    string
	}{
		{name: "index", handler: handler.IndexFile(), path: "/dashboard/"},
		{name: "asset", handler: handler.ServeAsset(), path: "/robots.txt"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			test.handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, test.path, nil))

			assert.Equal(t, "default-src 'self'", recorder.Header().Get(cspReportOnlyHeader))
			assert.Empty(t, recorder.Header().Get(cspHeader))
		})
	}
}

func TestHandlerCSPDisabledByDefault(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "index.html"), []byte("<html></html>"), 0600))

	handler := NewUIHandler(&Options{
		Path:    setting(dir),
		Offline: setting("true"),
	})

	recorder := httptest.NewRecorder()
	handler.IndexFile().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dashboard/", nil))

	assert.Empty(t, recorder.Header().Get(cspHeader))
	assert.Empty(t, recorder.Header().Get(cspReportOnlyHeader))
}

func setting(value string) StringSetting {
	return func() string {
		return value
	}
}
