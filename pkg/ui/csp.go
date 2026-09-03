package ui

import (
	"bytes"
	"context"
	"crypto/rand"
	"net/http"
	"regexp"
	"strings"

	"github.com/sirupsen/logrus"
)

// How the configured policy is delivered, selected by Options.CSPMode.
const (
	// CSPModeOff disables the header. This is the default.
	CSPModeOff = "off"
	// CSPModeReportOnly sends Content-Security-Policy-Report-Only, which makes
	// browsers report violations without blocking anything.
	CSPModeReportOnly = "report-only"
	// CSPModeEnforce sends Content-Security-Policy, which blocks violations.
	CSPModeEnforce = "enforce"
)

// NoncePlaceholder is replaced with a freshly generated, per-request nonce
// everywhere it appears in the configured policy. When the policy uses it, the
// same nonce is added to every inline script and style in the index file.
const NoncePlaceholder = "{nonce}"

const (
	cspHeader           = "Content-Security-Policy"
	cspReportOnlyHeader = "Content-Security-Policy-Report-Only"
)

type nonceContextKey struct{}

// nonceFromContext returns the nonce generated for this request, or an empty
// string when the configured policy does not use one.
func nonceFromContext(ctx context.Context) string {
	nonce, _ := ctx.Value(nonceContextKey{}).(string)
	return nonce
}

// resolvePolicy expands policy for a single request. It returns the header to
// set, its value, and the nonce that was substituted in.
func resolvePolicy(policy, mode string) (header, value, nonce string) {
	policy = strings.TrimSpace(policy)
	if policy == "" {
		return "", "", ""
	}

	switch mode {
	case CSPModeEnforce:
		header = cspHeader
	case CSPModeReportOnly:
		header = cspReportOnlyHeader
	default:
		// Anything unrecognized is treated as off, so that a typo in the
		// setting can never enforce an unintended policy.
		return "", "", ""
	}

	if strings.Contains(policy, NoncePlaceholder) {
		// rand.Text returns base32, so the result is always safe both as a
		// policy token and as an HTML attribute value.
		nonce = rand.Text()
		policy = strings.ReplaceAll(policy, NoncePlaceholder, nonce)
	}

	return header, policy, nonce
}

// csp sets the configured policy and makes the request's nonce available to
// injectNonce further down the chain.
func csp(policy, mode StringSetting) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			header, value, nonce := resolvePolicy(policy(), mode())
			if header == "" {
				next.ServeHTTP(rw, req)
				return
			}

			rw.Header().Set(header, value)
			if nonce != "" {
				req = req.WithContext(context.WithValue(req.Context(), nonceContextKey{}, nonce))
			}

			next.ServeHTTP(rw, req)
		})
	}
}

// cspStatic sets the configured policy on cacheable responses. Policies using a
// per-request nonce are skipped: these responses are served with a year-long
// max-age, and a stale nonce is worse than no header at all. Such policies
// still apply to the documents that load these assets, which is where the
// browser enforces them.
func cspStatic(policy, mode StringSetting) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		withCSP := csp(policy, mode)(next)
		return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if strings.Contains(policy(), NoncePlaceholder) {
				next.ServeHTTP(rw, req)
				return
			}
			withCSP.ServeHTTP(rw, req)
		})
	}
}

// inlineTagRegexp matches the start of a script or style tag, capturing the
// character that terminates the tag name so it can be preserved.
var inlineTagRegexp = regexp.MustCompile(`(?i)<(script|style)([\s>])`)

// injectNonce rewrites the served document so every script and style carries
// the request's nonce. The index file is a dashboard build artifact and may be
// proxied from a remote, so the attribute cannot be baked in at build time.
func injectNonce(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		nonce := nonceFromContext(req.Context())
		if nonce == "" {
			next.ServeHTTP(rw, req)
			return
		}

		buffered := &bufferedWriter{ResponseWriter: rw}
		next.ServeHTTP(buffered, req)
		buffered.writeNonced(nonce)
	})
}

// bufferedWriter holds the whole response in memory so the body can be
// rewritten before anything is sent. It deliberately does not implement
// http.Flusher, since a flush would send the unmodified body.
type bufferedWriter struct {
	http.ResponseWriter

	body   bytes.Buffer
	status int
}

func (b *bufferedWriter) WriteHeader(status int) {
	if b.status == 0 {
		b.status = status
	}
}

func (b *bufferedWriter) Write(p []byte) (int, error) {
	return b.body.Write(p)
}

func (b *bufferedWriter) writeNonced(nonce string) {
	body := inlineTagRegexp.ReplaceAll(b.body.Bytes(), []byte(`<${1} nonce="`+nonce+`"${2}`))

	// The body changed length, and serveRemote copies the upstream
	// Content-Length verbatim.
	b.Header().Del("Content-Length")

	if b.status != 0 {
		b.ResponseWriter.WriteHeader(b.status)
	}
	if _, err := b.ResponseWriter.Write(body); err != nil {
		logrus.Errorf("failed to write index file: %v", err)
	}
}
