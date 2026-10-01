package ui

import (
	"net/http"
	"strings"
)

const cspHeader = "Content-Security-Policy"

// DefaultCSPPolicy is the Content-Security-Policy served with the UI when none
// is configured.
const DefaultCSPPolicy = "base-uri 'self'; object-src 'none'; frame-ancestors 'self'"

// sanitizeCSPPolicy makes a configured policy safe to write into a response
// header.
func sanitizeCSPPolicy(policy string) string {
	printable := func(r rune) rune {
		if r == '\t' || (r >= ' ' && r <= '~') {
			return r
		}
		return -1
	}

	return strings.TrimSpace(strings.Map(printable, policy))
}

// CSP serves the configured Content-Security-Policy. An empty policy sends no header.
func CSP(policy StringSetting) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if value := sanitizeCSPPolicy(policy()); value != "" {
				rw.Header().Set(cspHeader, value)
			}

			next.ServeHTTP(rw, req)
		})
	}
}
