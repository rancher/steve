package ui

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

const cspHeader = "Content-Security-Policy"

// DefaultCSPPolicy is the Content-Security-Policy served with the UI when none
// is configured.
const DefaultCSPPolicy = "base-uri 'self'; object-src 'none'; frame-ancestors 'self'"

// sanitizeCSPPolicy makes a configured policy safe to write into a response
// header. The value is operator-supplied free text, so anything outside the
// characters a header value may hold is dropped: a carriage return or newline
// would otherwise let the setting inject arbitrary further headers.
func sanitizeCSPPolicy(policy string) string {
	printable := func(r rune) rune {
		if r == '\t' || (r >= ' ' && r <= '~') {
			return r
		}
		return -1
	}

	return strings.TrimSpace(strings.Map(printable, policy))
}

// schemesNeedingColon are source expressions that look like a scheme but are
// read as a host name unless they end in a colon.
var schemesNeedingColon = map[string]bool{
	"blob": true, "data": true, "filesystem": true, "http": true,
	"https": true, "mediastream": true, "ws": true, "wss": true,
}

// checkCSPPolicy reports mistakes a browser accepts without complaint. A
// malformed source is dropped rather than refused, so the policy ends up
// quietly narrower than intended and nothing says so.
//
// These are warnings only. The policy is still served, because refusing it
// would leave an operator whose policy has locked them out of the UI with no
// way to correct it.
func checkCSPPolicy(policy string) []string {
	var warnings []string

	if strings.Contains(policy, ",") {
		warnings = append(warnings, "a comma starts a second policy rather than a second directive, and browsers enforce every policy they are given, so this is more restrictive than a semicolon")
	}

	for _, directive := range strings.Split(policy, ";") {
		fields := strings.Fields(directive)
		if len(fields) == 0 {
			continue
		}

		name := fields[0]
		for _, source := range fields[1:] {
			// Keywords, nonces and hashes are quoted, and have no
			// scheme or host syntax to get wrong.
			if strings.HasPrefix(source, "'") {
				continue
			}

			if schemesNeedingColon[strings.ToLower(source)] {
				warnings = append(warnings, fmt.Sprintf("%s source %q is read as a host name; write %q to mean the scheme", name, source, source+":"))
			}

			if strings.Contains(source, "*") && source != "*" && !strings.HasPrefix(source, "*.") {
				warnings = append(warnings, fmt.Sprintf(`%s source %q is not a valid wildcard; only "*" and a leading "*." match`, name, source))
			}
		}
	}

	return warnings
}

// The policy last reported by warnCSPPolicy. This is shared by every CSP
// middleware in the process: the same policy is usually served by more than one
// of them, and the warnings are about the policy rather than about the route.
var (
	warnedMu     sync.Mutex
	warnedPolicy string
)

// warnCSPPolicy logs what is wrong with a policy the first time that policy is
// served. The setting can change while the server runs, so this happens on the
// request path rather than once at startup.
func warnCSPPolicy(policy string) {
	warnedMu.Lock()
	defer warnedMu.Unlock()

	if policy == warnedPolicy {
		return
	}
	warnedPolicy = policy

	for _, warning := range checkCSPPolicy(policy) {
		logrus.Warnf("Content-Security-Policy: %s", warning)
	}
}

// CSP serves the configured Content-Security-Policy. An empty policy disables
// the header, which lets an operator turn it off entirely.
//
// The UI handler applies this to the routes it serves. Mount it higher up the
// chain as well to cover everything else, including the HTML API browser.
func CSP(policy StringSetting) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if value := sanitizeCSPPolicy(policy()); value != "" {
				warnCSPPolicy(value)
				rw.Header().Set(cspHeader, value)
			}

			next.ServeHTTP(rw, req)
		})
	}
}
