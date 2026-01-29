package common

import (
	"fmt"
	"strings"
	"time"
)

// ParseRestartField parses restart field format "1 (38m ago)"
// Returns: count, duration, error
func ParseRestartField(s string) (count string, duration time.Duration, err error) {
	if !strings.Contains(s, "(") {
		// Just a count like "0" or "1", no restart yet
		return s, 0, nil
	}

	parts := strings.SplitN(s, " (", 2)
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid restart format: %s", s)
	}

	count = strings.TrimSpace(parts[0])
	durStr := strings.TrimSuffix(strings.TrimSpace(parts[1]), " ago)")

	duration, err = ParseHumanReadableDuration(durStr)
	if err != nil {
		return "", 0, fmt.Errorf("failed to parse duration in %s: %w", s, err)
	}

	return count, duration, nil
}
