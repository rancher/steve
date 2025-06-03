package common

import (
	"fmt"
	"strings"
	"time"
)

func ParseHumanReadableDuration(s string) (time.Duration, error) {
	var total time.Duration
	var val int
	var unit byte

	r := strings.NewReader(s)
	for r.Len() > 0 {
		if _, err := fmt.Fscanf(r, "%d%c", &val, &unit); err != nil {
			return 0, fmt.Errorf("invalid duration in %s: %w", s, err)
		}

		switch unit {
		case 'd':
			total += time.Duration(val) * 24 * time.Hour
		case 'h':
			total += time.Duration(val) * time.Hour
		case 'm':
			total += time.Duration(val) * time.Minute
		case 's':
			total += time.Duration(val) * time.Second
		default:
			return 0, fmt.Errorf("invalid duration unit %s in %s", string(unit), s)
		}
	}

	return total, nil
}
