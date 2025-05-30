package common

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

var humanReadableDurationRe = regexp.MustCompile(
	`^(?:(?P<days>\d+)d)?` +
		`(?:(?P<hours>\d+)h)?` +
		`(?:(?P<minutes>\d+)m)?` +
		`(?:(?P<seconds>\d+)s)?$`,
)

func ParseHumanReadableDuration(s string) (time.Duration, error) {
	matches := humanReadableDurationRe.FindStringSubmatch(s)
	if matches == nil {
		return 0, fmt.Errorf("invalid duration: %q", s)
	}

	var d time.Duration
	for i, name := range humanReadableDurationRe.SubexpNames() {
		if i == 0 || matches[i] == "" {
			continue
		}
		val, err := strconv.Atoi(matches[i])
		if err != nil {
			return 0, err
		}
		switch name {
		case "days":
			d += time.Duration(val) * 24 * time.Hour
		case "hours":
			d += time.Duration(val) * time.Hour
		case "minutes":
			d += time.Duration(val) * time.Minute
		case "seconds":
			d += time.Duration(val) * time.Second
		}
	}
	return d, nil
}
