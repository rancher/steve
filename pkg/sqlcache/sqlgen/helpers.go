package sqlgen

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
)

const (
	escapeBackslashDirective = ` ESCAPE '\'`
	matchFmt                 = `%%%s%%`
	strictMatchFmt           = `%s`
)

func formatMatchTarget(filter sqltypes.Filter) string {
	format := strictMatchFmt
	if filter.Partial {
		format = matchFmt
	}
	return formatMatchTargetWithFormatter(filter.Matches[0], format)
}

func formatMatchTargetForLabel(filter sqltypes.Filter) string {
	format := strictMatchFmt
	if filter.Partial {
		format = matchFmt
	}
	return formatMatchTargetWithFormatter(filter.Matches[0], format)
}

func formatMatchTargetWithFormatter(match string, format string) string {
	match = strings.ReplaceAll(match, `\`, `\\`)
	match = strings.ReplaceAll(match, `_`, `\_`)
	match = strings.ReplaceAll(match, `%`, `\%`)
	return fmt.Sprintf(format, match)
}

func isLabelFilter(f *sqltypes.Filter) bool {
	return len(f.Field) >= 2 && f.Field[0] == "metadata" && f.Field[1] == "labels"
}

func isLabelsFieldList(fields []string) bool {
	return len(fields) == 3 && fields[0] == "metadata" && fields[1] == "labels"
}

func prepareComparisonParameters(op sqltypes.Op, target string) (string, float64, error) {
	num, err := parseFloat(target)
	if err != nil {
		return "", 0, err
	}
	switch op {
	case sqltypes.Lt:
		return "<", num, nil
	case sqltypes.Gt:
		return ">", num, nil
	}
	return "", 0, fmt.Errorf("unrecognized operator when expecting '<' or '>': '%s'", op)
}

func parseFloat(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}
