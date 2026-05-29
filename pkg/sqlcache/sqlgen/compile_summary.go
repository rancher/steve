package sqlgen

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/rancher/steve/pkg/sqlcache/sqlexpr"
)

// SummaryFieldQuery holds the SQL and params for a single summary field query.
type SummaryFieldQuery struct {
	Query  string
	Params []any
}

// CompileSummaryFieldQuery builds the final SQL for a summary field query.
// For fields with no active filters (isEmpty), it builds a simple SELECT with GROUP BY.
// For fields with active filters, it builds a WITH/CTE-based query.
func CompileSummaryFieldQuery(
	fieldParts []string,
	fieldNum int,
	dbName string,
	sc *SummaryCompilation,
	fields map[string]IndexedField,
) (*SummaryFieldQuery, error) {
	columnName := toColumnName(fieldParts)
	isLabelField := isLabelsFieldList(fieldParts)

	if sc.IsEmpty {
		return compileSummarySimple(fieldParts, dbName, columnName, isLabelField, fields)
	}
	return compileSummaryComplex(fieldParts, fieldNum, dbName, columnName, isLabelField, sc, fields)
}

func compileSummarySimple(fieldParts []string, dbName, columnName string, isLabelField bool, fields map[string]IndexedField) (*SummaryFieldQuery, error) {
	if isLabelField {
		return compileSummarySimpleLabel(fieldParts, dbName, columnName)
	}
	return compileSummarySimpleField(fieldParts, dbName, columnName, fields)
}

func compileSummarySimpleLabel(fieldParts []string, dbName, columnName string) (*SummaryFieldQuery, error) {
	columnNameToDisplay, err := getLabelColumnNameToDisplay(fieldParts)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf("SELECT '%s' AS p, COUNT(*) AS c, value AS k\n\tFROM \"%s_labels\"\n\tWHERE label = ? AND k != \"\"\n\tGROUP BY k",
		columnNameToDisplay, dbName)
	return &SummaryFieldQuery{Query: query, Params: []any{fieldParts[2]}}, nil
}

func compileSummarySimpleField(fieldParts []string, dbName, columnName string, fields map[string]IndexedField) (*SummaryFieldQuery, error) {
	// No prefix for simple queries
	displayExpr, err := DisplayColumn(fields, fieldParts, "")
	if err != nil {
		return nil, err
	}
	displaySQL, _ := displayExpr.Resolve()
	query := fmt.Sprintf("SELECT '%s' AS p, COUNT(*) AS c, %s AS k\n\tFROM \"%s_fields\"\n\tWHERE k != \"\"\n\tGROUP BY k",
		columnName, displaySQL, dbName)
	return &SummaryFieldQuery{Query: query, Params: []any{}}, nil
}

func compileSummaryComplex(fieldParts []string, fieldNum int, dbName, columnName string, isLabelField bool, sc *SummaryCompilation, fields map[string]IndexedField) (*SummaryFieldQuery, error) {
	prefix := sc.Prefix
	withPrefix := fmt.Sprintf("w%d", fieldNum)

	// Determine targetField and extra label join/where
	var targetField string
	var extraJoin *sqlexpr.Join
	var extraWhereParam *string
	var params []any

	// Collect all params from WHERE
	if sc.Where != nil {
		_, wp := sc.Where.Resolve()
		params = append(params, wp...)
	}

	if isLabelField {
		labelName := fieldParts[2]
		// Check if this label already has a join
		ltAlias, exists := sc.JoinCtx.AliasFor(labelName)
		if !exists {
			// Create a new label join
			ltAlias = sc.JoinCtx.NextAlias()
			sc.JoinCtx.RegisterAlias(labelName, ltAlias)
			join := sqlexpr.Join{
				Kind:  sqlexpr.LeftOuterJoin,
				Table: sqlexpr.TableRef{Name: dbName + "_labels", Alias: ltAlias},
				On:    sqlexpr.Raw{SQL: fmt.Sprintf("%s.key = %s.key", prefix, ltAlias)},
			}
			extraJoin = &join
			extraWhereParam = &labelName
		}
		targetField = fmt.Sprintf("%s.value", ltAlias)
	} else {
		displayExpr, err := DisplayColumn(fields, fieldParts, prefix)
		if err != nil {
			return nil, err
		}
		displaySQL, _ := displayExpr.Resolve()
		targetField = displaySQL
	}

	// Build the query
	var b strings.Builder
	b.WriteString(fmt.Sprintf("WITH %s(key, finalField) AS (\n", withPrefix))

	// SELECT [DISTINCT] prefix.key, targetField
	b.WriteString("\tSELECT")
	if sc.UsesLabels || isLabelField {
		b.WriteString(" DISTINCT")
	}
	b.WriteString(fmt.Sprintf(" %s.key, %s FROM \"%s_fields\" %s\n", prefix, targetField, dbName, prefix))

	// JOINs
	for _, j := range sc.Joins {
		js, _ := j.Resolve()
		b.WriteString(fmt.Sprintf("  %s\n", js))
	}
	if extraJoin != nil {
		js, _ := extraJoin.Resolve()
		b.WriteString(fmt.Sprintf("  %s\n", js))
	}

	// WHERE
	var whereClauses []string
	if sc.Where != nil {
		ws, _ := sc.Where.Resolve()
		whereClauses = append(whereClauses, ws)
	}
	if extraWhereParam != nil {
		// Find the alias for the extra label
		ltAlias, _ := sc.JoinCtx.AliasFor(*extraWhereParam)
		whereClauses = append(whereClauses, fmt.Sprintf("%s.label = ?", ltAlias))
		params = append(params, *extraWhereParam)
	}

	switch len(whereClauses) {
	case 0: // nothing
	case 1:
		b.WriteString(fmt.Sprintf("\tWHERE %s\n", whereClauses[0]))
	default:
		b.WriteString(fmt.Sprintf("\tWHERE (%s)\n", strings.Join(whereClauses, ")\n\t\tAND (")))
	}

	// ORDER BY
	if len(sc.OrderBy) > 0 {
		orderParts := make([]string, len(sc.OrderBy))
		for i, ob := range sc.OrderBy {
			s, _ := ob.Resolve()
			orderParts[i] = s
		}
		b.WriteString("\tORDER BY " + strings.Join(orderParts, ", ") + "\n")
	}

	// LIMIT/OFFSET
	if sc.Limit != nil {
		b.WriteString(fmt.Sprintf("\n  LIMIT %d\n", *sc.Limit))
	}
	if sc.Offset != nil && *sc.Offset > 0 {
		b.WriteString(fmt.Sprintf("\n  OFFSET %d\n", *sc.Offset))
	}

	b.WriteString(")\n")
	b.WriteString(fmt.Sprintf("SELECT '%s' AS p, COUNT(*) AS c, %s.finalField AS k FROM %s\n", columnName, withPrefix, withPrefix))
	b.WriteString("\tWHERE k != \"\"\n\tGROUP BY k")

	return &SummaryFieldQuery{Query: b.String(), Params: params}, nil
}

// toColumnName converts field parts to a column name for display.
func toColumnName(s []string) string {
	if len(s) == 0 {
		return ""
	}
	// Label fields: "metadata.labels.labelName"
	if len(s) >= 3 && s[0] == "metadata" && s[1] == "labels" {
		return strings.Join(s, ".")
	}
	// Numeric indexed: "spec.containers.image[3]"
	if len(s) > 1 {
		lastPart := s[len(s)-1]
		if !containsNonNumeric.MatchString(lastPart) {
			return strings.Join(s[:len(s)-1], ".") + "[" + lastPart + "]"
		}
	}
	return strings.Join(s, ".")
}

func getLabelColumnNameToDisplay(fieldParts []string) (string, error) {
	lastPart := fieldParts[2]
	const nameLimit = 63
	if len(lastPart) > nameLimit {
		return "", fmt.Errorf("label value %s..%s (%d chars, max %d) is too long", lastPart[0:10], lastPart[len(lastPart)-10:], len(lastPart), nameLimit)
	}
	simpleName := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	if simpleName.MatchString(lastPart) {
		return strings.Join(fieldParts, "."), nil
	}
	compoundName := regexp.MustCompile(`[^a-zA-Z0-9_\-./]`)
	if compoundName.MatchString(lastPart) {
		return "", fmt.Errorf("invalid label name: %s", lastPart)
	}
	return fmt.Sprintf("metadata.labels[%s]", lastPart), nil
}
