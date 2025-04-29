package informer

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
)

var (
	badTableNameChars  = regexp.MustCompile(`[^-a-zA-Z0-9._]+`)
	nonIdentifierChars = regexp.MustCompile(`[^a-zA-Z0-9_]+`)
)

func (l *ListOptionIndexer) constructQuery(lo *sqltypes.ListOptions, partitions []partition.Partition, namespace string, dbName string) (*QueryInfo, error) {
	indirectSortDirective, err := checkForIndirectSortDirective(lo)
	if err != nil {
		return nil, err
	}
	if indirectSortDirective != nil {
		if err = l.validateDirective(indirectSortDirective); err != nil {
			return nil, err
		}
	}
	joinTableIndexByLabelName := make(map[string]int)
	if indirectSortDirective != nil && isLabelsFieldList(indirectSortDirective.Fields) {
		return l.constructIndirectSortQuery(lo, partitions, namespace, dbName, joinTableIndexByLabelName)
	}
	ensureSortLabelsAreSelected(lo)
	return l.finishConstructQuery(lo, partitions, namespace, dbName, joinTableIndexByLabelName)
}

/** constructIndirectSortQuery - process indirect-sorting
 * Here we create two queries:
 * one that has an existence test for the sorter,
 * and one with a non-existence test, so each of these is AND-ed with the other WHERE tests (filters).
 * Then do a `UNION ALL` on the two different queries.
 * The unobvious part: have the original options-list do only the one indirect sort.
 * Have the copy process any other sort options.
 *
 * Two limitations:
 * 1. Only at most one indirect sort per query
 * 2. The indirect sort will go before the other ones (todo: fix this)
 */

func (l *ListOptionIndexer) constructIndirectSortQuery(lo *sqltypes.ListOptions, partitions []partition.Partition, namespace string, dbName string, joinTableIndexByLabelName map[string]int) (*QueryInfo, error) {
	var loNoLabel sqltypes.ListOptions
	// We want to make sure that the want-sort-label options test for the label's existence,
	// but we want the non-label to have a not-exists test on it.  So first ensure it exists,
	// then ensure a non-exists test exists on the non-label filter.
	// The other thing is we put all the non-indirect sort directives on the copy of the list options
	var indirectSortDirective sqltypes.Sort
	newSortList1 := make([]sqltypes.Sort, 1)
	newSortList2 := make([]sqltypes.Sort, 0, len(lo.SortList.SortDirectives)-1)
	indirectSortPosition := -1
	for i, sd := range lo.SortList.SortDirectives {
		if sd.IsIndirect {
			indirectSortDirective = lo.SortList.SortDirectives[i]
			newSortList1[0] = indirectSortDirective
			indirectSortPosition = i
		} else {
			newSortList2 = append(newSortList2, sd)
		}
	}
	if indirectSortPosition == -1 {
		return nil, fmt.Errorf("expected an indirect sort directive, didn't find one")
	}
	bytes, err := json.Marshal(*lo)
	if err != nil {
		return nil, fmt.Errorf("can't json-encode list options: %w", err)
	}
	err = json.Unmarshal(bytes, &loNoLabel)
	if err != nil {
		return nil, fmt.Errorf("can't json-decode list options: %w", err)
	}
	applyIndirectLabelTests(lo, &loNoLabel, &indirectSortDirective)
	lo.SortList.SortDirectives = newSortList1
	loNoLabel.SortList.SortDirectives = newSortList2
	joinParts1, whereClauses1, params1, needsDistinctModifier1, _, _, _, err1 := l.getQueryParts(lo, partitions, namespace, dbName, joinTableIndexByLabelName)
	if err1 != nil {
		return nil, err1
	}
	// Now add clauses for the indirectSortDirective
	joinParts2, whereClauses2, params2, needsDistinctModifier2, orderByClauses2, orderByParams2, _, err2 := l.getQueryParts(&loNoLabel, partitions, namespace, dbName, joinTableIndexByLabelName)
	if err2 != nil {
		return nil, err2
	}
	addFalseTest := false
	if whereClauses1[len(whereClauses1)-1] == "FALSE" {
		whereClauses1 = whereClauses1[:len(whereClauses1)-1]
		addFalseTest = true
	}
	if whereClauses2[len(whereClauses2)-1] == "FALSE" {
		whereClauses2 = whereClauses2[:len(whereClauses2)-1]
		addFalseTest = true
	}
	distinctModifier := ""
	if needsDistinctModifier1 || needsDistinctModifier2 {
		distinctModifier = " DISTINCT"
	}

	externalTableName := getExternalTableName(&indirectSortDirective)
	extIndex, ok := joinTableIndexByLabelName[externalTableName]
	if !ok {
		return nil, fmt.Errorf("internal error: unable to find an entry for external table %s", externalTableName)
	}
	sortParts, importWithParts, importAsNullParts := processOrderByFields(&indirectSortDirective, extIndex, orderByClauses2)
	selectLine := fmt.Sprintf("SELECT%s o.object AS __ix_object, o.objectnonce AS __ix_objectnonce, o.dekid AS __ix_dekid", distinctModifier)
	indent1 := "  "
	indent2 := indent1 + indent1
	indent3 := indent2 + indent1
	where1 := joinWhereClauses(whereClauses1, indent2, indent3, "AND")
	where2 := joinWhereClauses(whereClauses2, indent2, indent3, "AND")

	parts := []string{
		"SELECT __ix_object, __ix_objectnonce, __ix_dekid FROM (",
		fmt.Sprintf(`%s%s, %s FROM %s`, indent1, selectLine, strings.Join(importWithParts, ", "), strings.Join(joinParts1, "\n"+indent2)),
		where1,
		"UNION ALL",
		fmt.Sprintf(`%s%s, %s FROM %s`, indent1, selectLine, strings.Join(importAsNullParts, ", "), strings.Join(joinParts2, "\n"+indent2)),
		where2,
		")",
	}
	if addFalseTest {
		parts = append(parts, "WHERE FALSE")
	}
	params := make([]any, 0, len(params1)+len(params2)+len(orderByParams2))
	params = append(params, params1...)
	params = append(params, params2...)
	fullQuery := strings.Join(parts, "\n")
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", fullQuery)
	countParams := params[:]
	params = append(params, orderByParams2...)

	fixedSortParts := putIndirectSortInPosition(sortParts, indirectSortPosition)
	fullQuery += fmt.Sprintf("\n%sORDER BY %s", indent1, strings.Join(fixedSortParts, ", "))
	queryInfo := &QueryInfo{
		query:       fullQuery,
		params:      params,
		countQuery:  countQuery,
		countParams: countParams,
	}
	return queryInfo, nil
}

func putIndirectSortInPosition(sortParts []string, indirectSortPosition int) []string {
	fixedSortParts := make([]string, 0, len(sortParts))
	indirectSortPart := sortParts[0]
	sortParts = sortParts[1:]
	fixedSortParts = append(fixedSortParts, sortParts[0:indirectSortPosition]...)
	fixedSortParts = append(fixedSortParts, indirectSortPart)
	fixedSortParts = append(fixedSortParts, sortParts[indirectSortPosition:len(sortParts)]...)
	return fixedSortParts
}

func (l *ListOptionIndexer) finishConstructQuery(lo *sqltypes.ListOptions, partitions []partition.Partition, namespace string, dbName string, joinTableIndexByLabelName map[string]int) (*QueryInfo, error) {

	joinParts, whereClauses, params, needsDistinctModifier, orderByClauses, orderByParams, sortSelectField, err := l.getQueryParts(lo, partitions, namespace, dbName, joinTableIndexByLabelName)
	if err != nil {
		return nil, err
	}
	distinctModifier := ""
	if needsDistinctModifier {
		distinctModifier = " DISTINCT"
	}
	queryInfo := &QueryInfo{}

	if len(sortSelectField) > 0 {
		if sortSelectField[0] != ' ' {
			sortSelectField = " " + sortSelectField
		}
	}
	query := fmt.Sprintf(`SELECT%s o.object, o.objectnonce, o.dekid%s FROM `, distinctModifier, sortSelectField)
	query += strings.Join(joinParts, "\n  ")

	if len(whereClauses) > 0 {
		indent := "    "
		separator := fmt.Sprintf(") AND\n%s(", indent)
		query += fmt.Sprintf("\n  WHERE\n%s(%s)", indent, strings.Join(whereClauses, separator))
	}

	// before proceeding, save a copy of the query and params without LIMIT/OFFSET/ORDER info
	// for COUNTing all results later
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", query)
	countParams := params[:]
	if len(orderByClauses) > 0 {
		query += "\n  ORDER BY "
		query += strings.Join(orderByClauses, ", ")
		params = append(params, orderByParams...)
	}

	// 4- sqltypes.Pagination: LIMIT clause (from lo.Pagination and/or lo.ChunkSize/lo.Resume)

	limitClause := ""
	// take the smallest limit between lo.Pagination and lo.ChunkSize
	limit := lo.Pagination.PageSize
	if limit == 0 || (lo.ChunkSize > 0 && lo.ChunkSize < limit) {
		limit = lo.ChunkSize
	}
	if limit > 0 {
		limitClause = "\n  LIMIT ?"
		params = append(params, limit)
	}

	// OFFSET clause (from lo.Pagination and/or lo.Resume)
	offsetClause := ""
	offset := 0
	if lo.Resume != "" {
		offsetInt, err := strconv.Atoi(lo.Resume)
		if err != nil {
			return queryInfo, err
		}
		offset = offsetInt
	}
	if lo.Pagination.Page >= 1 {
		offset += lo.Pagination.PageSize * (lo.Pagination.Page - 1)
	}
	if offset > 0 {
		offsetClause = "\n  OFFSET ?"
		params = append(params, offset)
	}
	if limit > 0 || offset > 0 {
		query += limitClause
		query += offsetClause
		queryInfo.countQuery = countQuery
		queryInfo.countParams = countParams
		queryInfo.limit = limit
		queryInfo.offset = offset
	}
	// Otherwise leave these as default values and the executor won't do pagination work

	queryInfo.query = query
	queryInfo.params = params

	return queryInfo, nil
}

// Other ListOptionIndexer methods for generating SQL in alphabetical order:

// buildORClause creates an SQLite compatible query that ORs conditions built from passed filters
func (l *ListOptionIndexer) buildORClauseFromFilters(orFilters sqltypes.OrFilter, dbName string, joinTableIndexByLabelName map[string]int, joinedTables map[string]bool) (string, []string, []any, bool, error) {
	params := make([]any, 0)
	whereClauses := make([]string, 0, len(orFilters.Filters))
	joinClauses := make([]string, 0)
	needDistinct := false

	for _, filter := range orFilters.Filters {
		if isLabelFilter(&filter) {
			fullName := fmt.Sprintf("%s:%s", dbName, filter.Field[2])
			labelIndex, ok := joinTableIndexByLabelName[fullName]
			if !ok {
				labelIndex = len(joinTableIndexByLabelName) + 1
				joinTableIndexByLabelName[fullName] = labelIndex
			}
			_, ok = joinedTables[fullName]
			if !ok {
				joinedTables[fullName] = true
				joinClauses = append(joinClauses, fmt.Sprintf(`LEFT OUTER JOIN "%s_labels" lt%d ON o.key = lt%d.key`, dbName, labelIndex, labelIndex))
			}
			needDistinct = true
			labelFunc := l.getLabelFilter
			if isIndirectFilter(&filter) {
				labelFunc = l.getIndirectLabelFilter
			}
			newWhereClause, newJoins, newParams, err := labelFunc(filter, dbName, joinTableIndexByLabelName, joinedTables)
			if err != nil {
				return "", nil, nil, needDistinct, err
			}
			joinClauses = append(joinClauses, newJoins...)
			if newWhereClause != "" {
				whereClauses = append(whereClauses, newWhereClause)
			}
			params = append(params, newParams...)
		} else if isIndirectFilter(&filter) {
			newWhereClause, newJoins, newParams, err := l.getIndirectNonLabelFilter(filter, dbName, joinTableIndexByLabelName, joinedTables)
			if err != nil {
				return "", nil, nil, needDistinct, err
			}
			joinClauses = append(joinClauses, newJoins...)
			if newWhereClause != "" {
				whereClauses = append(whereClauses, newWhereClause)
			}
			params = append(params, newParams...)
		} else {
			newWhereClause, newParams, err := l.getFieldFilter(filter)
			if err != nil {
				return "", nil, nil, needDistinct, err
			}
			if newWhereClause != "" {
				whereClauses = append(whereClauses, newWhereClause)
			}
			params = append(params, newParams...)
		}
	}
	finalWhereClause := ""
	switch len(whereClauses) {
	case 0:
		finalWhereClause = "" // no change
	case 1:
		finalWhereClause = whereClauses[0]
	default:
		finalWhereClause = fmt.Sprintf("(%s)", strings.Join(whereClauses, ") OR ("))
	}
	return finalWhereClause, joinClauses, params, needDistinct, nil
}

// ensureSortLabelsAreSelected - if the user tries to sort on a particular label without mentioning it in a query,
// and it's not an indirect sort directive, we need to ensure the label is added.
func ensureSortLabelsAreSelected(lo *sqltypes.ListOptions) {
	if len(lo.SortList.SortDirectives) == 0 {
		return
	}
	unboundSortLabels := make(map[string]bool)
	for _, sortDirective := range lo.SortList.SortDirectives {
		fields := sortDirective.Fields
		if isLabelsFieldList(fields) {
			unboundSortLabels[fields[2]] = true
		}
	}
	if len(unboundSortLabels) == 0 {
		return
	}
	// If we have sort directives but no filters, add an exists-filter for each label.
	if lo.Filters == nil || len(lo.Filters) == 0 {
		lo.Filters = make([]sqltypes.OrFilter, 1)
		lo.Filters[0].Filters = make([]sqltypes.Filter, len(unboundSortLabels))
		i := 0
		for labelName := range unboundSortLabels {
			lo.Filters[0].Filters[i] = sqltypes.Filter{
				Field: []string{"metadata", "labels", labelName},
				Op:    sqltypes.Exists,
			}
			i++
		}
		return
	}
	// Find any labels that are already mentioned in an existing filter
	// The gotcha is we have to bind the labels for each set of orFilters, so copy them each time
	for i, orFilters := range lo.Filters {
		copyUnboundSortLabels := make(map[string]bool, len(unboundSortLabels))
		for k, v := range unboundSortLabels {
			copyUnboundSortLabels[k] = v
		}
		for _, filter := range orFilters.Filters {
			if isLabelFilter(&filter) {
				copyUnboundSortLabels[filter.Field[2]] = false
			}
		}
		// Now for any labels that are still true, add another where clause
		for labelName, needsBinding := range copyUnboundSortLabels {
			if needsBinding {
				// `orFilters` is a copy of lo.Filters[i], so reference the original.
				lo.Filters[i].Filters = append(lo.Filters[i].Filters, sqltypes.Filter{
					Field: []string{"metadata", "labels", labelName},
					Op:    sqltypes.Exists,
				})
			}
		}
	}
}

// Possible ops from the k8s parser:
// KEY = and == (same) VALUE
// KEY != VALUE
// KEY exists []  # ,KEY, => this filter
// KEY ! []  # ,!KEY, => assert KEY doesn't exist
// KEY in VALUES
// KEY notin VALUES

func (l *ListOptionIndexer) getFieldFilter(filter sqltypes.Filter) (string, []any, error) {
	opString := ""
	escapeString := ""
	columnName := toColumnName(filter.Field)
	if err := l.validateColumn(columnName); err != nil {
		return "", nil, err
	}
	switch filter.Op {
	case sqltypes.Eq:
		if filter.Partial {
			opString = "LIKE"
			escapeString = escapeBackslashDirective
		} else {
			opString = "="
		}
		clause := fmt.Sprintf(`f."%s" %s ?%s`, columnName, opString, escapeString)
		return clause, []any{formatMatchTarget(filter)}, nil
	case sqltypes.NotEq:
		if filter.Partial {
			opString = "NOT LIKE"
			escapeString = escapeBackslashDirective
		} else {
			opString = "!="
		}
		clause := fmt.Sprintf(`f."%s" %s ?%s`, columnName, opString, escapeString)
		return clause, []any{formatMatchTarget(filter)}, nil

	case sqltypes.Lt, sqltypes.Gt:
		sym, target, err := prepareComparisonParameters(filter.Op, filter.Matches[0])
		if err != nil {
			return "", nil, err
		}
		clause := fmt.Sprintf(`f."%s" %s ?`, columnName, sym)
		return clause, []any{target}, nil

	case sqltypes.Exists, sqltypes.NotExists:
		return "", nil, errors.New("NULL and NOT NULL tests aren't supported for non-label queries")

	case sqltypes.In:
		fallthrough
	case sqltypes.NotIn:
		target := "()"
		if len(filter.Matches) > 0 {
			target = fmt.Sprintf("(?%s)", strings.Repeat(", ?", len(filter.Matches)-1))
		}
		opString = "IN"
		if filter.Op == sqltypes.NotIn {
			opString = "NOT IN"
		}
		clause := fmt.Sprintf(`f."%s" %s %s`, columnName, opString, target)
		matches := make([]any, len(filter.Matches))
		for i, match := range filter.Matches {
			matches[i] = match
		}
		return clause, matches, nil
	}

	return "", nil, fmt.Errorf("unrecognized operator: %s", opString)
}

func (l *ListOptionIndexer) getIndirectLabelFilter(filter sqltypes.Filter, dbName string, joinTableIndexByLabelName map[string]int, joinedTables map[string]bool) (string, []string, []any, error) {
	if len(filter.IndirectFields) != 4 {
		s := "<empty>"
		if len(filter.IndirectFields) > 0 {
			s = strings.Join(filter.IndirectFields, " ")
		}
		return "", nil, nil, fmt.Errorf("expected exactly 4 indirect field parts, got %s (%d)", s, len(filter.IndirectFields))
	}
	labelName := filter.Field[2]
	fullName := fmt.Sprintf("%s:%s", dbName, labelName)
	labelIndex, ok := joinTableIndexByLabelName[fullName]
	if !ok {
		return "", nil, nil, fmt.Errorf("internal error: can't find an entry for table %s", fullName)
	}

	joinClauses := make([]string, 0)

	extDBName := fmt.Sprintf("%s_%s", filter.IndirectFields[0], filter.IndirectFields[1])
	extDBName = badTableNameChars.ReplaceAllString(extDBName, "_")
	extDBName = strings.ReplaceAll(extDBName, "/", "_")
	extIndex, ok := joinTableIndexByLabelName[extDBName]
	if !ok {
		extIndex = len(joinTableIndexByLabelName) + 1
		joinTableIndexByLabelName[extDBName] = extIndex
	}

	selectorFieldName := filter.IndirectFields[2]
	if badTableNameChars.MatchString(selectorFieldName) {
		return "", nil, nil, fmt.Errorf("invalid database column name '%s'", selectorFieldName)
	}
	targetFieldName := filter.IndirectFields[3]
	if badTableNameChars.MatchString(targetFieldName) {
		return "", nil, nil, fmt.Errorf("invalid database column name '%s'", targetFieldName)
	}
	extDBNameFields := fmt.Sprintf("%s_fields", extDBName)
	_, ok = joinedTables[extDBNameFields]
	if !ok {
		joinedTables[extDBNameFields] = true
		joinClauses = append(joinClauses, fmt.Sprintf(`JOIN "%s" ext%d ON lt%d.value = ext%d."%s"`, extDBNameFields, extIndex, labelIndex, extIndex, selectorFieldName))
	}
	labelWhereSubClause := fmt.Sprintf("lt%d.label = ?", labelIndex)
	targetFieldReference := fmt.Sprintf(`ext%d."%s"`, extIndex, targetFieldName)
	var clause string
	var op string
	params := []any{labelName}

	opString := ""
	escapeString := ""
	matchFmtToUse := strictMatchFmt
	switch filter.Op {
	case sqltypes.Eq:
		if filter.Partial {
			opString = "LIKE"
			escapeString = escapeBackslashDirective
			matchFmtToUse = matchFmt
		} else {
			opString = "="
		}
		clause = fmt.Sprintf(`%s AND %s %s ?%s`, labelWhereSubClause, targetFieldReference, opString, escapeString)
		params = append(params, formatMatchTargetWithFormatter(filter.Matches[0], matchFmtToUse))
		return clause, joinClauses, params, nil

	case sqltypes.NotEq:
		if filter.Partial {
			opString = "NOT LIKE"
			escapeString = escapeBackslashDirective
			matchFmtToUse = matchFmt
		} else {
			opString = "!="
		}
		clause = fmt.Sprintf(`%s AND %s %s ?%s`, labelWhereSubClause, targetFieldReference, opString, escapeString)
		params = append(params, formatMatchTargetWithFormatter(filter.Matches[0], matchFmtToUse))
		return clause, joinClauses, params, nil

	case sqltypes.Lt, sqltypes.Gt:
		sym, target, err := prepareComparisonParameters(filter.Op, filter.Matches[0])
		if err != nil {
			return "", nil, nil, err
		}
		clause := fmt.Sprintf(`%s AND %s %s ?`, labelWhereSubClause, targetFieldReference, sym)
		params = append(params, target)
		return clause, joinClauses, params, nil

	case sqltypes.Exists:
		clause := fmt.Sprintf(`%s AND %s != NULL`, labelWhereSubClause, targetFieldReference)
		return clause, joinClauses, params, nil

	case sqltypes.NotExists:
		clause := fmt.Sprintf(`%s AND %s == NULL`, labelWhereSubClause, targetFieldReference)
		return clause, joinClauses, params, nil

	case sqltypes.In, sqltypes.NotIn:
		target := "(?"
		if len(filter.Matches) > 0 {
			target += strings.Repeat(", ?", len(filter.Matches)-1)
		}
		target += ")"
		op = "IN"
		if filter.Op == sqltypes.NotIn {
			op = "NOT IN"
		}
		clause := fmt.Sprintf(`%s AND %s %s %s`, labelWhereSubClause, targetFieldReference, op, target)
		for _, match := range filter.Matches {
			params = append(params, match)
		}
		return clause, joinClauses, params, nil

		// See getLabelFilter for rest of operators
	}
	return "", nil, nil, fmt.Errorf("unrecognized operator: %s", opString)
}

func (l *ListOptionIndexer) getIndirectNonLabelFilter(filter sqltypes.Filter, dbName string, joinTableIndexByLabelName map[string]int, joinedTables map[string]bool) (string, []string, []any, error) {
	if len(filter.IndirectFields) != 4 {
		s := "<empty>"
		if len(filter.IndirectFields) > 0 {
			s = strings.Join(filter.IndirectFields, " ")
		}
		return "", nil, nil, fmt.Errorf("expected exactly 4 indirect field parts, got %s (%d)", s, len(filter.IndirectFields))
	}
	columnName := toColumnName(filter.Field)
	if err := l.validateColumn(columnName); err != nil {
		return "", nil, nil, err
	}
	extDBName := fmt.Sprintf("%s_%s", filter.IndirectFields[0], filter.IndirectFields[1])
	extDBName = badTableNameChars.ReplaceAllString(extDBName, "_")
	extIndex, ok := joinTableIndexByLabelName[extDBName]
	if !ok {
		extIndex = len(joinTableIndexByLabelName) + 1
		joinTableIndexByLabelName[extDBName] = extIndex
	}

	selectorFieldName := filter.IndirectFields[2]
	if badTableNameChars.MatchString(selectorFieldName) {
		return "", nil, nil, fmt.Errorf("invalid database column name '%s'", selectorFieldName)
	}
	externalFieldName := filter.IndirectFields[3]
	if badTableNameChars.MatchString(externalFieldName) {
		return "", nil, nil, fmt.Errorf("invalid database column name '%s'", externalFieldName)
	}
	extDBNameFields := fmt.Sprintf("%s_fields", extDBName)
	_, ok = joinedTables[extDBNameFields]
	joinClauses := make([]string, 0)
	if !ok {
		joinedTables[extDBNameFields] = true
		joinClauses = append(joinClauses, fmt.Sprintf(`JOIN "%s_fields" ext%d ON f."%s" = ext%d."%s"`, extDBName, extIndex, columnName, extIndex, selectorFieldName))
	}
	params := make([]any, 0)

	opString := ""
	escapeString := ""
	matchFmtToUse := strictMatchFmt
	switch filter.Op {
	case sqltypes.Eq:
		if filter.Partial {
			opString = "LIKE"
			escapeString = escapeBackslashDirective
			matchFmtToUse = matchFmt
		} else {
			opString = "="
		}
		clause := fmt.Sprintf(`ext%d."%s" %s ?%s`, extIndex, externalFieldName, opString, escapeString)
		return clause, joinClauses, []any{formatMatchTargetWithFormatter(filter.Matches[0], matchFmtToUse)}, nil

	case sqltypes.NotEq:
		if filter.Partial {
			opString = "NOT LIKE"
			escapeString = escapeBackslashDirective
			matchFmtToUse = matchFmt
		} else {
			opString = "!="
		}
		clause := fmt.Sprintf(`ext%d."%s" %s ?%s`, extIndex, externalFieldName, opString, escapeString)
		return clause, joinClauses, []any{formatMatchTargetWithFormatter(filter.Matches[0], matchFmtToUse)}, nil

	case sqltypes.Lt, sqltypes.Gt:
		sym, target, err := prepareComparisonParameters(filter.Op, filter.Matches[0])
		if err != nil {
			return "", nil, nil, err
		}
		clause := fmt.Sprintf(`ext%d."%s" %s ?`, extIndex, externalFieldName, sym)
		return clause, joinClauses, []any{target}, nil

	case sqltypes.Exists:
		clause := fmt.Sprintf(`ext%d."%s" != NULL`, extIndex, externalFieldName)
		return clause, joinClauses, []any{}, nil

	case sqltypes.NotExists:
		clause := fmt.Sprintf(`ext%d."%s" == NULL`, extIndex, externalFieldName)
		return clause, joinClauses, []any{}, nil

	case sqltypes.In, sqltypes.NotIn:
		target := "(?"
		if len(filter.Matches) > 0 {
			target += strings.Repeat(", ?", len(filter.Matches)-1)
		}
		target += ")"
		opString = "IN"
		if filter.Op == sqltypes.NotIn {
			opString = "NOT IN"
		}
		clause := fmt.Sprintf(`ext%d."%s" %s %s`, extIndex, externalFieldName, opString, target)
		for _, match := range filter.Matches {
			params = append(params, match)
		}
		return clause, joinClauses, params, nil
	}
	return "", nil, nil, fmt.Errorf("unrecognized operator: %s", opString)
}

func (l *ListOptionIndexer) getLabelFilter(filter sqltypes.Filter, dbName string, joinTableIndexByLabelName map[string]int, joinedTables map[string]bool) (string, []string, []any, error) {
	opString := ""
	escapeString := ""
	matchFmtToUse := strictMatchFmt
	labelName := filter.Field[2]
	fullName := fmt.Sprintf("%s:%s", dbName, labelName)
	labelIndex, ok := joinTableIndexByLabelName[fullName]
	if !ok {
		return "", nil, nil, fmt.Errorf("internal error: can't find an entry for table %s", fullName)
	}

	joinClauses := make([]string, 0)

	switch filter.Op {
	case sqltypes.Eq:
		if filter.Partial {
			opString = "LIKE"
			escapeString = escapeBackslashDirective
			matchFmtToUse = matchFmt
		} else {
			opString = "="
		}
		clause := fmt.Sprintf(`lt%d.label = ? AND lt%d.value %s ?%s`, labelIndex, labelIndex, opString, escapeString)
		return clause, joinClauses, []any{labelName, formatMatchTargetWithFormatter(filter.Matches[0], matchFmtToUse)}, nil

	case sqltypes.NotEq:
		if filter.Partial {
			opString = "NOT LIKE"
			escapeString = escapeBackslashDirective
			matchFmtToUse = matchFmt
		} else {
			opString = "!="
		}
		subFilter := sqltypes.Filter{
			Field: filter.Field,
			Op:    sqltypes.NotExists,
		}
		existenceClause, _, subParams, err := l.getLabelFilter(subFilter, dbName, joinTableIndexByLabelName, joinedTables)
		if err != nil {
			return "", nil, nil, err
		}
		clause := fmt.Sprintf(`(%s) OR (lt%d.label = ? AND lt%d.value %s ?%s)`, existenceClause, labelIndex, labelIndex, opString, escapeString)
		params := append(subParams, labelName, formatMatchTargetWithFormatter(filter.Matches[0], matchFmtToUse))
		return clause, joinClauses, params, nil

	case sqltypes.Lt, sqltypes.Gt:
		sym, target, err := prepareComparisonParameters(filter.Op, filter.Matches[0])
		if err != nil {
			return "", nil, nil, err
		}
		clause := fmt.Sprintf(`lt%d.label = ? AND lt%d.value %s ?`, labelIndex, labelIndex, sym)
		return clause, joinClauses, []any{labelName, target}, nil

	case sqltypes.Exists:
		clause := fmt.Sprintf(`lt%d.label = ?`, labelIndex)
		return clause, joinClauses, []any{labelName}, nil

	case sqltypes.NotExists:
		clause := fmt.Sprintf(`o.key NOT IN (SELECT o1.key FROM "%s" o1
		JOIN "%s_fields" f1 ON o1.key = f1.key
		LEFT OUTER JOIN "%s_labels" lt%di1 ON o1.key = lt%di1.key
		WHERE lt%di1.label = ?)`, dbName, dbName, dbName, labelIndex, labelIndex, labelIndex)
		return clause, joinClauses, []any{labelName}, nil

	case sqltypes.In:
		target := "(?"
		if len(filter.Matches) > 0 {
			target += strings.Repeat(", ?", len(filter.Matches)-1)
		}
		target += ")"
		clause := fmt.Sprintf(`lt%d.label = ? AND lt%d.value IN %s`, labelIndex, labelIndex, target)
		matches := make([]any, len(filter.Matches)+1)
		matches[0] = labelName
		for i, match := range filter.Matches {
			matches[i+1] = match
		}
		return clause, joinClauses, matches, nil

	case sqltypes.NotIn:
		target := "(?"
		if len(filter.Matches) > 0 {
			target += strings.Repeat(", ?", len(filter.Matches)-1)
		}
		target += ")"
		subFilter := sqltypes.Filter{
			Field: filter.Field,
			Op:    sqltypes.NotExists,
		}
		existenceClause, _, subParams, err := l.getLabelFilter(subFilter, dbName, joinTableIndexByLabelName, joinedTables)
		if err != nil {
			return "", nil, nil, err
		}
		clause := fmt.Sprintf(`(%s) OR (lt%d.label = ? AND lt%d.value NOT IN %s)`, existenceClause, labelIndex, labelIndex, target)
		matches := append(subParams, labelName)
		for _, match := range filter.Matches {
			matches = append(matches, match)
		}
		return clause, joinClauses, matches, nil
	}
	return "", nil, nil, fmt.Errorf("unrecognized operator: %s", opString)
}

func (l *ListOptionIndexer) getQueryParts(lo *sqltypes.ListOptions, partitions []partition.Partition, namespace string, dbName string, joinTableIndexByLabelName map[string]int) ([]string, []string, []any, bool, []string, []any, string, error) {
	joinParts := []string{fmt.Sprintf(`"%s" o`, dbName), fmt.Sprintf(`JOIN "%s_fields" f ON o.key = f.key`, dbName)}
	whereClauses := make([]string, 0)
	params := make([]any, 0)
	needDistinctFinal := false
	joinedTables := make(map[string]bool)
	joinedTables[dbName] = true
	joinedTables[fmt.Sprintf("%s_fields", dbName)] = true
	// 1- Figure out what we'll be joining and testing
	for _, orFilters := range lo.Filters {
		newWhereClause, newJoinParts, newParams, needDistinct, err := l.buildORClauseFromFilters(orFilters, dbName, joinTableIndexByLabelName, joinedTables)

		if err != nil {
			return joinParts, whereClauses, params, needDistinctFinal, nil, nil, "", err
		}
		joinParts = append(joinParts, newJoinParts...)
		if len(newWhereClause) > 0 {
			whereClauses = append(whereClauses, newWhereClause)
		}
		params = append(params, newParams...)
		if needDistinct {
			needDistinctFinal = true
		}
	}

	// WHERE clauses (from namespace)
	if namespace != "" && namespace != "*" {
		whereClauses = append(whereClauses, fmt.Sprintf(`f."metadata.namespace" = ?`))
		params = append(params, namespace)
	}

	// WHERE clauses (from partitions and their corresponding parameters)
	partitionClauses := make([]string, 0)
	for _, thisPartition := range partitions {
		if thisPartition.Passthrough {
			// nothing to do, no extra filtering to apply by definition
		} else {
			singlePartitionClauses := make([]string, 0)

			// filter by namespace
			if thisPartition.Namespace != "" && thisPartition.Namespace != "*" {
				singlePartitionClauses = append(singlePartitionClauses, fmt.Sprintf(`f."metadata.namespace" = ?`))
				params = append(params, thisPartition.Namespace)
			}

			// optionally filter by names
			if !thisPartition.All {
				names := thisPartition.Names

				if names.Len() == 0 {
					// degenerate case, there will be no results
					singlePartitionClauses = append(singlePartitionClauses, "FALSE")
				} else {
					singlePartitionClauses = append(singlePartitionClauses, fmt.Sprintf(`f."metadata.name" IN (?%s)`, strings.Repeat(", ?", thisPartition.Names.Len()-1)))
					// sort for reproducibility
					sortedNames := thisPartition.Names.UnsortedList()
					sort.Strings(sortedNames)
					for _, name := range sortedNames {
						params = append(params, name)
					}
				}
			}

			if len(singlePartitionClauses) > 0 {
				partitionClauses = append(partitionClauses, strings.Join(singlePartitionClauses, " AND "))
			}
		}
	}
	if len(partitions) == 0 {
		// degenerate case, there will be no results
		whereClauses = append(whereClauses, "FALSE")
	}
	if len(partitionClauses) == 1 {
		whereClauses = append(whereClauses, partitionClauses[0])
	}
	if len(partitionClauses) > 1 {
		whereClauses = append(whereClauses, "(\n      ("+strings.Join(partitionClauses, ") OR\n      (")+")\n)")
	}
	sortSelectField, sortJoinClauses, sortWhereClauses, orderByClauses, orderByParams, err := l.getSortDirectives(lo, dbName, joinTableIndexByLabelName)
	joinParts = append(joinParts, sortJoinClauses...)
	whereClauses = append(whereClauses, sortWhereClauses...)

	return joinParts, whereClauses, params, needDistinctFinal, orderByClauses, orderByParams, sortSelectField, err
}

func (l *ListOptionIndexer) getSortDirectives(lo *sqltypes.ListOptions, dbName string, joinTableIndexByLabelName map[string]int) (string, []string, []string, []string, []any, error) {
	sortSelectField := ""
	sortJoinClauses := make([]string, 0)
	sortWhereClauses := make([]string, 0)
	orderByClauses := make([]string, 0)
	orderByParams := make([]any, 0)
	if len(lo.SortList.SortDirectives) == 0 {
		// make sure at least one default order is always picked
		orderByClauses = append(orderByClauses, `f."metadata.name" ASC`)
		if l.namespaced {
			orderByClauses = append(orderByClauses, `f."metadata.namespace" ASC`)
		}
		return sortSelectField, sortJoinClauses, sortWhereClauses, orderByClauses, orderByParams, nil
	}
	for _, sortDirective := range lo.SortList.SortDirectives {
		fields := sortDirective.Fields
		if isLabelsFieldList(fields) {
			labelName := sortDirective.Fields[2]
			fullName := fmt.Sprintf("%s:%s", dbName, labelName)
			labelIndex, ok := joinTableIndexByLabelName[fullName]
			if !ok {
				if sortDirective.IsIndirect {
					return sortSelectField, sortJoinClauses, sortWhereClauses, orderByClauses, orderByParams, fmt.Errorf(`internal error: no join-table index given for labelName "%s"`, labelName)
				}
				labelIndex = len(joinTableIndexByLabelName) + 1
				joinTableIndexByLabelName[fullName] = labelIndex
			}
			if sortDirective.IsIndirect {
				//TODO: check the external table name.
				externalTableName := getExternalTableName(&sortDirective)
				extIndex, ok := joinTableIndexByLabelName[externalTableName]
				if !ok {
					extIndex = len(joinTableIndexByLabelName) + 1
					joinTableIndexByLabelName[externalTableName] = extIndex
				}
				selectorFieldName := sortDirective.IndirectFields[2]
				if badTableNameChars.MatchString(selectorFieldName) {
					return sortSelectField, sortJoinClauses, sortWhereClauses, orderByClauses, orderByParams, fmt.Errorf("invalid database column name '%s'", selectorFieldName)
				}
				externalFieldName := sortDirective.IndirectFields[3]
				if badTableNameChars.MatchString(externalFieldName) {
					return sortSelectField, sortJoinClauses, sortWhereClauses, orderByClauses, orderByParams, fmt.Errorf("invalid database column name '%s'", externalFieldName)
				}
				sortJoinClauses = append(sortJoinClauses, fmt.Sprintf(`JOIN "%s_fields" ext%d ON lt%d.value = ext%d."%s"`, externalTableName, extIndex, labelIndex, extIndex, selectorFieldName))
				//TODO: Verify the field name
				sortSelectField = fmt.Sprintf(`ext%d."%s" as ext%d_target`, extIndex, externalFieldName, extIndex)

			}
			clause, sortParam, err := buildSortLabelsClause(fields[2], dbName, joinTableIndexByLabelName, sortDirective.Order == sqltypes.ASC)
			if err != nil {
				return sortSelectField, sortJoinClauses, sortWhereClauses, orderByClauses, orderByParams, err
			}
			orderByClauses = append(orderByClauses, clause)
			orderByParams = append(orderByParams, sortParam)
		} else if sortDirective.IsIndirect {
			if len(sortDirective.IndirectFields) != 4 {
				return sortSelectField, sortJoinClauses, sortWhereClauses, orderByClauses, orderByParams, fmt.Errorf("expected indirect sort directive to have 4 indirect fields, got %d", len(sortDirective.IndirectFields))
			}
			externalTableName := getExternalTableName(&sortDirective)
			extIndex, ok := joinTableIndexByLabelName[externalTableName]
			if !ok {
				extIndex = len(joinTableIndexByLabelName) + 1
				joinTableIndexByLabelName[externalTableName] = extIndex
			}
			selectorFieldName := sortDirective.IndirectFields[2]
			if badTableNameChars.MatchString(selectorFieldName) {
				return sortSelectField, sortJoinClauses, sortWhereClauses, orderByClauses, orderByParams, fmt.Errorf("invalid database column name '%s'", selectorFieldName)
			}
			externalFieldName := sortDirective.IndirectFields[3]
			if badTableNameChars.MatchString(externalFieldName) {
				return sortSelectField, sortJoinClauses, sortWhereClauses, orderByClauses, orderByParams, fmt.Errorf("invalid database column name '%s'", externalFieldName)
			}
			columnName := toColumnName(fields)
			if err := l.validateColumn(columnName); err != nil {
				return sortSelectField, sortJoinClauses, sortWhereClauses, orderByClauses, orderByParams, err
			}
			sortJoinClauses = append(sortJoinClauses, fmt.Sprintf(`JOIN "%s_fields" ext%d ON f."%s" = ext%d."%s"`, externalTableName, extIndex, columnName, extIndex, selectorFieldName))
			direction := "ASC"
			nullsPlace := "LAST"
			if sortDirective.Order == sqltypes.DESC {
				direction = "DESC"
				nullsPlace = "FIRST"
			}
			orderByClauses = append(orderByClauses, fmt.Sprintf(`ext%d."%s" %s NULLS %s`, extIndex, externalFieldName, direction, nullsPlace))
		} else {
			columnName := toColumnName(fields)
			if err := l.validateColumn(columnName); err != nil {
				return sortSelectField, sortJoinClauses, sortWhereClauses, orderByClauses, orderByParams, err
			}
			direction := "ASC"
			if sortDirective.Order == sqltypes.DESC {
				direction = "DESC"
			}
			orderByClauses = append(orderByClauses, fmt.Sprintf(`f."%s" %s`, columnName, direction))
		}
	}
	return sortSelectField, sortJoinClauses, sortWhereClauses, orderByClauses, orderByParams, nil
}

func (l *ListOptionIndexer) validateColumn(column string) error {
	for _, v := range l.indexedFields {
		if v == column {
			return nil
		}
	}
	return fmt.Errorf("column is invalid [%s]: %w", column, ErrInvalidColumn)
}

func makeGVK(gv string, k string) string {
	ga := strings.SplitN(gv, "/", 2)
	if len(ga) == 1 {
		return fmt.Sprintf("%s_%s", ga[0], k)
	}
	g := ga[0]
	v := ga[1]
	return fmt.Sprintf("%s_%s_%s", g, v, k)
}

func getGVKFieldsFromIndirectFields(indirectFields []string) (g string, v string, k string) {
	a := strings.SplitN(indirectFields[0], "/", 2)
	if len(a) == 2 {
		return a[0], a[1], indirectFields[1]
	}
	if len(a[0]) > 0 && a[0][0] == '_' {
		a[0] = a[0][1:]
	}
	return "", a[0], indirectFields[1]
}

func (l *ListOptionIndexer) validateDirective(isd *sqltypes.Sort) error {
	if !isd.IsIndirect {
		return nil
	}
	if len(isd.IndirectFields) != 4 {
		return fmt.Errorf("expected indirect sort directive to have 4 indirect fields, got %d", len(isd.IndirectFields))
	}
	g, v, k := getGVKFieldsFromIndirectFields(isd.IndirectFields)
	//gvk := makeGVK(isd.IndirectFields[0], isd.IndirectFields[1])
	fieldsList := l.fieldGetterForGroupName(g, v, k)
	found := false
	for _, fields := range fieldsList {
		if smartJoin(fields) == isd.IndirectFields[2] {
			found = true
			break
		}
	}
	errorTemplate := "invalid database column name '%s'"
	if !found {
		return fmt.Errorf(errorTemplate, isd.IndirectFields[2])
	}
	for _, fields := range fieldsList {
		if smartJoin(fields) == isd.IndirectFields[3] {
			return nil
		}
	}
	return fmt.Errorf(errorTemplate, isd.IndirectFields[3])
}

// Helper functions for generating SQL in alphabetical order:

func applyIndirectLabelTests(loWithLabel *sqltypes.ListOptions, loWithoutLabel *sqltypes.ListOptions, indirectSortDirective *sqltypes.Sort) {
	labelFilter := sqltypes.Filter{
		Field: indirectSortDirective.Fields[:],
		//Matches: make([]string, 0),
		Op: sqltypes.Exists,
	}
	loWithLabel.Filters = append(loWithLabel.Filters, sqltypes.OrFilter{Filters: []sqltypes.Filter{labelFilter}})

	// And add an AND-test that the label does not exists for the second test
	nonLabelFilter := labelFilter
	nonLabelFilter.Op = sqltypes.NotExists
	loWithoutLabel.Filters = append(loWithoutLabel.Filters, sqltypes.OrFilter{Filters: []sqltypes.Filter{nonLabelFilter}})
}

func buildSortLabelsClause(labelName string, dbName string, joinTableIndexByLabelName map[string]int, isAsc bool) (string, string, error) {
	fullName := fmt.Sprintf("%s:%s", dbName, labelName)
	labelIndex, ok := joinTableIndexByLabelName[fullName]
	if !ok {
		return "", "", fmt.Errorf(`internal error: no join-table index given for labelName "%s"`, labelName)
	}
	stmt := fmt.Sprintf(`CASE lt%d.label WHEN ? THEN lt%d.value ELSE NULL END`, labelIndex, labelIndex)
	dir := "ASC"
	nullsPosition := "LAST"
	if !isAsc {
		dir = "DESC"
		nullsPosition = "FIRST"
	}
	return fmt.Sprintf("(%s) %s NULLS %s", stmt, dir, nullsPosition), labelName, nil
}

func checkForIndirectSortDirective(lo *sqltypes.ListOptions) (*sqltypes.Sort, error) {
	indirectSortDirectives := make([]string, 0)
	var id *sqltypes.Sort
	for _, sd := range lo.SortList.SortDirectives {
		if sd.IsIndirect {
			id = &sd
			indirectSortDirectives = append(indirectSortDirectives, fmt.Sprintf("[%s]", strings.Join(sd.IndirectFields, "][")))
		}
	}
	if len(indirectSortDirectives) > 1 {
		return nil, fmt.Errorf("can have at most one indirect sort directive, have %d: %s", len(indirectSortDirectives), indirectSortDirectives)
	}
	return id, nil
}

func formatMatchTarget(filter sqltypes.Filter) string {
	format := strictMatchFmt
	if filter.Partial {
		format = matchFmt
	}
	return formatMatchTargetWithFormatter(filter.Matches[0], format)
}

func formatMatchTargetWithFormatter(match string, format string) string {
	// To allow matches on the backslash itself, the character needs to be replaced first.
	// Otherwise, it will undo the following replacements.
	match = strings.ReplaceAll(match, `\`, `\\`)
	match = strings.ReplaceAll(match, `_`, `\_`)
	match = strings.ReplaceAll(match, `%`, `\%`)
	return fmt.Sprintf(format, match)
}

func getExternalTableName(sd *sqltypes.Sort) string {
	s := strings.Join(sd.IndirectFields[0:2], "_")
	return strings.ReplaceAll(s, "/", "_")
}

func isIndirectFilter(filter *sqltypes.Filter) bool {
	return filter.IsIndirect
}

func isLabelFilter(f *sqltypes.Filter) bool {
	return len(f.Field) >= 2 && f.Field[0] == "metadata" && f.Field[1] == "labels"
}

func isLabelsFieldList(fields []string) bool {
	return len(fields) == 3 && fields[0] == "metadata" && fields[1] == "labels"
}

func joinWhereClauses(whereClauses []string, leadingIndent string, continuingIndent string, op string) string {
	switch len(whereClauses) {
	case 0:
		return ""
	case 1:
		return fmt.Sprintf("%sWHERE %s\n", leadingIndent, whereClauses[0])
	}
	separator := fmt.Sprintf(") %s\n%s(", op, continuingIndent)
	return fmt.Sprintf("%sWHERE (%s)\n", leadingIndent, strings.Join(whereClauses, separator))
}

func prepareComparisonParameters(op sqltypes.Op, target string) (string, float64, error) {
	num, err := strconv.ParseFloat(target, 32)
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

func processOrderByFields(sd *sqltypes.Sort, extIndex int, orderByClauses []string) ([]string, []string, []string) {
	sortFieldMap := make(map[string]string)
	externalFieldName := sd.IndirectFields[3]
	newName := fmt.Sprintf("__ix_ext%d_%s", extIndex, nonIdentifierChars.ReplaceAllString(externalFieldName, "_"))
	sortFieldMap[externalFieldName] = newName
	sortParts := make([]string, 1+len(orderByClauses))
	direction := "ASC"
	nullPosition := "LAST"
	if sd.Order == sqltypes.DESC {
		direction = "DESC"
		nullPosition = "FIRST"
	}
	sortParts[0] = fmt.Sprintf("%s %s NULLS %s", newName, direction, nullPosition)
	importWithParts := make([]string, 1+len(orderByClauses))
	importWithParts[0] = fmt.Sprintf(`ext%d."%s" AS %s`, extIndex, externalFieldName, newName)
	importAsNullParts := make([]string, 1+len(orderByClauses))
	importAsNullParts[0] = fmt.Sprintf("NULL AS %s", newName)
	for i, clause := range orderByClauses {
		orderParts := strings.SplitN(clause, " ", 2)
		fieldName := orderParts[0]
		_, ok := sortFieldMap[fieldName]
		if ok {
			continue
		}
		fieldParts := strings.SplitN(fieldName, ".", 2)
		prefix := fieldParts[0]
		baseName := fieldParts[1]
		if baseName[0] == '"' {
			baseName = baseName[1 : len(baseName)-1]
		}
		newBaseName := nonIdentifierChars.ReplaceAllString(baseName, "_")
		newName := fmt.Sprintf("__ix_%s_%s", prefix, newBaseName)
		sortFieldMap[fieldName] = newName
		importWithParts[i+1] = fmt.Sprintf("%s AS %s", fieldName, newName)
		importAsNullParts[i+1] = importWithParts[i+1]
		sortParts[i+1] = fmt.Sprintf("%s %s", newName, orderParts[1])
	}
	return sortParts, importWithParts, importAsNullParts
}

// There are two kinds of string arrays to turn into a string, based on the last value in the array
// simple: ["a", "b", "conformsToIdentifier"] => "a.b.conformsToIdentifier"
// complex: ["a", "b", "foo.io/stuff"] => "a.b[foo.io/stuff]"

func smartJoin(s []string) string {
	if len(s) == 0 {
		return ""
	}
	if len(s) == 1 {
		return s[0]
	}
	lastBit := s[len(s)-1]
	simpleName := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	if simpleName.MatchString(lastBit) {
		return strings.Join(s, ".")
	}
	return fmt.Sprintf("%s[%s]", strings.Join(s[0:len(s)-1], "."), lastBit)
}
