package informer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/sqlcache/db"
	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
	"github.com/sirupsen/logrus"
)

func (l *ListOptionIndexer) ListSummaryFields(ctx context.Context, lo *sqltypes.ListOptions, partitions []partition.Partition, dbName string, namespace string) (*types.APISummary, error) {
	joinTableIndexByLabelName := make(map[string]int)
	const mainObjectPrefix = "o"
	const mainFieldPrefix = "f1"
	const isSummaryFilter = true
	includeSort := lo.SummaryFieldList != nil
	filterComponents, err := l.compileQuery(lo, partitions, namespace, dbName, mainFieldPrefix, joinTableIndexByLabelName, includeSort, isSummaryFilter)
	if err != nil {
		return nil, err
	}
	summaryNamespaced := lo.SummaryNamespaced
	countsByProperty := make(map[string]any)
	// We have to copy the current data-structures because processing other label summary-fields
	// could modify them, but we don't want to see those changes on subsequent fields
	for fieldNum, field := range lo.SummaryFieldList {
		//TODO: Don't make copies on the last run
		copyOfJoinTableIndexByLabelName := make(map[string]int)
		for k, v := range joinTableIndexByLabelName {
			copyOfJoinTableIndexByLabelName[k] = v
		}
		copyOfFilterComponents := filterComponents.copy()
		data, err := l.ListSummaryForField(ctx, field, fieldNum, dbName, &copyOfFilterComponents, mainFieldPrefix, copyOfJoinTableIndexByLabelName, summaryNamespaced)
		if err != nil {
			return nil, err
		}
		for k, v := range data {
			countsByProperty[k] = v
		}
	}
	return convertMapToAPISummary(countsByProperty, summaryNamespaced), nil
}

func (l *ListOptionIndexer) ListSummaryForField(ctx context.Context, field []string, fieldNum int, dbName string, filterComponents *filterComponentsT, mainFieldPrefix string, joinTableIndexByLabelName map[string]int, summaryNamespaced bool) (map[string]any, error) {
	queryInfo, err := l.constructSummaryQueryForField(field, fieldNum, dbName, filterComponents, mainFieldPrefix, joinTableIndexByLabelName, summaryNamespaced)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Summary ListOptionIndexer prepared statement: %v", queryInfo.query)
	logrus.Debugf("Params: %v", queryInfo.params)
	return l.executeSummaryQueryForField(ctx, queryInfo, field, summaryNamespaced)
}

func (l *ListOptionIndexer) constructSummaryQueryForField(fieldParts []string, fieldNum int, dbName string, filterComponents *filterComponentsT, mainFieldPrefix string, joinTableIndexByLabelName map[string]int, summaryNamespaced bool) (*QueryInfo, error) {
	columnName := toColumnName(fieldParts)
	var columnNameToDisplay string
	var err error
	if isLabelsFieldList(fieldParts) {
		columnNameToDisplay, err = getLabelColumnNameToDisplay(fieldParts)
	} else {
		columnNameToDisplay, err = l.getStandardColumnNameToDisplay(fieldParts, mainFieldPrefix)
	}
	if err != nil {
		return nil, err
	}
	if filterComponents.isEmpty && !summaryNamespaced {
		if !isLabelsFieldList(fieldParts) {
			// No need for a main-field prefix, so recalc
			var err error
			columnNameToDisplay, err = l.getStandardColumnNameToDisplay(fieldParts, "")
			if err != nil {
				//TODO: Prove that this can't happen
				return nil, err
			}
		}
		return l.constructSimpleSummaryQueryForField(fieldParts, dbName, columnName, columnNameToDisplay)
	}
	return l.constructComplexSummaryQueryForField(fieldParts, fieldNum, dbName, columnName, columnNameToDisplay, filterComponents, mainFieldPrefix, joinTableIndexByLabelName, summaryNamespaced)
}

func (l *ListOptionIndexer) constructSimpleSummaryQueryForField(fieldParts []string, dbName, columnName, columnNameToDisplay string) (*QueryInfo, error) {
	if isLabelsFieldList(fieldParts) {
		return l.constructSimpleSummaryQueryForLabelField(fieldParts, dbName, columnName, columnNameToDisplay)
	}
	return l.constructSimpleSummaryQueryForStandardField(fieldParts, dbName, columnName, columnNameToDisplay)
}

func (l *ListOptionIndexer) constructSimpleSummaryQueryForLabelField(fieldParts []string, dbName, columnName, columnNameToDisplay string) (*QueryInfo, error) {
	query := fmt.Sprintf(`SELECT '%s' AS p, COUNT(*) AS c, value AS k
	FROM "%s_labels"
	WHERE label = ? AND k != ""
	GROUP BY k`,
		columnNameToDisplay, dbName)
	args := make([]any, 1)
	args[0] = fieldParts[2]
	return &QueryInfo{query: query, params: args}, nil
}

func (l *ListOptionIndexer) constructSimpleSummaryQueryForStandardField(fieldParts []string, dbName, columnName, columnNameToDisplay string) (*QueryInfo, error) {
	query := fmt.Sprintf(`SELECT '%s' AS p, COUNT(*) AS c, %s AS k
	FROM "%s_fields"
	WHERE k != ""
	GROUP BY k`,
		columnName, columnNameToDisplay, dbName)
	return &QueryInfo{query: query}, nil
}

func convertMapToAPISummary(countsByProperty map[string]any, summaryNamespaced bool) *types.APISummary {
	total := len(countsByProperty)
	blocksToSort := make([]types.SummaryEntry, 0, total)
	for property, v := range countsByProperty {
		fixedCounts := make(map[string]types.SummaryWithBreakdown)
		counts := v.(map[string]any)["counts"].(map[string]int)
		for k1, v1 := range counts {
			summary := types.SummaryWithBreakdown{Total: v1}
			if summaryNamespaced {
				summary.Namespace = map[string]int{"*": v1}
			}
			fixedCounts[k1] = summary
		}
		blocksToSort = append(blocksToSort, types.SummaryEntry{Property: property, Counts: fixedCounts})
	}

	sortedBlocks := slices.SortedFunc(slices.Values(blocksToSort), func(a, b types.SummaryEntry) int {
		return strings.Compare(a.Property, b.Property)
	})
	return &types.APISummary{SummaryItems: sortedBlocks}
}

func (l *ListOptionIndexer) executeSummaryQueryForField(ctx context.Context, queryInfo *QueryInfo, field []string, summaryNamespaced bool) (map[string]any, error) {
	stmt, err := l.Prepare(queryInfo.query)
	if err != nil {
		return nil, err
	}
	params := queryInfo.params
	defer func() {
		if cerr := stmt.Close(); cerr != nil && err == nil {
			err = errors.Join(err, cerr)
		}
	}()

	var items [][]string
	err = l.WithTransaction(ctx, false, func(tx db.TxClient) error {
		now := time.Now()
		rows, err := tx.Stmt(stmt).QueryContext(ctx, params...)
		if err != nil {
			return err
		}
		elapsed := time.Since(now)
		logLongQuery(elapsed, queryInfo.query, params)
		items, err = l.ReadStringIntString1or2(rows, summaryNamespaced)
		if err != nil {
			return fmt.Errorf("executeSummaryQueryForField: read objects: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	propertyBlock := make(map[string]any)
	var countsBlock map[string]int
	for _, item := range items {
		propertyName := item[0]
		thisPBlock, ok := propertyBlock[propertyName]
		if !ok {
			propertyBlock[propertyName] = make(map[string]any)
			thisPBlock = propertyBlock[propertyName]
			thisPBlock.(map[string]any)["counts"] = make(map[string]int)
		}
		countsBlock = thisPBlock.(map[string]any)["counts"].(map[string]int)
		val, err := strconv.Atoi(item[1])
		if err != nil {
			return nil, err
		}
		countsBlock[item[2]] = val
	}

	return propertyBlock, nil
}

func (l *ListOptionIndexer) executeSummaryQuery(ctx context.Context, queryInfo *QueryInfo) (*types.APISummary, error) {
	stmt, err := l.Prepare(queryInfo.query)
	if err != nil {
		return nil, err
	}
	params := queryInfo.params
	defer func() {
		if cerr := stmt.Close(); cerr != nil && err == nil {
			err = errors.Join(err, cerr)
		}
	}()

	var items [][]string
	err = l.WithTransaction(ctx, false, func(tx db.TxClient) error {
		now := time.Now()
		rows, err := tx.Stmt(stmt).QueryContext(ctx, params...)
		if err != nil {
			return err
		}
		elapsed := time.Since(now)
		logLongQuery(elapsed, queryInfo.query, params)
		items, err = l.ReadStringIntString1or2(rows, false)
		if err != nil {
			return fmt.Errorf("executeSummaryQuery: read objects: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	countsByProperty := make(map[string]map[string]any)

	for _, item := range items {
		propertyName := item[0]
		propertyBlock, ok := countsByProperty[propertyName]
		if !ok {
			propertyBlock = make(map[string]any)
			countsByProperty[propertyName] = propertyBlock
			propertyBlock["property"] = propertyName
			propertyBlock["counts"] = make(map[string]any)
		}
		val, err := strconv.Atoi(item[1])
		if err != nil {
			return nil, err
		}
		propertyBlock["counts"].(map[string]any)[item[2]] = val
	}

	total := len(countsByProperty)
	blocksToSort := make([]types.SummaryEntry, 0, total)
	for _, v := range countsByProperty {
		property := v["property"].(string)
		fixedCounts := make(map[string]types.SummaryWithBreakdown)
		countMap := v["counts"].(map[string]any)
		for k1, v1 := range countMap {
			fmt.Printf("QQQ: value is %v\n", v1)
			fixedCounts[k1] = types.SummaryWithBreakdown{Total: 42} //.Total
		}
		blocksToSort = append(blocksToSort, types.SummaryEntry{Property: property, Counts: fixedCounts})
	}
	sortedBlocks := slices.SortedFunc(slices.Values(blocksToSort), func(a, b types.SummaryEntry) int {
		return strings.Compare(a.Property, b.Property)
	})
	summary := types.APISummary{SummaryItems: sortedBlocks}
	return &summary, nil
}
