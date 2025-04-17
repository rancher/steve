/*
Copyright 2024, 2025 SUSE LLC
*/

package informer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/rancher/steve/pkg/sqlcache/sqltypes"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/rancher/steve/pkg/stores/sqlpartition/listprocessor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"
)

var vaiFile *os.File
var dbClient *sql.DB

func getListOptionIndexer(t *testing.T, ctx context.Context, namespaced bool) (*ListOptionIndexer, error) {
	myStore := NewMockStore(gomock.NewController(t))

	i := &Indexer{
		ctx:      ctx,
		Store:    myStore,
		indexers: cache.Indexers{},
	}
	fields := []string{"metadata.name", "metadata.creationTimestamp", "metadata.fields[0]", "metadata.fields[1]", "metadata.fields[2]", "id", "metadata.state.name"}
	l := &ListOptionIndexer{
		Indexer:       i,
		namespaced:    namespaced,
		indexedFields: fields,
	}
	return l, nil
}

func getListOptionIndexerForQuery(t *testing.T, ctx context.Context, query string) (*ListOptionIndexer, *sqltypes.ListOptions, error) {
	l, err := getListOptionIndexer(t, ctx, false)
	if err != nil {
		return nil, nil, err
	}
	apiOp := &types.APIRequest{
		Type:      "namespace",
		Method:    "GET",
		Namespace: "",
		Request: &http.Request{
			Method: "GET",
			URL: &url.URL{
				RawQuery: query,
			},
		},
	}
	lo, err := listprocessor.ParseQuery(apiOp, nil)
	return l, &lo, err
}

func TestEmptyFilter(t *testing.T) {
	l, lo, err := getListOptionIndexerForQuery(t, context.Background(), "")
	require.Nil(t, err)
	p := partition.Partition{Passthrough: true}
	partitions := []partition.Partition{p}
	namespace := ""
	queryInfo, err := l.constructQuery(lo, partitions, namespace, "_v1_Namespace")
	require.Nil(t, err)
	require.Equal(t, queryInfo.query, "SELECT o.object, o.objectnonce, o.dekid FROM \"_v1_Namespace\" o\n  JOIN \"_v1_Namespace_fields\" f ON o.key = f.key\n  ORDER BY f.\"metadata.name\" ASC\n  LIMIT ?")
	require.Equal(t, 1, len(queryInfo.params))
	q := queryInfo.query
	q = "SELECT o.key, " + q[len("SELECT")+1:]
	stmt, err := dbClient.Prepare(q)
	require.Nil(t, err)
	defer stmt.Close()
	rows, err := stmt.Query(queryInfo.params[0])
	require.Nil(t, err)
	numRows := 0
	for rows.Next() {
		numRows += 1
		var key string
		var o2, o3, o4 string
		err = rows.Scan(&key, &o2, &o3, &o4)
		require.Nil(t, err)
		require.NotEqual(t, "", key)
	}
	err = rows.Err()
	require.Nil(t, err)
	assert.Equal(t, 20, numRows) // TODO: Improve this
}

func TestSimpleFilterOnName(t *testing.T) {
	l, lo, err := getListOptionIndexerForQuery(t, context.Background(), "filter=metadata.name~cluster-")
	require.Nil(t, err)
	p := partition.Partition{Passthrough: true}
	partitions := []partition.Partition{p}
	namespace := ""
	queryInfo, err := l.constructQuery(lo, partitions, namespace, "_v1_Namespace")
	require.Nil(t, err)
	// Other tests should verify the returned query, so these tests are just for sanity checking
	require.True(t, strings.Contains(queryInfo.query, "f.\"metadata.name\" LIKE"))
	require.True(t, strings.Contains(queryInfo.query, "ORDER BY f.\"metadata.name\" ASC"))
	require.Equal(t, len(queryInfo.params), 2)
	require.Equal(t, queryInfo.params[0], "%cluster-%")
	q := queryInfo.query
	q = "SELECT o.key, " + q[len("SELECT")+1:]
	stmt, err := dbClient.Prepare(q)
	require.Nil(t, err)
	defer stmt.Close()
	rows, err := stmt.Query(queryInfo.params...)
	require.Nil(t, err)
	numRows := 0
	sawKeys := sets.NewString()
	for rows.Next() {
		numRows += 1
		var key string
		var o2, o3, o4 string
		err = rows.Scan(&key, &o2, &o3, &o4)
		require.Nil(t, err)
		require.NotEqual(t, "", key)
		require.False(t, sawKeys.Has(key))
		sawKeys.Insert(key)
	}
	err = rows.Err()
	require.Nil(t, err)
	assert.Equal(t, 4, numRows)
	assert.True(t, sawKeys.HasAll("cluster-01", "cluster-02", "cluster-bacon", "cluster-eggs"))
}

func wrapStartOfQueryGetRows(t *testing.T, ctx context.Context, query string) (*sql.Rows, error) {
	l, lo, err := getListOptionIndexerForQuery(t, ctx, query)
	if err != nil {
		return nil, err
	}
	p := partition.Partition{Passthrough: true}
	partitions := []partition.Partition{p}
	namespace := ""
	queryInfo, err := l.constructQuery(lo, partitions, namespace, "_v1_Namespace")
	if err != nil {
		return nil, err
	}
	stmt, err := dbClient.Prepare(queryInfo.query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.Query(queryInfo.params...)
}

func getFirstFieldFromRows(rows *sql.Rows) ([]string, error) {
	names := make([]string, 0)
	for rows.Next() {
		var key string
		var o2, o3 string
		if err := rows.Scan(&key, &o2, &o3); err != nil {
			return names, err
		}
		names = append(names, key)
	}
	return names, rows.Err()
}

func TestNonIndirectQueries(t *testing.T) {
	type testCase struct {
		description     string
		query           string
		expectedResults []string
	}
	var tests []testCase
	tests = append(tests, testCase{
		description:     "simple matching query sort ascending",
		query:           "filter=metadata.name~cluster-&sort=metadata.name",
		expectedResults: []string{"cluster-01", "cluster-02", "cluster-bacon", "cluster-eggs"},
	})
	tests = append(tests, testCase{
		description:     "simple matching query sort descending",
		query:           "filter=metadata.name~cluster-&sort=-metadata.name",
		expectedResults: []string{"cluster-eggs", "cluster-bacon", "cluster-02", "cluster-01"},
	})
	tests = append(tests, testCase{
		description:     "cluster or nervous, sort name asc",
		query:           "filter=metadata.name~cluster-,metadata.state.name=nervous&sort=metadata.name",
		expectedResults: []string{"cluster-01", "cluster-02", "cluster-bacon", "cluster-eggs", "project-04"},
	})
	tests = append(tests, testCase{
		description:     "name contains a '0', sort by state asc",
		query:           "filter=metadata.name~0&sort=metadata.state.name",
		expectedResults: []string{"cluster-01", "project-02", "project-05", "user-01", "cluster-02", "project-03", "project-01", "project-04"},
	})
	tests = append(tests, testCase{
		description: "label contains a fcio/cattleId', sort by state desc only",
		query:       "filter=metadata.labels[field.cattle.io/projectId]&sort=-metadata.state.name",
		expectedResults: []string{"cattle-pears", "cluster-bacon", "cattle-limes", "cluster-eggs",
			"cattle-lemons", "cattle-mangoes", "fleet-local", "fleet-default", "default"},
	})
	tests = append(tests, testCase{
		description: "label contains a fcio/cattleId', sort by state desc only, name asc",
		query:       "filter=metadata.labels[field.cattle.io/projectId]&sort=-metadata.state.name,metadata.name",
		expectedResults: []string{"cattle-pears", "cluster-bacon", "cattle-limes", "cluster-eggs",
			"cattle-lemons", "cattle-mangoes", "default", "fleet-default", "fleet-local"},
	})
	tests = append(tests, testCase{
		description:     "label contains a fcio/cattleId', sort by state desc only, name desc",
		query:           "filter=metadata.labels[field.cattle.io/projectId]&sort=-metadata.state.name,-metadata.name",
		expectedResults: []string{"cluster-bacon", "cattle-pears", "cluster-eggs", "cattle-limes", "fleet-local", "fleet-default", "default", "cattle-mangoes", "cattle-lemons"},
	})
	tests = append(tests, testCase{
		description:     "label contains a fcio/cattleId, age between 206 and 210 (using set notation)', sort by state desc only, name desc",
		query:           "filter=metadata.fields[2] in (206, 207, 208, 209),metadata.fields[2]=210&filter=metadata.fields[2]<211&filter=metadata.labels[field.cattle.io/projectId]&sort=-metadata.state.name,-metadata.name",
		expectedResults: []string{"cluster-bacon", "cattle-limes", "cattle-mangoes"},
	})
	//tests = append(tests, testCase{
	//	description:     "label contains a fcio/cattleId, age between 206 and 210', sort by state desc only, name desc",
	//	query:           "filter=metadata.fields[2]>205&filter=metadata.fields[2]<211&filter=metadata.labels[field.cattle.io/projectId]&sort=-metadata.state.name,-metadata.name",
	//	expectedResults: []string{"cluster-bacon", "cattle-limes", "cattle-mangoes"},
	//})
	//tests = append(tests, testCase{
	//	description:     "TEMP TEST: fields[2] 206 - 208",
	//	query:           "filter=metadata.fields[2]>205&filter=metadata.fields[2]<209&sort=metadata.fields[2]",
	//	expectedResults: []string{"cattle-mangoes", "cattle-limes", "cattle-kiwis"},
	//})

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			rows, err := wrapStartOfQueryGetRows(t, context.Background(), test.query)
			require.Nil(t, err)
			names, err := getFirstFieldFromRows(rows)
			require.Nil(t, err)
			assert.Equal(t, len(test.expectedResults), len(names))
			assert.Equal(t, test.expectedResults, names)
		})
	}
}

func TestSimpleIndirectQueries(t *testing.T) {
	type testCase struct {
		description     string
		query           string
		expectedResults []string
	}
	var tests []testCase
	tests = append(tests, testCase{
		description:     "indirect on cluster-*, accepting all, ASC",
		query:           "filter=metadata.name~cluster-&sort=metadata.labels[field.cattle.io/projectId]=>[management.cattle.io/v3][Project][metadata.name][spec.clusterName]",
		expectedResults: []string{"cluster-eggs", "cluster-bacon", "cluster-01", "cluster-02"},
	})
	tests = append(tests, testCase{
		description:     "indirect on cluster-*, accepting all, DESC (so nulls first)",
		query:           "filter=metadata.name~cluster-&sort=-metadata.labels[field.cattle.io/projectId]=>[management.cattle.io/v3][Project][metadata.name][spec.clusterName]",
		expectedResults: []string{"cluster-01", "cluster-02", "cluster-bacon", "cluster-eggs"},
	})
	tests = append(tests, testCase{
		description:     "label contains a fcio/cattleId, age between 206 and 210 (using set notation)', indirect sort by state desc only, name desc",
		query:           "filter=metadata.fields[2] in (206, 207, 208, 209),metadata.fields[2]=210&filter=metadata.fields[2]<211&filter=metadata.labels[field.cattle.io/projectId]&sort=metadata.labels[field.cattle.io/projectId]=>[management.cattle.io/v3][Project][metadata.name][spec.clusterName],-metadata.name",
		expectedResults: []string{"cattle-limes", "cluster-bacon", "cattle-mangoes"},
	})
	tests = append(tests, testCase{
		description:     "label contains a fcio/cattleId, age between 206 and 210 (using set notation)', indirect sort by state desc only, name desc, redundant label accessors",
		query:           "filter=metadata.fields[2] in (206, 207, 208, 209, 210)&filter=metadata.labels[field.cattle.io/projectId]&filter=metadata.labels[field.cattle.io/projectId]&sort=metadata.labels[field.cattle.io/projectId]=>[management.cattle.io/v3][Project][metadata.name][spec.clusterName],-metadata.name",
		expectedResults: []string{"cattle-limes", "cluster-bacon", "cattle-mangoes"},
	})

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			ctx := context.Background()
			rows, err := wrapStartOfQueryGetRows(t, ctx, test.query)
			require.Nil(t, err)
			names, err := getFirstFieldFromRows(rows)
			require.Nil(t, err)
			assert.Equal(t, len(test.expectedResults), len(names))
			assert.Equal(t, test.expectedResults, names)
		})
	}
}

func TestMultiSortWithIndirect(t *testing.T) {
	type testCase struct {
		description     string
		query           string
		expectedResults []string
	}
	var tests []testCase
	tests = append(tests, testCase{
		description: "indirect on cluster-*, accepting all, ASC",
		query:       "filter=metadata.name~cluster-,metadata.name~cattle-&sort=metadata.state.name,metadata.labels[field.cattle.io/projectId]=>[management.cattle.io/v3][Project][metadata.name][spec.clusterName],metadata.name",
		expectedResults: []string{
			// state: "active"
			// cluster-01 clusterName
			"cattle-lemons",
			// local clusterName
			"cattle-mangoes",
			// no clusterName
			"cattle-kiwis",
			"cattle-plums",
			"cluster-01",

			// state: "hungry"
			// cluster-01 clusterName
			"cluster-eggs",
			// cluster-02 clusterName
			"cattle-limes",
			// no clusterName
			"cluster-02",

			// state: "muddy"
			// local clusterName
			"cattle-pears",
			"cluster-bacon",
		},
	})

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			ctx := context.Background()
			l, lo, err := getListOptionIndexerForQuery(t, ctx, test.query)
			require.Nil(t, err)
			p := partition.Partition{Passthrough: true}
			partitions := []partition.Partition{p}
			namespace := ""
			queryInfo, err := l.constructQuery(lo, partitions, namespace, "_v1_Namespace")
			require.Nil(t, err)
			stmt, err := dbClient.Prepare(queryInfo.query)
			require.Nil(t, err)
			defer stmt.Close()
			rows, err := stmt.Query(queryInfo.params...)
			require.Nil(t, err)
			names, err := getFirstFieldFromRows(rows)
			require.Nil(t, err)
			assert.Equal(t, len(test.expectedResults), len(names))
			assert.Equal(t, test.expectedResults, names)
		})
	}
}

func TestMain(m *testing.M) {
	err := setupTests()
	if err != nil {
		panic(fmt.Sprintf("Awp! verify_generator_test.go tests failed to setup: %s", err))
	}
	m.Run()
	err = teardownTests()
	if err != nil {
		fmt.Fprintf(os.Stderr, "teardown tests failed: %s\n", err)
	}
}

func setupTests() error {
	var err error
	vaiFile, err = ioutil.TempFile("", "vaidb")
	//fmt.Fprintf(os.Stderr, "vaiFile: %s\n", vaiFile.Name())
	if err != nil {
		return err
	}
	db, err := sql.Open("sqlite3", vaiFile.Name())
	if err != nil {
		return err
	}
	dbClient = db
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return errors.New("test setup: runtime.Caller() failed")
	}
	fixtureDir := filepath.Join(filepath.Dir(filename), "fixtures")
	fileNames := []string{
		"schema.txt", "_v1_Namespace.txt", "_v1_Namespace_fields.txt",
		"_v1_Namespace_labels.txt", "management.cattle.io_v3_Project.txt",
		"management.cattle.io_v3_Project_fields.txt",
		"management.cattle.io_v3_Project_labels.txt",
	}
	for _, fileName := range fileNames {
		fullPath := filepath.Join(fixtureDir, fileName)
		sqlStmt, err := ioutil.ReadFile(fullPath)
		if err != nil {
			return err
		}
		if len(sqlStmt) == 0 {
			continue
		}
		_, err = db.Exec(string(sqlStmt))
		if err != nil {
			return fmt.Errorf("setup: can't create execute file %s: %w", fullPath, err)
		}
	}
	return nil
}

func teardownTests() error {
	if dbClient != nil {
		if err := dbClient.Close(); err != nil {
			return err
		}
	}
	if vaiFile != nil {
		return os.Remove(vaiFile.Name())
	}
	return nil
}
