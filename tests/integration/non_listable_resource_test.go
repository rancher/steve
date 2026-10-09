package tests

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime/schema"
	_ "modernc.org/sqlite"
)

func (i *IntegrationSuite) TestIssue51930() {
	ctx := i.T().Context()
	// Start steve with SQL Cache enabled
	steveHandler, err := server.New(ctx, i.restCfg, &server.Options{
		SQLCache: true,
		SQLCacheFactoryOptions: factory.CacheFactoryOptions{
			GCInterval:  15 * time.Minute,
			GCKeepCount: 1000,
		},
	})
	i.Require().NoError(err)

	httpServer := httptest.NewServer(steveHandler)
	defer httpServer.Close()

	baseURL := httpServer.URL

	// Wait for schemas to be ready
	// We wait for TokenReviews schema to be registered in Steve.
	// This confirms Steve has completed discovery.
	i.waitForSchema(baseURL, schema.GroupVersionResource{Group: "authentication.k8s.io", Version: "v1", Resource: "tokenreviews"})

	// The TokenReview schema ID.
	schemaID := "authentication.k8s.io.tokenreviews"

	// Attempt to list TokenReviews
	url := fmt.Sprintf("%s/v1/%s", baseURL, schemaID)

	client := &http.Client{
		Timeout: 20 * time.Second,
	}

	fmt.Printf("Making request to %s\n", url)
	resp, err := client.Get(url)

	// If the request times out, err will be a timeout error.
	// We expect the request to return (likely with an error status because TokenReview is not listable).
	// But crucially, we expect it NOT to hang.
	i.Require().NoError(err, "Request timed out or failed")
	defer resp.Body.Close()

	fmt.Printf("TokenReview list response: %d\n", resp.StatusCode)

	// Check if the table was created in the SQLite database.
	// The DB path is in i.sqliteDatabaseFile

	db, err := sql.Open("sqlite", i.sqliteDatabaseFile)
	i.Require().NoError(err)
	defer db.Close()

	// The table name usually follows the GVK pattern: group_version_kind
	// authentication.k8s.io_v1_TokenReview
	tableName := "authentication.k8s.io_v1_TokenReview"

	var name string
	err = db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?", tableName).Scan(&name)

	// We expect this to Fail (err == sql.ErrNoRows) if the bug is fixed.
	// If the bug is present, the table might have been created (if the request didn't hang before creating it).
	// Actually, the hang usually happens *because* it tries to start an informer for a non-watchable resource.
	// If it starts the informer, it likely creates the table first.
	// So checking for table existence is a good proxy for "did it try to cache this?".

	// If the fix is to PREVENT starting the cache, then the table should not exist.
	assert.ErrorIs(i.T(), err, sql.ErrNoRows, "Table %s should not exist", tableName)
}
