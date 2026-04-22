package metrics

import (
	"context"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const defaultUpdateInterval = 15 // seconds

var (
	SqliteDBSizeMetrics = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "sqlcache",
			Subsystem: "db_main",
			Name:      "bytes",
			Help:      "Size of the sqlite DB file",
		},
	)
	SqliteDBWalSizeMetrics = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "sqlcache",
			Subsystem: "db_wal",
			Name:      "bytes",
			Help:      "Size of the auxiliary sqlite DB WAL file",
		},
	)
	SqliteDBShmSizeMetrics = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "sqlcache",
			Subsystem: "db_shm",
			Name:      "bytes",
			Help:      "Size of the auxiliary sqlite DB SHM file",
		},
	)
)

func StartDatabaseMetricsLogger(ctx context.Context, dbPath string, updateInterval int) {
	if !prometheusMetrics {
		return
	}
	if updateInterval == 0 {
		updateInterval = defaultUpdateInterval
	}
	go func() {
		ticker := time.NewTicker(time.Duration(updateInterval) * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				doDatabaseMetrics(dbPath)
			}
		}
	}()
}

func doDatabaseMetrics(dbPath string) {
	fstat, err := os.Stat(dbPath)
	if err == nil {
		SqliteDBSizeMetrics.Set(float64(fstat.Size()))
	}
	dbPathWal := dbPath + "-wal"
	fstat, err = os.Stat(dbPathWal)
	if err == nil {
		SqliteDBWalSizeMetrics.Set(float64(fstat.Size()))
	}
	dbPathShm := dbPath + "-shm"
	fstat, err = os.Stat(dbPathShm)
	if err == nil {
		SqliteDBShmSizeMetrics.Set(float64(fstat.Size()))
	}
}
