package metrics

import (
	"context"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	updateInterval = 15 * time.Second
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

func SetUpdateInterval(interval int) {
	updateInterval = time.Duration(interval) * time.Second
}

func StartDatabaseMetricsLogger(ctx context.Context, dbPath string) {
	if !prometheusMetrics {
		return
	}
	go func() {
		ticker := time.NewTicker(updateInterval)
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
