package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/iand/pontium/prom"
	"github.com/iand/pontium/run"
	"github.com/iand/pontium/wait"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slog"
)

var daemonCommand = &cli.Command{
	Name:   "daemon",
	Usage:  "Run a daemon that continually keeps collections up to date.",
	Action: Daemon,
	Flags: union([]cli.Flag{
		&cli.StringFlag{
			Name:        "diag-addr",
			Aliases:     []string{"da"},
			Usage:       "Run diagnostics server for metrics on `ADDRESS:PORT`",
			Value:       "",
			EnvVars:     []string{envPrefix + "DIAG_ADDR"},
			Destination: &daemonOpts.diagnosticsAddr,
		},
	}, dbFlags, loggingFlags, hlogDefaultFalse),
}

var daemonOpts struct {
	diagnosticsAddr string
}

func Daemon(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging()

	g := new(run.Group)

	qc := new(QueryCollector)
	qc.db = NewDB(dbConnStr())
	qc.ss = new(SecretStore)
	qc.monitors = new(sync.Map)
	g.Add(qc)

	// Init metric reporting if required
	if daemonOpts.diagnosticsAddr != "" {
		pr, err := prom.NewPrometheusServer(daemonOpts.diagnosticsAddr, "/metrics", appName)
		if err != nil {
			return fmt.Errorf("failed to initialize metric reporting: %w", err)
		}
		g.Add(pr)
	}

	return g.RunAndWait(ctx)
}

type QueryCollector struct {
	db                 *DB
	ss                 *SecretStore
	monitors           *sync.Map
	activeQueriesGauge prom.Gauge
	monitorGauge       prom.Gauge
}

func (qc *QueryCollector) Run(ctx context.Context) error {
	var err error
	qc.activeQueriesGauge, err = prom.NewPrometheusGauge("active_queries", "Current number of active queries", nil)
	if err != nil {
		return fmt.Errorf("create active_queries gauge: %w", err)
	}
	qc.monitorGauge, err = prom.NewPrometheusGauge("monitored_queries", "Current number of queries being monitored", nil)
	if err != nil {
		return fmt.Errorf("create monitored_queries gauge: %w", err)
	}
	return wait.Forever(ctx, qc.monitorActiveQueries, 0, 10*time.Minute, 0.1)
}

func (qc *QueryCollector) monitorActiveQueries(ctx context.Context) error {
	qs, err := FetchActiveQueries(ctx, qc.db)
	if err != nil {
		slog.Error("failed to fetch active queries", "error", err)
		return nil
	}

	qc.activeQueriesGauge.Set(float64(len(qs)))

	for _, q := range qs {
		q := q
		slog.Debug("found active query", "query_id", q.ID, "name", q.Name)
		ps, err := qc.ss.Secrets(q.ProviderID, q.AuthType)
		if err != nil {
			slog.Error("failed to get secrets for provider", "provider_id", q.ProviderID, "error", err)
			continue
		}

		qm := &QueryMonitor{
			db:    qc.db,
			query: q,
			ps:    ps,
		}
		if _, running := qc.monitors.LoadOrStore(qm.query.ID, qm); !running {
			slog.Debug("no monitor found for query", "query_id", q.ID, "name", q.Name)
			qc.monitorGauge.Inc()
			go func(ctx context.Context, qm *QueryMonitor) {
				defer qc.monitors.Delete(qm.query.ID)
				defer qc.monitorGauge.Dec()

				slog.Info("starting query monitor", "query_id", qm.query.ID, "name", q.Name)
				if err := qm.Run(ctx); err != nil {
					if errors.Is(err, context.Canceled) {
						slog.Info("monitor query stopped", "query_id", qm.query.ID)
					} else {
						slog.Error("monitor query stopped", "query_id", qm.query.ID, "error", err)
					}
				}
			}(ctx, qm)
		}

	}

	return nil
}

type QueryMonitor struct {
	db                *DB
	query             *Query
	ps                ProviderSecrets
	collectionCounter prom.Counter
	errorCounter      prom.Counter
}

func (m *QueryMonitor) Run(ctx context.Context) error {
	var err error
	m.collectionCounter, err = prom.NewPrometheusCounter("query_collection_total", "Total number of collections made for a query", map[string]string{
		"query_id": strconv.Itoa(m.query.ID),
	})
	m.errorCounter, err = prom.NewPrometheusCounter("query_error_total", "Total number of errors encountered when collecting for a query", map[string]string{
		"query_id": strconv.Itoa(m.query.ID),
	})
	if err != nil {
		return fmt.Errorf("create active_queries gauge: %w", err)
	}

	return wait.Forever(ctx, m.MonitorQuery, 10*time.Second, 10*time.Minute, 0.5)
}

func (m *QueryMonitor) MonitorQuery(ctx context.Context) error {
	logger := slog.With("query_id", m.query.ID)
	logger.Info("looking for collection gaps", "name", m.query.Name)

	seqs, err := FindCollectionGaps(ctx, m.db, m.query.ID)
	if err != nil {
		return fmt.Errorf("find collection gaps: %w", err)
	}

	if len(seqs) == 0 {
		logger.Info("no gaps found")
		return nil
	}
	logger.Info(fmt.Sprintf("found %d gaps to be collected", len(seqs)))

	errsEncountered := 0
	for i, seq := range seqs {
		logger := logger.With("seq", seq, "time", m.query.SeqTime(seq))
		if i > 0 {
			if err := wait.WithJitter(ctx, 3*time.Second, 0.1); err != nil {
				return err
			}
		}
		logger.Info("filling gap")
		m.collectionCounter.Inc()
		points, err := DispatchQuery(ctx, m.query, seq, m.ps)
		if err != nil {
			logger.Error("failed to execute query", "error", err)
			m.errorCounter.Inc()
			errsEncountered++
			continue
		}

		if len(points) == 0 {
			logger.Error("no points found")
			m.errorCounter.Inc()
			errsEncountered++
			continue
		}

		if len(points) > 1 {
			logger.Error(fmt.Sprintf("too many points found: %d", len(points)))
			m.errorCounter.Inc()
			errsEncountered++
			continue
		}

		logger.Info("writing collection sequence", "value", points[0].Value)
		if err := WriteCollectionSeq(ctx, m.db, m.query.ID, points[0].Seq, points[0].Value, false); err != nil {
			logger.Error("failed to write collection sequence", "error", err)
			m.errorCounter.Inc()
			errsEncountered++
			continue
		}
	}

	if errsEncountered == 0 {
		logger.Info("gap fill completed with no errors")
	} else {
		logger.Warn(fmt.Sprintf("gap fill completed with %d errors", errsEncountered))
	}
	return nil
}
