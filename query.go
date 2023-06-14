package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slog"
)

var queryCommand = &cli.Command{
	Name:  "query",
	Usage: "Commands for managing queries",
	Subcommands: []*cli.Command{
		{
			Name:   "list",
			Usage:  "List known queries.",
			Action: QueryList,
			Flags:  union([]cli.Flag{}, dbFlags, loggingFlags, hlogDefaultTrue),
		},
		{
			Name:   "add",
			Usage:  "Add a query.",
			Action: QueryAdd,
			Flags: union([]cli.Flag{
				&cli.StringFlag{
					Name:     "name",
					Required: true,
					Usage:    "Name of query.",
				},
				&cli.IntFlag{
					Name:     "source-id",
					Required: true,
					Usage:    "ID of source.",
				},
				&cli.StringFlag{
					Name:     "query",
					Required: true,
					Usage:    "Query to be executed.",
				},
				&cli.StringFlag{
					Name:     "query-type",
					Required: true,
					Usage:    "Type of query syntax.",
				},
				&cli.StringFlag{
					Name:     "interval",
					Required: true,
					Usage:    "Interval at which query should be executed.",
				},
				&cli.StringFlag{
					Name:     "start",
					Required: true,
					Usage:    "The time at which the query's collected data should start.",
				},
				&cli.StringFlag{
					Name:     "finish",
					Required: false,
					Usage:    "The time at which the query's collected data should finish.",
				},
			}, dbFlags, loggingFlags),
		},
		{
			Name:   "finish",
			Usage:  "Finish a query.",
			Action: QueryFinish,
			Flags: union([]cli.Flag{
				&cli.IntFlag{
					Name:     "id",
					Required: true,
					Usage:    "ID of query.",
				},
				&cli.StringFlag{
					Name:     "finish",
					Required: true,
					Usage:    "The time at which the query's collected data should finish, a valid RFC3339 timestamp or the keyword 'now'.",
					Value:    "now",
				},
			}, dbFlags, loggingFlags),
		},
		{
			Name:   "exec",
			Usage:  "Execute a query.",
			Action: QueryExec,
			Flags: union([]cli.Flag{
				&cli.IntFlag{
					Name:     "id",
					Required: true,
					Usage:    "ID of query.",
				},
				&cli.IntFlag{
					Name:  "seq",
					Usage: "Sequence number of query series to execute.",
				},
			}, dbFlags, loggingFlags),
		},
		{
			Name:   "nextseq",
			Usage:  "Show the expected next sequence number after the current time.",
			Action: QueryNextSeq,
			Flags: union([]cli.Flag{
				&cli.IntFlag{
					Name:     "id",
					Required: true,
					Usage:    "ID of query.",
				},
			}, dbFlags, loggingFlags),
		},
		{
			Name:   "test",
			Usage:  "Test a query.",
			Action: QueryTest,
			Flags: union([]cli.Flag{
				&cli.IntFlag{
					Name:     "source-id",
					Required: true,
					Usage:    "ID of source.",
				},
				&cli.StringFlag{
					Name:     "query",
					Required: true,
					Usage:    "Query to be executed.",
				},
				&cli.StringFlag{
					Name:     "query-type",
					Required: true,
					Usage:    "Type of query syntax.",
				},
				&cli.StringFlag{
					Name:     "interval",
					Required: true,
					Usage:    "Interval at which query should be executed.",
				},
				&cli.StringFlag{
					Name:     "start",
					Required: true,
					Usage:    "The time at which the query's collected data should start.",
				},
				&cli.IntFlag{
					Name:  "seq",
					Usage: "Sequence number of query series to execute.",
				},
			}, dbFlags, loggingFlags),
		},
	},
}

func QueryList(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging()

	db := NewDB(dbConnStr())
	conn, err := db.NewConn(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	rows, err := conn.Query(ctx, "select q.id, q.name, s.name, p.name, q.query, q.query_type, q.interval, q.start from queries q join sources s on s.id=q.source_id join providers p on p.id=s.provider_id")
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	type QueryInfoRow struct {
		ID           int
		Name         string
		SourceName   string
		ProviderName string
		Query        string
		QueryType    QueryType
		Interval     string
		Start        time.Time
	}

	qis, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByPos[QueryInfoRow])
	if err != nil {
		return fmt.Errorf("collect: %w", err)
	}

	if len(qis) == 0 {
		fmt.Println("No queries found")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 1, 1, 4, ' ', 0)
	fmt.Fprintln(w, "ID\t| Name\t| Source\t| Provider\t| Start\t| Interval\t| Type\t| Query")
	for _, qi := range qis {
		fmt.Fprintf(w, "%d\t| %s\t| %s\t| %s\t| %s\t| %s\t| %s\t| %s\n", qi.ID, qi.Name, qi.SourceName, qi.ProviderName, qi.Start.Format("2006-01-02T15:04:05Z"), qi.Interval, qi.QueryType, qi.Query)
	}
	return w.Flush()
}

func QueryAdd(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging()

	name := strings.TrimSpace(cc.String("name"))
	sourceID := cc.Int("source-id")
	query := strings.TrimSpace(cc.String("query"))
	queryType := strings.TrimSpace(cc.String("query-type"))
	interval := strings.TrimSpace(cc.String("interval"))
	startStr := strings.TrimSpace(cc.String("start"))
	finishStr := strings.TrimSpace(cc.String("finish"))

	if name == "" {
		return fmt.Errorf("name must be supplied")
	}

	if query == "" {
		return fmt.Errorf("query must be supplied")
	}

	if queryType == "" {
		return fmt.Errorf("query-type must be supplied")
	}

	if interval == "" {
		return fmt.Errorf("interval must be supplied")
	}

	if startStr == "" {
		return fmt.Errorf("start must be supplied")
	}

	if sourceID < 0 {
		return fmt.Errorf("source ID must be a positive integer")
	}

	start, err := time.Parse("2006-01-02T15:04:05Z", startStr)
	if err != nil {
		// attempt to parse as unix timestamp (seconds since epoch)
		ts, err := strconv.ParseInt(startStr, 10, 32)
		if err != nil {
			return fmt.Errorf("start must be a time formatted as '2006-01-02T15:04:05Z' or a unix timestamp")
		}
		start = time.Unix(ts, 0)
	}

	var finish *time.Time
	if finishStr != "" {
		f, err := time.Parse("2006-01-02T15:04:05Z", finishStr)
		if err != nil {
			// attempt to parse as unix timestamp (seconds since epoch)
			ts, err := strconv.ParseInt(finishStr, 10, 32)
			if err != nil {
				return fmt.Errorf("start must be a time formatted as '2006-01-02T15:04:05Z' or a unix timestamp")
			}
			f = time.Unix(ts, 0)
		}

		finish = &f
	}

	db := NewDB(dbConnStr())
	if err := ValidateEnumValue(ctx, db, "interval_type", interval); err != nil {
		return fmt.Errorf("unsupported interval type: %w", err)
	}
	if err := ValidateEnumValue(ctx, db, "query_type", queryType); err != nil {
		return fmt.Errorf("unsupported query type: %w", err)
	}

	startOrig := start
	switch interval {
	case "hourly":
		start = start.Truncate(time.Hour)
	case "daily":
		start = start.Truncate(24 * time.Hour)
	case "weekly":
		start = start.Truncate(7 * 24 * time.Hour)
	default:
		return fmt.Errorf("unsupported interval: must be one of 'hourly','daily','weekly'")

	}

	if !startOrig.Equal(start) {
		slog.Info("truncated start to " + start.Format("2006-01-02T15:04:05Z"))
	}

	conn, err := db.NewConn(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "insert into queries(name,source_id,query,query_type,interval,start,finish) values ($1,$2,$3,$4,$5,$6,$7)", name, sourceID, query, queryType, interval, start, finish)
	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

func QueryExec(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging()

	queryID := cc.Int("id")
	seq := cc.Int("seq")

	if queryID < 0 {
		return fmt.Errorf("ID must be a positive integer")
	}

	if seq <= 0 {
		return fmt.Errorf("sequence must be greater than zero")
	}

	db := NewDB(dbConnStr())

	qry, err := GetQuery(ctx, db, queryID)
	if err != nil {
		return fmt.Errorf("get query: %w", err)
	}

	ss := new(SecretStore)
	secrets, err := ss.Secrets(qry.ProviderID, qry.AuthType)
	if err != nil {
		return fmt.Errorf("failed to get secrets for provider: %w", err)
	}

	points, err := DispatchQuery(ctx, qry, seq, secrets)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	if len(points) == 0 {
		return fmt.Errorf("no points found")
	}

	w := tabwriter.NewWriter(os.Stdout, 1, 1, 4, ' ', 0)
	fmt.Fprintln(w, "Seq\t| Time\t| Value")
	for _, pt := range points {
		fmt.Fprintf(w, "%d\t| %s\t| %v\t\n", pt.Seq, pt.Time.Format("2006-01-02T15:04:05Z"), formatFloat64(pt.Value))
	}
	return w.Flush()
}

func QueryNextSeq(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging()

	queryID := cc.Int("id")

	if queryID < 0 {
		return fmt.Errorf("ID must be a positive integer")
	}

	db := NewDB(dbConnStr())

	qry, err := GetQuery(ctx, db, queryID)
	if err != nil {
		return fmt.Errorf("get query: %w", err)
	}

	fmt.Printf("Expected next sequence: %d\n", qry.SeqAfter(time.Now().UTC()))

	return nil
}

func QueryTest(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging()

	sourceID := cc.Int("source-id")
	query := strings.TrimSpace(cc.String("query"))
	queryType := strings.TrimSpace(cc.String("query-type"))
	interval := strings.TrimSpace(cc.String("interval"))
	startStr := strings.TrimSpace(cc.String("start"))
	seq := cc.Int("seq")

	if query == "" {
		return fmt.Errorf("query must be supplied")
	}

	if queryType == "" {
		return fmt.Errorf("query-type must be supplied")
	}

	if interval == "" {
		return fmt.Errorf("interval must be supplied")
	}

	if startStr == "" {
		return fmt.Errorf("start must be supplied")
	}

	if sourceID < 0 {
		return fmt.Errorf("source ID must be a positive integer")
	}

	if seq < 0 {
		return fmt.Errorf("seq must be a positive integer")
	}

	start, err := time.Parse("2006-01-02T15:04:05Z", startStr)
	if err != nil {
		// attempt to parse as unix timestamp (seconds since epoch)
		ts, err := strconv.ParseInt(startStr, 10, 32)
		if err != nil {
			return fmt.Errorf("start must be a time formatted as '2006-01-02T15:04:05Z' or a unix timestamp")
		}
		start = time.Unix(ts, 0)
	}

	db := NewDB(dbConnStr())
	if err := ValidateEnumValue(ctx, db, "interval_type", interval); err != nil {
		return fmt.Errorf("unsupported interval type %q: %w", interval, err)
	}
	if err := ValidateEnumValue(ctx, db, "query_type", queryType); err != nil {
		return fmt.Errorf("unsupported query type %q: %w", queryType, err)
	}

	startOrig := start
	switch interval {
	case "hourly":
		start = start.Truncate(time.Hour)
	case "daily":
		start = start.Truncate(24 * time.Hour)
	case "weekly":
		start = start.Truncate(7 * 24 * time.Hour)
	default:
		return fmt.Errorf("unsupported interval: must be one of 'hourly','daily','weekly'")

	}

	if !startOrig.Equal(start) {
		slog.Info("truncated start to " + start.Format("2006-01-02T15:04:05Z"))
	}

	s, err := GetSource(ctx, db, sourceID)
	if err != nil {
		return fmt.Errorf("failed to get source: %w", err)
	}

	q := &Query{
		Name:       query,
		Query:      query,
		Interval:   QueryInterval(interval),
		Start:      start,
		QueryType:  QueryType(queryType),
		Dataset:    s.Dataset,
		ProviderID: s.ProviderID,
		ApiType:    s.ApiType,
		ApiURL:     s.ApiURL,
		AuthType:   s.AuthType,
	}

	ss := new(SecretStore)
	secrets, err := ss.Secrets(q.ProviderID, q.AuthType)
	if err != nil {
		return fmt.Errorf("failed to get secrets for provider: %w", err)
	}

	points, err := DispatchQuery(ctx, q, seq, secrets)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	if len(points) == 0 {
		return fmt.Errorf("no points found")
	}

	w := tabwriter.NewWriter(os.Stdout, 1, 1, 4, ' ', 0)
	fmt.Fprintln(w, "Seq\t| Time\t| Value")
	for _, pt := range points {
		fmt.Fprintf(w, "%d\t| %s\t| %v\t\n", pt.Seq, pt.Time.Format("2006-01-02T15:04:05Z"), formatFloat64(pt.Value))
	}
	return w.Flush()
}

func QueryFinish(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging()

	queryID := cc.Int("id")
	finishStr := strings.TrimSpace(cc.String("finish"))

	if queryID < 0 {
		return fmt.Errorf("ID must be a positive integer")
	}

	var finish *time.Time
	if finishStr == "" {
		return fmt.Errorf("finish time must be supplied")
	}

	if finishStr == "now" {
		f := time.Now().UTC()
		finish = &f
	} else {
		f, err := time.Parse("2006-01-02T15:04:05Z", finishStr)
		if err != nil {
			// attempt to parse as unix timestamp (seconds since epoch)
			ts, err := strconv.ParseInt(finishStr, 10, 32)
			if err != nil {
				return fmt.Errorf("start must be a time formatted as '2006-01-02T15:04:05Z' or a unix timestamp")
			}
			f = time.Unix(ts, 0)
		}

		finish = &f
	}

	db := NewDB(dbConnStr())
	conn, err := db.NewConn(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "update queries set finish=$1 where id=$2", finish, queryID)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}
