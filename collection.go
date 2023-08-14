package main

import (
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slog"
)

var collectionCommand = &cli.Command{
	Name:  "collection",
	Usage: "Commands for managing collections",
	Subcommands: []*cli.Command{
		{
			Name:   "list",
			Usage:  "List known collections.",
			Action: CollectionList,
			Flags:  union([]cli.Flag{}, dbFlags, loggingFlags, hlogDefaultTrue),
		},
		{
			Name:   "gaps",
			Usage:  "List missing sequences in a collection.",
			Action: CollectionGaps,
			Flags: union([]cli.Flag{
				&cli.IntFlag{
					Name:     "id",
					Required: true,
					Usage:    "ID of query.",
				},
			}, dbFlags, loggingFlags),
		},
		{
			Name:   "fill",
			Usage:  "Fill missing sequences in a collection.",
			Action: CollectionFill,
			Flags: union([]cli.Flag{
				&cli.IntFlag{
					Name:     "id",
					Required: true,
					Usage:    "ID of query.",
				},
			}, dbFlags, loggingFlags),
		},
		{
			Name:   "collect",
			Usage:  "Collect a result from a query and write to the collection.",
			Action: CollectionCollect,
			Flags: union([]cli.Flag{
				&cli.IntFlag{
					Name:     "id",
					Required: true,
					Usage:    "ID of query.",
				},
				&cli.IntFlag{
					Name:  "seq",
					Usage: "Sequence number of query series to collect.",
				},
				&cli.BoolFlag{
					Name:  "force",
					Usage: "Force collected value to be written to sequence.",
				},
			}, dbFlags, loggingFlags),
		},
		{
			Name:   "get",
			Usage:  "Get values values from a collection.",
			Action: CollectionGet,
			Flags: union([]cli.Flag{
				&cli.IntFlag{
					Name:     "id",
					Required: true,
					Usage:    "ID of query.",
				},
				&cli.IntFlag{
					Name:     "from",
					Required: false,
					Usage:    "Show values with sequence equal to or greater than this number.",
				},
				&cli.IntFlag{
					Name:     "to",
					Required: false,
					Usage:    "Show values with sequence equal to or less than this number.",
				},
			}, dbFlags, loggingFlags),
		},
	},
}

func CollectionList(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging()

	db := NewDB(dbConnStr())
	conn, err := db.NewConn(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	rows, err := conn.Query(ctx, "select q.id, q.name, max(c.seq) from queries q left join collections c on q.id=c.query_id group by q.id, q.name order by q.id, q.name")
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	type CollectionInfoRow struct {
		QueryID int
		Name    string
		Seq     *int
	}

	cis, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByPos[CollectionInfoRow])
	if err != nil {
		return fmt.Errorf("collect: %w", err)
	}

	if len(cis) == 0 {
		fmt.Println("No collections found")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 1, 1, 4, ' ', 0)
	fmt.Fprintln(w, "Query ID\t| Name\t| Last Seq")
	for _, ci := range cis {
		seq := "--"
		if ci.Seq != nil {
			seq = strconv.Itoa(*ci.Seq)
		}
		fmt.Fprintf(w, "%d\t| %s\t| %s\n", ci.QueryID, ci.Name, seq)
	}
	return w.Flush()
}

func CollectionGaps(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging()

	queryID := cc.Int("id")

	if queryID < 0 {
		return fmt.Errorf("ID must be a positive integer")
	}

	db := NewDB(dbConnStr())

	seqs, err := FindCollectionGaps(ctx, db, queryID)
	if err != nil {
		return fmt.Errorf("find collection gaps: %w", err)
	}
	if len(seqs) == 0 {
		fmt.Println("No gaps found")
		return nil
	}

	q, err := GetQuery(ctx, db, queryID)
	if err != nil {
		return fmt.Errorf("get query: %w", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 1, 1, 4, ' ', 0)
	fmt.Fprintln(w, "Time\t| Seq")
	for _, seq := range seqs {
		fmt.Fprintf(w, "%s\t| %d\n", q.SeqTime(seq).Format("2006-01-02T15:04:05Z"), seq)
	}
	return w.Flush()
}

func CollectionFill(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging()

	queryID := cc.Int("id")

	if queryID < 0 {
		return fmt.Errorf("ID must be a positive integer")
	}

	db := NewDB(dbConnStr())

	seqs, err := FindCollectionGaps(ctx, db, queryID)
	if err != nil {
		return fmt.Errorf("find collection gaps: %w", err)
	}

	if len(seqs) == 0 {
		fmt.Println("No gaps found")
		return nil
	}

	conn, err := db.NewConn(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	qry, err := GetQuery(ctx, db, queryID)
	if err != nil {
		return fmt.Errorf("get query: %w", err)
	}

	ss := new(SecretStore)
	secrets, err := ss.Secrets(qry.ProviderID, qry.AuthType)
	if err != nil {
		return fmt.Errorf("failed to get secrets for provider: %w", err)
	}

	for _, seq := range seqs {
		slog.Info("filling gap", "query_id", queryID, "seq", seq)

		points, err := DispatchQuery(ctx, qry, seq, secrets)
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}

		if len(points) == 0 {
			return fmt.Errorf("no points found")
		}

		if len(points) > 1 {
			return fmt.Errorf("too many points found: %d", len(points))
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin transaction: %w", err)
		}
		defer tx.Rollback(ctx)

		slog.Info("inserting collected value", "query_id", queryID, "seq", points[0].Seq, "value", points[0].Value)
		_, err = tx.Exec(ctx, "insert into collections(query_id,seq,value) values ($1,$2,$3)", queryID, points[0].Seq, points[0].Value)
		if err != nil {
			return fmt.Errorf("exec (%T): %w", err, err)
		}

		err = tx.Commit(ctx)
		if err != nil {
			return fmt.Errorf("commit: %w", err)
		}

		time.Sleep(time.Second)
	}

	return nil
}

func CollectionCollect(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging()

	queryID := cc.Int("id")
	seq := cc.Int("seq")
	force := cc.Bool("force")

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

	if len(points) > 1 {
		return fmt.Errorf("too many points found: %d", len(points))
	}

	slog.Info("inserting collected value", "query_id", queryID, "seq", points[0].Seq, "value", points[0].Value)
	if err := WriteCollectionSeq(ctx, db, queryID, points[0].Seq, points[0].Value, force); err != nil {
		return fmt.Errorf("write collection sequence: %w", err)
	}

	return nil
}

func CollectionGet(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging()

	queryID := cc.Int("id")
	if queryID < 0 {
		return fmt.Errorf("ID must be a positive integer")
	}

	var fromSeq *int
	var toSeq *int

	if cc.IsSet("from") {
		from := cc.Int("from")
		fromSeq = &from
		if *fromSeq <= 0 {
			return fmt.Errorf("from must be greater than zero")
		}
	}
	if cc.IsSet("to") {
		to := cc.Int("to")
		toSeq = &to

		if *toSeq <= 0 {
			return fmt.Errorf("to must be greater than zero")
		}

		if fromSeq != nil && *fromSeq > *toSeq {
			return fmt.Errorf("from must not be greater than to")
		}

	}

	db := NewDB(dbConnStr())

	points, err := GetCollectionValues(ctx, db, queryID, fromSeq, toSeq)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	if len(points) == 0 {
		return fmt.Errorf("no points found")
	}

	w := tabwriter.NewWriter(os.Stdout, 1, 1, 4, ' ', 0)
	fmt.Fprintln(w, "Seq\t| Time\t| Value")
	for _, pt := range points {
		v := "(missing)"
		if pt.Value != nil {
			v = formatFloat64(*pt.Value)
		}
		fmt.Fprintf(w, "%d\t| %s\t| %v\t\n", pt.Seq, pt.Time.Format("2006-01-02T15:04:05Z"), v)
	}
	return w.Flush()
}
