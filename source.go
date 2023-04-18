package main

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/jackc/pgx/v5"
	"github.com/urfave/cli/v2"
)

var sourceCommand = &cli.Command{
	Name:  "source",
	Usage: "Commands for managing data sources",
	Subcommands: []*cli.Command{
		{
			Name:   "list",
			Usage:  "List known sources",
			Action: SourceList,
			Flags:  union([]cli.Flag{}, dbFlags, loggingFlags, hlogDefaultTrue),
		},
		{
			Name:   "add",
			Usage:  "Add a new source",
			Action: SourceAdd,
			Flags: union([]cli.Flag{
				&cli.StringFlag{
					Name:     "name",
					Required: true,
					Usage:    "Name of source.",
				},
				&cli.IntFlag{
					Name:     "provider-id",
					Required: true,
					Usage:    "ID of provider.",
				},
				&cli.StringFlag{
					Name:     "dataset",
					Required: false,
					Usage:    "Optional dataset within the provider for source.",
				},
			}, dbFlags, loggingFlags),
		},
	},
}

func SourceList(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging()

	db := NewDB(dbConnStr())
	conn, err := db.NewConn(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	rows, err := conn.Query(ctx, "select s.id, s.name, p.name, s.dataset from sources s join providers p on p.id=s.provider_id;")
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	type SourceInfoRow struct {
		ID           int
		Name         string
		ProviderName string
		Dataset      string
	}

	dss, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByPos[SourceInfoRow])
	if err != nil {
		return fmt.Errorf("collect: %w", err)
	}

	if len(dss) == 0 {
		fmt.Println("No sources found")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 1, 1, 4, ' ', 0)

	fmt.Fprintln(w, "ID\t| Name\t| Provider\t| Dataset")
	for _, ds := range dss {
		fmt.Fprintf(w, "%d\t| %s\t| %s\t| %s\n", ds.ID, ds.Name, ds.ProviderName, ds.Dataset)
	}
	return w.Flush()
}

func SourceAdd(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging()

	name := strings.TrimSpace(cc.String("name"))
	providerID := cc.Int("provider-id")
	dataset := strings.TrimSpace(cc.String("dataset"))

	if name == "" {
		return fmt.Errorf("name must be supplied")
	}

	if providerID < 0 {
		return fmt.Errorf("provider ID must be a positive integer")
	}

	db := NewDB(dbConnStr())
	conn, err := db.NewConn(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "insert into sources(name,provider_id,dataset) values ($1,$2,$3)", name, providerID, dataset)
	if err != nil {
		return fmt.Errorf("exec (%T): %w", err, err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}
