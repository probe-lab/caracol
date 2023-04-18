package main

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/jackc/pgx/v5"
	"github.com/urfave/cli/v2"
)

var providerCommand = &cli.Command{
	Name:  "provider",
	Usage: "Commands for managing data providers",
	Subcommands: []*cli.Command{
		{
			Name:   "list",
			Usage:  "List known providers",
			Action: ProviderList,
			Flags:  union([]cli.Flag{}, dbFlags, loggingFlags, hlogDefaultTrue),
		},
		{
			Name:   "add",
			Usage:  "Add a provider",
			Action: ProviderAdd,
			Flags: union([]cli.Flag{
				&cli.StringFlag{
					Name:     "name",
					Required: true,
					Usage:    "Name of provider.",
				},
				&cli.StringFlag{
					Name:     "api-type",
					Required: true,
					Usage:    "Type of api supported by provider.",
				},
				&cli.StringFlag{
					Name:     "api-url",
					Required: true,
					Usage:    "URL of api supported by provider.",
				},
				&cli.StringFlag{
					Name:     "auth-type",
					Required: true,
					Usage:    "URL of api supported by provider.",
				},
			}, dbFlags, loggingFlags),
		},
		{
			Name:   "expected-env",
			Usage:  "List expected environment variables for provider secrets.",
			Action: ProviderExpectedEnv,
			Flags:  union([]cli.Flag{}, dbFlags, loggingFlags),
		},
		{
			Name:   "check-env",
			Usage:  "List expected environment variables for provider secrets.",
			Action: ProviderCheckEnv,
			Flags:  union([]cli.Flag{}, dbFlags, loggingFlags),
		},
	},
}

func ProviderList(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging()

	db := NewDB(dbConnStr())
	conn, err := db.NewConn(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	rows, err := conn.Query(ctx, "select id, name, api_type, api_url, auth_type from providers;")
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	type ProviderInfoRow struct {
		ID       int
		Name     string
		ApiType  ApiType
		ApiURL   string
		AuthType string
	}

	dps, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByPos[ProviderInfoRow])
	if err != nil {
		return fmt.Errorf("collect: %w", err)
	}

	if len(dps) == 0 {
		fmt.Println("No providers found")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 1, 1, 4, ' ', 0)
	fmt.Fprintln(w, "ID\t| Name\t| API Type\t| API URL\t| Auth Type")
	for _, dp := range dps {
		fmt.Fprintf(w, "%d\t| %s\t| %s\t| %s\t| %s\n", dp.ID, dp.Name, dp.ApiType, dp.ApiURL, dp.AuthType)
	}
	return w.Flush()
}

func ProviderAdd(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging()

	name := strings.TrimSpace(cc.String("name"))
	apiType := strings.TrimSpace(cc.String("api-type"))
	apiURL := strings.TrimSpace(cc.String("api-url"))
	authType := strings.TrimSpace(cc.String("auth-type"))

	if name == "" {
		return fmt.Errorf("name must be supplied")
	}

	if apiType == "" {
		return fmt.Errorf("api type must be supplied")
	}

	if apiURL == "" {
		return fmt.Errorf("api url must be supplied")
	}

	if authType == "" {
		return fmt.Errorf("auth type must be supplied")
	}

	db := NewDB(dbConnStr())
	if err := ValidateEnumValue(ctx, db, "api_type", apiType); err != nil {
		return fmt.Errorf("unsupported api type: %w", err)
	}
	if err := ValidateEnumValue(ctx, db, "auth_type", authType); err != nil {
		return fmt.Errorf("unsupported auth type: %w", err)
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

	_, err = tx.Exec(ctx, "insert into providers(name,api_type,api_url,auth_type) values ($1,$2,$3,$4)", name, apiType, apiURL, authType)
	if err != nil {
		return fmt.Errorf("exec (%T): %w", err, err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

func ProviderExpectedEnv(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging()

	db := NewDB(dbConnStr())
	conn, err := db.NewConn(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	rows, err := conn.Query(ctx, "select id, name, api_type, api_url, auth_type from providers;")
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	type ProviderInfoRow struct {
		ID       int
		Name     string
		ApiType  ApiType
		ApiURL   string
		AuthType AuthType
	}

	dps, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByPos[ProviderInfoRow])
	if err != nil {
		return fmt.Errorf("collect: %w", err)
	}

	if len(dps) == 0 {
		fmt.Println("No providers found")
		return nil
	}

	providerVars := make(map[int][]string)
	for _, dp := range dps {
		vars, err := SecretEnvVarNames(dp.ID, dp.AuthType)
		if err != nil {
			return fmt.Errorf("secret env var names: %w", err)
		}
		for _, name := range vars {
			providerVars[dp.ID] = append(providerVars[dp.ID], name)
		}
	}

	w := tabwriter.NewWriter(os.Stdout, 1, 1, 4, ' ', 0)
	fmt.Fprintln(w, "ID\t| Name\t| API URL\t| Auth Type\t| Expected Env Variables")
	for _, dp := range dps {
		fmt.Fprintf(w, "%d\t| %s\t| %s\t| %s\t| %s\n", dp.ID, dp.Name, dp.ApiURL, dp.AuthType, strings.Join(providerVars[dp.ID], ", "))
	}
	return w.Flush()
}

func ProviderCheckEnv(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging()

	db := NewDB(dbConnStr())
	conn, err := db.NewConn(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	rows, err := conn.Query(ctx, "select id, name, auth_type from providers;")
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	type ProviderInfoRow struct {
		ID       int
		Name     string
		AuthType AuthType
	}

	dps, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByPos[ProviderInfoRow])
	if err != nil {
		return fmt.Errorf("collect: %w", err)
	}

	if len(dps) == 0 {
		fmt.Println("No providers found")
		return nil
	}

	type ProviderEnvStatus struct {
		ID    int
		Name  string
		Var   string
		Found bool
	}

	var anyMissing bool
	statuses := make([]ProviderEnvStatus, 0)
	for _, dp := range dps {
		vars, err := SecretEnvVarNames(dp.ID, dp.AuthType)
		if err != nil {
			continue
		}

		for _, name := range vars {
			_, ok := os.LookupEnv(name)
			if !ok {
				anyMissing = true
			}
			statuses = append(statuses, ProviderEnvStatus{
				ID:    dp.ID,
				Name:  dp.Name,
				Var:   name,
				Found: ok,
			})
		}

	}

	w := tabwriter.NewWriter(os.Stdout, 1, 1, 4, ' ', 0)
	fmt.Fprintln(w, "ID\t| Name\t| Variable\t| Exists")
	for _, st := range statuses {
		fmt.Fprintf(w, "%d\t| %s\t| %s\t| %v\n", st.ID, st.Name, st.Var, st.Found)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if anyMissing {
		return fmt.Errorf("some expected environment variables were missing")
	}
	return nil
}
