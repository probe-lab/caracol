package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/urfave/cli/v2"
)

var ErrNotFound = errors.New("not found")

var dbFlags = []cli.Flag{
	&cli.StringFlag{
		Name:        "dburl",
		Usage:       "URL of the postgres database. URLs take the form 'postgres://username:password@hostname:5432/database_name'",
		Destination: &dbOpts.dbURL,
		EnvVars:     []string{envPrefix + "DBURL"},
	},

	&cli.StringFlag{
		Name:        "db-host",
		Usage:       "The hostname/address of the database server",
		EnvVars:     []string{envPrefix + "DB_HOST"},
		Destination: &dbOpts.dbHost,
	},
	&cli.IntFlag{
		Name:        "db-port",
		Usage:       "The port number of the database server",
		EnvVars:     []string{envPrefix + "DB_PORT"},
		Value:       5432,
		Destination: &dbOpts.dbPort,
	},
	&cli.StringFlag{
		Name:        "db-name",
		Usage:       "The name of the database to use",
		EnvVars:     []string{envPrefix + "DB_NAME"},
		Destination: &dbOpts.dbName,
	},
	&cli.StringFlag{
		Name:        "db-password",
		Usage:       "The password to use when connecting the the database",
		EnvVars:     []string{envPrefix + "DB_PASSWORD"},
		Destination: &dbOpts.dbPassword,
	},
	&cli.StringFlag{
		Name:        "db-user",
		Usage:       "The user to use when connecting the the database",
		EnvVars:     []string{envPrefix + "DB_USER"},
		Destination: &dbOpts.dbUser,
	},
	&cli.StringFlag{
		Name:        "db-sslmode",
		Usage:       "The sslmode to use when connecting the the database",
		EnvVars:     []string{envPrefix + "DB_SSL_MODE"},
		Value:       "prefer",
		Destination: &dbOpts.dbSSLMode,
	},
}

var dbOpts struct {
	dbURL      string
	dbHost     string
	dbPort     int
	dbName     string
	dbSSLMode  string
	dbUser     string
	dbPassword string
}

func dbConnStr() string {
	if dbOpts.dbURL != "" {
		return dbOpts.dbURL
	}
	return fmt.Sprintf("host=%s port=%d dbname=%s sslmode=%s user=%s password=%s",
		dbOpts.dbHost, dbOpts.dbPort, dbOpts.dbName, dbOpts.dbSSLMode, dbOpts.dbUser, dbOpts.dbPassword)
}

type DB struct {
	connstr  string
	poolOnce sync.Once
	err      error
	pool     *pgxpool.Pool
}

func NewDB(connstr string) *DB {
	return &DB{
		connstr: connstr,
	}
}

func (p *DB) NewConn(ctx context.Context) (*pgxpool.Conn, error) {
	p.poolOnce.Do(func() {
		conf, err := pgxpool.ParseConfig(p.connstr)
		if err != nil {
			p.err = fmt.Errorf("unable to parse connection string: %w", err)
			return
		}
		if dbLogger != nil {
			conf.ConnConfig.Tracer = &tracelog.TraceLog{
				Logger:   dbLogger,
				LogLevel: tracelog.LogLevelTrace,
			}
		}

		pool, err := pgxpool.NewWithConfig(context.Background(), conf)
		if err != nil {
			p.err = fmt.Errorf("unable to connect to database: %w", err)
			return
		}
		p.pool = pool
	})

	if p.err != nil {
		return nil, p.err
	}

	return p.pool.Acquire(ctx)
}

type Tx interface {
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}
