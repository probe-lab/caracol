package main

import (
	"context"
	"os"

	"github.com/iand/pontium/hlog"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slog"
)

var loggingFlags = []cli.Flag{
	&cli.BoolFlag{
		Name:        "verbose",
		Aliases:     []string{"v"},
		EnvVars:     []string{envPrefix + "VERBOSE"},
		Usage:       "Set logging level more verbose to include info level logs",
		Value:       false,
		Destination: &loggingOpts.Verbose,
	},

	&cli.BoolFlag{
		Name:        "veryverbose",
		Aliases:     []string{"vv"},
		EnvVars:     []string{envPrefix + "VERYVERBOSE"},
		Usage:       "Set logging level more verbose to include debug level logs",
		Destination: &loggingOpts.VeryVerbose,
	},

	&cli.BoolFlag{
		Name:        "dbtrace",
		EnvVars:     []string{envPrefix + "DBTRACE"},
		Usage:       "Trace database calls and activity (requires --veryverbose too)",
		Destination: &loggingOpts.DBTrace,
	},
}

var hlogDefaultTrue = []cli.Flag{
	&cli.BoolFlag{
		Name:        "hlog",
		EnvVars:     []string{envPrefix + "HLOG"},
		Usage:       "Use human friendly log output",
		Value:       true,
		Destination: &loggingOpts.Hlog,
	},
}

var hlogDefaultFalse = []cli.Flag{
	&cli.BoolFlag{
		Name:        "hlog",
		EnvVars:     []string{envPrefix + "HLOG"},
		Usage:       "Use human friendly log output",
		Value:       false,
		Destination: &loggingOpts.Hlog,
	},
}

var loggingOpts struct {
	Verbose     bool
	VeryVerbose bool
	Hlog        bool
	DBTrace     bool
}

var dbLogger tracelog.LoggerFunc

func setupLogging() {
	logLevel := new(slog.LevelVar)
	logLevel.Set(slog.LevelWarn)
	if loggingOpts.Verbose {
		logLevel.Set(slog.LevelInfo)
	}
	if loggingOpts.VeryVerbose {
		logLevel.Set(slog.LevelDebug)
	}

	var h slog.Handler
	if loggingOpts.Hlog {
		h = new(hlog.Handler).WithLevel(logLevel.Level())
	} else {
		h = (slog.HandlerOptions{
			Level: logLevel,
		}).NewJSONHandler(os.Stdout)
	}
	slog.SetDefault(slog.New(h))

	if loggingOpts.DBTrace {
		dbLogger = tracelog.LoggerFunc(func(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]interface{}) {
			logger := slog.With("pgx_level", level)
			if data != nil {
				attrs := make([]any, 0, len(data)*2)
				for k, v := range data {
					attrs = append(attrs, k, v)
				}

				logger = logger.With(attrs...)
			}
			logger.Debug(msg)
		})
	}
}
