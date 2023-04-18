package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

const (
	appName   = "caracol"
	envPrefix = "CARACOL_"
)

func main() {
	app := &cli.App{
		Name:     appName,
		HelpName: appName,
		Commands: []*cli.Command{
			daemonCommand,
			providerCommand,
			sourceCommand,
			queryCommand,
			collectionCommand,
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}

func union(sets ...[]cli.Flag) []cli.Flag {
	var flags []cli.Flag
	for _, s := range sets {
		flags = append(flags, s...)
	}
	return flags
}
