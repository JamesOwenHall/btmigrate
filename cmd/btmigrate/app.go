package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"cloud.google.com/go/bigtable"
	root "github.com/JamesOwenHall/btmigrate"
	btmigrate "github.com/JamesOwenHall/btmigrate/pkg"
	"github.com/urfave/cli"
)

type AppParams struct {
	Project    string
	Instance   string
	Definition string
}

func NewApp(out io.Writer) *cli.App {
	var params AppParams

	app := cli.NewApp()
	app.Name = "btmigrate"
	app.Usage = "declarative Bigtable migrations"
	app.Version = root.Version
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "project",
			Destination: &params.Project,
			Usage:       "the Google Cloud project",
		},
		cli.StringFlag{
			Name:        "instance",
			Destination: &params.Instance,
			Usage:       "the Bigtable instance",
		},
		cli.StringFlag{
			Name:        "definition",
			Destination: &params.Definition,
			Value:       "bigtable_state.yml",
			Usage:       "the path to the migration definition file",
			EnvVar:      "BT_DEFINITION",
		},
	}
	app.Commands = []cli.Command{
		cli.Command{
			Name:      "plan",
			ShortName: "p",
			Action: func(c *cli.Context) {
				migrator, err := buildMigrator(params)
				if err != nil {
					errExit(err)
				}

				def, err := btmigrate.LoadDefinitionFile(params.Definition)
				if err != nil {
					errExit(err)
				}

				actions, err := migrator.Plan(def)
				if err != nil {
					errExit(err)
				}

				fmt.Fprintln(out, "BTMIGRATE: PLAN")
				fmt.Fprintln(out, "===============")
				if len(actions) == 0 {
					fmt.Fprintln(out, "No actions to take.")
					return
				}

				for i, action := range actions {
					fmt.Fprintf(out, "%d. %s\n", i+1, action.HumanOutput())
				}
			},
		},
	}

	return app
}

func buildMigrator(params AppParams) (*btmigrate.Migrator, error) {
	admin, err := bigtable.NewAdminClient(
		context.Background(),
		params.Project,
		params.Instance,
	)

	return &btmigrate.Migrator{AdminClient: admin}, err
}

func errExit(err error) {
	fmt.Fprintln(os.Stderr, err.Error())
	os.Exit(1)
}
