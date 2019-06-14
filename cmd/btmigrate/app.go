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

func NewCLI(out io.Writer) *cli.App {
	params := AppParams{Out: out}

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
				app, err := NewApp(params)
				if err != nil {
					errExit(err)
				}

				actions, err := app.Migrator.Plan(app.Definition)
				if err != nil {
					errExit(err)
				}

				app.OutputPlan(actions)
			},
		},
	}

	return app
}

type AppParams struct {
	Out        io.Writer
	Project    string
	Instance   string
	Definition string
}

type App struct {
	Out        io.Writer
	Migrator   *btmigrate.Migrator
	Definition btmigrate.MigrationDefinition
}

func NewApp(params AppParams) (*App, error) {
	admin, err := bigtable.NewAdminClient(
		context.Background(),
		params.Project,
		params.Instance,
	)
	if err != nil {
		return nil, err
	}

	def, err := btmigrate.LoadDefinitionFile(params.Definition)
	if err != nil {
		return nil, err
	}

	return &App{
		Out:        params.Out,
		Migrator:   &btmigrate.Migrator{AdminClient: admin},
		Definition: def,
	}, nil
}

func (a *App) OutputPlan(actions []btmigrate.Action) {
	fmt.Fprintln(a.Out, "BTMIGRATE: PLAN")
	fmt.Fprintln(a.Out, "===============")
	if len(actions) == 0 {
		fmt.Fprintln(a.Out, "No actions to take.")
		return
	}

	for i, action := range actions {
		fmt.Fprintf(a.Out, "%d. %s\n", i+1, action.HumanOutput())
	}
}

func errExit(err error) {
	fmt.Fprintln(os.Stderr, err.Error())
	os.Exit(1)
}
