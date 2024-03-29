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
			Value:       "bigtable_state.toml",
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
				errExit(app.RunPlan())
			},
		},
		cli.Command{
			Name:      "apply",
			ShortName: "a",
			Action: func(c *cli.Context) {
				app, err := NewApp(params)
				if err != nil {
					errExit(err)
				}
				errExit(app.RunApply())
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
	AppParams
	Migrator *btmigrate.Migrator
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

	return &App{
		AppParams: params,
		Migrator:  &btmigrate.Migrator{AdminClient: admin},
	}, nil
}

func (a *App) RunPlan() error {
	def, err := btmigrate.LoadDefinitionFile(a.Definition)
	if err != nil {
		return err
	}

	actions, err := a.Migrator.Plan(def)
	if err != nil {
		return err
	}

	a.outputPlan(actions)
	return nil
}

func (a *App) RunApply() error {
	def, err := btmigrate.LoadDefinitionFile(a.Definition)
	if err != nil {
		return err
	}

	actions, err := a.Migrator.Plan(def)
	if err != nil {
		return err
	}

	a.outputPlan(actions)
	fmt.Fprintln(a.Out, "")
	return a.apply(actions)
}

func (a *App) outputPlan(actions []btmigrate.Action) {
	fmt.Fprintln(a.Out, "Plan")
	fmt.Fprintln(a.Out, "===============")
	if len(actions) == 0 {
		fmt.Fprintln(a.Out, "No actions to take.")
		return
	}

	for i, action := range actions {
		fmt.Fprintf(a.Out, "%d. %s\n", i+1, action.HumanOutput())
	}
}

func (a *App) apply(actions []btmigrate.Action) error {
	for i, action := range actions {
		fmt.Fprintf(a.Out, "Applying %d (%s).\n", i+1, action.HumanOutput())

		err := a.Migrator.Apply(action)
		if err != nil {
			fmt.Fprintln(a.Out, "Failed.")
			return err
		}
	}

	fmt.Fprintln(a.Out, "Complete.")
	return nil
}

func errExit(err error) {
	if err == nil {
		return
	}

	fmt.Fprintln(os.Stderr, err.Error())
	os.Exit(1)
}
