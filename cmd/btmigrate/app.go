package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigtable"
	root "github.com/JamesOwenHall/btmigrate"
	btmigrate "github.com/JamesOwenHall/btmigrate/pkg"
	"github.com/urfave/cli"
)

func NewApp() *cli.App {
	app := cli.NewApp()
	app.Name = "btmigrate"
	app.Usage = "declarative Bigtable migrations"
	app.Version = root.Version
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "project",
			Usage: "the Google Cloud project",
		},
		cli.StringFlag{
			Name:  "instance",
			Usage: "the Bigtable instance",
		},
	}
	app.Commands = []cli.Command{
		cli.Command{
			Name:      "plan",
			ShortName: "p",
			Action: func(c *cli.Context) {
				_, err := buildMigrator(c)
				if err != nil {
					panic(err)
				}

				fmt.Println("Planning... Done.")
			},
		},
	}

	return app
}

func buildMigrator(c *cli.Context) (*btmigrate.Migrator, error) {
	admin, err := bigtable.NewAdminClient(
		context.Background(),
		c.String("project"),
		c.String("instance"),
	)

	return &btmigrate.Migrator{AdminClient: admin}, err
}
