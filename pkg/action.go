package btmigrate

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigtable"
)

type Action interface {
	Perform(*bigtable.AdminClient) error
	HumanOutput() string
}

type CreateTable struct {
	table    string
	families map[string]bigtable.GCPolicy
}

func (c CreateTable) Perform(admin *bigtable.AdminClient) error {
	conf := bigtable.TableConf{
		TableID:  c.table,
		Families: c.families,
	}

	return admin.CreateTableFromConf(context.TODO(), &conf)
}

func (c CreateTable) HumanOutput() string {
	return fmt.Sprintf("Create table (including families) => %s", c.table)
}

type CreateFamily struct {
	table  string
	family string
}

func (c CreateFamily) Perform(admin *bigtable.AdminClient) error {
	return admin.CreateColumnFamily(context.TODO(), c.table, c.family)
}

func (c CreateFamily) HumanOutput() string {
	return fmt.Sprintf("Create column family => %s.%s", c.table, c.family)
}

type SetGCPolicy struct {
	table  string
	family string
	policy bigtable.GCPolicy
}

func (s SetGCPolicy) Perform(admin *bigtable.AdminClient) error {
	return admin.SetGCPolicy(context.TODO(), s.table, s.family, s.policy)
}

func (s SetGCPolicy) HumanOutput() string {
	return fmt.Sprintf("Update GC policy => %s.%s %s", s.table, s.family, s.policy.String())
}

type DeleteFamily struct {
	table  string
	family string
}

func (d DeleteFamily) Perform(admin *bigtable.AdminClient) error {
	return admin.DeleteColumnFamily(context.TODO(), d.table, d.family)
}

func (d DeleteFamily) HumanOutput() string {
	return fmt.Sprintf("Delete column family => %s.%s", d.table, d.family)
}

type DropTable struct {
	table string
}

func (d DropTable) Perform(admin *bigtable.AdminClient) error {
	return admin.DeleteTable(context.TODO(), d.table)
}

func (d DropTable) HumanOutput() string {
	return fmt.Sprintf("Drop table => %s", d.table)
}
