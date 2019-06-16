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
	Table    string
	Families map[string]bigtable.GCPolicy
}

func (c CreateTable) Perform(admin *bigtable.AdminClient) error {
	conf := bigtable.TableConf{
		TableID:  c.Table,
		Families: c.Families,
	}

	return admin.CreateTableFromConf(context.TODO(), &conf)
}

func (c CreateTable) HumanOutput() string {
	return fmt.Sprintf("Create table %s (including families)", c.Table)
}

type CreateFamily struct {
	Table  string
	Family string
}

func (c CreateFamily) Perform(admin *bigtable.AdminClient) error {
	return admin.CreateColumnFamily(context.TODO(), c.Table, c.Family)
}

func (c CreateFamily) HumanOutput() string {
	return fmt.Sprintf("Create column family %s.%s", c.Table, c.Family)
}

type SetGCPolicy struct {
	Table  string
	Family string
	Policy bigtable.GCPolicy
}

func (s SetGCPolicy) Perform(admin *bigtable.AdminClient) error {
	return admin.SetGCPolicy(context.TODO(), s.Table, s.Family, s.Policy)
}

func (s SetGCPolicy) HumanOutput() string {
	return fmt.Sprintf("Set GC policy %s.%s %s", s.Table, s.Family, s.Policy.String())
}

type DeleteFamily struct {
	Table  string
	Family string
}

func (d DeleteFamily) Perform(admin *bigtable.AdminClient) error {
	return admin.DeleteColumnFamily(context.TODO(), d.Table, d.Family)
}

func (d DeleteFamily) HumanOutput() string {
	return fmt.Sprintf("Delete column family %s.%s", d.Table, d.Family)
}

type DropTable struct {
	Table string
}

func (d DropTable) Perform(admin *bigtable.AdminClient) error {
	return admin.DeleteTable(context.TODO(), d.Table)
}

func (d DropTable) HumanOutput() string {
	return fmt.Sprintf("Drop table %s", d.Table)
}
