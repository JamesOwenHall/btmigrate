package btmigrate

import (
	"context"

	"cloud.google.com/go/bigtable"
)

type Action interface {
	Perform(*bigtable.AdminClient) error
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

type CreateFamily struct {
	table  string
	family string
}

func (c CreateFamily) Perform(admin *bigtable.AdminClient) error {
	return admin.CreateColumnFamily(context.TODO(), c.table, c.family)
}

type SetGCPolicy struct {
	table  string
	family string
	policy bigtable.GCPolicy
}

func (s SetGCPolicy) Perform(admin *bigtable.AdminClient) error {
	return admin.SetGCPolicy(context.TODO(), s.table, s.family, s.policy)
}

type DeleteFamily struct {
	table  string
	family string
}

func (d DeleteFamily) Perform(admin *bigtable.AdminClient) error {
	return admin.DeleteColumnFamily(context.TODO(), d.table, d.family)
}

type DropTable struct {
	table string
}

func (d DropTable) Perform(admin *bigtable.AdminClient) error {
	return admin.DeleteTable(context.TODO(), d.table)
}
