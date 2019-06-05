package btmigrate

import (
	"context"

	"cloud.google.com/go/bigtable"
)

type action interface {
	perform(*bigtable.AdminClient) error
}

type createTable struct {
	table    string
	families map[string]bigtable.GCPolicy
}

func (c createTable) perform(admin *bigtable.AdminClient) error {
	conf := bigtable.TableConf{
		TableID:  c.table,
		Families: c.families,
	}

	return admin.CreateTableFromConf(context.TODO(), &conf)
}

type createFamily struct {
	table  string
	family string
}

func (c createFamily) perform(admin *bigtable.AdminClient) error {
	return admin.CreateColumnFamily(context.TODO(), c.table, c.family)
}

type setGCPolicy struct {
	table  string
	family string
	policy bigtable.GCPolicy
}

func (s setGCPolicy) perform(admin *bigtable.AdminClient) error {
	return admin.SetGCPolicy(context.TODO(), s.table, s.family, s.policy)
}

type deleteFamily struct {
	table  string
	family string
}

func (d deleteFamily) perform(admin *bigtable.AdminClient) error {
	return admin.DeleteColumnFamily(context.TODO(), d.table, d.family)
}

type dropTable struct {
	table string
}

func (d dropTable) perform(admin *bigtable.AdminClient) error {
	return admin.DeleteTable(context.TODO(), d.table)
}
