package btmigrate

import (
	"context"

	"cloud.google.com/go/bigtable"
)

type Migrator struct {
	AdminClient *bigtable.AdminClient
	Client      *bigtable.Client
}

func (m *Migrator) Apply(def MigrationDefinition) error {
	if err := m.createTables(def.Create); err != nil {
		return err
	}
	return m.dropTables(def.Drop)
}

func (m *Migrator) createTables(tables map[string]map[string]GCDefinition) error {
	for name, families := range tables {
		tableConf := bigtable.TableConf{
			TableID:  name,
			Families: make(map[string]bigtable.GCPolicy),
		}

		for fam, gc := range families {
			tableConf.Families[fam] = gc.toGCPolicy()
		}

		if err := m.AdminClient.CreateTableFromConf(context.Background(), &tableConf); err != nil {
			return err
		}
	}

	return nil
}

func (m *Migrator) dropTables(tables []string) error {
	for _, name := range tables {
		if err := m.AdminClient.DeleteTable(context.Background(), name); err != nil {
			return err
		}
	}

	return nil
}
