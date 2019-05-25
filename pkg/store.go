package btmigrate

import (
	"context"

	"cloud.google.com/go/bigtable"
)

type Store struct {
	AdminClient     *bigtable.AdminClient
	Client          *bigtable.Client
	MigrationsTable string
}

func (s *Store) Apply(def MigrationDefinition) error {
	if err := s.createTables(def.Create); err != nil {
		return err
	}
	return s.dropTables(def.Drop)
}

func (s *Store) createTables(tables map[string]map[string]GCDefinition) error {
	for name, families := range tables {
		tableConf := bigtable.TableConf{
			TableID:  name,
			Families: make(map[string]bigtable.GCPolicy),
		}

		for fam, gc := range families {
			tableConf.Families[fam] = gc.toGCPolicy()
		}

		if err := s.AdminClient.CreateTableFromConf(context.Background(), &tableConf); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) dropTables(tables []string) error {
	for _, name := range tables {
		if err := s.AdminClient.DeleteTable(context.Background(), name); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) CreateMigrationsTable() error {
	tableConf := bigtable.TableConf{
		TableID: s.MigrationsTable,
		Families: map[string]bigtable.GCPolicy{
			"meta": bigtable.NoGcPolicy(),
		},
	}
	return s.AdminClient.CreateTableFromConf(context.Background(), &tableConf)
}
