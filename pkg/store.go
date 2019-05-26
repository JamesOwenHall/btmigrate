package btmigrate

import (
	"context"
	"sort"

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

func (s *Store) Tables() (map[string][]bigtable.FamilyInfo, error) {
	tableNames, err := s.AdminClient.Tables(context.Background())
	if err != nil {
		return nil, err
	}

	infos := map[string][]bigtable.FamilyInfo{}
	for _, table := range tableNames {
		info, err := s.AdminClient.TableInfo(context.Background(), table)
		if err != nil {
			return nil, err
		}

		sort.Slice(info.FamilyInfos, func(i, j int) bool {
			return info.FamilyInfos[i].Name < info.FamilyInfos[j].Name
		})

		infos[table] = info.FamilyInfos
	}

	return infos, nil
}
