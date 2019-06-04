package btmigrate

import (
	"context"
	"sort"

	"cloud.google.com/go/bigtable"
)

type Migrator struct {
	AdminClient     *bigtable.AdminClient
	Client          *bigtable.Client
	MigrationsTable string
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

func (m *Migrator) Tables() (map[string][]bigtable.FamilyInfo, error) {
	tableNames, err := m.AdminClient.Tables(context.Background())
	if err != nil {
		return nil, err
	}

	infos := map[string][]bigtable.FamilyInfo{}
	for _, table := range tableNames {
		info, err := m.AdminClient.TableInfo(context.Background(), table)
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
