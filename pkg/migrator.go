package btmigrate

import (
	"context"

	"cloud.google.com/go/bigtable"
)

type Migrator struct {
	AdminClient *bigtable.AdminClient
}

func (m *Migrator) Apply(def MigrationDefinition) error {
	currentState, err := m.Tables()
	if err != nil {
		return err
	}

	if err := m.createTables(def.Create, currentState); err != nil {
		return err
	}
	return m.dropTables(def.Drop, currentState)
}

func (m *Migrator) createTables(create CreateTablesDefinition, currentState map[string]map[string]string) error {
	var actions []action

	for table, families := range create {
		policies := families.toPolicyMap()

		// Find tables that need to be created.
		currentTable, exists := currentState[table]
		if !exists {
			actions = append(actions, createTable{
				table:    table,
				families: policies,
			})
			continue
		}

		// Find families that need to be created or altered.
		for desiredFamily, desiredPolicy := range policies {
			currentPolicy, exists := currentTable[desiredFamily]
			if !exists {
				actions = append(actions, createFamily{table: table, family: desiredFamily})
			}

			if currentPolicy != desiredPolicy.String() {
				actions = append(actions, setGCPolicy{table: table, family: desiredFamily, policy: desiredPolicy})
			}
		}

		// Find families that need to be deleted.
		for currentFamily := range currentTable {
			if _, exists := policies[currentFamily]; !exists {
				actions = append(actions, deleteFamily{table: table, family: currentFamily})
			}
		}
	}

	for _, action := range actions {
		if err := action.perform(m.AdminClient); err != nil {
			return err
		}
	}

	return nil
}

func (m *Migrator) dropTables(tables []string, currentState map[string]map[string]string) error {
	var actions []action
	for _, table := range tables {
		if _, exists := currentState[table]; !exists {
			continue
		}

		actions = append(actions, dropTable{table: table})
	}

	for _, action := range actions {
		if err := action.perform(m.AdminClient); err != nil {
			return err
		}
	}

	return nil
}

func (m *Migrator) Tables() (map[string]map[string]string, error) {
	tableNames, err := m.AdminClient.Tables(context.Background())
	if err != nil {
		return nil, err
	}

	infos := make(map[string]map[string]string)
	for _, table := range tableNames {
		info, err := m.AdminClient.TableInfo(context.Background(), table)
		if err != nil {
			return nil, err
		}

		policies := make(map[string]string)
		for _, famInfo := range info.FamilyInfos {
			policies[famInfo.Name] = famInfo.GCPolicy
		}

		infos[table] = policies
	}

	return infos, nil
}
