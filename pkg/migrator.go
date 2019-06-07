package btmigrate

import (
	"context"

	"cloud.google.com/go/bigtable"
)

type Migrator struct {
	AdminClient *bigtable.AdminClient
}

func (m *Migrator) Plan(def MigrationDefinition) ([]Action, error) {
	currentState, err := m.Tables()
	if err != nil {
		return nil, err
	}

	var actions []Action

	for table, families := range def.Create {
		policies := families.toPolicyMap()

		// Find tables that need to be created.
		currentTable, exists := currentState[table]
		if !exists {
			actions = append(actions, CreateTable{
				table:    table,
				families: policies,
			})
			continue
		}

		// Find families that need to be created or altered.
		for desiredFamily, desiredPolicy := range policies {
			currentPolicy, exists := currentTable[desiredFamily]
			if !exists {
				actions = append(actions, CreateFamily{table: table, family: desiredFamily})
			}

			if currentPolicy != desiredPolicy.String() {
				actions = append(actions, SetGCPolicy{table: table, family: desiredFamily, policy: desiredPolicy})
			}
		}

		// Find families that need to be deleted.
		for currentFamily := range currentTable {
			if _, exists := policies[currentFamily]; !exists {
				actions = append(actions, DeleteFamily{table: table, family: currentFamily})
			}
		}
	}

	// Find tables to drop.
	for table := range def.Drop {
		if _, exists := currentState[table]; !exists {
			continue
		}

		actions = append(actions, DropTable{table: table})
	}

	return actions, nil
}

func (m *Migrator) Apply(def MigrationDefinition) error {
	actions, err := m.Plan(def)
	if err != nil {
		return err
	}

	for _, action := range actions {
		if err := action.Perform(m.AdminClient); err != nil {
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
