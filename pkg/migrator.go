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
				Table:    table,
				Families: policies,
			})
			continue
		}

		// Find families that need to be created or altered.
		for desiredFamily, desiredPolicy := range policies {
			currentPolicy, exists := currentTable[desiredFamily]
			if !exists {
				actions = append(actions, CreateFamily{
					Table:  table,
					Family: desiredFamily,
				})
			}

			if currentPolicy != desiredPolicy.String() {
				actions = append(actions, SetGCPolicy{
					Table:  table,
					Family: desiredFamily,
					Policy: desiredPolicy,
				})
			}
		}

		// Find families that need to be deleted.
		for currentFamily := range currentTable {
			if _, exists := policies[currentFamily]; !exists {
				actions = append(actions, DeleteFamily{
					Table:  table,
					Family: currentFamily,
				})
			}
		}
	}

	// Find tables to drop.
	for table := range def.Drop {
		if _, exists := currentState[table]; !exists {
			continue
		}

		actions = append(actions, DropTable{Table: table})
	}

	return actions, nil
}

func (m *Migrator) Apply(actions ...Action) error {
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
