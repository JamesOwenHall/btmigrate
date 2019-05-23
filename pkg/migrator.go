package btmigrate

import (
	"io/ioutil"
	"path/filepath"

	"github.com/kezhuw/toml"
)

type Migrator struct {
	config Config
}

func NewMigrator(config Config) *Migrator {
	return &Migrator{
		config: config,
	}
}

// GetDefinitions parses the migration definitions from the migration
// directory.
// TODO: Fail if TOML contains unknown keys.
func (m *Migrator) GetDefinitions() ([]MigrationDefinition, error) {
	files, err := ioutil.ReadDir(m.config.MigrationDir)
	if err != nil {
		return nil, err
	}

	var defs []MigrationDefinition
	for _, file := range files {
		path := filepath.Join(m.config.MigrationDir, file.Name())
		payload, err := ioutil.ReadFile(path)
		if err != nil {
			return nil, err
		}

		var def MigrationDefinition
		if err := toml.Unmarshal(payload, &def); err != nil {
			return nil, err
		}

		defs = append(defs, def)
	}

	return defs, nil
}
