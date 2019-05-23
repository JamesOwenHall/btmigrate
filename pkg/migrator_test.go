package btmigrate_test

import (
	"os"
	"testing"

	. "github.com/JamesOwenHall/btmigrate/pkg"
	"github.com/kezhuw/toml"
	"github.com/stretchr/testify/require"
)

func TestMigratorGetDefinitions(t *testing.T) {
	config := Config{MigrationDir: "../fixtures/TestMigratorGetDefinitions"}
	migrator := NewMigrator(config)

	defs, err := migrator.GetDefinitions()
	require.NoError(t, err)

	expected := []MigrationDefinition{
		{
			Create: map[string]map[string]GCDefinition{
				"table-1": map[string]GCDefinition{
					"fam-1": {},
					"fam-2": {MaxVersions: 3, MaxAge: "5d"},
				},
			},
			Drop: map[string]struct{}{
				"table-2": {},
			},
		},
	}

	require.Equal(t, expected, defs)
}

func TestMigratorGetDefinitionsUnknownDir(t *testing.T) {
	config := Config{MigrationDir: "../fixtures/DirThatDoesNotExist"}
	migrator := NewMigrator(config)

	_, err := migrator.GetDefinitions()
	require.Error(t, err)
	require.IsType(t, &os.PathError{}, err)
}

func TestMigratorGetDefinitionsInvalidToml(t *testing.T) {
	config := Config{MigrationDir: "../fixtures/TestMigratorGetDefinitionsInvalidToml"}
	migrator := NewMigrator(config)

	_, err := migrator.GetDefinitions()
	require.Error(t, err)
	require.IsType(t, &toml.ParseError{}, err)
}
