package btmigrate_test

import (
	"testing"
	"time"

	. "github.com/JamesOwenHall/btmigrate/pkg"
	"github.com/stretchr/testify/require"
)

func TestLoadDefinition(t *testing.T) {
	input := `
		[create.table-1]
		fam-1 = {}
		fam-2 = {max-versions = 2, max-age = "1h"}

		[drop.table-2]
	`

	def, err := LoadDefinition(input)
	require.NoError(t, err)

	expected := MigrationDefinition{
		Create: CreateTablesDefinition{
			"table-1": CreateFamiliesDefinition{
				"fam-1": GCDefinition{},
				"fam-2": GCDefinition{MaxVersions: 2, MaxAge: TomlDuration(time.Hour)},
			},
		},
		Drop: map[string]struct{}{
			"table-2": struct{}{},
		},
	}

	require.Equal(t, expected, def)
}

func TestLoadDefinitionFile(t *testing.T) {
	def, err := LoadDefinitionFile("../fixtures/TestLoadDefinitionFile.toml")
	require.NoError(t, err)

	expected := MigrationDefinition{
		Create: CreateTablesDefinition{
			"table-1": CreateFamiliesDefinition{
				"fam-1": GCDefinition{},
				"fam-2": GCDefinition{MaxVersions: 2, MaxAge: TomlDuration(time.Hour)},
			},
		},
		Drop: map[string]struct{}{
			"table-2": struct{}{},
		},
	}

	require.Equal(t, expected, def)
}
