package btmigrate_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"
	. "github.com/JamesOwenHall/btmigrate/pkg"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

func TestMigratorCreateNewTables(t *testing.T) {
	withBigtable(t, func(admin *bigtable.AdminClient) {
		migrator := &Migrator{AdminClient: admin}
		def := MigrationDefinition{
			Create: CreateTablesDefinition{
				"table-1": CreateFamiliesDefinition{
					"fam-1": GCDefinition{},
					"fam-2": GCDefinition{MaxVersions: 1},
					"fam-3": GCDefinition{MaxAge: time.Hour},
					"fam-4": GCDefinition{MaxVersions: 1, MaxAge: time.Hour},
				},
			},
		}

		err := migrator.Apply(def)
		require.NoError(t, err)

		actual, err := migrator.Tables()
		require.NoError(t, err)

		expected := map[string]map[string]string{
			"table-1": map[string]string{
				"fam-1": "",
				"fam-2": "versions() > 1",
				"fam-3": "age() > 1h",
				"fam-4": "(versions() > 1 || age() > 1h)",
			},
		}
		require.Equal(t, expected, actual)
	})
}

func TestMigratorCreateExistingTable(t *testing.T) {
	withBigtable(t, func(admin *bigtable.AdminClient) {
		migrator := &Migrator{AdminClient: admin}
		def := MigrationDefinition{
			Create: CreateTablesDefinition{
				"table-1": CreateFamiliesDefinition{
					"fam-1": GCDefinition{MaxVersions: 1, MaxAge: time.Hour},
				},
			},
		}

		err := migrator.Apply(def)
		require.NoError(t, err)

		// Apply the same migration twice.
		err = migrator.Apply(def)
		require.NoError(t, err)

		actual, err := migrator.Tables()
		require.NoError(t, err)

		expected := map[string]map[string]string{
			"table-1": map[string]string{
				"fam-1": "(versions() > 1 || age() > 1h)",
			},
		}
		require.Equal(t, expected, actual)
	})
}

func TestMigratorAddColumnFamily(t *testing.T) {
	withBigtable(t, func(admin *bigtable.AdminClient) {
		migrator := &Migrator{AdminClient: admin}
		def := MigrationDefinition{
			Create: CreateTablesDefinition{
				"table-1": CreateFamiliesDefinition{
					"fam-1": GCDefinition{MaxVersions: 1, MaxAge: time.Hour},
				},
			},
		}

		err := migrator.Apply(def)
		require.NoError(t, err)

		def = MigrationDefinition{
			Create: CreateTablesDefinition{
				"table-1": CreateFamiliesDefinition{
					"fam-1": GCDefinition{MaxVersions: 1, MaxAge: time.Hour},
					"fam-2": GCDefinition{MaxVersions: 2, MaxAge: 2 * time.Hour},
				},
			},
		}

		err = migrator.Apply(def)
		require.NoError(t, err)

		actual, err := migrator.Tables()
		require.NoError(t, err)

		expected := map[string]map[string]string{
			"table-1": map[string]string{
				"fam-1": "(versions() > 1 || age() > 1h)",
				"fam-2": "(versions() > 2 || age() > 2h)",
			},
		}
		require.Equal(t, expected, actual)
	})
}

func TestMigratorAlterColumnFamily(t *testing.T) {
	withBigtable(t, func(admin *bigtable.AdminClient) {
		migrator := &Migrator{AdminClient: admin}
		def := MigrationDefinition{
			Create: CreateTablesDefinition{
				"table-1": CreateFamiliesDefinition{
					"fam-1": GCDefinition{MaxVersions: 1, MaxAge: time.Hour},
				},
			},
		}

		err := migrator.Apply(def)
		require.NoError(t, err)

		def = MigrationDefinition{
			Create: CreateTablesDefinition{
				"table-1": CreateFamiliesDefinition{
					"fam-1": GCDefinition{},
				},
			},
		}

		err = migrator.Apply(def)
		require.NoError(t, err)

		actual, err := migrator.Tables()
		require.NoError(t, err)

		expected := map[string]map[string]string{
			"table-1": map[string]string{
				"fam-1": "",
			},
		}
		require.Equal(t, expected, actual)
	})
}

func TestMigratorDeleteColumnFamily(t *testing.T) {
	withBigtable(t, func(admin *bigtable.AdminClient) {
		migrator := &Migrator{AdminClient: admin}
		def := MigrationDefinition{
			Create: CreateTablesDefinition{
				"table-1": CreateFamiliesDefinition{
					"fam-1": GCDefinition{MaxVersions: 1, MaxAge: time.Hour},
				},
			},
		}

		err := migrator.Apply(def)
		require.NoError(t, err)

		def = MigrationDefinition{
			Create: CreateTablesDefinition{
				"table-1": CreateFamiliesDefinition{},
			},
		}

		err = migrator.Apply(def)
		require.NoError(t, err)

		actual, err := migrator.Tables()
		require.NoError(t, err)

		expected := map[string]map[string]string{
			"table-1": map[string]string{},
		}
		require.Equal(t, expected, actual)
	})
}

func TestMigratorDrop(t *testing.T) {
	withBigtable(t, func(admin *bigtable.AdminClient) {
		migrator := &Migrator{AdminClient: admin}
		def := MigrationDefinition{
			Create: CreateTablesDefinition{
				"table-1": CreateFamiliesDefinition{
					"fam-1": GCDefinition{},
				},
			},
		}

		err := migrator.Apply(def)
		require.NoError(t, err)

		def = MigrationDefinition{
			Drop: []string{"table-1"},
		}

		err = migrator.Apply(def)
		require.NoError(t, err)

		actual, err := migrator.Tables()
		require.NoError(t, err)
		require.Empty(t, actual)
	})
}

func withBigtable(t *testing.T, fn func(*bigtable.AdminClient)) {
	server, err := bttest.NewServer("localhost:0")
	require.NoError(t, err)
	defer server.Close()

	conn, err := grpc.Dial(server.Addr, grpc.WithInsecure())
	require.NoError(t, err)

	adminClient, err := bigtable.NewAdminClient(
		context.Background(), "", "", option.WithGRPCConn(conn),
	)
	require.NoError(t, err)
	defer adminClient.Close()

	fn(adminClient)
}
