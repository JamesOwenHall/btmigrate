package btmigrate_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"
	. "github.com/JamesOwenHall/btmigrate/pkg"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

func TestStoreCreate(t *testing.T) {
	withBigtable(t, func(admin *bigtable.AdminClient, client *bigtable.Client) {
		store := &Store{AdminClient: admin, Client: client}
		def := MigrationDefinition{
			Create: CreateDefinition{
				"table-1": map[string]GCDefinition{
					"fam-1": GCDefinition{},
					"fam-2": GCDefinition{MaxVersions: 1},
					"fam-3": GCDefinition{MaxAge: time.Hour},
					"fam-4": GCDefinition{MaxVersions: 1, MaxAge: time.Hour},
				},
			},
		}

		err := store.Apply(def)
		require.NoError(t, err)

		actual := getTables(t, admin)
		expected := map[string][]bigtable.FamilyInfo{
			"table-1": []bigtable.FamilyInfo{
				{Name: "fam-1"},
				{Name: "fam-2", GCPolicy: "versions() > 1"},
				{Name: "fam-3", GCPolicy: "age() > 1h"},
				{Name: "fam-4", GCPolicy: "(versions() > 1 || age() > 1h)"},
			},
		}

		require.Equal(t, expected, actual)
	})
}

func TestStoreDrop(t *testing.T) {
	withBigtable(t, func(admin *bigtable.AdminClient, client *bigtable.Client) {
		store := &Store{AdminClient: admin, Client: client}
		def := MigrationDefinition{
			Create: CreateDefinition{
				"table-1": map[string]GCDefinition{
					"fam-1": GCDefinition{},
				},
			},
		}

		err := store.Apply(def)
		require.NoError(t, err)

		def = MigrationDefinition{
			Drop: []string{"table-1"},
		}

		err = store.Apply(def)
		require.NoError(t, err)

		actual := getTables(t, admin)
		require.Empty(t, actual)
	})
}

func TestStoreCreateMigrationsTable(t *testing.T) {
	withBigtable(t, func(admin *bigtable.AdminClient, client *bigtable.Client) {
		store := &Store{
			AdminClient:     admin,
			Client:          client,
			MigrationsTable: "migrations",
		}

		err := store.CreateMigrationsTable()
		require.NoError(t, err)

		actual := getTables(t, admin)
		expected := map[string][]bigtable.FamilyInfo{
			"migrations": []bigtable.FamilyInfo{
				{Name: "meta"},
			},
		}

		require.Equal(t, expected, actual)
	})
}

func withBigtable(t *testing.T, fn func(*bigtable.AdminClient, *bigtable.Client)) {
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

	client, err := bigtable.NewClient(
		context.Background(), "", "", option.WithGRPCConn(conn),
	)
	require.NoError(t, err)
	defer client.Close()

	fn(adminClient, client)
}

func getTables(t *testing.T, admin *bigtable.AdminClient) map[string][]bigtable.FamilyInfo {
	tableNames, err := admin.Tables(context.Background())
	require.NoError(t, err)

	infos := map[string][]bigtable.FamilyInfo{}
	for _, table := range tableNames {
		info, err := admin.TableInfo(context.Background(), table)
		require.NoError(t, err)

		sort.Slice(info.FamilyInfos, func(i, j int) bool {
			return info.FamilyInfos[i].Name < info.FamilyInfos[j].Name
		})

		infos[table] = info.FamilyInfos
	}

	return infos
}
