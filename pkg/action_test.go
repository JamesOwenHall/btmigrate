package btmigrate_test

import (
	"testing"

	"cloud.google.com/go/bigtable"
	. "github.com/JamesOwenHall/btmigrate/pkg"
	"github.com/stretchr/testify/require"
)

func TestCreateTableHumanOutput(t *testing.T) {
	action := CreateTable{Table: "t1"}
	expected := "Create table t1 (including families)"
	require.Equal(t, expected, action.HumanOutput())
}

func TestCreateFamilyHumanOutput(t *testing.T) {
	action := CreateFamily{Table: "t1", Family: "f1"}
	expected := "Create column family t1.f1"
	require.Equal(t, expected, action.HumanOutput())
}

func TestSetGCPolicyHumanOutput(t *testing.T) {
	action := SetGCPolicy{
		Table:  "t1",
		Family: "f1",
		Policy: bigtable.MaxVersionsPolicy(2),
	}
	expected := "Set GC policy t1.f1 versions() > 2"
	require.Equal(t, expected, action.HumanOutput())
}

func TestDeleteFamilyHumanOutput(t *testing.T) {
	action := DeleteFamily{Table: "t1", Family: "f1"}
	expected := "Delete column family t1.f1"
	require.Equal(t, expected, action.HumanOutput())
}

func TestDropTableHumanOutput(t *testing.T) {
	action := DropTable{Table: "t1"}
	expected := "Drop table t1"
	require.Equal(t, expected, action.HumanOutput())
}
