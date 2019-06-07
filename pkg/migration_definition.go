package btmigrate

import (
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/BurntSushi/toml"
)

type MigrationDefinition struct {
	Create CreateTablesDefinition `toml:"create"`
	Drop   map[string]struct{}    `toml:"drop"`
}

func LoadDefinition(input string) (MigrationDefinition, error) {
	var def MigrationDefinition
	_, err := toml.Decode(input, &def)
	return def, err
}

type CreateTablesDefinition map[string]CreateFamiliesDefinition

type CreateFamiliesDefinition map[string]GCDefinition

func (c CreateFamiliesDefinition) toPolicyMap() map[string]bigtable.GCPolicy {
	out := make(map[string]bigtable.GCPolicy, len(c))
	for family, gcDef := range c {
		out[family] = gcDef.toGCPolicy()
	}

	return out
}

type GCDefinition struct {
	MaxVersions int          `toml:"max-versions"`
	MaxAge      TomlDuration `toml:"max-age"`
}

func (g GCDefinition) toGCPolicy() bigtable.GCPolicy {
	var policies []bigtable.GCPolicy

	if g.MaxVersions != 0 {
		policies = append(policies, bigtable.MaxVersionsPolicy(g.MaxVersions))
	}
	if g.MaxAge != 0 {
		policies = append(policies, bigtable.MaxAgePolicy(time.Duration(g.MaxAge)))
	}

	switch len(policies) {
	case 0:
		return bigtable.NoGcPolicy()
	case 1:
		return policies[0]
	default:
		return bigtable.UnionPolicy(policies...)
	}
}

type TomlDuration time.Duration

func (d *TomlDuration) UnmarshalText(text []byte) error {
	parsed, err := time.ParseDuration(string(text))
	*d = TomlDuration(parsed)
	return err
}
