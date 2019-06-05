package btmigrate

import (
	"time"

	"cloud.google.com/go/bigtable"
)

type MigrationDefinition struct {
	Create CreateTablesDefinition
	Drop   []string
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
	MaxVersions int
	MaxAge      time.Duration
}

func (g GCDefinition) toGCPolicy() bigtable.GCPolicy {
	var policies []bigtable.GCPolicy

	if g.MaxVersions != 0 {
		policies = append(policies, bigtable.MaxVersionsPolicy(g.MaxVersions))
	}
	if g.MaxAge != 0 {
		policies = append(policies, bigtable.MaxAgePolicy(g.MaxAge))
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
