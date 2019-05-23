package btmigrate

type MigrationDefinition struct {
	Create map[string]map[string]GCDefinition `toml:"create"`
	Drop   map[string]struct{}                `toml:"drop"`
}

type GCDefinition struct {
	MaxVersions int    `toml:"max-versions"`
	MaxAge      string `toml:"max-age"`
}
