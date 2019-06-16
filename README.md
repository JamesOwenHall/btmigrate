<img src="logo.png" width="300">

btmigrate is a tool for declaratively managing tables and column families in Bigtable.

## Installation

btmigrate can be installed like any other Go application.

```
go install github.com/JamesOwenHall/btmigrate/cmd/btmigrate
```

## Usage

btmigrate works by reading the state file and comparing it to the actual state of the Bigtable instance. If there are any differences between the two, btmigrate will alter the instance to match the state file.

A state file is a TOML file that declares which tables, along with their column families, must exist. Here's an example.

###### `bigtable_state.toml`

```toml
# Ensure table1 exists with no column families.
[create.table1]

# Ensure table2 exists with two families.
[create.table2]
fam-1 = {}
fam-2 = {max-versions = 1, max-age = "6h"}

# Ensure table3 does not exist.
[drop.table3]
```

With your state file defined, btmigrate can manage your Bigtable instance.

### Plan

To view which actions btmigrate will perform, use the `plan` command. This outputs the list of actions without performing them.

```sh
$ btmigrate plan
Plan
===============
1. Create table table1 (including families)
2. Create table table2 (including families)
```

### Apply

Apply creates a plan of actions to synchronize your Bigtable instance to your state file, then performs these actions.

**Caution: before using btmigrate for the first time, it is highly recommended to run `plan` to ensure no destructive actions are applied to your Bigtable instance.**

```sh
$ btmigrate apply
Plan
===============
1. Create table table1 (including families)
2. Create table table2 (including families)

Applying 1 (Create table table1 (including families)).
Applying 2 (Create table table2 (including families)).
Complete.
```
