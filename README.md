# btmigrate

## Example

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
