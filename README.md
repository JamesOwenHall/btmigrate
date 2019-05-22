# btmigrate

## Example

```toml
# Create a table with no families.
[create.table1]

# Create a table with two families.
[create.table2]
fam-1 = {}
fam-2 = {max-versions = 1, max-age = "6h"}

# Add a family to an existing table.
[add.table1]
fam-1 = {}

# Update an existing family's GC policy.
[update.table1]
fam-1 = {max-versions = 1}

# Delete a familiy.
[remove.table2]
families = ["fam-2"]

# Drop table.
[drop.table3]
```
