# BigQuery Table Management Schemas

This directory contains versioned schema change scripts for BigQuery tables managed by the BigQuery Table Management Framework.

## Directory Structure

**Structure:** `schemas/{dataset}/{table}/`

- `{dataset}` — BigQuery dataset ID (e.g., `domain_account_management`, `domain_technical`)
- `{table}` — Table name (e.g., `account`, `test_table_management`)

## File Naming Convention

All schema files must follow strict naming conventions:

### Format
```
V<version>__<OPERATION>.<extension>
```

### Rules
- **Version**: Must start with uppercase `V` followed by a positive integer (e.g., `V1`, `V2`, `V12`)
- **Separator**: Double underscore `__` (required)
- **Operation**: Uppercase operation name (for SQL files only)
- **Extension**: `.sql` or `.yaml` (case-insensitive)

### Supported File Types

#### SQL Scripts (`.sql`)
- **Allowed Operations**: `CREATE`, `ALTER`, `DROP`
- **Examples**:
  - `V1__CREATE_TABLE_ACCOUNT.sql`
  - `V2__ALTER_ADD_COLUMN.sql`
  - `V3__DROP_TABLE.sql`

#### YAML Configs (`.yaml`)
- **Content-based routing**: Operation type detected from YAML content
- **Supported Operations**:
  - `clustering_fields` → ClusteringProcessor (updates table clustering)
  - `partition_field` → PartitioningHandler (future)
  - `transformations` → SchemaEvolutionHandler (future)
- **Examples**:
  - `V3__CLUSTER_BY_ID.yaml` (contains `clustering_fields`)

## Version Sequence Requirements

1. **Start at V1**: First script must be `V1__...`
2. **Sequential**: Versions must be consecutive (V1, V2, V3, ...)
3. **No gaps**: Cannot skip versions (V1, V3 is invalid)
4. **No duplicates**: Each version number can only appear once per table

## Example Structure

```
schemas/
├── domain_account_management/
│   └── account/
│       ├── V1__CREATE_TABLE_ACCOUNT.sql
│       ├── V2__ALTER_ADD_COLUMN_EMAIL.sql
│       └── V3__CLUSTER_BY_ID.yaml
└── domain_technical/
    └── test_table_management/
        ├── V1__CREATE_TABLE_TEST_TABLE_MANAGEMENT.sql
        ├── V2__ALTER_ADD_COLUMN_EMAIL.sql
        ├── V3__CLUSTER_BY_ID.yaml
        └── V4__ALTER_ADD_COLUMN_UPDATED_AT.sql
```

## SQL Script Requirements

- **Fully qualified table references**: Must use `project.dataset.table` format
- **Environment placeholders**: Use `{env}` for environment-specific values (replaced at runtime)
- **Safe DDL only**: Only CREATE/ALTER/DROP TABLE operations allowed
- **Single statement**: One SQL statement per file
- **No DML**: INSERT, UPDATE, DELETE, MERGE, SELECT are not allowed

## YAML Config Format

### Clustering Example (`V3__CLUSTER_BY_ID.yaml`)
```yaml
clustering_fields:
  - id
  - created_at
```

## Configuration

Add your schema scripts to `bigquery_table_management_config.yaml`:

```yaml
{dataset}_{table_name}_table_management:
  table_ref: pcb-{env}-landing.<dataset>.<TABLE_NAME>
  scripts:
    - bigquery_table_management/schemas/<dataset>/<table>/V1__CREATE_TABLE.sql
    - bigquery_table_management/schemas/<dataset>/<table>/V2__ALTER_ADD_COLUMN.sql
    - bigquery_table_management/schemas/<dataset>/<table>/V3__CLUSTER_BY_ID.yaml
```

See the config template in `dags/config/bigquery_table_management_config.yaml` for complete examples.
