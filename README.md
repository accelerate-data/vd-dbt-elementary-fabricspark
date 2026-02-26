# dbt-elementary-fabricspark

Elementary data observability support for Microsoft Fabric Lakehouse (fabricspark adapter).

## Why This Package Exists

**Problem:** dbt Elementary is not officially supported for Microsoft Fabric Lakehouse. When trying to configure and run Elementary with the `fabricspark` adapter, it fails with SQL syntax errors:

```
Runtime Error
  Database Error
    [PARSE_SYNTAX_ERROR] Syntax error at or near ':': extra input ':'.(line 6, pos 21)

    == SQL ==
    insert into ai_fork_lake.elementary.dbt_run_results
             (...)
             values
        (...,
        current_timestamp::timestamp  -- ❌ PostgreSQL syntax, not supported in Spark
```

**Root Cause:** Elementary has adapter-specific implementations for:
- ✅ snowflake, bigquery, redshift, postgres, databricks, spark, athena, trino, clickhouse, dremio
- ❌ **fabricspark** - Not supported

From Elementary documentation:
- CLI support: Fabric Lakehouse not listed ([Elementary CLI docs](https://docs.elementary-data.com/oss/quickstart/quickstart-cli#install-elementary-cli))
- Paid integrations: Microsoft Fabric mentioned but unclear ([Elementary integrations](https://www.elementary-data.com/integrations))

**Solution:** This package provides `fabricspark__` macro implementations that make Elementary work with Microsoft Fabric Lakehouse by delegating to Elementary's Spark implementations.

## Installation

### 1. Add to your `packages.yml`:

```yaml
packages:
  - package: elementary-data/elementary
    version: 0.22.1
  - git: https://github.com/acceleratedata/dbt-elementary-fabricspark.git
    revision: main
```

### 2. Configure dispatch in `dbt_project.yml`:

```yaml
dispatch:
  - macro_namespace: elementary
    search_order:
      - elementary_fabricspark  # Check our package first
      - elementary              # Then Elementary's own macros
```

### 3. Configure Elementary models:

```yaml
models:
  elementary:
    +schema: elementary
    +tags: ["elementary"]
```

### 4. Install dependencies:

```bash
dbt deps
```

## What This Package Does

Provides 16 `fabricspark__` macro implementations for Elementary:

### Data Type Macros
- `fabricspark__edr_type_string()` - Returns "string" type
- `fabricspark__data_type_list(data_type)` - Returns type lists for normalization
- `fabricspark__get_normalized_data_type(exact_data_type)` - Type synonym handling

### Timestamp & Date Functions
- `fabricspark__edr_current_timestamp()` - Current timestamp casting
- `fabricspark__edr_current_timestamp_in_utc()` - UTC timestamp
- `fabricspark__edr_datediff(first_date, second_date, datepart)` - Date difference calculations
- `fabricspark__edr_to_char(column, format)` - Date formatting

### Casting & Type Conversion
- `fabricspark__edr_safe_cast(field, type)` - Safe casting with try_cast

### Table Operations
- `fabricspark__has_temp_table_support()` - Returns false (Spark limitation)
- `fabricspark__edr_make_temp_relation(base_relation, suffix)` - Temp table naming
- `fabricspark__create_or_replace(temporary, relation, sql_query)` - Table creation
- `fabricspark__replace_table_data(relation, rows)` - Truncate and insert
- `fabricspark__get_delete_and_insert_queries(...)` - Delta merge operations
- `fabricspark__get_relation_max_name_length()` - Returns 127 chars

### Metadata & Configuration
- `fabricspark__target_database()` - Returns target.catalog
- `fabricspark__get_columns_from_information_schema(...)` - Column metadata (returns empty)
- `fabricspark__generate_elementary_profile_args(...)` - Elementary profile config

### Advanced Operations
- `fabricspark__complete_buckets_cte(...)` - Time bucket generation for anomaly detection
- `fabricspark__get_clean_elementary_test_tables_queries(...)` - Test cleanup

## Implementation Approach

All macros are **direct copies** of Elementary's `spark__` implementations with find & replace:
- `spark__` → `fabricspark__`

This ensures:
- ✅ Correct parameter signatures
- ✅ Proper SQL syntax for Spark/Fabric
- ✅ Compatibility with Elementary's internal calls
- ✅ Easy updates when Elementary releases new versions

## Verified Working

Tested with:
- ✅ Microsoft Fabric Lakehouse
- ✅ dbt-fabricspark adapter v1.9.2
- ✅ Elementary v0.22.1
- ✅ dbt-core v1.11.6

Confirmed functionality:
- ✅ All Elementary runtime tables populated (dbt_models, dbt_tests, dbt_sources, etc.)
- ✅ Anomaly detection tests execute successfully
- ✅ Metadata collection on every dbt run
- ✅ Error handling and logging

## Elementary Tables Created

When configured, Elementary creates these tables in your `elementary` schema:

**Core Runtime Tables:**
- `dbt_models` - Model metadata and configuration
- `dbt_tests` - Test definitions and configurations
- `dbt_sources` - Source metadata
- `dbt_snapshots` - Snapshot configurations
- `dbt_invocations` - dbt run metadata

**Execution Tables:**
- `model_executions` - Model run results
- `test_executions` - Test run results
- `elementary_test_results` - Anomaly detection results

## Example Usage

```yaml
# models/schema.yml
models:
  - name: my_model
    columns:
      - name: id
        tests:
          - unique
          - not_null
          - elementary.column_anomalies:
              column_anomalies:
                - null_count
                - missing_count
```

Run your models:
```bash
dbt run
# Elementary on-run-end hook automatically populates runtime tables
```

Query runtime data:
```sql
SELECT
  name,
  materialization,
  database_name,
  schema_name
FROM elementary.dbt_models
WHERE name = 'my_model';
```

## Limitations

- ❌ **Elementary CLI**: Not supported for Fabric Lakehouse (use dbt-native Elementary features only)
- ⚠️ **Information Schema**: `get_columns_from_information_schema` returns empty (Spark limitation)
- ⚠️ **Temp Tables**: Fabric Spark doesn't support true temp tables (uses regular tables with temp naming)

## Alternatives Considered

1. **Use databricks flavor** - Databricks adapter is different from fabricspark
2. **Fork Elementary** - Hard to maintain, doesn't scale
3. **Use dbt_artifacts only** - Missing anomaly detection features
4. ✅ **This package** - Lightweight, maintainable, full Elementary support

## Maintenance

When Elementary releases a new version:
1. Check if new `spark__` macros were added
2. Copy new macros to `fabricspark_overrides.sql`
3. Find & replace: `spark__` → `fabricspark__`
4. Test with your project
5. Update version in README

## Contributing

This package is maintained by the AccelerateData team for internal use but is open source.

Issues and PRs welcome at: https://github.com/acceleratedata/dbt-elementary-fabricspark

## License

MIT License - See LICENSE file

## Related

- [Elementary Documentation](https://docs.elementary-data.com/)
- [dbt-fabricspark adapter](https://github.com/microsoft/dbt-fabricspark)
- [Linear Issue VD-982](https://linear.app/acceleratedata/issue/VD-982)

---

**Status:** ✅ Production Ready

**Last Updated:** 2026-02-26
