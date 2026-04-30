# Pipeline Metadata Persistence Design

## Summary

Persist all pipeline metadata (view definitions, constraints, schedules, run logs, expectation logs) to DuckDB system catalog tables in a `__pipeline__` schema. The in-memory catalog becomes a working cache; the system tables are the single source of truth. The `__pipeline__` schema is created **per database** -- each attached database (file, S3, iceberg) can carry its own pipeline definitions.

## Multi-Database Model

Each attached database gets its own `__pipeline__` schema. The target database is determined by the **materialized view name**:

```sql
-- __pipeline__ created in the default database
CREATE OR REFRESH MATERIALIZED VIEW my_view AS SELECT ...;

-- __pipeline__ created in iceberg_catalog
CREATE OR REFRESH MATERIALIZED VIEW iceberg_catalog.my_view AS SELECT ...;

-- __pipeline__ created in s3_warehouse
CREATE OR REFRESH MATERIALIZED VIEW s3_warehouse.sales_summary AS SELECT ...;
```

On extension load, all attached databases are scanned for existing `__pipeline__` schemas. Each one is hydrated into the in-memory catalog with its database qualifier.

This means:
- An iceberg catalog carries its own pipeline definitions alongside its data
- Detaching and reattaching a database restores its pipelines
- Different databases have independent pipeline configurations
- The default (main) database works the same as before

## Persistence Rules

- **Lazy initialization:** The `__pipeline__` schema and tables are created on the first pipeline mutation in a given database, not at extension load.
- **Write-through:** Every mutation writes to both the in-memory catalog and the persistent `__pipeline__` tables in the target database.
- **All reads from system tables:** All monitoring functions (`pipeline_status()`, `pipeline_expectations()`, etc.) read from the `__pipeline__` tables, not from in-memory state.
- **`:memory:` mode:** Tables exist during the session (fully functional), lost on exit. No special handling or warnings.
- **File-based / S3 / iceberg:** Everything persists across restarts.

## Schema

All tables are created in the `__pipeline__` schema of the target database.

### `__pipeline__.views`

| Column | Type | Description |
|--------|------|-------------|
| `name` | VARCHAR PRIMARY KEY | View name (unqualified) |
| `query` | VARCHAR NOT NULL | The AS query |
| `comment` | VARCHAR | Optional comment |
| `dependencies` | VARCHAR | Comma-separated explicit dependencies (empty = auto-detect) |
| `is_materialized` | BOOLEAN DEFAULT false | Has been executed at least once |
| `created_at` | TIMESTAMP DEFAULT current_timestamp | Creation time |
| `updated_at` | TIMESTAMP DEFAULT current_timestamp | Last modification time |

### `__pipeline__.constraints`

| Column | Type | Description |
|--------|------|-------------|
| `view_name` | VARCHAR NOT NULL | FK to views |
| `constraint_name` | VARCHAR NOT NULL | Constraint name |
| `expression` | VARCHAR NOT NULL | SQL boolean expression |
| `action` | VARCHAR NOT NULL | `'WARN'`, `'DROP_ROW'`, `'FAIL_UPDATE'` |
| PRIMARY KEY | `(view_name, constraint_name)` | |

### `__pipeline__.schedules`

| Column | Type | Description |
|--------|------|-------------|
| `view_name` | VARCHAR PRIMARY KEY | FK to views |
| `schedule_type` | INTEGER NOT NULL | 1=EVERY, 2=CRON, 3=ON_UPDATE |
| `interval_value` | INTEGER | For EVERY type |
| `interval_unit` | VARCHAR | SECOND/MINUTE/HOUR/DAY/WEEK |
| `cron_expression` | VARCHAR | For CRON type |
| `paused` | BOOLEAN DEFAULT false | Whether schedule is paused |
| `last_run_at` | TIMESTAMP | Last scheduler-triggered run time |

### `__pipeline__.run_logs`

Append-only. Cascade-deleted on `DROP MATERIALIZED VIEW`.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | INTEGER (auto-increment) | Unique run identifier |
| `view_name` | VARCHAR NOT NULL | FK to views |
| `started_at` | TIMESTAMP NOT NULL | Run start time |
| `finished_at` | TIMESTAMP | Run end time |
| `success` | BOOLEAN | True if succeeded, false if failed |
| `error_message` | VARCHAR | Error text on failure, NULL on success |
| `trigger` | VARCHAR | `'manual'`, `'schedule'`, `'refresh_all'` |
| `rows_affected` | BIGINT | Number of rows in the materialized result |

### `__pipeline__.expectation_logs`

Append-only. Cascade-deleted on `DROP MATERIALIZED VIEW`.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | INTEGER NOT NULL | FK to run_logs |
| `view_name` | VARCHAR NOT NULL | FK to views |
| `constraint_name` | VARCHAR NOT NULL | Constraint name |
| `total_rows` | BIGINT | Total input rows |
| `passed` | BIGINT | Rows that passed |
| `failed` | BIGINT | Rows that failed |
| `action` | VARCHAR | `'WARN'`, `'DROP_ROW'`, `'FAIL_UPDATE'` |

## Write-Through Mapping

| Operation | Persistent Write |
|-----------|-----------------|
| `CREATE OR REFRESH MV` | INSERT/REPLACE into `views`, `constraints`, `schedules` in the target database |
| `REFRESH MV` | INSERT into `run_logs` + `expectation_logs`, UPDATE `views.is_materialized` |
| `REFRESH ALL` | Same per view, trigger = `'refresh_all'` |
| `ALTER MV ... AS` | UPDATE `views.query`, `views.updated_at` |
| `ALTER MV ADD CONSTRAINT` | INSERT into `constraints` |
| `ALTER MV DROP CONSTRAINT` | DELETE from `constraints` |
| `ALTER MV PAUSE/RESUME` | UPDATE `schedules.paused` |
| `DROP MV` | DELETE from all 5 tables WHERE `view_name = name` |
| Scheduler fires | INSERT into `run_logs` (trigger = `'schedule'`), UPDATE `schedules.last_run_at` |

## Startup & Recovery

On extension load:

1. Scan all attached databases for `__pipeline__` schemas.
2. For each database with a `__pipeline__` schema (returning session):
   - Read all rows from `views`, `constraints`, `schedules`.
   - Hydrate the in-memory `MaterializedViewCatalog` with database-qualified names.
   - For each schedule where `paused = false`:
     - Compute next run from `now()` (skip missed runs, no backfill).
     - Call `PipelineScheduler::AddSchedule()`.
3. For databases without `__pipeline__` (fresh or first use):
   - Do nothing. Pure in-memory until first mutation triggers lazy init.

On first mutation in a database (lazy init):

1. Create `__pipeline__` schema in the target database.
2. Create all 5 tables + sequence.
3. Track which databases are initialized so subsequent mutations skip the check.

## Database Resolution

The target database is extracted from the materialized view name:

- `CREATE OR REFRESH MATERIALIZED VIEW my_view ...` → default database
- `CREATE OR REFRESH MATERIALIZED VIEW catalog.my_view ...` → `catalog` database
- `ALTER/DROP/REFRESH MATERIALIZED VIEW catalog.my_view` → `catalog` database

The `PipelinePersistence` class resolves the database qualifier and routes writes to the correct `__pipeline__` schema.

## Monitoring Functions

All monitoring functions read from `__pipeline__` system tables. When multiple databases have `__pipeline__` schemas, results are aggregated across all of them. A `database` column is added to the output (not stored in the tables -- derived at query time from which database's `__pipeline__` schema the row came from).

| Function | Reads From |
|----------|-----------|
| `pipeline_status()` | `__pipeline__.views` (all databases) |
| `pipeline_expectations()` | `__pipeline__.expectation_logs` filtered by MAX(`run_id`) per `view_name` from `run_logs` |
| `pipeline_schedules()` | `__pipeline__.schedules` (all databases) |
| `pipeline_fires()` | Fires all scheduled views, writes to `run_logs` |
| `pipeline_run_logs()` | `__pipeline__.run_logs` (all databases) |
| `pipeline_expectation_logs()` | `__pipeline__.expectation_logs` (all databases) |

### Renamed function

`pipeline_check_schedules()` is renamed to `pipeline_fires()`.

### New functions

- `pipeline_run_logs()` -- full run history (run_id, view_name, started_at, finished_at, success, error_message, trigger, rows_affected)
- `pipeline_expectation_logs()` -- per-run constraint results (run_id, view_name, constraint_name, total_rows, passed, failed, action)

## Cascade Delete

When `DROP MATERIALIZED VIEW <name>` is executed, delete all rows from `constraints`, `schedules`, `run_logs`, and `expectation_logs` where `view_name = <name>` in the target database, then delete the row from `views`.

## Implementation Component

A new `PipelinePersistence` class wraps all SQL writes to the `__pipeline__` tables. It:

1. Resolves the target database from the view name qualifier
2. Lazy-initializes the `__pipeline__` schema in that database on first use
3. Routes all reads and writes to the correct database's `__pipeline__` schema

```
User SQL -> Parser -> Function -> Catalog (in-memory) -> PipelinePersistence (SQL write to target db)
                                                       -> Materializer (execute query)
```

## No Migration System

No schema versioning for v1. If the table schema changes in a future version, a `__pipeline__.meta` table with `schema_version` can be added later.
