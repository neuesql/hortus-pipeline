# Hortus Pipeline — DuckDB Declarative Pipeline Extension

## Overview

A DuckDB extension that brings declarative pipeline semantics (inspired by Databricks Lakeflow / Spark Declarative Pipelines) to DuckDB. Users define materialized views as SQL transformations; the extension resolves the dependency DAG and materializes them in order.

**Two repositories:**

| Repo | Purpose | Language |
|------|---------|----------|
| `hortus-pipeline` | DuckDB SQL extension — new SQL syntax, DAG resolution, materialization engine | C++ |
| `hortus-pipeline-python` | Extension of `duckdb-python` — Python decorator API wrapping the SQL extension | Python + C++ bindings |

The SQL extension is the core. The Python library is a thin layer: decorators generate SQL and call the extension.

This spec covers `hortus-pipeline` (SQL extension). Python repo gets its own spec.

---

## Complete Feature & Grammar Reference

| Feature | SQL | Python SDK | Phase |
|---------|-----|-----------|-------|
| **Materialized View** | | | |
| Create | `CREATE OR REFRESH MATERIALIZED VIEW name AS query` | `@dp.materialized_view()` | 1 |
| Alter definition | `ALTER MATERIALIZED VIEW name AS query` | `dp.alter_materialized_view(name, query)` | 1 |
| Alter add constraint | `ALTER MATERIALIZED VIEW name ADD CONSTRAINT name EXPECT (expr) [ON VIOLATION ...]` | `dp.alter_materialized_view(name, add_constraint=...)` | 1 |
| Alter drop constraint | `ALTER MATERIALIZED VIEW name DROP CONSTRAINT name` | `dp.alter_materialized_view(name, drop_constraint=...)` | 1 |
| Drop | `DROP MATERIALIZED VIEW name` | `dp.drop_materialized_view(name)` | 1 |
| Inspect DAG | `CALL pipeline_status()` | `dp.pipeline_status()` | 1 |
| **Refresh (Manual)** | | | |
| Sync (wait) | `REFRESH MATERIALIZED VIEW name` | `dp.refresh_materialized_view(name)` | 1 |
| Async (return immediately) | `REFRESH MATERIALIZED VIEW name ASYNC` | `dp.refresh_materialized_view(name, async_=True)` | 1 |
| Full recompute | `REFRESH MATERIALIZED VIEW name FULL` | `dp.refresh_materialized_view(name, full=True)` | 1 |
| Refresh all | `REFRESH ALL MATERIALIZED VIEWS` | `dp.refresh_all()` | 1 |
| **Table / View** | | | |
| Temp view | `CREATE TEMP VIEW name AS query` | `@dp.table(name='name', temp=True)` | 1 |
| View | `CREATE VIEW name AS query` | `@dp.table(name='name', temp=False)` | 1 |
| **Expectations** | | | |
| Warn (keep rows) | `CONSTRAINT name EXPECT (expr)` | `@dp.expect(name, expr)` | 1 |
| Drop bad rows | `CONSTRAINT name EXPECT (expr) ON VIOLATION DROP ROW` | `@dp.expect_or_drop(name, expr)` | 1 |
| Fail pipeline | `CONSTRAINT name EXPECT (expr) ON VIOLATION FAIL UPDATE` | `@dp.expect_or_fail(name, expr)` | 1 |
| Grouped expectations | — | `@dp.expect_all(dict)` / `expect_all_or_drop` / `expect_all_or_fail` | 2 |
| Expectation metrics | `SELECT * FROM pipeline_expectations()` | `dp.pipeline_expectations()` | 2 |
| **Clauses** | | | |
| Dependency override | `DEPENDS ON (table1, table2)` | `@dp.materialized_view(depends_on=[...])` | 1 |
| Comment | `COMMENT 'text'` | `@dp.materialized_view(comment='text')` | 1 |
| Explicit schema | `(col_name TYPE [NOT NULL], ...)` | `@dp.materialized_view(schema=...)` | 2 |
| **Schedule (on Materialized View)** | | | |
| Manual (default) | No clause | `@dp.materialized_view()` | 1 |
| Interval | `... SCHEDULE EVERY 1 HOUR AS query` | `@dp.materialized_view(schedule='every 1 hour')` | 2 |
| Cron | `... SCHEDULE CRON '0 6 * * *' AS query` | `@dp.materialized_view(schedule='cron 0 6 * * *')` | 2 |
| Trigger on update | `... SCHEDULE TRIGGER ON UPDATE AS query` | `@dp.materialized_view(schedule='on_update')` | 2 |
| **Scheduler Engine (internal)** | | | |
| Background thread | Extension spawns scheduler thread on load | Same thread via extension | 2 |
| Priority queue | Schedules sorted by next-run time | — | 2 |
| Check schedules | `CALL pipeline_check_schedules()` | `dp.check_schedules()` | 2 |
| List schedules | `CALL pipeline_schedules()` | `dp.schedules()` | 2 |
| Pause schedule | `ALTER MATERIALIZED VIEW name PAUSE SCHEDULE` | `dp.pause_schedule(name)` | 2 |
| Resume schedule | `ALTER MATERIALIZED VIEW name RESUME SCHEDULE` | `dp.resume_schedule(name)` | 2 |
| Thread stops | Automatically on DuckDB shutdown | — | 2 |
| **Streaming Table** | | | |
| Create | `CREATE OR REFRESH STREAMING TABLE name AS query` | `@dp.streaming_table()` | 3+ |
| Alter | `ALTER STREAMING TABLE name ...` | `dp.alter_streaming_table(...)` | 3+ |
| Drop | `DROP STREAMING TABLE name` | `dp.drop_streaming_table(name)` | 3+ |
| Refresh | `REFRESH STREAMING TABLE name` | `dp.refresh_streaming_table(name)` | 3+ |
| **Flow** | | | |
| Create flow | `CREATE FLOW name INSERT INTO table BY NAME AS query` | `@dp.append_flow(target=name)` | 3+ |
| One-time flow | `CREATE FLOW name INSERT ONCE INTO table BY NAME AS query` | `@dp.append_flow(target=name, once=True)` | 3+ |
| **CDC** | | | |
| Auto CDC | `AUTO CDC INTO target ...` | `dp.create_auto_cdc_flow(...)` | Future |
| **Explain** | | | |
| Explain | `EXPLAIN CREATE MATERIALIZED VIEW name AS query` | `dp.explain_materialized_view(name)` | 2 |

---

## Dependency Resolution

| Method | How |
|--------|-----|
| Default | Auto-detect from SQL parsing |
| Override | `DEPENDS ON (...)` / `depends_on=[...]` |

---

## Execution Model

| Behavior | Description | Phase |
|----------|------------|-------|
| Lazy | Query triggers materialization of upstream | 1 |
| Explicit targeted | `REFRESH MATERIALIZED VIEW name` | 1 |
| Explicit all | `REFRESH ALL MATERIALIZED VIEWS` | 1 |
| Fail fast | Abort on first error | 1 |
| Best effort | Skip failed, continue non-dependents | 2 |

---

## Storage

| Decision | Choice |
|----------|--------|
| Where tables live | Current DuckDB database (in-memory or persistent) |
| Data sources | Anything DuckDB can read (`read_csv`, `read_parquet`, `read_json`, extensions like `httpfs`, `postgres_scanner`) |
| Internal data format | `DuckDBPyRelation` between stages (zero-copy), Arrow at boundaries |

---

## Data Quality Expectations

### Constraint Rules
- Standard SQL boolean expressions only
- SQL functions allowed (e.g., `year(date) >= 2020`)
- Boolean combinations allowed (`AND`, `OR`)
- No subqueries, no external calls

### Three Modes

| Mode | SQL | Behavior |
|------|-----|----------|
| Warn (default) | `CONSTRAINT name EXPECT (expr)` | Keep bad rows, log violation count |
| Drop | `CONSTRAINT name EXPECT (expr) ON VIOLATION DROP ROW` | Filter out bad rows before write |
| Fail | `CONSTRAINT name EXPECT (expr) ON VIOLATION FAIL UPDATE` | Abort pipeline immediately |

---

## Scheduler Engine (Phase 2)

- Extension spawns a background scheduler thread when loaded (same pattern as [Query-farm/cronjob](https://github.com/Query-farm/cronjob))
- Thread manages a priority queue of scheduled refreshes sorted by next-run time
- When a schedule fires, it runs the refresh on the DuckDB connection
- Thread stops automatically when DuckDB shuts down
- Users can pause/resume individual schedules via `ALTER MATERIALIZED VIEW name PAUSE/RESUME SCHEDULE`

---

## Phase Roadmap

| Phase | Features |
|-------|----------|
| 1 | `MATERIALIZED VIEW` (create/alter/drop/refresh sync/async/full), views (native DuckDB), expectations (warn/drop/fail), DAG resolution, `pipeline_status()` |
| 2 | Scheduler engine (background thread, priority queue, pause/resume), schedule modes (interval/cron/on_update), grouped expectations, expectation metrics, explicit schema, best-effort mode, `EXPLAIN` |
| 3+ | `STREAMING TABLE`, `FLOW` (deferred) |
| Future | `AUTO CDC` (deferred) |

---

## SQL Examples

### Basic DAG

```sql
-- Source: ingest from CSV
CREATE OR REFRESH MATERIALIZED VIEW customers AS
  SELECT * FROM read_csv('data/customers.csv');

-- Derived: filter active
CREATE OR REFRESH MATERIALIZED VIEW active_customers AS
  SELECT * FROM customers WHERE active = true;

-- Aggregation
CREATE OR REFRESH MATERIALIZED VIEW customer_summary AS
  SELECT region, COUNT(*) as cnt FROM active_customers GROUP BY region;

-- Refresh entire DAG
REFRESH ALL MATERIALIZED VIEWS;

-- Inspect
CALL pipeline_status();
```

### With Expectations

```sql
CREATE OR REFRESH MATERIALIZED VIEW clean_orders
  CONSTRAINT valid_price EXPECT (price > 0)
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW
  CONSTRAINT no_nulls EXPECT (amount IS NOT NULL) ON VIOLATION FAIL UPDATE
AS SELECT * FROM read_csv('data/orders.csv');
```

### With Schedule (Phase 2)

```sql
CREATE OR REFRESH MATERIALIZED VIEW daily_revenue
  SCHEDULE EVERY 1 HOUR
AS SELECT date, SUM(amount) FROM orders GROUP BY date;

CREATE OR REFRESH MATERIALIZED VIEW live_summary
  SCHEDULE TRIGGER ON UPDATE
AS SELECT category, COUNT(*) FROM clean_orders GROUP BY category;
```

### Python SDK Examples

```python
import duckdb.pipeline as dp

# Materialized view returning SQL string
@dp.materialized_view()
def customers():
    return "SELECT * FROM read_csv('data/customers.csv')"

# Materialized view using relational API
@dp.materialized_view()
def active_customers(customers):
    return customers.filter("active = true")

# With expectations
@dp.expect("valid_price", "price > 0")
@dp.expect_or_drop("valid_id", "id IS NOT NULL")
@dp.expect_or_fail("no_nulls", "amount IS NOT NULL")
@dp.materialized_view()
def clean_orders():
    return "SELECT * FROM read_csv('data/orders.csv')"

# Temp view
@dp.table(name='customer_summary', temp=True)
def customer_summary():
    return "SELECT region, COUNT(*) FROM active_customers GROUP BY region"

# With schedule (Phase 2)
@dp.materialized_view(schedule='every 1 hour')
def daily_revenue():
    return "SELECT date, SUM(amount) FROM orders GROUP BY date"

# Refresh
dp.refresh_materialized_view('customers')
dp.refresh_all()
dp.pipeline_status()
```
