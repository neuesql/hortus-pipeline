# Hortus Pipeline

A DuckDB extension for declarative data pipelines. Define materialized views with data quality expectations, automatic dependency resolution, and scheduled refreshes -- all in SQL.

Built for DuckDB v1.5.1. DuckDB v1.5.2. All platforms.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Sample Data Setup](#sample-data-setup)
3. [CREATE OR REFRESH MATERIALIZED VIEW](#1-create-or-refresh-materialized-view)
4. [Data Quality Expectations](#2-data-quality-expectations)
   - [WARN (default)](#case-2a-warn-default)
   - [DROP ROW](#case-2b-drop-row)
   - [FAIL UPDATE](#case-2c-fail-update)
   - [Multiple Constraints](#case-2d-multiple-constraints)
5. [Dependency Resolution (DAG)](#3-dependency-resolution-dag)
   - [Auto-detected Dependencies](#case-3a-auto-detected-dependencies)
   - [Explicit Dependencies](#case-3b-explicit-dependencies)
6. [REFRESH MATERIALIZED VIEW](#4-refresh-materialized-view)
7. [REFRESH ALL MATERIALIZED VIEWS](#5-refresh-all-materialized-views)
   - [Best-Effort Mode](#case-5b-best-effort-mode)
8. [ALTER MATERIALIZED VIEW](#6-alter-materialized-view)
   - [Change Query](#case-6a-change-query)
   - [Add/Drop Constraint](#case-6b-adddrop-constraint)
   - [Pause/Resume Schedule](#case-6c-pauseresume-schedule)
9. [DROP MATERIALIZED VIEW](#7-drop-materialized-view)
10. [EXPLAIN](#8-explain)
11. [Scheduling](#9-scheduling)
12. [Monitoring Functions](#10-monitoring-functions)
    - [pipeline_status()](#pipeline_status)
    - [pipeline_expectations()](#pipeline_expectations)
    - [pipeline_schedules()](#pipeline_schedules)
    - [pipeline_fires()](#pipeline_fires)
    - [pipeline_run_logs()](#pipeline_run_logs)
    - [pipeline_expectation_logs()](#pipeline_expectation_logs)
13. [Persistence](#persistence)
14. [Building](#building)
15. [Installation](#installation)

## Quick Start

```sh
duckdb -unsigned
-- Install from GitHub Pages repository
INSTALL hortus_pipeline FROM 'https://neuesql.github.io/hortus-pipeline';
```
```sh
./build/release/duckdb # build local
```

```sql
LOAD hortus_pipeline;
```

## Sample Data Setup

All examples below use these two tables. Run this first to follow along.

```sql
-- Orders table: includes some bad data for testing expectations
CREATE TABLE raw_orders (
    id      INTEGER,
    region  VARCHAR,
    amount  DECIMAL(10,2),
    product VARCHAR
);

INSERT INTO raw_orders VALUES
    (1,    'US',   100.00,  'Widget'),
    (2,    'EU',   200.00,  'Gadget'),
    (3,    NULL,    50.00,  'Widget'),   -- NULL region
    (4,    'US',   -10.00,  'Gadget'),   -- negative amount
    (5,    'EU',   300.00,  'Widget'),
    (6,    'APAC',   0.00,  'Gadget'),   -- zero amount
    (7,    'US',   150.00,  NULL);       -- NULL product

-- Customers table
CREATE TABLE raw_customers (
    customer_id INTEGER,
    name        VARCHAR,
    region      VARCHAR
);

INSERT INTO raw_customers VALUES
    (1, 'Alice',   'US'),
    (2, 'Bob',     'EU'),
    (3, 'Charlie', 'APAC'),
    (4, 'Diana',   'US');
```

---

## 1. CREATE OR REFRESH MATERIALIZED VIEW

Creates a materialized view and immediately executes the query, persisting results as a table. If the view already exists, it is replaced.

**Syntax**

```sql
CREATE OR REFRESH MATERIALIZED VIEW <name>
  [CONSTRAINT <cname> EXPECT (<expr>) [ON VIOLATION {DROP ROW | FAIL UPDATE}]]
  [DEPENDS ON (<view1>, <view2>, ...)]
  [COMMENT '<text>']
  [SCHEDULE {EVERY <n> {SECOND|MINUTE|HOUR|DAY|WEEK} | CRON '<expr>' | TRIGGER ON UPDATE}]
AS <query>;
```

All clauses are optional and can appear in any order before `AS`.

**Case 1a: Simple materialized view**

```sql
CREATE OR REFRESH MATERIALIZED VIEW all_orders AS
  SELECT * FROM raw_orders;

SELECT * FROM all_orders;
-- ┌───────┬────────┬────────┬─────────┐
-- │  id   │ region │ amount │ product │
-- ├───────┼────────┼────────┼─────────┤
-- │   1   │ US     │ 100.00 │ Widget  │
-- │   2   │ EU     │ 200.00 │ Gadget  │
-- │   3   │ NULL   │  50.00 │ Widget  │
-- │   4   │ US     │ -10.00 │ Gadget  │
-- │   5   │ EU     │ 300.00 │ Widget  │
-- │   6   │ APAC   │   0.00 │ Gadget  │
-- │   7   │ US     │ 150.00 │ NULL    │
-- └───────┴────────┴────────┴─────────┘
-- 7 rows
```

**Case 1b: Replace an existing view**

Running `CREATE OR REFRESH` again replaces the view with the new query:

```sql
CREATE OR REFRESH MATERIALIZED VIEW all_orders AS
  SELECT id, region, amount FROM raw_orders WHERE amount > 0;

SELECT * FROM all_orders;
-- ┌───────┬────────┬────────┐
-- │  id   │ region │ amount │
-- ├───────┼────────┼────────┤
-- │   1   │ US     │ 100.00 │
-- │   2   │ EU     │ 200.00 │
-- │   3   │ NULL   │  50.00 │
-- │   5   │ EU     │ 300.00 │
-- │   7   │ US     │ 150.00 │
-- └───────┴────────┴────────┘
-- 5 rows
```

**Case 1c: With a comment**

```sql
CREATE OR REFRESH MATERIALIZED VIEW all_orders
  COMMENT 'All orders from raw source'
AS SELECT * FROM raw_orders;
```

---

## 2. Data Quality Expectations

Constraints validate rows during materialization. Each constraint has a name, a boolean expression, and a violation action.

| Action | Behavior |
|--------|----------|
| *(default)* | **Warn** -- keep all rows, log violation count |
| `ON VIOLATION DROP ROW` | Filter out rows that fail the constraint |
| `ON VIOLATION FAIL UPDATE` | Abort the entire materialization if any rows fail |

**Execution order:** `FAIL UPDATE` checks run first (abort early), then `DROP ROW` filters apply, then `WARN` counts are recorded.

### Case 2a: WARN (default)

All rows are kept. Violations are counted and visible via `pipeline_expectations()`.

```sql
CREATE OR REFRESH MATERIALIZED VIEW orders_warn
  CONSTRAINT positive_amount EXPECT (amount > 0)
AS SELECT * FROM raw_orders;

SELECT * FROM orders_warn;
-- 7 rows -- all rows kept, violations only logged

CALL pipeline_expectations();
-- ┌─────────────┬───────────────────┬────────────┬────────┬────────┬────────┐
-- │  view_name  │ expectation_name  │ total_rows │ passed │ failed │ action │
-- ├─────────────┼───────────────────┼────────────┼────────┼────────┼────────┤
-- │ orders_warn │ positive_amount   │          7 │      5 │      2 │ WARN   │
-- └─────────────┴───────────────────┴────────────┴────────┴────────┴────────┘
-- id=4 (amount=-10) and id=6 (amount=0) failed, but rows are still in the table
```

### Case 2b: DROP ROW

Rows that violate the constraint are filtered out.

```sql
CREATE OR REFRESH MATERIALIZED VIEW orders_clean
  CONSTRAINT valid_region EXPECT (region IS NOT NULL) ON VIOLATION DROP ROW
AS SELECT * FROM raw_orders;

SELECT * FROM orders_clean;
-- ┌───────┬────────┬────────┬─────────┐
-- │  id   │ region │ amount │ product │
-- ├───────┼────────┼────────┼─────────┤
-- │   1   │ US     │ 100.00 │ Widget  │
-- │   2   │ EU     │ 200.00 │ Gadget  │
-- │   4   │ US     │ -10.00 │ Gadget  │
-- │   5   │ EU     │ 300.00 │ Widget  │
-- │   6   │ APAC   │   0.00 │ Gadget  │
-- │   7   │ US     │ 150.00 │ NULL    │
-- └───────┴────────┴────────┴─────────┘
-- 6 rows -- id=3 (region=NULL) was dropped
```

### Case 2c: FAIL UPDATE

Materialization aborts entirely if any row violates the constraint.

```sql
-- This will ERROR because id=4 has amount=-10
CREATE OR REFRESH MATERIALIZED VIEW orders_strict
  CONSTRAINT positive_amount EXPECT (amount > 0) ON VIOLATION FAIL UPDATE
AS SELECT * FROM raw_orders;
-- Error: Expectation violated -- view is NOT created

-- This succeeds because all amounts are > 0 after filtering
CREATE OR REFRESH MATERIALIZED VIEW orders_strict
  CONSTRAINT positive_amount EXPECT (amount > 0) ON VIOLATION FAIL UPDATE
AS SELECT * FROM raw_orders WHERE amount > 0;

SELECT COUNT(*) FROM orders_strict;
-- 5
```

### Case 2d: Multiple Constraints

Combine multiple constraints with different actions on the same view:

```sql
CREATE OR REFRESH MATERIALIZED VIEW orders_validated
  CONSTRAINT has_product  EXPECT (product IS NOT NULL) ON VIOLATION FAIL UPDATE
  CONSTRAINT has_region   EXPECT (region IS NOT NULL)  ON VIOLATION DROP ROW
  CONSTRAINT good_amount  EXPECT (amount > 0)
AS SELECT * FROM raw_orders WHERE product IS NOT NULL;
-- Step 1: FAIL UPDATE checks product IS NOT NULL on the 6 rows where product IS NOT NULL -- passes
-- Step 2: DROP ROW filters out rows where region IS NULL (drops id=3)
-- Step 3: WARN counts rows where amount <= 0 (id=4 and id=6)

SELECT * FROM orders_validated;
-- ┌───────┬────────┬────────┬─────────┐
-- │  id   │ region │ amount │ product │
-- ├───────┼────────┼────────┼─────────┤
-- │   1   │ US     │ 100.00 │ Widget  │
-- │   2   │ EU     │ 200.00 │ Gadget  │
-- │   4   │ US     │ -10.00 │ Gadget  │
-- │   5   │ EU     │ 300.00 │ Widget  │
-- │   6   │ APAC   │   0.00 │ Gadget  │
-- └───────┴────────┴────────┴─────────┘
-- 5 rows (id=3 dropped by has_region, id=7 excluded by WHERE)

CALL pipeline_expectations();
-- Shows metrics for all 3 constraints on orders_validated
```

---

## 3. Dependency Resolution (DAG)

Dependencies are **auto-detected** by parsing `FROM` and `JOIN` references in the query. External sources (`read_csv`, `read_parquet`, `range`, etc.) are excluded automatically.

### Case 3a: Auto-detected Dependencies

```sql
-- Layer 1: source
CREATE OR REFRESH MATERIALIZED VIEW orders AS
  SELECT * FROM raw_orders WHERE amount > 0 AND region IS NOT NULL;

-- Layer 2: depends on "orders" (auto-detected from FROM clause)
CREATE OR REFRESH MATERIALIZED VIEW us_orders AS
  SELECT * FROM orders WHERE region = 'US';

-- Layer 2: also depends on "orders"
CREATE OR REFRESH MATERIALIZED VIEW eu_orders AS
  SELECT * FROM orders WHERE region = 'EU';

-- Layer 3: depends on "us_orders" (auto-detected)
CREATE OR REFRESH MATERIALIZED VIEW us_revenue AS
  SELECT SUM(amount) AS total FROM us_orders;

-- Check the detected dependencies (use ORDER BY for stable output;
-- pipeline_status() does not promise a specific row order).
SELECT name, dependencies FROM pipeline_status() ORDER BY name;
-- ┌────────────┬──────────────┐
-- │    name    │ dependencies │
-- ├────────────┼──────────────┤
-- │ eu_orders  │ orders       │
-- │ orders     │              │
-- │ us_orders  │ orders       │
-- │ us_revenue │ us_orders    │
-- └────────────┴──────────────┘

-- REFRESH ALL materializes in dependency order regardless of the listing
-- above: orders -> us_orders, eu_orders -> us_revenue.
REFRESH ALL MATERIALIZED VIEWS;
```

### Case 3b: Explicit Dependencies

Use `DEPENDS ON` when auto-detection is insufficient (e.g., dynamic SQL or complex joins):

```sql
CREATE OR REFRESH MATERIALIZED VIEW customers AS
  SELECT * FROM raw_customers;

CREATE OR REFRESH MATERIALIZED VIEW order_summary
  DEPENDS ON (orders, customers)
AS SELECT o.region, c.name, SUM(o.amount) AS total
  FROM orders o
  JOIN customers c ON o.region = c.region
  GROUP BY o.region, c.name;

SELECT name, dependencies FROM pipeline_status() WHERE name = 'order_summary';
-- ┌───────────────┬──────────────────┐
-- │     name      │   dependencies   │
-- ├───────────────┼──────────────────┤
-- │ order_summary │ orders,customers │
-- └───────────────┴──────────────────┘
```

Cycles are detected and raise an error.

---

## 4. REFRESH MATERIALIZED VIEW

Re-executes the query and updates the persisted table.

**Syntax**

```sql
REFRESH MATERIALIZED VIEW <name> [SYNC | ASYNC | FULL];
```

| Mode | Behavior |
|------|----------|
| `SYNC` (default) | Blocks until refresh completes |
| `ASYNC` | Returns immediately, refreshes in background |
| `FULL` | Drops and recreates the table (forces full recompute) |

**Case 4a: Sync refresh (default)**

```sql
-- Insert new data into the source table
INSERT INTO raw_orders VALUES (8, 'EU', 400.00, 'Widget');

-- Refresh to pick up changes
REFRESH MATERIALIZED VIEW orders;

SELECT COUNT(*) FROM orders;
-- count increased by 1
```

**Case 4b: Async refresh**

```sql
REFRESH MATERIALIZED VIEW orders ASYNC;
-- Returns immediately, refresh happens in background
```

**Case 4c: Full refresh**

```sql
REFRESH MATERIALIZED VIEW orders FULL;
-- Drops and recreates the underlying table
```

---

## 5. REFRESH ALL MATERIALIZED VIEWS

Refreshes all views in dependency order (topological sort).

**Case 5a: Strict mode (default)**

```sql
REFRESH ALL MATERIALIZED VIEWS;
-- Refreshes: orders -> us_orders, eu_orders -> us_revenue -> order_summary
-- Aborts on first error
```

### Case 5b: Best-Effort Mode

Continue on errors instead of aborting. Failed views and their dependents are skipped; independent branches continue.

```sql
REFRESH ALL MATERIALIZED VIEWS WITH (on_failure = 'best_effort');
-- If "orders" fails, us_orders/eu_orders/us_revenue are skipped
-- But independent views (e.g., "customers") still refresh
-- Errors are collected and reported at the end
```

---

## 6. ALTER MATERIALIZED VIEW

Modifies an existing materialized view without dropping it.

### Case 6a: Change Query

Changes the query definition. Does **not** re-execute until the next `REFRESH`.

```sql
ALTER MATERIALIZED VIEW us_orders AS
  SELECT id, region, amount FROM orders WHERE region = 'US' AND amount > 100;

-- View still has old data until refreshed
REFRESH MATERIALIZED VIEW us_orders;

SELECT * FROM us_orders;
-- ┌───────┬────────┬────────┐
-- │  id   │ region │ amount │
-- ├───────┼────────┼────────┤
-- │   7   │ US     │ 150.00 │
-- └───────┴────────┴────────┘
-- Only US orders with amount > 100
```

### Case 6b: Add/Drop Constraint

```sql
-- Add a new constraint
ALTER MATERIALIZED VIEW orders ADD CONSTRAINT big_order EXPECT (amount >= 50);

-- Refresh to apply and see metrics
REFRESH MATERIALIZED VIEW orders;
CALL pipeline_expectations();
-- Shows metrics for the new "big_order" constraint

-- Remove the constraint
ALTER MATERIALIZED VIEW orders DROP CONSTRAINT big_order;
```

### Case 6c: Pause/Resume Schedule

```sql
-- First create a scheduled view
CREATE OR REFRESH MATERIALIZED VIEW hourly_stats
  SCHEDULE EVERY 1 HOUR
AS SELECT region, COUNT(*) AS cnt FROM raw_orders GROUP BY region;

-- Pause the schedule
ALTER MATERIALIZED VIEW hourly_stats PAUSE SCHEDULE;

SELECT name, paused FROM pipeline_schedules() WHERE name = 'hourly_stats';
-- ┌──────────────┬────────┐
-- │     name     │ paused │
-- ├──────────────┼────────┤
-- │ hourly_stats │ true   │
-- └──────────────┴────────┘

-- Resume the schedule
ALTER MATERIALIZED VIEW hourly_stats RESUME SCHEDULE;

SELECT name, paused FROM pipeline_schedules() WHERE name = 'hourly_stats';
-- ┌──────────────┬────────┐
-- │     name     │ paused │
-- ├──────────────┼────────┤
-- │ hourly_stats │ false  │
-- └──────────────┴────────┘
```

---

## 7. DROP MATERIALIZED VIEW

Removes the view definition and deletes the underlying table.

```sql
DROP MATERIALIZED VIEW us_revenue;

SELECT * FROM us_revenue;
-- Error: Table with name us_revenue does not exist
```

---

## 8. EXPLAIN

Shows the query plan, detected dependencies, and constraints without executing anything.

```sql
EXPLAIN CREATE MATERIALIZED VIEW derived AS
  SELECT region, SUM(amount) AS total FROM orders GROUP BY region;
-- Output includes:
--   - View name and type
--   - Full query execution plan
--   - Dependencies: orders
--   - Constraints: (none)
-- The query is NOT executed
```

---

## 9. Scheduling

Attach a schedule to automatically refresh a view in the background.

| Type | Syntax | Description |
|------|--------|-------------|
| Interval | `SCHEDULE EVERY <n> {SECOND\|MINUTE\|HOUR\|DAY\|WEEK}` | Fixed interval refresh |
| Cron | `SCHEDULE CRON '<expr>'` | Standard 5-field cron expression |
| Trigger | `SCHEDULE TRIGGER ON UPDATE` | Refresh when source data changes |

**Case 9a: Interval schedule**

```sql
CREATE OR REFRESH MATERIALIZED VIEW stats_hourly
  SCHEDULE EVERY 1 HOUR
AS SELECT region, COUNT(*) AS cnt, SUM(amount) AS total
  FROM raw_orders GROUP BY region;
```

**Case 9b: Cron schedule**

```sql
CREATE OR REFRESH MATERIALIZED VIEW daily_report
  SCHEDULE CRON '0 6 * * *'
AS SELECT region, SUM(amount) AS total FROM raw_orders GROUP BY region;
```

**Case 9c: Trigger on update**

```sql
CREATE OR REFRESH MATERIALIZED VIEW live_summary
  SCHEDULE TRIGGER ON UPDATE
AS SELECT product, COUNT(*) AS cnt FROM raw_orders WHERE product IS NOT NULL GROUP BY product;
```

**Manage schedules:**

```sql
CALL pipeline_schedules();
-- ┌────────────────┬──────────────────┬────────┐
-- │      name      │     schedule     │ paused │
-- ├────────────────┼──────────────────┼────────┤
-- │ hourly_stats   │ EVERY 1 HOUR     │ false  │
-- │ stats_hourly   │ EVERY 1 HOUR     │ false  │
-- │ daily_report   │ CRON 0 6 * * *   │ false  │
-- │ live_summary   │ TRIGGER ON UPDATE│ false  │
-- └────────────────┴──────────────────┴────────┘

-- Force all scheduled views to refresh now
CALL pipeline_fires();
```

---

## 10. Monitoring Functions

Each read-only inspection function below can be invoked in three equivalent forms:

```sql
SELECT * FROM pipeline_status();   -- standard form; supports WHERE / ORDER BY / etc.
CALL pipeline_status();            -- procedural form
SHOW pipeline_status();            -- strict sugar; no trailing clauses (use SELECT * FROM for filtering)
```

`SHOW` works for the five read-only functions: `pipeline_status`, `pipeline_expectations`, `pipeline_schedules`, `pipeline_run_logs`, `pipeline_expectation_logs`. It is **not** available for `pipeline_fires()` (which has side effects — it fires schedules).

### pipeline_status()

Returns metadata for all materialized views.

```sql
CALL pipeline_status();
```

| Column | Type | Description |
|--------|------|-------------|
| `name` | VARCHAR | View name |
| `query` | VARCHAR | Query definition |
| `dependencies` | VARCHAR | Comma-separated dependency list |
| `is_materialized` | BOOLEAN | Whether the view has been executed |
| `comment` | VARCHAR | Optional comment text |

```sql
SELECT name, is_materialized, dependencies FROM pipeline_status();
```

### pipeline_expectations()

Returns expectation metrics from the last materialization.

```sql
CALL pipeline_expectations();
```

| Column | Type | Description |
|--------|------|-------------|
| `view_name` | VARCHAR | Materialized view name |
| `expectation_name` | VARCHAR | Expectation name |
| `total_rows` | BIGINT | Total input rows |
| `passed` | BIGINT | Rows that passed |
| `failed` | BIGINT | Rows that failed |
| `action` | VARCHAR | WARN, DROP ROW, or FAIL UPDATE |

```sql
-- Find views with data quality issues
SELECT * FROM pipeline_expectations() WHERE failed > 0 ORDER BY failed DESC;
```

### pipeline_schedules()

Lists all scheduled materialized views.

```sql
CALL pipeline_schedules();
```

| Column | Type | Description |
|--------|------|-------------|
| `name` | VARCHAR | View name |
| `schedule` | VARCHAR | Schedule description (e.g., "EVERY 1 HOUR") |
| `paused` | BOOLEAN | Whether the schedule is paused |

### pipeline_fires()

Immediately fires all scheduled views and returns execution status.

```sql
CALL pipeline_fires();
```

| Column | Type | Description |
|--------|------|-------------|
| `name` | VARCHAR | View name |
| `status` | VARCHAR | Execution result |

### pipeline_run_logs()

Returns full run history.

```sql
CALL pipeline_run_logs();
```

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | BIGINT | Unique run identifier |
| `view_name` | VARCHAR | Materialized view name |
| `started_at` | TIMESTAMP | Run start time |
| `finished_at` | TIMESTAMP | Run end time |
| `success` | BOOLEAN | True if succeeded |
| `error_message` | VARCHAR | Error text on failure |
| `trigger` | VARCHAR | What initiated the run: `manual` (user-typed `REFRESH MATERIALIZED VIEW` or initial CREATE), `schedule` (background scheduler thread or `pipeline_fires()`), `refresh_all` (`REFRESH ALL MATERIALIZED VIEWS`) |
| `rows_affected` | BIGINT | Row count in result |

### pipeline_expectation_logs()

Returns per-run expectation results.

```sql
CALL pipeline_expectation_logs();
```

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | BIGINT | FK to run_logs |
| `view_name` | VARCHAR | Materialized view name |
| `expectation_name` | VARCHAR | Expectation name |
| `total_rows` | BIGINT | Total input rows |
| `passed` | BIGINT | Rows passed |
| `failed` | BIGINT | Rows failed |
| `action` | VARCHAR | WARN, DROP ROW, or FAIL UPDATE |

---

## Cleanup

To remove all views created in the examples above:

```sql
DROP MATERIALIZED VIEW orders_warn;
DROP MATERIALIZED VIEW orders_clean;
DROP MATERIALIZED VIEW orders_strict;
DROP MATERIALIZED VIEW orders_validated;
DROP MATERIALIZED VIEW us_revenue;
DROP MATERIALIZED VIEW us_orders;
DROP MATERIALIZED VIEW eu_orders;
DROP MATERIALIZED VIEW order_summary;
DROP MATERIALIZED VIEW orders;
DROP MATERIALIZED VIEW customers;
DROP MATERIALIZED VIEW hourly_stats;
DROP MATERIALIZED VIEW stats_hourly;
DROP MATERIALIZED VIEW daily_report;
DROP MATERIALIZED VIEW live_summary;
DROP TABLE raw_orders;
DROP TABLE raw_customers;
```

---

## Persistence

Pipeline metadata is automatically persisted to a `__pipeline__` schema. This schema is created lazily on first pipeline creation.

- **File-based DuckDB:** Metadata persists across restarts. Schedules auto-resume.
- **`:memory:` mode:** Metadata exists during the session but is lost on exit.
- **Multi-database:** Each attached database gets its own `__pipeline__` schema, determined by the view name qualifier.

```sql
-- Default database
CREATE OR REFRESH MATERIALIZED VIEW my_view AS SELECT ...;

-- Iceberg catalog -- __pipeline__ created in iceberg_catalog
CREATE OR REFRESH MATERIALIZED VIEW iceberg_catalog.my_view AS SELECT ...;
```

The `__pipeline__` schema contains these system tables:

| Table | Purpose |
|-------|---------|
| `__pipeline__.materialized_views` | View definitions |
| `__pipeline__.expectations` | Expectation definitions |
| `__pipeline__.schedules` | Schedule configurations |
| `__pipeline__.run_logs` | Run history (append-only) |
| `__pipeline__.expectation_logs` | Per-run expectation results |

All data is cascade-deleted when a view is dropped.

---

## Building

```sh
make        # release build
make debug  # debug build
make test   # run tests
```

Build outputs:

```
./build/release/duckdb                                                      # DuckDB shell with extension loaded
./build/release/test/unittest                                               # test runner
./build/release/extension/hortus_pipeline/hortus_pipeline.duckdb_extension  # loadable extension binary
```

## Installation

**From the extension repository (recommended):**

```sql
-- Install from GitHub Pages repository
INSTALL hortus_pipeline FROM 'https://neuesql.github.io/hortus-pipeline';
LOAD hortus_pipeline;
```

**From build directory:**

```sh
# Use the bundled DuckDB binary (extension auto-loaded)
./build/release/duckdb

# Or load into any DuckDB CLI
duckdb -unsigned
```

```sql
LOAD '/path/to/hortus_pipeline.duckdb_extension';
```
