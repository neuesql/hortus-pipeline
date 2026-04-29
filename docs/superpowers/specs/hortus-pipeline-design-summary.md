# Hortus Pipeline — Design Session Summary

> Sessions: 2026-04-29 | Author: Qunfei Wu | Reviewed via Claude (Cowork)

---

## Session 1 — Design Review: DuckDB Declarative Pipeline Extension

### Background

Qunfei shared a design document (`2026-04-29-declarative-pipeline-design.md`) for **Hortus Pipeline** — a DuckDB extension that ports Databricks Lakeflow / Spark Declarative Pipelines semantics to DuckDB. The motivation: give developers who don't need a Spark cluster or cloud account the same declarative pipeline experience, running entirely embedded and locally.

---

### Repository Structure

Two repositories with a strict separation of concerns:

| Repo | Language | Role |
|---|---|---|
| `hortus-pipeline` | C++ | DuckDB SQL extension — new SQL syntax, DAG resolution, materialization engine, scheduler, expectations |
| `hortus-pipeline-python` | Python (no C++ bindings) | Thin decorator API — translates Python to SQL strings, calls the extension via `duckdb.execute()` |

> **Key architectural principle:** The SQL extension is the authoritative core. The Python library is a string-level translation layer only. It has no C++ bindings and no independent execution logic.

---

### Complete Feature Set

#### Materialized Views (Phase 1)

```sql
CREATE OR REFRESH MATERIALIZED VIEW name AS query
ALTER MATERIALIZED VIEW name AS query
ALTER MATERIALIZED VIEW name ADD CONSTRAINT name EXPECT (expr) [ON VIOLATION ...]
ALTER MATERIALIZED VIEW name DROP CONSTRAINT name
DROP MATERIALIZED VIEW name
REFRESH MATERIALIZED VIEW name          -- sync
REFRESH MATERIALIZED VIEW name ASYNC    -- async, returns immediately
REFRESH MATERIALIZED VIEW name FULL     -- force full recompute
REFRESH ALL MATERIALIZED VIEWS
CALL pipeline_status()
```

#### Views (Phase 1 — native DuckDB, no new syntax needed)

```sql
CREATE TEMP VIEW name AS query
CREATE VIEW name AS query
```

#### Expectations / Data Quality (Phase 1)

```sql
CONSTRAINT name EXPECT (expr)                          -- WARN: keep rows, log count
CONSTRAINT name EXPECT (expr) ON VIOLATION DROP ROW    -- filter bad rows
CONSTRAINT name EXPECT (expr) ON VIOLATION FAIL UPDATE -- abort pipeline
```

Phase 2 additions: grouped expectations (`expect_all`, `expect_all_or_drop`, `expect_all_or_fail`), `SELECT * FROM pipeline_expectations()` metrics table.

#### Schedule Modes (Phase 2)

```sql
SCHEDULE EVERY 1 HOUR
SCHEDULE CRON '0 6 * * *'
SCHEDULE TRIGGER ON UPDATE   -- fires at most once per minute when source changes
```

#### Scheduler Engine (Phase 2)

- Background thread spawned on extension load (same pattern as `Query-farm/cronjob`)
- Priority queue of scheduled refreshes sorted by next-run time
- `ALTER MATERIALIZED VIEW name PAUSE/RESUME SCHEDULE`
- Thread stops automatically on DuckDB shutdown

#### Streaming Table + Flow (Phase 3+)

```sql
CREATE OR REFRESH STREAMING TABLE name AS query
CREATE FLOW name INSERT INTO table BY NAME AS query
CREATE FLOW name INSERT ONCE INTO table BY NAME AS query
```

#### CDC (Future)

```sql
AUTO CDC INTO target ...
```

---

### DAG Dependency Resolution

| Method | Mechanism |
|---|---|
| Auto-detect | Parse SQL AST, extract all `TableRef` nodes, match against registered MV names |
| Manual override | `DEPENDS ON (table1, table2)` clause / `depends_on=[...]` in Python |

**Implementation notes:**

- Use `duckdb_parser` to walk the AST and collect table references
- Filter out function calls like `read_csv(...)`, `read_parquet(...)` — these are sources, not table dependencies
- Handle CTE internal references correctly (do not treat CTE alias as an external dependency)
- `DEPENDS ON` is the escape hatch when auto-detection fails or is ambiguous
- Use **Kahn's algorithm** (topological sort) to determine execution order

---

### Execution Model

| Mode | Description | Phase |
|---|---|---|
| Lazy | Querying a downstream MV triggers upstream materialization if stale | 1 |
| Explicit targeted | `REFRESH MATERIALIZED VIEW name` | 1 |
| Explicit all | `REFRESH ALL MATERIALIZED VIEWS` | 1 |
| Fail fast | Abort entire pipeline on first error | 1 |
| Best effort | Skip failed nodes, continue non-dependent branches | 2 |

---

### Storage Decisions

| Decision | Choice |
|---|---|
| Table storage | Current DuckDB database (in-memory or persistent file) |
| Data sources | Anything DuckDB can read: `read_csv`, `read_parquet`, `read_json`, `httpfs` (S3/GCS/Azure), `postgres_scanner`, `delta`, `iceberg`, `mysql`, `sqlite` |
| Internal format | `DuckDBPyRelation` between pipeline stages (zero-copy), Arrow at boundaries |
| Metadata persistence | Internal schema `__pipeline_internal__` with 3 system tables: `pipeline_views`, `pipeline_constraints`, `pipeline_schedules` — survives restart |

---

### Expectations — Internal SQL Rewriting

```sql
-- User writes:
CREATE OR REFRESH MATERIALIZED VIEW clean_orders
  CONSTRAINT valid_price EXPECT (price > 0)
  CONSTRAINT valid_id    EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW
  CONSTRAINT no_nulls    EXPECT (amount IS NOT NULL) ON VIOLATION FAIL UPDATE
AS SELECT * FROM read_csv('orders.csv');

-- C++ extension internally rewrites to:

-- FAIL mode: pre-check first
-- SELECT COUNT(*) FROM (...) WHERE NOT (amount IS NOT NULL);
-- if count > 0 → raise exception, abort

-- DROP ROW mode: inject WHERE filter
-- SELECT * FROM (
--   SELECT *, (price > 0) AS __chk_valid_price__
--   FROM read_csv('orders.csv')
--   WHERE id IS NOT NULL
--   AND   amount IS NOT NULL
-- )

-- WARN mode: keep all rows, log COUNT(CASE WHEN NOT price > 0 THEN 1 END)
```

> Expectations are **injected directly into the query plan** — not a separate post-processing step. Same design as Databricks (confirmed from their docs: *"Expectations modify the Spark query plan of your transformations"*).

---

### Incremental Refresh — The Hardest Problem

DuckDB has no built-in Change Data Feed (Delta Lake CDF equivalent). Four strategies discussed:

| Strategy | How | Tradeoff |
|---|---|---|
| **Full refresh** (Phase 1 default) | Drop and recompute everything | Simple, reliable, expensive for large tables |
| **Watermark / timestamp** | `WHERE updated_at > last_refresh_time` | Requires source tables to have a timestamp column |
| **Row hash snapshot diff** | MD5/CRC snapshot comparison between runs | Precise but high overhead |
| **Source change notification** | Watch Postgres WAL, file `mtime` | Complex, Phase 3+ |

> **Recommendation:** Phase 1 defaults to full refresh. `REFRESH FULL` is documented as default behavior. Incremental via watermark added in Phase 2, more advanced strategies in Phase 3+.

---

### `pipeline_status()` — Recommended Return Schema

```sql
CALL pipeline_status();
-- name             | status | last_refresh     | rows_written | violations | depends_on
-- customers        | ok     | 2026-04-29 10:00 | 50000        | 0          | []
-- active_customers | stale  | 2026-04-29 09:00 | 32000        | 5          | [customers]
```

Additional suggested utility (not in original doc):

```sql
CALL pipeline_lineage('customer_summary');
-- Returns full upstream dependency tree:
-- customers → active_customers → customer_summary
```

---

### DuckDB vs Databricks Lakeflow — Positioning

| | Databricks Lakeflow | Hortus Pipeline |
|---|---|---|
| Scale | Distributed, PB-scale | Single-node, ~100 GB |
| Deployment | Cloud-native, requires account | Embedded, zero dependencies |
| Cost | Pay-per-compute | Free, open source |
| Target users | Enterprise data teams | Developers, small/mid teams, local analytics |
| Incremental refresh | Enzyme + Delta CDF (automatic) | Watermark / full (manual strategy) |
| Streaming | Native (Structured Streaming) | Phase 3+ (deferred) |
| Connectors | Lakeflow Connect (managed) | DuckDB native extensions (flexible) |
| Distribution | Cloud only | `pip install`, fully portable |

> **Target user overlap:** dbt + DuckDB users who want pipeline DAG semantics without Spark.

---

### Phase Roadmap

| Phase | Scope |
|---|---|
| **1** | `MATERIALIZED VIEW` (create/alter/drop/refresh sync/async/full), views (native DuckDB), expectations (warn/drop/fail), DAG resolution, `pipeline_status()` |
| **2** | Scheduler engine (background thread, priority queue, pause/resume), schedule modes (interval/cron/on_update), grouped expectations, expectation metrics, explicit schema, best-effort mode, `EXPLAIN` |
| **3+** | `STREAMING TABLE`, `FLOW` (append + once) |
| **Future** | `AUTO CDC` |

> **Phase 1 success criterion:** The Basic DAG SQL example from the design doc runs end-to-end correctly.

---

## Session 2 — Python Implementation Strategy

### The Question

Should `hortus-pipeline-python` be implemented as:

- **(A)** C++ Python bindings (pybind11/Cython) wrapping the C++ extension internals, or
- **(B)** Pure Python that generates SQL strings and calls `duckdb.execute()`?

### Answer: Pure Python — Option B

The Python library is a **string-level translation layer only**. It has no knowledge of C++ internals. Its entire job is:

1. Collect metadata from decorators at decoration time (`name`, `constraints`, `schedule`, `depends_on`)
2. When the decorated function is invoked, assemble a SQL string using the syntax defined by the C++ extension
3. Call `duckdb.execute(sql)` — the C++ extension handles everything from there

```
User writes Python decorators
        ↓  (metadata collection at decoration time)
Python assembles SQL string
        ↓  (single duckdb.execute() call)
C++ Extension: Parser → DAG → Materialization → Quality checks → Storage
```

---

### Core Python Implementation

```python
import duckdb, functools

_conn = None

def _get_conn():
    global _conn
    if _conn is None:
        _conn = duckdb.connect()
        _conn.execute("LOAD hortus_pipeline")  # load C++ extension
    return _conn


class _MaterializedViewBuilder:
    """Collects all decorator metadata, assembles the final SQL string."""

    def __init__(self, name, comment, schedule, depends_on):
        self.name = name
        self.comment = comment
        self.schedule = schedule
        self.depends_on = depends_on
        self.constraints = []   # populated by @expect* decorators

    def build_sql(self, query: str) -> str:
        parts = [f"CREATE OR REFRESH MATERIALIZED VIEW {self.name}"]
        for c in self.constraints:
            violation = {
                "drop": " ON VIOLATION DROP ROW",
                "fail": " ON VIOLATION FAIL UPDATE",
            }.get(c["mode"], "")
            parts.append(f"  CONSTRAINT {c['name']} EXPECT ({c['expr']}){violation}")
        if self.schedule:
            parts.append(f"  SCHEDULE {self.schedule.upper()}")
        parts.append(f"AS {query}")
        return "\n".join(parts)


def materialized_view(name=None, comment=None, schedule=None, depends_on=None):
    def decorator(fn):
        builder = _MaterializedViewBuilder(
            name or fn.__name__, comment, schedule, depends_on
        )
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            result = fn(*args, **kwargs)
            # accepts SQL string or DuckDBPyRelation
            query = result.query if hasattr(result, "query") else result
            _get_conn().execute(builder.build_sql(query))  # ← only line that matters
        wrapper.__pipeline_builder__ = builder  # for @expect* decorators to attach to
        return wrapper
    return decorator


def expect_or_drop(constraint_name, expr):
    def decorator(fn):
        fn.__pipeline_builder__.constraints.append(
            {"name": constraint_name, "expr": expr, "mode": "drop"}
        )
        return fn
    return decorator


def expect_or_fail(constraint_name, expr):
    def decorator(fn):
        fn.__pipeline_builder__.constraints.append(
            {"name": constraint_name, "expr": expr, "mode": "fail"}
        )
        return fn
    return decorator


def expect(constraint_name, expr):
    def decorator(fn):
        fn.__pipeline_builder__.constraints.append(
            {"name": constraint_name, "expr": expr, "mode": "warn"}
        )
        return fn
    return decorator


# All utility functions are single duckdb.execute() calls
def refresh_materialized_view(name, full=False, async_=False):
    suffix = (" FULL" if full else "") + (" ASYNC" if async_ else "")
    _get_conn().execute(f"REFRESH MATERIALIZED VIEW {name}{suffix}")

def refresh_all():
    _get_conn().execute("REFRESH ALL MATERIALIZED VIEWS")

def pipeline_status():
    return _get_conn().execute("CALL pipeline_status()").df()
```

---

### DuckDBPyRelation Support

When Python decorators return a `DuckDBPyRelation` (relational API result) rather than a raw SQL string, use `.query` to extract the underlying SQL:

```python
@dp.materialized_view()
def active_customers(customers):            # parameter name = dependency table name
    return customers.filter("active = true")    # returns DuckDBPyRelation

# Python layer: result.query → "SELECT * FROM customers WHERE active = true"
# Then assembles full DDL and calls duckdb.execute()
```

> This makes the **function-parameter-as-dependency-name** pattern work cleanly — more Pythonic than the Databricks equivalent.

---

### Option Comparison

| | Pure Python (chosen) | C++ Bindings |
|---|---|---|
| Implementation complexity | Low — string concatenation only | High — pybind11/Cython required |
| Maintenance | SQL syntax change → Python change only | C++ change → recompile all platform wheels |
| Performance | Identical (Python runs only at planning time) | Identical |
| Distribution | `pip install hortus-pipeline` | Must build per-platform wheels (linux/mac/win × x86/arm) |
| Debugging | Print the SQL string to inspect | Requires C++ debugger |
| Dependency | Only `duckdb` | C++ build toolchain |

---

### The Contract Between the Two Layers

> **Python knows what SQL syntax the C++ extension supports. Python generates conforming SQL strings. Python passes them to `duckdb.execute()`. That is the entire interface.**

The two layers are fully decoupled:

- C++ has no knowledge of Python
- Python has no knowledge of C++ internals
- The interface is a **SQL string** — nothing more

```
┌─────────────────────────────────────────────────────┐
│  Python layer (pure Python, string translation only) │
│                                                      │
│  @dp.expect_or_drop("valid_id", "id IS NOT NULL")   │
│  @dp.materialized_view()                            │
│  def customers():                                    │
│      return "SELECT * FROM read_csv('data.csv')"    │
│                                                      │
│              ↓ assembles SQL string                  │
│                                                      │
│  """                                                 │
│  CREATE OR REFRESH MATERIALIZED VIEW customers       │
│    CONSTRAINT valid_id EXPECT (id IS NOT NULL)       │
│    ON VIOLATION DROP ROW                             │
│  AS SELECT * FROM read_csv('data.csv')               │
│  """                                                 │
│                                                      │
│  duckdb.execute(sql)  ← the only call that matters   │
└──────────────────────┬──────────────────────────────┘
                       │ plain SQL string
┌──────────────────────▼──────────────────────────────┐
│  C++ Extension (hortus-pipeline)                     │
│                                                      │
│  Parser   → recognise new SQL syntax                 │
│  DAG      → resolve table dependencies               │
│  Engine   → materialise, run quality checks          │
│  Scheduler→ manage scheduled refreshes               │
│  Storage  → write to DuckDB tables                   │
└─────────────────────────────────────────────────────┘
```

---

## References

### DuckDB

- [DuckDB Community Extensions Development](https://duckdb.org/community_extensions/development)
- [DuckDB CREATE VIEW](https://duckdb.org/docs/lts/sql/statements/create_view)
- [DuckDB Python API](https://github.com/duckdb/duckdb-python)
- [Query-farm/cronjob](https://github.com/Query-farm/cronjob) — reference pattern for scheduler background thread

### Databricks Lakeflow / Spark Declarative Pipelines

- [Spark Declarative Pipelines Programming Guide](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
- [Pipeline SQL Language Reference](https://docs.databricks.com/aws/en/ldp/developer/sql-ref)
- [CREATE MATERIALIZED VIEW](https://docs.databricks.com/aws/en/ldp/developer/ldp-sql-ref-create-materialized-view)
- [CREATE STREAMING TABLE](https://docs.databricks.com/aws/en/ldp/developer/ldp-sql-ref-create-streaming-table)
- [CREATE FLOW](https://docs.databricks.com/aws/en/ldp/developer/ldp-sql-ref-create-flow)
- [Pipeline Expectations](https://docs.databricks.com/aws/en/ldp/expectations)
- [Python SDK Reference (pyspark.pipelines)](https://docs.databricks.com/aws/en/ldp/developer/python-ref)
- [Incremental Refresh for Materialized Views](https://docs.databricks.com/aws/en/optimizations/incremental-refresh)
- [Triggered vs Continuous Pipeline Mode](https://docs.databricks.com/aws/en/ldp/pipeline-mode)
- [Optimizing Materialized View Recomputes](https://www.databricks.com/blog/optimizing-materialized-views-recomputes)
