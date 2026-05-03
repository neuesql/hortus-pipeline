# pipeline_status() — show auto-detected dependencies (lazy)

**Date:** 2026-05-03
**Scope:** `pipeline_status()` only. No changes to `EXPLAIN`, `pipeline_schedules`, or other monitoring surfaces.
**Sequencing:** Assumes the rename in `2026-05-03-rename-system-tables-design.md` lands first. References to `__pipeline__.materialized_views` below were `__pipeline__.views` pre-rename; if the rename slips, substitute the old name in the implementation only.

## Problem

`pipeline_status()` returns an empty `dependencies` column for materialized views whose dependencies are auto-detected from their FROM/JOIN clauses. Reproduction:

```sql
CREATE TABLE raw_orders(amount DOUBLE, region VARCHAR);
CREATE OR REFRESH MATERIALIZED VIEW orders AS
  SELECT * FROM raw_orders WHERE amount > 0;
CREATE OR REFRESH MATERIALIZED VIEW us_orders AS
  SELECT * FROM orders WHERE region = 'US';
SELECT name, dependencies FROM pipeline_status();
-- Actual:   us_orders has empty dependencies
-- Expected: us_orders has 'orders'
```

Refresh ordering already works correctly — `REFRESH ALL MATERIALIZED VIEWS` produces correct results because `DAGResolver::Resolve` runs `ExtractDependencies` lazily at refresh time. The bug is display-only.

## Root cause

The `dependencies` column in `__pipeline__.materialized_views` is populated from the explicit `DEPENDS ON (...)` clause only. When deps are auto-detected, nothing is written to the column. `pipeline_status()` reads the column directly and displays it as-is.

The DAG resolver already does the right thing: explicit deps take precedence, otherwise regex-extract from the query and filter to the MV set in the same database.

## Goal

`pipeline_status()` displays the same effective dependencies that `DAGResolver::Resolve` uses, computed lazily on each call from the stored query text. No schema change. No changes to write-side persistence.

## Non-goals

- Storing auto-detected deps at create time (lazy mode chosen — staleness-proof, no schema migration).
- Changing the semantics of the `__pipeline__.materialized_views.dependencies` column (still stores explicit deps only).
- Updating `pipeline_explain_mv`, `pipeline_schedules`, or any other monitoring surface.
- Cross-database dependency resolution (preserves current intra-database-only behavior).

## Design

### Shared helper in DAGResolver

Add a small static helper that captures the explicit-vs-auto resolution rule in one place. Both `pipeline_status()` and the existing `Resolve()`/`ResolveFor()` call it.

```cpp
// src/include/executor/dag_resolver.hpp
static vector<string> ResolveEffectiveDeps(
    const ViewDefinition &def,
    const unordered_set<string> &mv_set);
```

Behavior:

- If `def.explicit_dependencies` is non-empty → return those, filtered to names present in `mv_set`.
- Otherwise → return `ExtractDependencies(def.query)`, filtered to names present in `mv_set`.
- In both branches, exclude `def.name` itself (defensive — matches existing self-loop guard in `Resolve`).

`Resolve()` and `ResolveFor()` are refactored to call this helper, eliminating the duplicate either-or blocks at `dag_resolver.cpp:55-58` and `dag_resolver.cpp:110-114`.

### pipeline_status() change

Current init builds a single UNION ALL across all databases' `__pipeline__.materialized_views` tables. This loses the database-of-origin for each row, which we need because dependency resolution is intra-database.

Restructure init to iterate per-database:

```
results = []
for each database with __pipeline__ schema (or "memory" if none):
    all_names = persistence.GetAllNames(db, database)
    mv_set = unordered_set<string>(all_names)
    for each name in all_names:
        def = persistence.GetView(db, database, name)
        effective = DAGResolver::ResolveEffectiveDeps(def, mv_set)
        deps_str = comma-join(effective)
        results.push_back({name, def.query, deps_str, def.is_materialized, def.comment})
```

The function-emit path (`PipelineStatusFunc`) reads from this in-memory `results` vector instead of a `MaterializedQueryResult`. Output schema unchanged: `(name VARCHAR, query VARCHAR, dependencies VARCHAR, is_materialized BOOLEAN, comment VARCHAR)`.

`comma-join` uses `,` with no spaces, matching the on-disk format already used for explicit deps (`pipeline_persistence.cpp:118-124`). Order of names within `dependencies` follows the order returned by the helper (regex match order for auto-detect, list order for explicit).

### Edge cases

| Case | Behavior |
|---|---|
| View references a non-MV table (e.g., `raw_orders`) | Filtered out by `mv_set` — does not appear in `dependencies` |
| Explicit `DEPENDS ON (a, b)` where `b` is not an MV | Only `a` shown (matches resolver) |
| Self-reference in FROM clause | Excluded |
| Malformed/unparseable query | `ExtractDependencies` returns empty → empty `dependencies` cell, no error |
| Table function source (`read_csv`, `read_parquet`, etc.) | Already in `skip_names` — ignored |
| Same-named MV exists in two attached databases | Each row resolved against its own database's MV set; no cross-db leakage |

## What does not change

- `__pipeline__.materialized_views.dependencies` column semantics — still stores explicit deps only.
- `PipelinePersistence::PersistView` — unchanged.
- `DAGResolver::Resolve` ordering semantics — `REFRESH ALL` already produces correct results.
- `pipeline_explain_mv`, `pipeline_schedules`, `pipeline_run_logs`, `pipeline_fires`, `pipeline_expectations`, `pipeline_expectation_logs` — unchanged.

## Files touched

- `src/include/executor/dag_resolver.hpp` — declare `ResolveEffectiveDeps`.
- `src/executor/dag_resolver.cpp` — implement helper, refactor `Resolve`/`ResolveFor` to use it.
- `src/functions/pipeline_status.cpp` — restructure init to per-database iteration; compute effective deps via helper.
- `test/sql/` — add a new test file for auto-detected dependency display.

## Tests

New `test/sql/pipeline_status_auto_deps.test`:

1. **User's reproduction** — 4-MV chain (`orders` → `us_orders`, `eu_orders` → `us_revenue`).
   - Assert: `orders` has empty deps, `us_orders` and `eu_orders` show `orders`, `us_revenue` shows `us_orders`.
2. **Filters non-MV table references** — view referencing a regular table must not include that table in its deps.
3. **Explicit `DEPENDS ON` takes precedence** — view with `DEPENDS ON (a)` and a FROM-clause reference to `b` (both MVs) shows only `a`.
4. **Explicit dep that isn't an MV** — `DEPENDS ON (not_an_mv)` produces empty deps (filtered).
5. **Self-reference** — view whose query somehow references itself shows empty deps.

Existing tests must continue to pass — particularly any test that exercises `REFRESH ALL` ordering, since the helper refactor must not change resolution behavior.

## Performance

Per call: O(N × Q) where N is view count in the database and Q is average query length (regex). For pipeline sizes seen in practice (tens to low hundreds of views), this is negligible compared to the query roundtrip.
