# Rename `__pipeline__` system tables — `views` → `materialized_views`, `constraints` → `expectations`

**Date:** 2026-05-03
**Scope:** Internal `__pipeline__` schema table names, related column names, related C++ symbols, and the user-visible output schemas of two monitoring functions. Clean break — no migration of existing on-disk data.

## Motivation

The internal `__pipeline__.views` and `__pipeline__.constraints` table names are leftovers of an earlier vocabulary. The user-facing terminology is "materialized view" and "expectation" (DDL keyword `CONSTRAINT name EXPECT (...)` notwithstanding — that keyword stays). The system tables and the C++ surface should match the user-facing concept names.

## Goal

Rename the two system tables, their FK-shaped columns, the matching C++ persistence-API symbols, and the two monitoring functions whose result schemas expose the old column name. After the change, no internal code path references the strings `"views"` (as table) or `"constraints"` (as table) or any C++ identifier ending in `Constraint`/`constraint_name` outside of the SQL keyword `CONSTRAINT` itself.

## Non-goals

- Migrating existing on-disk databases. Per user direction (no production users), `EnsureInitialized` simply creates tables under the new names. An attached database that was initialized by an older build will produce a "table not found" binder error on the first query — accepted.
- Changing the SQL keyword `CONSTRAINT name EXPECT (...)`. This is user-facing DDL syntax with a separate concern (parser, README, examples) and is out of scope here.
- Renaming the `__pipeline__` schema itself.
- Renaming `view_name` columns in `run_logs` / `schedules` / `expectation_logs` — they reference the entity, and the entity is a "materialized view" whose short name is "view". Keeping `view_name` keeps the column terse without misleading.
- Renaming the C++ struct `Expectation` (already correctly named).

## Renames

### SQL system tables (in each `__pipeline__` schema)

| Before | After |
|---|---|
| `__pipeline__.views` | `__pipeline__.materialized_views` |
| `__pipeline__.constraints` | `__pipeline__.expectations` |

### Column renames

| Table | Before | After |
|---|---|---|
| `__pipeline__.expectations` (renamed from `constraints`) | `constraint_name` | `expectation_name` |
| `__pipeline__.expectation_logs` (table name unchanged) | `constraint_name` | `expectation_name` |

`expectations.PRIMARY KEY (view_name, constraint_name)` becomes `PRIMARY KEY (view_name, expectation_name)`.

### Public output columns of monitoring functions (breaking change)

| Function | Before | After |
|---|---|---|
| `pipeline_expectations()` | `constraint_name` | `expectation_name` |
| `pipeline_expectation_logs()` | `constraint_name` | `expectation_name` |

### C++ symbols

| Before | After | Where |
|---|---|---|
| `PipelinePersistence::AddConstraint` | `AddExpectation` | `pipeline_persistence.{hpp,cpp}` |
| `PipelinePersistence::DropConstraint` | `DropExpectation` | `pipeline_persistence.{hpp,cpp}` |
| `ExpectationMetric.constraint_name` | `expectation_name` | `pipeline_persistence.hpp` |
| `PipelinePersistence::InsertExpectationLog(... constraint_name ...)` parameter | `expectation_name` | `pipeline_persistence.{hpp,cpp}` |
| `PipelineParseData::drop_constraint_name` | `drop_expectation_name` | `pipeline_parse_data.hpp` |
| Local/parameter `constraint_name` | `expectation_name` | `alter_materialized_view.cpp`, `pipeline_parser.cpp`, persistence cpp internals |

The user-facing SQL keyword `CONSTRAINT` continues to be parsed unchanged — only the parsed output is stored in fields named `expectation_name`.

## Files touched

- `src/persistence/pipeline_persistence.cpp` — `CREATE TABLE` DDL for both renamed tables and the column rename in `expectation_logs`; all 17 call sites of `QualifyTable(..., "views"|"constraints")`; method-body SQL string literals (`view_name`/`constraint_name` column lists become `view_name`/`expectation_name`); method renames.
- `src/include/persistence/pipeline_persistence.hpp` — method declarations, struct field, parameter names.
- `src/functions/pipeline_status.cpp` — `QualifyTable(databases[i] ..., "views")` → `"materialized_views"`.
- `src/functions/pipeline_expectations.cpp` — output column name (`names.emplace_back("constraint_name")` → `"expectation_name"`) and inner SELECT (`e.constraint_name` → `e.expectation_name`).
- `src/functions/pipeline_expectation_logs.cpp` — same: result-schema column name and inner SELECT column.
- `src/functions/alter_materialized_view.cpp` — local field renames; method calls now `AddExpectation`/`DropExpectation`.
- `src/parser/pipeline_parser.cpp` — `data->drop_constraint_name = ...` → `drop_expectation_name`.
- `src/include/parser/pipeline_parse_data.hpp` — field + copy-ctor rename.
- `test/sql/persistence.test` — `__pipeline__.views` → `__pipeline__.materialized_views` (2 lines: lines 24 and 34).
- `test/sql/run_logs.test` — line 32: `SELECT constraint_name, ...` → `SELECT expectation_name, ...`.
- `README.md` — sweep for any reference to the renamed tables/columns and update.

## Implementation order

1. Update `pipeline_persistence.{hpp,cpp}` end-to-end: schema DDL, all internal SQL strings, method names. This produces compile errors elsewhere.
2. Fix consumers driven by compiler errors: `alter_materialized_view.cpp`, `pipeline_parser.cpp`, `pipeline_parse_data.hpp`, `pipeline_status.cpp`, `pipeline_expectations.cpp`, `pipeline_expectation_logs.cpp`.
3. Update tests that reference user-visible names directly: `persistence.test`, `run_logs.test`.
4. Build the extension; run the full test suite (`make test`); fix any remaining failures.
5. README sweep: replace any occurrence of the old names.

## Verification

After implementation, none of the following greps should produce a hit (excluding the SQL keyword `CONSTRAINT` and historical comments):

```sh
grep -rn 'QualifyTable.*"views"\|QualifyTable.*"constraints"' src/
grep -rn 'AddConstraint\|DropConstraint\|drop_constraint_name' src/ test/
grep -rn '__pipeline__\.views\b\|__pipeline__\.constraints\b' src/ test/ README.md
grep -rn '\bconstraint_name\b' src/ test/   # keyword "CONSTRAINT" is fine; identifier "constraint_name" should be gone
```

The full test suite must pass with the new names.

## Edge cases

| Case | Behavior |
|---|---|
| Attached DB previously initialized by older build | Old tables are not auto-renamed; new code never queries old names. First operation referencing `materialized_views` errors. Accepted (no production users). |
| `GetAllPipelineDatabases` lookup | Detects schemas by `__pipeline__` schema name in DuckDB system catalog; does not reference our table names — unaffected. |
| Cross-version test fixture using old name | None known; if any binary fixture exists, regenerate. |
