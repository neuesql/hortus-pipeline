# Rename `views`→`materialized_views`, `constraints`→`expectations` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename two system tables in the `__pipeline__` schema and the matching column / C++ symbol surface. Clean break — no migration of existing on-disk data.

**Architecture:** This is a coordinated rename across schema DDL, internal SQL strings, C++ method signatures, struct fields, parse-data, monitoring-function output schemas, tests, and README. The work is driven by `pipeline_persistence.{hpp,cpp}` first; downstream files are then fixed by chasing compiler errors.

**Tech Stack:** C++ (DuckDB extension), SQLLogic tests.

**Spec:** `docs/superpowers/specs/2026-05-03-rename-system-tables-design.md`

---

## File Structure

| File | Change |
|---|---|
| `src/persistence/pipeline_persistence.cpp` | DDL renames + ~17 `QualifyTable("views"\|"constraints")` call sites + column references in SQL strings + method renames |
| `src/include/persistence/pipeline_persistence.hpp` | Method declarations, parameter names, `ExpectationMetric.constraint_name` → `expectation_name` |
| `src/functions/pipeline_status.cpp` | `QualifyTable(..., "views")` → `"materialized_views"` |
| `src/functions/pipeline_expectations.cpp` | Output column name + inner SELECT |
| `src/functions/pipeline_expectation_logs.cpp` | Output column name + inner SELECT |
| `src/functions/alter_materialized_view.cpp` | Local field renames + new method names |
| `src/parser/pipeline_parser.cpp` | `data->drop_constraint_name` → `drop_expectation_name` |
| `src/include/parser/pipeline_parse_data.hpp` | Field + copy-ctor rename |
| `test/sql/persistence.test` | Update direct table references (lines 22-24, 32-34) |
| `test/sql/run_logs.test` | Update column reference on line 32 |
| `README.md` | Sweep for any reference to old names |

No new files. No new public C++ symbols beyond the rename targets.

---

### Task 1: Update `__pipeline__` schema DDL in `CreateSchema`

**Files:**
- Modify: `src/persistence/pipeline_persistence.cpp:54-70` and `:97-105`

- [ ] **Step 1: Rename the `views` table DDL (around line 54)**

Find:
```cpp
	conn.Query("CREATE TABLE IF NOT EXISTS " + QualifyTable(database, "views") +
	           " ("
	           "name VARCHAR PRIMARY KEY, "
```

Replace `"views"` with `"materialized_views"`:
```cpp
	conn.Query("CREATE TABLE IF NOT EXISTS " + QualifyTable(database, "materialized_views") +
	           " ("
	           "name VARCHAR PRIMARY KEY, "
```

- [ ] **Step 2: Rename the `constraints` table DDL and its `constraint_name` column (around line 64)**

Find:
```cpp
	conn.Query("CREATE TABLE IF NOT EXISTS " + QualifyTable(database, "constraints") +
	           " ("
	           "view_name VARCHAR NOT NULL, "
	           "constraint_name VARCHAR NOT NULL, "
	           "expression VARCHAR NOT NULL, "
	           "action VARCHAR NOT NULL, "
	           "PRIMARY KEY (view_name, constraint_name))");
```

Replace with:
```cpp
	conn.Query("CREATE TABLE IF NOT EXISTS " + QualifyTable(database, "expectations") +
	           " ("
	           "view_name VARCHAR NOT NULL, "
	           "expectation_name VARCHAR NOT NULL, "
	           "expression VARCHAR NOT NULL, "
	           "action VARCHAR NOT NULL, "
	           "PRIMARY KEY (view_name, expectation_name))");
```

- [ ] **Step 3: Rename the `expectation_logs.constraint_name` column (around line 97)**

Find:
```cpp
	conn.Query("CREATE TABLE IF NOT EXISTS " + QualifyTable(database, "expectation_logs") +
	           " ("
	           "run_id BIGINT NOT NULL, "
	           "view_name VARCHAR NOT NULL, "
	           "constraint_name VARCHAR NOT NULL, "
	           "total_rows BIGINT, "
	           "passed BIGINT, "
	           "failed BIGINT, "
	           "action VARCHAR)");
```

Replace `constraint_name` with `expectation_name`:
```cpp
	conn.Query("CREATE TABLE IF NOT EXISTS " + QualifyTable(database, "expectation_logs") +
	           " ("
	           "run_id BIGINT NOT NULL, "
	           "view_name VARCHAR NOT NULL, "
	           "expectation_name VARCHAR NOT NULL, "
	           "total_rows BIGINT, "
	           "passed BIGINT, "
	           "failed BIGINT, "
	           "action VARCHAR)");
```

(The table name `expectation_logs` is unchanged; only the column changes.)

---

### Task 2: Update internal SQL in `pipeline_persistence.cpp` to use the renamed tables

All references to `QualifyTable(database, "views")` become `QualifyTable(database, "materialized_views")`. All `QualifyTable(database, "constraints")` become `QualifyTable(database, "expectations")`. Column references to `constraint_name` inside SQL string literals become `expectation_name`.

**Files:**
- Modify: `src/persistence/pipeline_persistence.cpp` — global text substitution + manual review

- [ ] **Step 1: Replace `QualifyTable(database, "views")` everywhere in this file**

There are 12 such occurrences (after Task 1). Replace each `QualifyTable(database, "views")` with `QualifyTable(database, "materialized_views")`. Affected sites include:
- The `views_table` local at line 129
- Lines 138, 143 (INSERT)
- Line 190 (UPDATE for query)
- Line 200 (UPDATE for is_materialized)
- Line 254 (cascade DELETE)
- Line 300 (Exists check)
- Line 314 (GetView SELECT)
- Line 383 (GetAllNames SELECT)

Also the local variable name `views_table` (line 129) → keep the variable name; only its assigned string changes. (Renaming the local is optional polish.)

- [ ] **Step 2: Replace `QualifyTable(database, "constraints")` everywhere**

There are 5 such occurrences. Each `QualifyTable(database, "constraints")` becomes `QualifyTable(database, "expectations")`. Sites:
- Line 148 (DELETE during PersistView)
- Line 164 (INSERT during PersistView)
- Line 211 (INSERT during AddConstraint — method itself renamed in Task 4)
- Line 221 (DELETE during DropConstraint)
- Line 252 (DELETE during CascadeDelete)
- Line 341 (SELECT during GetView)

- [ ] **Step 3: Update SQL column lists that reference `constraint_name`**

Find the INSERT in `PersistView` (around line 164):
```cpp
		conn.Query("INSERT INTO " + QualifyTable(database, "expectations") +
		           " (view_name, constraint_name, expression, action) VALUES ('" + ...
```
Change `constraint_name` to `expectation_name`:
```cpp
		conn.Query("INSERT INTO " + QualifyTable(database, "expectations") +
		           " (view_name, expectation_name, expression, action) VALUES ('" + ...
```

Find the SELECT in `GetView` (around line 341):
```cpp
	    conn.Query("SELECT constraint_name, expression, action FROM " + QualifyTable(database, "expectations") +
```
Change to:
```cpp
	    conn.Query("SELECT expectation_name, expression, action FROM " + QualifyTable(database, "expectations") +
```

Find the INSERT in `AddConstraint` (around line 212):
```cpp
	conn.Query("INSERT INTO " + QualifyTable(database, "expectations") +
	           " (view_name, constraint_name, expression, action) VALUES ('" + EscapeSQL(name) + "', '" +
	           EscapeSQL(constraint_name) + "', '" + EscapeSQL(expression) + "', '" + EscapeSQL(action) + "')");
```
Change to:
```cpp
	conn.Query("INSERT INTO " + QualifyTable(database, "expectations") +
	           " (view_name, expectation_name, expression, action) VALUES ('" + EscapeSQL(name) + "', '" +
	           EscapeSQL(expectation_name) + "', '" + EscapeSQL(expression) + "', '" + EscapeSQL(action) + "')");
```

(The C++ parameter `constraint_name` will be renamed in Task 4. Treat the SQL column name and the C++ parameter as separate edits.)

Find the DELETE in `DropConstraint` (around line 222):
```cpp
	conn.Query("DELETE FROM " + QualifyTable(database, "expectations") + " WHERE view_name = '" + EscapeSQL(name) +
	           "' AND constraint_name = '" + EscapeSQL(constraint_name) + "'");
```
Change SQL column only:
```cpp
	conn.Query("DELETE FROM " + QualifyTable(database, "expectations") + " WHERE view_name = '" + EscapeSQL(name) +
	           "' AND expectation_name = '" + EscapeSQL(expectation_name) + "'");
```

Find `InsertExpectationLog` (around line 290):
```cpp
	conn.Query("INSERT INTO " + QualifyTable(database, "expectation_logs") +
	           " (run_id, view_name, constraint_name, total_rows, passed, failed, action) VALUES (" +
	           std::to_string(run_id) + ", '" + EscapeSQL(view_name) + "', '" + EscapeSQL(constraint_name) + "', " +
```
Change SQL column name:
```cpp
	conn.Query("INSERT INTO " + QualifyTable(database, "expectation_logs") +
	           " (run_id, view_name, expectation_name, total_rows, passed, failed, action) VALUES (" +
	           std::to_string(run_id) + ", '" + EscapeSQL(view_name) + "', '" + EscapeSQL(expectation_name) + "', " +
```

(The C++ parameter `constraint_name` of this method will be renamed in Task 4 along with the header.)

---

### Task 3: Rename `pipeline_status.cpp` table reference

**Files:**
- Modify: `src/functions/pipeline_status.cpp:55`

- [ ] **Step 1: Update the `QualifyTable` argument**

Find line 55:
```cpp
		string table = PipelinePersistence::QualifyTable(databases[i] == "memory" ? "" : databases[i], "views");
```

Replace `"views"` with `"materialized_views"`:
```cpp
		string table = PipelinePersistence::QualifyTable(databases[i] == "memory" ? "" : databases[i], "materialized_views");
```

---

### Task 4: Rename `AddConstraint`/`DropConstraint` and `constraint_name` parameters

**Files:**
- Modify: `src/include/persistence/pipeline_persistence.hpp:58-61, 70-72`
- Modify: `src/persistence/pipeline_persistence.cpp:206-223, 283-294`

- [ ] **Step 1: Update header method declarations**

In `pipeline_persistence.hpp`, find:
```cpp
	void AddConstraint(DatabaseInstance &db, const string &database, const string &name, const string &constraint_name,
	                   const string &expression, const string &action);
	void DropConstraint(DatabaseInstance &db, const string &database, const string &name,
	                    const string &constraint_name);
```

Replace with:
```cpp
	void AddExpectation(DatabaseInstance &db, const string &database, const string &name, const string &expectation_name,
	                   const string &expression, const string &action);
	void DropExpectation(DatabaseInstance &db, const string &database, const string &name,
	                    const string &expectation_name);
```

- [ ] **Step 2: Update `InsertExpectationLog` parameter name in header**

In the same file, find:
```cpp
	void InsertExpectationLog(DatabaseInstance &db, const string &database, int64_t run_id, const string &view_name,
	                          const string &constraint_name, int64_t total_rows, int64_t passed, int64_t failed,
	                          const string &action);
```

Replace `constraint_name` with `expectation_name`:
```cpp
	void InsertExpectationLog(DatabaseInstance &db, const string &database, int64_t run_id, const string &view_name,
	                          const string &expectation_name, int64_t total_rows, int64_t passed, int64_t failed,
	                          const string &action);
```

- [ ] **Step 3: Update the `ExpectationMetric` struct field**

Find:
```cpp
struct ExpectationMetric {
	string constraint_name;
	int64_t total_rows;
```

Replace with:
```cpp
struct ExpectationMetric {
	string expectation_name;
	int64_t total_rows;
```

- [ ] **Step 4: Update the method definitions in the cpp**

In `pipeline_persistence.cpp`, find the `AddConstraint` definition (line 206):
```cpp
void PipelinePersistence::AddConstraint(DatabaseInstance &db, const string &database, const string &name,
                                        const string &constraint_name, const string &expression, const string &action) {
```

Replace with:
```cpp
void PipelinePersistence::AddExpectation(DatabaseInstance &db, const string &database, const string &name,
                                         const string &expectation_name, const string &expression, const string &action) {
```

(Inside the body, the SQL column was already renamed in Task 2 Step 3. The local parameter is now named `expectation_name` to match the SQL column.)

Find the `DropConstraint` definition (line 216):
```cpp
void PipelinePersistence::DropConstraint(DatabaseInstance &db, const string &database, const string &name,
                                         const string &constraint_name) {
```

Replace with:
```cpp
void PipelinePersistence::DropExpectation(DatabaseInstance &db, const string &database, const string &name,
                                          const string &expectation_name) {
```

Find `InsertExpectationLog` definition (line 283):
```cpp
void PipelinePersistence::InsertExpectationLog(DatabaseInstance &db, const string &database, int64_t run_id,
                                               const string &view_name, const string &constraint_name,
                                               int64_t total_rows, int64_t passed, int64_t failed,
                                               const string &action) {
```

Replace `constraint_name` with `expectation_name`:
```cpp
void PipelinePersistence::InsertExpectationLog(DatabaseInstance &db, const string &database, int64_t run_id,
                                               const string &view_name, const string &expectation_name,
                                               int64_t total_rows, int64_t passed, int64_t failed,
                                               const string &action) {
```

---

### Task 5: Update `alter_materialized_view.cpp` to use new method names and field

**Files:**
- Modify: `src/functions/alter_materialized_view.cpp:14-37, 81-87`

- [ ] **Step 1: Rename local field `constraint_name` and `drop_constraint_name`**

In the bind-data struct near line 14:
```cpp
	string constraint_name;
	string expression;
	string action;
	string drop_constraint_name;
```

Rename to:
```cpp
	string expectation_name;
	string expression;
	string action;
	string drop_expectation_name;
```

- [ ] **Step 2: Update assignments in bind**

Around lines 33-37, find:
```cpp
		data->constraint_name = StringValue::Get(input.inputs[2]);
```
Change to:
```cpp
		data->expectation_name = StringValue::Get(input.inputs[2]);
```

And:
```cpp
		data->drop_constraint_name = StringValue::Get(input.inputs[2]);
```
Change to:
```cpp
		data->drop_expectation_name = StringValue::Get(input.inputs[2]);
```

- [ ] **Step 3: Update method calls in execute**

Around line 81, find:
```cpp
		persistence.AddConstraint(db, database, unqualified_name, bind_data.constraint_name, bind_data.expression,
		                          bind_data.action);
		string status =
		    "Added constraint '" + bind_data.constraint_name + "' to materialized view '" + bind_data.view_name + "'";
```

Replace with:
```cpp
		persistence.AddExpectation(db, database, unqualified_name, bind_data.expectation_name, bind_data.expression,
		                           bind_data.action);
		string status =
		    "Added expectation '" + bind_data.expectation_name + "' to materialized view '" + bind_data.view_name + "'";
```

Around line 86, find:
```cpp
		persistence.DropConstraint(db, database, unqualified_name, bind_data.drop_constraint_name);
		status = "Dropped constraint '" + bind_data.drop_constraint_name + "' from materialized view '" +
```

Replace with:
```cpp
		persistence.DropExpectation(db, database, unqualified_name, bind_data.drop_expectation_name);
		status = "Dropped expectation '" + bind_data.drop_expectation_name + "' from materialized view '" +
```

(The user-visible status string changes "constraint" → "expectation". This matches the new internal vocabulary; the SQL keyword `CONSTRAINT` is unchanged.)

---

### Task 6: Update parse-data and parser

**Files:**
- Modify: `src/include/parser/pipeline_parse_data.hpp:37, 55`
- Modify: `src/parser/pipeline_parser.cpp:431, 751`

- [ ] **Step 1: Rename the parse-data field**

In `pipeline_parse_data.hpp`, around line 37:
```cpp
	string drop_constraint_name;
```
Replace with:
```cpp
	string drop_expectation_name;
```

Around line 55 (inside the copy logic):
```cpp
		copy->drop_constraint_name = drop_constraint_name;
```
Replace with:
```cpp
		copy->drop_expectation_name = drop_expectation_name;
```

- [ ] **Step 2: Update parser writes / reads**

In `pipeline_parser.cpp` around line 431:
```cpp
		data->drop_constraint_name = tokens[pos];
```
Replace with:
```cpp
		data->drop_expectation_name = tokens[pos];
```

Around line 751:
```cpp
			result.parameters.push_back(Value(data.drop_constraint_name));
```
Replace with:
```cpp
			result.parameters.push_back(Value(data.drop_expectation_name));
```

---

### Task 7: Update `pipeline_expectations` and `pipeline_expectation_logs` output schemas

**Files:**
- Modify: `src/functions/pipeline_expectations.cpp:23, 58`
- Modify: `src/functions/pipeline_expectation_logs.cpp:24, 58`

- [ ] **Step 1: Rename output column in `pipeline_expectations.cpp`**

Around line 23:
```cpp
	names.emplace_back("constraint_name");
```
Replace with:
```cpp
	names.emplace_back("expectation_name");
```

Around line 58:
```cpp
		query += "SELECT e.view_name, e.constraint_name, e.total_rows, e.passed, e.failed, e.action "
```
Replace with:
```cpp
		query += "SELECT e.view_name, e.expectation_name, e.total_rows, e.passed, e.failed, e.action "
```

(There may be a `FROM <expectation_logs> e LEFT JOIN <constraints> c ...` clause in the same SELECT — the actual `FROM` table strings come from `QualifyTable` calls, which are already updated in Task 2. Verify: open the file, look for any `"constraints"` literal — if present, rename.)

- [ ] **Step 2: Rename output column in `pipeline_expectation_logs.cpp`**

Around line 24:
```cpp
	names.emplace_back("constraint_name");
```
Replace with:
```cpp
	names.emplace_back("expectation_name");
```

Around line 58:
```cpp
		query += "SELECT run_id, view_name, constraint_name, total_rows, passed, failed, action FROM " + table;
```
Replace with:
```cpp
		query += "SELECT run_id, view_name, expectation_name, total_rows, passed, failed, action FROM " + table;
```

---

### Task 8: Update producer of `ExpectationMetric.constraint_name`

The `Materializer` populates `ExpectationMetric` instances; with the field rename, all writes now use `expectation_name`.

**Files:**
- Modify: `src/executor/expectation_checker.cpp` (search-and-replace)
- Modify: `src/executor/materializer.cpp:45-46`

- [ ] **Step 1: Rename in `expectation_checker.cpp`**

Run:
```bash
grep -n "constraint_name" src/executor/expectation_checker.cpp
```

For each hit, replace `constraint_name` with `expectation_name`. (This file populates the metric; its parameter is the C++ struct field.)

- [ ] **Step 2: Rename in `materializer.cpp`**

Around line 45-46:
```cpp
		for (auto &m : metrics) {
			persistence.InsertExpectationLog(db, database, run_id, unqualified_name, m.constraint_name, m.total_rows,
			                                 m.passed, m.failed, m.action);
		}
```

Replace with:
```cpp
		for (auto &m : metrics) {
			persistence.InsertExpectationLog(db, database, run_id, unqualified_name, m.expectation_name, m.total_rows,
			                                 m.passed, m.failed, m.action);
		}
```

---

### Task 9: Build and chase remaining compiler errors

- [ ] **Step 1: Build**

```bash
make
```

Expected: build may surface 1-2 stragglers in files we didn't enumerate (e.g., `dag_resolver.cpp` references `def.explicit_dependencies` not the renamed fields, but verify nothing else uses `constraint_name` as a C++ identifier).

- [ ] **Step 2: If errors appear, fix each**

For each error: open the file at the reported line, replace `constraint_name`→`expectation_name`, `AddConstraint`→`AddExpectation`, `DropConstraint`→`DropExpectation`, `drop_constraint_name`→`drop_expectation_name` as appropriate. Re-run `make` until it succeeds.

---

### Task 10: Update SQL tests that hit user-visible names

**Files:**
- Modify: `test/sql/persistence.test:22-24, 32-34`
- Modify: `test/sql/run_logs.test:32`

- [ ] **Step 1: Update `persistence.test`**

Around line 22, find:
```
# __pipeline__.views should have our view
query II
SELECT name, query FROM __pipeline__.views WHERE name = 'persist_test';
```

Replace with:
```
# __pipeline__.materialized_views should have our view
query II
SELECT name, query FROM __pipeline__.materialized_views WHERE name = 'persist_test';
```

Around line 32, find:
```
# View should be gone from __pipeline__.views
query I
SELECT COUNT(*) FROM __pipeline__.views WHERE name = 'persist_test';
```

Replace with:
```
# View should be gone from __pipeline__.materialized_views
query I
SELECT COUNT(*) FROM __pipeline__.materialized_views WHERE name = 'persist_test';
```

- [ ] **Step 2: Update `run_logs.test`**

Around line 32, find:
```
SELECT constraint_name, failed, action FROM pipeline_expectation_logs() WHERE view_name = 'log_test';
```

Replace with:
```
SELECT expectation_name, failed, action FROM pipeline_expectation_logs() WHERE view_name = 'log_test';
```

---

### Task 11: Run the full test suite

- [ ] **Step 1: Run tests**

```bash
make test
```

Expected: every existing test passes. If any test fails because it referenced an old column or table name we missed, fix the test (or fix the code if it's an actual bug we introduced) and re-run.

---

### Task 12: Sweep README and run verification greps

**Files:**
- Modify: `README.md` if any old names appear

- [ ] **Step 1: Grep README**

```bash
grep -nE '__pipeline__\.views\b|__pipeline__\.constraints\b|\bconstraint_name\b|AddConstraint\b|DropConstraint\b' README.md
```

For each hit, replace per the table:

| Old | New |
|---|---|
| `__pipeline__.views` | `__pipeline__.materialized_views` |
| `__pipeline__.constraints` | `__pipeline__.expectations` |
| `constraint_name` (column or identifier) | `expectation_name` |
| `AddConstraint` / `DropConstraint` | `AddExpectation` / `DropExpectation` |

(Do NOT alter the keyword `CONSTRAINT` in SQL examples — that stays.)

- [ ] **Step 2: Verification greps over `src/` and `test/`**

```bash
grep -rn 'QualifyTable.*"views"\|QualifyTable.*"constraints"' src/
grep -rn 'AddConstraint\|DropConstraint\|drop_constraint_name' src/ test/
grep -rn '__pipeline__\.views\b\|__pipeline__\.constraints\b' src/ test/ README.md
grep -rnE '\bconstraint_name\b' src/ test/
```

Each of these must produce **no output**. If any do, fix the offending file and re-run.

(The SQL keyword `CONSTRAINT` — uppercase, used in `CONSTRAINT name EXPECT (...)` — is unaffected by the lowercase-identifier grep.)

---

### Task 13: Commit

- [ ] **Step 1: Commit all renames**

```bash
git add src/ test/ README.md
git commit -m "refactor: rename __pipeline__.views and __pipeline__.constraints

Tables: views -> materialized_views, constraints -> expectations.
Columns: constraint_name -> expectation_name in expectations and
expectation_logs tables, and in pipeline_expectations() /
pipeline_expectation_logs() output schemas. C++ symbols:
AddConstraint -> AddExpectation, DropConstraint -> DropExpectation,
ExpectationMetric.constraint_name -> expectation_name,
drop_constraint_name -> drop_expectation_name. SQL keyword
CONSTRAINT EXPECT is unchanged. Clean break — no migration."
```

---

## Self-Review

- **Spec coverage:**
  - "SQL system tables" rename → Tasks 1, 2, 3.
  - "Column renames" → Tasks 1 (DDL), 2 (SQL strings), 4 (param).
  - "Public output columns of monitoring functions" → Task 7.
  - "C++ symbols" → Tasks 4, 5, 6, 8.
  - "Files touched" — every file listed in the spec is covered by at least one task.
  - "Verification" greps → Task 12 Step 2 runs the exact greps from the spec.
  - "Implementation order" in spec — followed (persistence first, then consumers, then tests, then README, then verification).
- **Placeholders:** none.
- **Type consistency:** `AddExpectation`, `DropExpectation` signatures are consistent across header (Task 4 Step 1) and cpp (Task 4 Step 4). `expectation_name` is the consistent identifier across SQL columns, C++ params, and struct fields. `drop_expectation_name` is consistent across `pipeline_parse_data.hpp` (Task 6 Step 1) and `alter_materialized_view.cpp` (Task 5 Step 1).
- **Edge case in Task 7 Step 1**: `pipeline_expectations.cpp` may have a `FROM <constraints> c ...` clause that uses a `QualifyTable(... "constraints")` literal. That literal is already covered by Task 2 Step 2; the reminder in Task 7 Step 1 is defensive.
