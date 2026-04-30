# Pipeline Metadata Persistence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist all pipeline metadata (views, constraints, schedules, run logs, expectation logs) to `__pipeline__` system tables, with lazy initialization, write-through on every mutation, and multi-database support.

**Architecture:** A new `PipelinePersistence` class manages the `__pipeline__` schema lifecycle and all SQL writes. It is called by the existing catalog and function layer after each mutation. Monitoring functions are rewired to read from the system tables instead of the in-memory catalog. The scheduler recovers from persisted state on startup.

**Tech Stack:** C++17, DuckDB extension API, DuckDB SQL (internal connections for DDL/DML)

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `src/include/persistence/pipeline_persistence.hpp` | Create | Header for `PipelinePersistence` class |
| `src/persistence/pipeline_persistence.cpp` | Create | Schema init, all write-through SQL, database resolution |
| `src/include/catalog/materialized_view_catalog.hpp` | Modify | Add `PipelinePersistence` pointer, `database` field on definition |
| `src/catalog/materialized_view_catalog.cpp` | Modify | Call persistence after each mutation |
| `src/functions/create_materialized_view.cpp` | Modify | Pass database qualifier, trigger = `'manual'` |
| `src/functions/refresh_materialized_view.cpp` | Modify | Write run logs with trigger type |
| `src/functions/alter_materialized_view.cpp` | Modify | Persistence calls for alter operations |
| `src/functions/drop_materialized_view.cpp` | Modify | Cascade delete from all tables |
| `src/executor/materializer.cpp` | Modify | Record run logs and expectation logs |
| `src/functions/pipeline_status.cpp` | Modify | Read from `__pipeline__.views` |
| `src/functions/pipeline_expectations.cpp` | Modify | Read from `__pipeline__.expectation_logs` |
| `src/functions/pipeline_schedules.cpp` | Modify | Read from `__pipeline__.schedules`, rename check function |
| `src/functions/pipeline_run_logs.cpp` | Create | New `pipeline_run_logs()` function |
| `src/functions/pipeline_expectation_logs.cpp` | Create | New `pipeline_expectation_logs()` function |
| `src/scheduler/scheduler.cpp` | Modify | Hydrate from persisted schedules, write `last_run_at` |
| `src/hortus_pipeline_extension.cpp` | Modify | Hydrate on load, register new functions, rename |
| `CMakeLists.txt` | Modify | Add new source files |
| `test/sql/persistence.test` | Create | Persistence tests |
| `test/sql/run_logs.test` | Create | Run log tests |
| `test/sql/pipeline_fires.test` | Create | Renamed function test |

---

### Task 1: Create PipelinePersistence Class (Schema + Lazy Init)

**Files:**
- Create: `src/include/persistence/pipeline_persistence.hpp`
- Create: `src/persistence/pipeline_persistence.cpp`
- Modify: `CMakeLists.txt`
- Test: `test/sql/persistence.test`

- [ ] **Step 1: Write the failing test**

Create `test/sql/persistence.test`:

```sql
# name: test/sql/persistence.test
# group: [hortus_pipeline]

require hortus_pipeline

# Before any pipeline creation, __pipeline__ schema should NOT exist
query I
SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '__pipeline__';
----
0

# Create a pipeline view -- should trigger lazy init
statement ok
CREATE OR REFRESH MATERIALIZED VIEW persist_test AS SELECT 1 AS id, 'hello' AS name;

# Now __pipeline__ schema should exist
query I
SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '__pipeline__';
----
1

# __pipeline__.views should have our view
query II
SELECT name, query FROM __pipeline__.views WHERE name = 'persist_test';
----
persist_test	SELECT 1 AS id, 'hello' AS name

# Clean up
statement ok
DROP MATERIALIZED VIEW persist_test;

# View should be gone from __pipeline__.views
query I
SELECT COUNT(*) FROM __pipeline__.views WHERE name = 'persist_test';
----
0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test 2>&1 | grep -A2 persistence`
Expected: FAIL (no `__pipeline__` schema exists)

- [ ] **Step 3: Create the header**

Create `src/include/persistence/pipeline_persistence.hpp`:

```cpp
#pragma once
#include "duckdb.hpp"
#include "catalog/materialized_view_catalog.hpp"
#include <string>
#include <unordered_set>
#include <mutex>

namespace duckdb {

class PipelinePersistence {
public:
    explicit PipelinePersistence(DatabaseInstance &db);

    // Lazy init: create __pipeline__ schema + tables in target database
    void EnsureInitialized(const string &database = "");

    // Write-through methods
    void PersistView(const string &database, const string &name, const string &query,
                     const string &comment, const vector<Expectation> &expectations,
                     const vector<string> &depends_on);
    void PersistSchedule(const string &database, const string &name,
                         int schedule_type, int interval_value,
                         const string &interval_unit, const string &cron_expression);
    void UpdateViewQuery(const string &database, const string &name, const string &query);
    void UpdateViewMaterialized(const string &database, const string &name);
    void AddConstraint(const string &database, const string &name,
                       const string &constraint_name, const string &expression,
                       const string &action);
    void DropConstraint(const string &database, const string &name,
                        const string &constraint_name);
    void UpdateSchedulePaused(const string &database, const string &name, bool paused);
    void UpdateScheduleLastRun(const string &database, const string &name);
    void CascadeDelete(const string &database, const string &name);

    // Run logging
    int64_t InsertRunLog(const string &database, const string &view_name,
                         const string &trigger);
    void CompleteRunLog(const string &database, int64_t run_id, bool success,
                        const string &error_message, int64_t rows_affected);
    void InsertExpectationLog(const string &database, int64_t run_id,
                              const string &view_name, const string &constraint_name,
                              int64_t total_rows, int64_t passed, int64_t failed,
                              const string &action);

    // Hydration: load persisted state into in-memory catalog
    void HydrateFromDatabase(const string &database, MaterializedViewCatalog &catalog);

    // Database resolution
    static pair<string, string> ResolveQualifiedName(const string &qualified_name);

    static PipelinePersistence &Get(DatabaseInstance &db);

private:
    DatabaseInstance &db;
    std::mutex mutex;
    unordered_set<string> initialized_databases; // tracks which dbs have __pipeline__

    void CreateSchema(const string &database);
    string QualifyTable(const string &database, const string &table);
};

} // namespace duckdb
```

- [ ] **Step 4: Write the implementation**

Create `src/persistence/pipeline_persistence.cpp`:

```cpp
#include "persistence/pipeline_persistence.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

static unique_ptr<PipelinePersistence> global_persistence_instance;
static std::mutex global_persistence_mutex;

PipelinePersistence &PipelinePersistence::Get(DatabaseInstance &db) {
    lock_guard<std::mutex> lock(global_persistence_mutex);
    if (!global_persistence_instance) {
        global_persistence_instance = make_uniq<PipelinePersistence>(db);
    }
    return *global_persistence_instance;
}

PipelinePersistence::PipelinePersistence(DatabaseInstance &db) : db(db) {
}

pair<string, string> PipelinePersistence::ResolveQualifiedName(const string &qualified_name) {
    auto dot_pos = qualified_name.find('.');
    if (dot_pos != string::npos) {
        return {qualified_name.substr(0, dot_pos), qualified_name.substr(dot_pos + 1)};
    }
    return {"", qualified_name};
}

string PipelinePersistence::QualifyTable(const string &database, const string &table) {
    if (database.empty()) {
        return "__pipeline__." + table;
    }
    return database + ".__pipeline__." + table;
}

void PipelinePersistence::EnsureInitialized(const string &database) {
    lock_guard<std::mutex> lock(mutex);
    if (initialized_databases.count(database)) {
        return;
    }
    CreateSchema(database);
    initialized_databases.insert(database);
}

void PipelinePersistence::CreateSchema(const string &database) {
    Connection conn(db);

    string schema_prefix = database.empty() ? "" : database + ".";

    conn.Query("CREATE SCHEMA IF NOT EXISTS " + schema_prefix + "__pipeline__");

    conn.Query("CREATE TABLE IF NOT EXISTS " + schema_prefix + "__pipeline__.views ("
               "name VARCHAR PRIMARY KEY, "
               "query VARCHAR NOT NULL, "
               "comment VARCHAR, "
               "dependencies VARCHAR, "
               "is_materialized BOOLEAN DEFAULT false, "
               "created_at TIMESTAMP DEFAULT current_timestamp, "
               "updated_at TIMESTAMP DEFAULT current_timestamp)");

    conn.Query("CREATE TABLE IF NOT EXISTS " + schema_prefix + "__pipeline__.constraints ("
               "view_name VARCHAR NOT NULL, "
               "constraint_name VARCHAR NOT NULL, "
               "expression VARCHAR NOT NULL, "
               "action VARCHAR NOT NULL, "
               "PRIMARY KEY (view_name, constraint_name))");

    conn.Query("CREATE TABLE IF NOT EXISTS " + schema_prefix + "__pipeline__.schedules ("
               "view_name VARCHAR PRIMARY KEY, "
               "schedule_type INTEGER NOT NULL, "
               "interval_value INTEGER, "
               "interval_unit VARCHAR, "
               "cron_expression VARCHAR, "
               "paused BOOLEAN DEFAULT false, "
               "last_run_at TIMESTAMP)");

    conn.Query("CREATE SEQUENCE IF NOT EXISTS " + schema_prefix + "__pipeline__.run_seq START 1");

    conn.Query("CREATE TABLE IF NOT EXISTS " + schema_prefix + "__pipeline__.run_logs ("
               "run_id INTEGER DEFAULT nextval('" + schema_prefix + "__pipeline__.run_seq'), "
               "view_name VARCHAR NOT NULL, "
               "started_at TIMESTAMP NOT NULL, "
               "finished_at TIMESTAMP, "
               "success BOOLEAN, "
               "error_message VARCHAR, "
               "trigger VARCHAR, "
               "rows_affected BIGINT)");

    conn.Query("CREATE TABLE IF NOT EXISTS " + schema_prefix + "__pipeline__.expectation_logs ("
               "run_id INTEGER NOT NULL, "
               "view_name VARCHAR NOT NULL, "
               "constraint_name VARCHAR NOT NULL, "
               "total_rows BIGINT, "
               "passed BIGINT, "
               "failed BIGINT, "
               "action VARCHAR)");
}

void PipelinePersistence::PersistView(const string &database, const string &name,
                                       const string &query, const string &comment,
                                       const vector<Expectation> &expectations,
                                       const vector<string> &depends_on) {
    EnsureInitialized(database);
    Connection conn(db);

    // Deps to comma-separated string
    string deps_str;
    for (idx_t i = 0; i < depends_on.size(); i++) {
        if (i > 0) deps_str += ",";
        deps_str += depends_on[i];
    }

    // Upsert view
    string views_table = QualifyTable(database, "views");
    conn.Query("DELETE FROM " + views_table + " WHERE name = '" + name + "'");
    conn.Query("INSERT INTO " + views_table +
               " (name, query, comment, dependencies, is_materialized, created_at, updated_at) VALUES ('" +
               name + "', '" + StringUtil::Replace(query, "'", "''") + "', '" +
               StringUtil::Replace(comment, "'", "''") + "', '" + deps_str +
               "', false, current_timestamp, current_timestamp)");

    // Upsert constraints
    string constraints_table = QualifyTable(database, "constraints");
    conn.Query("DELETE FROM " + constraints_table + " WHERE view_name = '" + name + "'");
    for (auto &exp : expectations) {
        string action_str;
        switch (exp.action) {
        case ExpectationAction::WARN: action_str = "WARN"; break;
        case ExpectationAction::DROP_ROW: action_str = "DROP_ROW"; break;
        case ExpectationAction::FAIL_UPDATE: action_str = "FAIL_UPDATE"; break;
        }
        conn.Query("INSERT INTO " + constraints_table +
                   " (view_name, constraint_name, expression, action) VALUES ('" +
                   name + "', '" + exp.name + "', '" +
                   StringUtil::Replace(exp.expression, "'", "''") + "', '" + action_str + "')");
    }
}

void PipelinePersistence::PersistSchedule(const string &database, const string &name,
                                            int schedule_type, int interval_value,
                                            const string &interval_unit,
                                            const string &cron_expression) {
    EnsureInitialized(database);
    Connection conn(db);
    string table = QualifyTable(database, "schedules");
    conn.Query("DELETE FROM " + table + " WHERE view_name = '" + name + "'");
    conn.Query("INSERT INTO " + table +
               " (view_name, schedule_type, interval_value, interval_unit, cron_expression, paused) VALUES ('" +
               name + "', " + std::to_string(schedule_type) + ", " +
               std::to_string(interval_value) + ", '" + interval_unit + "', '" +
               StringUtil::Replace(cron_expression, "'", "''") + "', false)");
}

void PipelinePersistence::UpdateViewQuery(const string &database, const string &name,
                                            const string &query) {
    EnsureInitialized(database);
    Connection conn(db);
    string table = QualifyTable(database, "views");
    conn.Query("UPDATE " + table + " SET query = '" +
               StringUtil::Replace(query, "'", "''") +
               "', updated_at = current_timestamp WHERE name = '" + name + "'");
}

void PipelinePersistence::UpdateViewMaterialized(const string &database, const string &name) {
    EnsureInitialized(database);
    Connection conn(db);
    string table = QualifyTable(database, "views");
    conn.Query("UPDATE " + table + " SET is_materialized = true, updated_at = current_timestamp WHERE name = '" + name + "'");
}

void PipelinePersistence::AddConstraint(const string &database, const string &name,
                                          const string &constraint_name,
                                          const string &expression, const string &action) {
    EnsureInitialized(database);
    Connection conn(db);
    string table = QualifyTable(database, "constraints");
    conn.Query("INSERT INTO " + table +
               " (view_name, constraint_name, expression, action) VALUES ('" +
               name + "', '" + constraint_name + "', '" +
               StringUtil::Replace(expression, "'", "''") + "', '" + action + "')");
}

void PipelinePersistence::DropConstraint(const string &database, const string &name,
                                           const string &constraint_name) {
    EnsureInitialized(database);
    Connection conn(db);
    string table = QualifyTable(database, "constraints");
    conn.Query("DELETE FROM " + table + " WHERE view_name = '" + name +
               "' AND constraint_name = '" + constraint_name + "'");
}

void PipelinePersistence::UpdateSchedulePaused(const string &database, const string &name,
                                                 bool paused) {
    EnsureInitialized(database);
    Connection conn(db);
    string table = QualifyTable(database, "schedules");
    conn.Query("UPDATE " + table + " SET paused = " + (paused ? "true" : "false") +
               " WHERE view_name = '" + name + "'");
}

void PipelinePersistence::UpdateScheduleLastRun(const string &database, const string &name) {
    EnsureInitialized(database);
    Connection conn(db);
    string table = QualifyTable(database, "schedules");
    conn.Query("UPDATE " + table + " SET last_run_at = current_timestamp WHERE view_name = '" + name + "'");
}

void PipelinePersistence::CascadeDelete(const string &database, const string &name) {
    EnsureInitialized(database);
    Connection conn(db);
    conn.Query("DELETE FROM " + QualifyTable(database, "expectation_logs") +
               " WHERE view_name = '" + name + "'");
    conn.Query("DELETE FROM " + QualifyTable(database, "run_logs") +
               " WHERE view_name = '" + name + "'");
    conn.Query("DELETE FROM " + QualifyTable(database, "constraints") +
               " WHERE view_name = '" + name + "'");
    conn.Query("DELETE FROM " + QualifyTable(database, "schedules") +
               " WHERE view_name = '" + name + "'");
    conn.Query("DELETE FROM " + QualifyTable(database, "views") +
               " WHERE name = '" + name + "'");
}

int64_t PipelinePersistence::InsertRunLog(const string &database, const string &view_name,
                                            const string &trigger) {
    EnsureInitialized(database);
    Connection conn(db);
    string table = QualifyTable(database, "run_logs");
    auto result = conn.Query("INSERT INTO " + table +
                             " (view_name, started_at, trigger) VALUES ('" +
                             view_name + "', current_timestamp, '" + trigger +
                             "') RETURNING run_id");
    if (result->HasError()) {
        return -1;
    }
    return result->GetValue(0, 0).GetValue<int64_t>();
}

void PipelinePersistence::CompleteRunLog(const string &database, int64_t run_id, bool success,
                                          const string &error_message, int64_t rows_affected) {
    EnsureInitialized(database);
    Connection conn(db);
    string table = QualifyTable(database, "run_logs");
    conn.Query("UPDATE " + table + " SET finished_at = current_timestamp, success = " +
               (success ? "true" : "false") + ", error_message = " +
               (error_message.empty() ? "NULL" : "'" + StringUtil::Replace(error_message, "'", "''") + "'") +
               ", rows_affected = " + std::to_string(rows_affected) +
               " WHERE run_id = " + std::to_string(run_id));
}

void PipelinePersistence::InsertExpectationLog(const string &database, int64_t run_id,
                                                 const string &view_name,
                                                 const string &constraint_name,
                                                 int64_t total_rows, int64_t passed,
                                                 int64_t failed, const string &action) {
    EnsureInitialized(database);
    Connection conn(db);
    string table = QualifyTable(database, "expectation_logs");
    conn.Query("INSERT INTO " + table +
               " (run_id, view_name, constraint_name, total_rows, passed, failed, action) VALUES (" +
               std::to_string(run_id) + ", '" + view_name + "', '" + constraint_name + "', " +
               std::to_string(total_rows) + ", " + std::to_string(passed) + ", " +
               std::to_string(failed) + ", '" + action + "')");
}

void PipelinePersistence::HydrateFromDatabase(const string &database,
                                                MaterializedViewCatalog &catalog) {
    Connection conn(db);
    string schema_prefix = database.empty() ? "" : database + ".";

    // Check if __pipeline__ schema exists
    auto schema_result = conn.Query(
        "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '__pipeline__'" +
        (database.empty() ? string("") : " AND catalog_name = '" + database + "'"));
    if (schema_result->HasError() || schema_result->GetValue(0, 0).GetValue<int64_t>() == 0) {
        return;
    }

    // Mark as initialized
    {
        lock_guard<std::mutex> lock(mutex);
        initialized_databases.insert(database);
    }

    string views_table = QualifyTable(database, "views");
    string constraints_table = QualifyTable(database, "constraints");
    string schedules_table = QualifyTable(database, "schedules");

    // Load views
    auto views_result = conn.Query("SELECT name, query, comment, dependencies, is_materialized FROM " + views_table);
    if (views_result->HasError()) {
        return;
    }

    for (idx_t i = 0; i < views_result->RowCount(); i++) {
        string name = views_result->GetValue(0, i).GetValue<string>();
        string query = views_result->GetValue(1, i).GetValue<string>();
        string comment = views_result->GetValue(2, i).IsNull() ? "" : views_result->GetValue(2, i).GetValue<string>();
        string deps_str = views_result->GetValue(3, i).IsNull() ? "" : views_result->GetValue(3, i).GetValue<string>();
        bool is_materialized = views_result->GetValue(4, i).GetValue<bool>();

        vector<string> deps;
        if (!deps_str.empty()) {
            deps = StringUtil::Split(deps_str, ",");
        }

        // Load constraints for this view
        vector<Expectation> expectations;
        auto constraints_result = conn.Query(
            "SELECT constraint_name, expression, action FROM " + constraints_table +
            " WHERE view_name = '" + name + "'");
        if (!constraints_result->HasError()) {
            for (idx_t j = 0; j < constraints_result->RowCount(); j++) {
                Expectation exp;
                exp.name = constraints_result->GetValue(0, j).GetValue<string>();
                exp.expression = constraints_result->GetValue(1, j).GetValue<string>();
                string action_str = constraints_result->GetValue(2, j).GetValue<string>();
                if (action_str == "DROP_ROW") {
                    exp.action = ExpectationAction::DROP_ROW;
                } else if (action_str == "FAIL_UPDATE") {
                    exp.action = ExpectationAction::FAIL_UPDATE;
                } else {
                    exp.action = ExpectationAction::WARN;
                }
                expectations.push_back(std::move(exp));
            }
        }

        // Qualify name with database if not default
        string qualified_name = database.empty() ? name : database + "." + name;
        catalog.CreateOrRefresh(qualified_name, query, comment, expectations, deps);
        if (is_materialized) {
            catalog.MarkMaterialized(qualified_name);
        }

        // Load schedule for this view
        auto schedule_result = conn.Query(
            "SELECT schedule_type, interval_value, interval_unit, cron_expression, paused, last_run_at FROM " +
            schedules_table + " WHERE view_name = '" + name + "'");
        if (!schedule_result->HasError() && schedule_result->RowCount() > 0) {
            auto &def = const_cast<MaterializedViewDefinition &>(catalog.Get(qualified_name));
            def.schedule_type = schedule_result->GetValue(0, 0).GetValue<int>();
            def.schedule_interval = schedule_result->GetValue(1, 0).IsNull() ? 0 : schedule_result->GetValue(1, 0).GetValue<int>();
            def.schedule_interval_unit = schedule_result->GetValue(2, 0).IsNull() ? "" : schedule_result->GetValue(2, 0).GetValue<string>();
            def.schedule_cron_expression = schedule_result->GetValue(3, 0).IsNull() ? "" : schedule_result->GetValue(3, 0).GetValue<string>();
            def.schedule_paused = schedule_result->GetValue(4, 0).GetValue<bool>();
        }
    }
}

} // namespace duckdb
```

- [ ] **Step 5: Add to CMakeLists.txt**

Add `src/persistence/pipeline_persistence.cpp` to `EXTENSION_SOURCES` in `CMakeLists.txt`:

```cmake
set(EXTENSION_SOURCES
    src/hortus_pipeline_extension.cpp
    src/catalog/materialized_view_catalog.cpp
    src/persistence/pipeline_persistence.cpp
    src/parser/pipeline_parser.cpp
    ...
```

- [ ] **Step 6: Build to verify compilation**

Run: `make release 2>&1 | tail -5`
Expected: Build succeeds

- [ ] **Step 7: Commit**

```bash
git add src/include/persistence/pipeline_persistence.hpp src/persistence/pipeline_persistence.cpp CMakeLists.txt test/sql/persistence.test
git commit -m "feat: add PipelinePersistence class with lazy schema init and write-through methods"
```

---

### Task 2: Wire Persistence into CREATE OR REFRESH

**Files:**
- Modify: `src/functions/create_materialized_view.cpp`

- [ ] **Step 1: Add persistence calls after catalog write**

In `src/functions/create_materialized_view.cpp`, add the persistence include and calls. After the existing catalog and schedule code (around line 118), add persistence writes:

```cpp
// Add include at top
#include "persistence/pipeline_persistence.hpp"
```

In `CreateMVFunc`, after `catalog.CreateOrRefresh(...)` and schedule setup, before `Materializer::Materialize(...)`:

```cpp
    // Persist to __pipeline__ tables
    auto [database, unqualified_name] = PipelinePersistence::ResolveQualifiedName(bind_data.view_name);
    auto &persistence = PipelinePersistence::Get(db);
    persistence.PersistView(database, unqualified_name, bind_data.query,
                            bind_data.comment, expectations, deps);

    // Persist schedule if present
    if (!bind_data.serialized_schedule.empty()) {
        auto &def = catalog.Get(bind_data.view_name);
        persistence.PersistSchedule(database, unqualified_name,
                                     def.schedule_type, def.schedule_interval,
                                     def.schedule_interval_unit, def.schedule_cron_expression);
    }
```

- [ ] **Step 2: Build and run tests**

Run: `make release && make test 2>&1 | tail -5`
Expected: All existing tests pass + persistence test passes

- [ ] **Step 3: Commit**

```bash
git add src/functions/create_materialized_view.cpp
git commit -m "feat: wire persistence into CREATE OR REFRESH MATERIALIZED VIEW"
```

---

### Task 3: Wire Persistence into Materializer (Run Logs)

**Files:**
- Modify: `src/executor/materializer.cpp`
- Modify: `src/include/executor/materializer.hpp`

- [ ] **Step 1: Write the failing test**

Add to `test/sql/run_logs.test`:

```sql
# name: test/sql/run_logs.test
# group: [hortus_pipeline]

require hortus_pipeline

statement ok
CREATE OR REFRESH MATERIALIZED VIEW log_test AS SELECT 1 AS id;

# Should have one run log entry from the CREATE
query I
SELECT COUNT(*) FROM __pipeline__.run_logs WHERE view_name = 'log_test';
----
1

# Check the run log has correct values
query III
SELECT view_name, success, trigger FROM __pipeline__.run_logs WHERE view_name = 'log_test';
----
log_test	true	manual

# Refresh should add another run log
statement ok
REFRESH MATERIALIZED VIEW log_test;

query I
SELECT COUNT(*) FROM __pipeline__.run_logs WHERE view_name = 'log_test';
----
2

# Clean up
statement ok
DROP MATERIALIZED VIEW log_test;

# Run logs should be cascade-deleted
query I
SELECT COUNT(*) FROM __pipeline__.run_logs WHERE view_name = 'log_test';
----
0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make test 2>&1 | grep -A2 run_logs`
Expected: FAIL

- [ ] **Step 3: Add trigger parameter to Materialize**

In `src/include/executor/materializer.hpp`, update signatures:

```cpp
class Materializer {
public:
    static void Materialize(ClientContext &context, MaterializedViewCatalog &catalog,
                            const string &view_name, const string &trigger = "manual");
    static void MaterializeAll(ClientContext &context, MaterializedViewCatalog &catalog,
                               bool best_effort = false, const string &trigger = "refresh_all");
};
```

- [ ] **Step 4: Update Materializer to write run logs**

In `src/executor/materializer.cpp`, add persistence include and run log writes:

```cpp
#include "persistence/pipeline_persistence.hpp"
```

Update `Materializer::Materialize`:

```cpp
void Materializer::Materialize(ClientContext &context, MaterializedViewCatalog &catalog,
                                const string &view_name, const string &trigger) {
    auto &def = catalog.Get(view_name);
    auto &db = DatabaseInstance::GetDatabase(context);
    auto &persistence = PipelinePersistence::Get(db);
    auto [database, unqualified_name] = PipelinePersistence::ResolveQualifiedName(view_name);

    // Start run log
    int64_t run_id = persistence.InsertRunLog(database, unqualified_name, trigger);

    try {
        // Apply expectations (may throw for FAIL_UPDATE)
        vector<ExpectationMetric> metrics;
        string final_query = ExpectationChecker::ApplyExpectations(context, def.query, def.expectations, metrics);

        // Execute CREATE OR REPLACE TABLE
        Connection conn(db);
        string create_sql = "CREATE OR REPLACE TABLE " + view_name + " AS (" + final_query + ")";
        auto result = conn.Query(create_sql);
        if (result->HasError()) {
            throw InvalidInputException("Failed to materialize view '%s': %s", view_name, result->GetError());
        }

        // Count rows
        auto count_result = conn.Query("SELECT COUNT(*) FROM " + view_name);
        int64_t row_count = 0;
        if (!count_result->HasError()) {
            row_count = count_result->GetValue(0, 0).GetValue<int64_t>();
        }

        // Store metrics in catalog (kept for in-session fast access)
        if (!metrics.empty()) {
            catalog.SetExpectationMetrics(view_name, metrics);
        }

        // Write expectation logs
        for (auto &m : metrics) {
            persistence.InsertExpectationLog(database, run_id, unqualified_name,
                                              m.constraint_name, m.total_rows,
                                              m.passed, m.failed, m.action);
        }

        // Complete run log
        persistence.CompleteRunLog(database, run_id, true, "", row_count);
        persistence.UpdateViewMaterialized(database, unqualified_name);
        catalog.MarkMaterialized(view_name);

    } catch (std::exception &e) {
        persistence.CompleteRunLog(database, run_id, false, e.what(), 0);
        throw; // Re-throw so caller sees the error
    }
}
```

Update `Materializer::MaterializeAll` to pass trigger:

```cpp
void Materializer::MaterializeAll(ClientContext &context, MaterializedViewCatalog &catalog,
                                   bool best_effort, const string &trigger) {
    auto order = DAGResolver::Resolve(catalog);

    if (!best_effort) {
        for (auto &name : order) {
            Materialize(context, catalog, name, trigger);
        }
        return;
    }

    // Best-effort mode: skip dependents of failed nodes
    unordered_set<string> failed_set;
    vector<string> errors;

    auto all_names = catalog.GetAllNames();
    unordered_set<string> mv_set(all_names.begin(), all_names.end());
    unordered_map<string, vector<string>> dep_map;
    for (auto &name : all_names) {
        auto &def = catalog.Get(name);
        vector<string> deps;
        if (!def.explicit_dependencies.empty()) {
            deps = def.explicit_dependencies;
        } else {
            deps = DAGResolver::ExtractDependencies(def.query);
        }
        vector<string> mv_deps;
        for (auto &d : deps) {
            if (mv_set.count(d) > 0 && d != name) {
                mv_deps.push_back(d);
            }
        }
        dep_map[name] = mv_deps;
    }

    for (auto &name : order) {
        bool skip = false;
        for (auto &dep : dep_map[name]) {
            if (failed_set.count(dep) > 0) {
                skip = true;
                break;
            }
        }
        if (skip) {
            failed_set.insert(name);
            errors.push_back("Skipped '" + name + "' due to failed dependency");
            continue;
        }

        try {
            Materialize(context, catalog, name, trigger);
        } catch (std::exception &e) {
            failed_set.insert(name);
            errors.push_back("Failed to materialize '" + name + "': " + string(e.what()));
        }
    }

    if (!errors.empty()) {
        string msg = "Best-effort refresh completed with errors:";
        for (auto &err : errors) {
            msg += "\n  - " + err;
        }
        Printer::Print(msg);
    }
}
```

- [ ] **Step 5: Build and run tests**

Run: `make release && make test 2>&1 | tail -5`
Expected: All tests pass including `run_logs.test`

- [ ] **Step 6: Commit**

```bash
git add src/executor/materializer.cpp src/include/executor/materializer.hpp test/sql/run_logs.test
git commit -m "feat: write run logs and expectation logs on materialize"
```

---

### Task 4: Wire Persistence into ALTER and DROP

**Files:**
- Modify: `src/functions/alter_materialized_view.cpp`
- Modify: `src/functions/drop_materialized_view.cpp`

- [ ] **Step 1: Update ALTER to persist changes**

In `src/functions/alter_materialized_view.cpp`, add persistence include:

```cpp
#include "persistence/pipeline_persistence.hpp"
```

In `AlterMVFunc`, add persistence calls after each catalog mutation:

```cpp
    auto [database, unqualified_name] = PipelinePersistence::ResolveQualifiedName(bind_data.view_name);
    auto &persistence = PipelinePersistence::Get(db);

    if (bind_data.alter_action == "SET_QUERY") {
        catalog.AlterQuery(bind_data.view_name, bind_data.new_query);
        persistence.UpdateViewQuery(database, unqualified_name, bind_data.new_query);
        status = ...;
    } else if (bind_data.alter_action == "ADD_CONSTRAINT") {
        // ... existing catalog call ...
        string action_str;
        if (bind_data.action_string == "DROP_ROW") action_str = "DROP_ROW";
        else if (bind_data.action_string == "FAIL_UPDATE") action_str = "FAIL_UPDATE";
        else action_str = "WARN";
        persistence.AddConstraint(database, unqualified_name,
                                   bind_data.constraint_name, bind_data.expression, action_str);
        status = ...;
    } else if (bind_data.alter_action == "DROP_CONSTRAINT") {
        catalog.DropConstraint(bind_data.view_name, bind_data.drop_constraint_name);
        persistence.DropConstraint(database, unqualified_name, bind_data.drop_constraint_name);
        status = ...;
    } else if (bind_data.alter_action == "PAUSE_SCHEDULE") {
        catalog.PauseSchedule(bind_data.view_name);
        PipelineScheduler::Get(db).PauseSchedule(bind_data.view_name);
        persistence.UpdateSchedulePaused(database, unqualified_name, true);
        status = ...;
    } else if (bind_data.alter_action == "RESUME_SCHEDULE") {
        catalog.ResumeSchedule(bind_data.view_name);
        PipelineScheduler::Get(db).ResumeSchedule(bind_data.view_name);
        persistence.UpdateSchedulePaused(database, unqualified_name, false);
        status = ...;
    }
```

- [ ] **Step 2: Update DROP to cascade-delete**

In `src/functions/drop_materialized_view.cpp`, add persistence include:

```cpp
#include "persistence/pipeline_persistence.hpp"
```

In `DropMVFunc`, add cascade delete before existing catalog drop:

```cpp
    auto [database, unqualified_name] = PipelinePersistence::ResolveQualifiedName(bind_data.view_name);
    auto &persistence = PipelinePersistence::Get(db);

    // Remove from scheduler if scheduled
    PipelineScheduler::Get(db).RemoveSchedule(bind_data.view_name);

    // Cascade delete from __pipeline__ tables
    persistence.CascadeDelete(database, unqualified_name);

    // Remove from our catalog (throws if not found)
    catalog.Drop(bind_data.view_name);

    // Drop the underlying materialized table
    Connection conn(db);
    auto result = conn.Query("DROP TABLE IF EXISTS " + bind_data.view_name);
    ...
```

- [ ] **Step 3: Build and run tests**

Run: `make release && make test 2>&1 | tail -5`
Expected: All tests pass (persistence.test verifies cascade delete works)

- [ ] **Step 4: Commit**

```bash
git add src/functions/alter_materialized_view.cpp src/functions/drop_materialized_view.cpp
git commit -m "feat: wire persistence into ALTER and DROP with cascade delete"
```

---

### Task 5: Rewire Monitoring Functions to Read from System Tables

**Files:**
- Modify: `src/functions/pipeline_status.cpp`
- Modify: `src/functions/pipeline_expectations.cpp`
- Modify: `src/functions/pipeline_schedules.cpp`

- [ ] **Step 1: Rewrite pipeline_status() to read from __pipeline__.views**

Replace the init and func in `src/functions/pipeline_status.cpp`:

```cpp
#include "persistence/pipeline_persistence.hpp"
#include "duckdb/main/connection.hpp"

// In PipelineStatusGlobalState, replace names with query result:
struct PipelineStatusGlobalState : public GlobalTableFunctionState {
    idx_t offset = 0;
    unique_ptr<MaterializedQueryResult> result;
};

// In PipelineStatusInit:
static unique_ptr<GlobalTableFunctionState> PipelineStatusInit(ClientContext &context, TableFunctionInitInput &input) {
    auto state = make_uniq<PipelineStatusGlobalState>();
    auto &db = DatabaseInstance::GetDatabase(context);
    auto &persistence = PipelinePersistence::Get(db);
    persistence.EnsureInitialized("");
    Connection conn(db);
    state->result = conn.Query("SELECT name, query, dependencies, is_materialized, comment FROM __pipeline__.views");
    return std::move(state);
}

// In PipelineStatusFunc:
static void PipelineStatusFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<PipelineStatusGlobalState>();
    if (!state.result || state.result->HasError() || state.offset >= state.result->RowCount()) {
        return;
    }
    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;
    while (state.offset < state.result->RowCount() && count < max_count) {
        output.SetValue(0, count, state.result->GetValue(0, state.offset));
        output.SetValue(1, count, state.result->GetValue(1, state.offset));
        output.SetValue(2, count, state.result->GetValue(2, state.offset));
        output.SetValue(3, count, state.result->GetValue(3, state.offset));
        output.SetValue(4, count, state.result->GetValue(4, state.offset));
        state.offset++;
        count++;
    }
    output.SetCardinality(count);
}
```

- [ ] **Step 2: Rewrite pipeline_expectations() to read from __pipeline__.expectation_logs**

Replace in `src/functions/pipeline_expectations.cpp`:

```cpp
#include "persistence/pipeline_persistence.hpp"
#include "duckdb/main/connection.hpp"

struct PipelineExpectationsGlobalState : public GlobalTableFunctionState {
    idx_t offset = 0;
    unique_ptr<MaterializedQueryResult> result;
};

// In PipelineExpectationsInit:
static unique_ptr<GlobalTableFunctionState> PipelineExpectationsInit(ClientContext &context, TableFunctionInitInput &input) {
    auto state = make_uniq<PipelineExpectationsGlobalState>();
    auto &db = DatabaseInstance::GetDatabase(context);
    auto &persistence = PipelinePersistence::Get(db);
    persistence.EnsureInitialized("");
    Connection conn(db);
    // Get latest run per view, then join to get expectation logs
    state->result = conn.Query(
        "SELECT e.view_name, e.constraint_name, e.total_rows, e.passed, e.failed, e.action "
        "FROM __pipeline__.expectation_logs e "
        "INNER JOIN (SELECT view_name, MAX(run_id) AS max_run_id "
        "            FROM __pipeline__.run_logs GROUP BY view_name) r "
        "ON e.view_name = r.view_name AND e.run_id = r.max_run_id");
    return std::move(state);
}

// PipelineExpectationsFunc follows same pattern as PipelineStatusFunc
```

- [ ] **Step 3: Rewrite pipeline_schedules() to read from __pipeline__.schedules**

Replace in `src/functions/pipeline_schedules.cpp` the `PipelineSchedulesInit`:

```cpp
#include "persistence/pipeline_persistence.hpp"

// In PipelineSchedulesInit:
static unique_ptr<GlobalTableFunctionState> PipelineSchedulesInit(ClientContext &context, TableFunctionInitInput &input) {
    auto state = make_uniq<PipelineSchedulesGlobalState>();
    auto &db = DatabaseInstance::GetDatabase(context);
    auto &persistence = PipelinePersistence::Get(db);
    persistence.EnsureInitialized("");
    Connection conn(db);
    auto result = conn.Query(
        "SELECT view_name, schedule_type, interval_value, interval_unit, cron_expression, paused "
        "FROM __pipeline__.schedules");
    if (!result->HasError()) {
        for (idx_t i = 0; i < result->RowCount(); i++) {
            PipelineScheduler::ScheduleInfo info;
            info.name = result->GetValue(0, i).GetValue<string>();
            int stype = result->GetValue(1, i).GetValue<int>();
            switch (stype) {
            case 1:
                info.schedule_description = "EVERY " +
                    result->GetValue(2, i).ToString() + " " +
                    result->GetValue(3, i).GetValue<string>();
                break;
            case 2:
                info.schedule_description = "CRON " + result->GetValue(4, i).GetValue<string>();
                break;
            case 3:
                info.schedule_description = "TRIGGER ON UPDATE";
                break;
            }
            info.paused = result->GetValue(5, i).GetValue<bool>();
            state->schedules.push_back(std::move(info));
        }
    }
    return std::move(state);
}
```

- [ ] **Step 4: Build and run tests**

Run: `make release && make test 2>&1 | tail -5`
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add src/functions/pipeline_status.cpp src/functions/pipeline_expectations.cpp src/functions/pipeline_schedules.cpp
git commit -m "feat: rewire monitoring functions to read from __pipeline__ system tables"
```

---

### Task 6: Add pipeline_run_logs() and pipeline_expectation_logs() Functions

**Files:**
- Create: `src/functions/pipeline_run_logs.cpp`
- Create: `src/functions/pipeline_expectation_logs.cpp`
- Modify: `src/hortus_pipeline_extension.cpp`
- Modify: `CMakeLists.txt`

- [ ] **Step 1: Create pipeline_run_logs.cpp**

```cpp
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "persistence/pipeline_persistence.hpp"

namespace duckdb {

struct RunLogsBindData : public TableFunctionData {};

struct RunLogsGlobalState : public GlobalTableFunctionState {
    idx_t offset = 0;
    unique_ptr<MaterializedQueryResult> result;
};

static unique_ptr<FunctionData> RunLogsBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
    auto data = make_uniq<RunLogsBindData>();
    names.emplace_back("run_id"); return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("view_name"); return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("started_at"); return_types.emplace_back(LogicalType::TIMESTAMP);
    names.emplace_back("finished_at"); return_types.emplace_back(LogicalType::TIMESTAMP);
    names.emplace_back("success"); return_types.emplace_back(LogicalType::BOOLEAN);
    names.emplace_back("error_message"); return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("trigger"); return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("rows_affected"); return_types.emplace_back(LogicalType::BIGINT);
    return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> RunLogsInit(ClientContext &context, TableFunctionInitInput &input) {
    auto state = make_uniq<RunLogsGlobalState>();
    auto &db = DatabaseInstance::GetDatabase(context);
    auto &persistence = PipelinePersistence::Get(db);
    persistence.EnsureInitialized("");
    Connection conn(db);
    state->result = conn.Query(
        "SELECT run_id, view_name, started_at, finished_at, success, error_message, trigger, rows_affected "
        "FROM __pipeline__.run_logs ORDER BY run_id");
    return std::move(state);
}

static void RunLogsFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<RunLogsGlobalState>();
    if (!state.result || state.result->HasError() || state.offset >= state.result->RowCount()) {
        return;
    }
    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;
    while (state.offset < state.result->RowCount() && count < max_count) {
        for (idx_t col = 0; col < 8; col++) {
            output.SetValue(col, count, state.result->GetValue(col, state.offset));
        }
        state.offset++;
        count++;
    }
    output.SetCardinality(count);
}

TableFunction GetPipelineRunLogsFunction() {
    TableFunction func("pipeline_run_logs", {}, RunLogsFunc, RunLogsBind, RunLogsInit);
    return func;
}

} // namespace duckdb
```

- [ ] **Step 2: Create pipeline_expectation_logs.cpp**

```cpp
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "persistence/pipeline_persistence.hpp"

namespace duckdb {

struct ExpLogsBindData : public TableFunctionData {};

struct ExpLogsGlobalState : public GlobalTableFunctionState {
    idx_t offset = 0;
    unique_ptr<MaterializedQueryResult> result;
};

static unique_ptr<FunctionData> ExpLogsBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
    auto data = make_uniq<ExpLogsBindData>();
    names.emplace_back("run_id"); return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("view_name"); return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("constraint_name"); return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("total_rows"); return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("passed"); return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("failed"); return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("action"); return_types.emplace_back(LogicalType::VARCHAR);
    return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> ExpLogsInit(ClientContext &context, TableFunctionInitInput &input) {
    auto state = make_uniq<ExpLogsGlobalState>();
    auto &db = DatabaseInstance::GetDatabase(context);
    auto &persistence = PipelinePersistence::Get(db);
    persistence.EnsureInitialized("");
    Connection conn(db);
    state->result = conn.Query(
        "SELECT run_id, view_name, constraint_name, total_rows, passed, failed, action "
        "FROM __pipeline__.expectation_logs ORDER BY run_id");
    return std::move(state);
}

static void ExpLogsFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<ExpLogsGlobalState>();
    if (!state.result || state.result->HasError() || state.offset >= state.result->RowCount()) {
        return;
    }
    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;
    while (state.offset < state.result->RowCount() && count < max_count) {
        for (idx_t col = 0; col < 7; col++) {
            output.SetValue(col, count, state.result->GetValue(col, state.offset));
        }
        state.offset++;
        count++;
    }
    output.SetCardinality(count);
}

TableFunction GetPipelineExpectationLogsFunction() {
    TableFunction func("pipeline_expectation_logs", {}, ExpLogsFunc, ExpLogsBind, ExpLogsInit);
    return func;
}

} // namespace duckdb
```

- [ ] **Step 3: Register new functions and rename check_schedules to fires**

In `src/hortus_pipeline_extension.cpp`, add forward declarations and register:

```cpp
// Add forward declarations
TableFunction GetPipelineRunLogsFunction();
TableFunction GetPipelineExpectationLogsFunction();
TableFunction GetPipelineFiresFunction(); // renamed from GetPipelineCheckSchedulesFunction
```

In `LoadInternal`, replace `GetPipelineCheckSchedulesFunction()` registration:

```cpp
    loader.RegisterFunction(GetPipelineFiresFunction());
    loader.RegisterFunction(GetPipelineRunLogsFunction());
    loader.RegisterFunction(GetPipelineExpectationLogsFunction());
```

- [ ] **Step 4: Rename pipeline_check_schedules to pipeline_fires**

In `src/functions/pipeline_schedules.cpp`, change the function name at the bottom:

```cpp
TableFunction GetPipelineFiresFunction() {
    TableFunction func("pipeline_fires", {},
                       CheckSchedulesFunc, CheckSchedulesBind, CheckSchedulesInit);
    return func;
}
```

And update the forward declaration name.

- [ ] **Step 5: Update parser to recognize pipeline_fires**

In `src/parser/pipeline_parser.cpp`, find where `pipeline_check_schedules` is referenced and replace with `pipeline_fires`.

- [ ] **Step 6: Add new files to CMakeLists.txt**

```cmake
    src/functions/pipeline_run_logs.cpp
    src/functions/pipeline_expectation_logs.cpp
```

- [ ] **Step 7: Build and run tests**

Run: `make release && make test 2>&1 | tail -5`
Expected: All tests pass (existing `pipeline_check_schedules` test will need updating)

- [ ] **Step 8: Update test for renamed function**

In `test/sql/schedule_syntax.test`, replace `pipeline_check_schedules` with `pipeline_fires`.

- [ ] **Step 9: Build and run tests again**

Run: `make release && make test 2>&1 | tail -5`
Expected: All tests pass

- [ ] **Step 10: Commit**

```bash
git add src/functions/pipeline_run_logs.cpp src/functions/pipeline_expectation_logs.cpp \
        src/functions/pipeline_schedules.cpp src/hortus_pipeline_extension.cpp \
        src/parser/pipeline_parser.cpp CMakeLists.txt test/sql/schedule_syntax.test
git commit -m "feat: add pipeline_run_logs(), pipeline_expectation_logs(), rename to pipeline_fires()"
```

---

### Task 7: Hydrate from Persisted State on Extension Load

**Files:**
- Modify: `src/hortus_pipeline_extension.cpp`
- Modify: `src/scheduler/scheduler.cpp`

- [ ] **Step 1: Add hydration call in LoadInternal**

In `src/hortus_pipeline_extension.cpp`, add persistence include and hydration before scheduler start:

```cpp
#include "persistence/pipeline_persistence.hpp"
#include "catalog/materialized_view_catalog.hpp"
```

In `LoadInternal`, after registering functions, before scheduler start:

```cpp
    // Hydrate from persisted state (default database)
    auto &persistence = PipelinePersistence::Get(db);
    auto &catalog = MaterializedViewCatalog::Get(db);
    persistence.HydrateFromDatabase("", catalog);

    // Start the background scheduler (will pick up hydrated schedules)
    auto &scheduler = PipelineScheduler::Get(db);

    // Register hydrated schedules with scheduler (compute next run from now)
    auto names = catalog.GetAllNames();
    for (auto &name : names) {
        auto &def = catalog.Get(name);
        if (def.schedule_type != 0 && !def.schedule_paused) {
            scheduler.AddSchedule(name);
        }
    }
```

- [ ] **Step 2: Update scheduler to write last_run_at on fire**

In `src/scheduler/scheduler.cpp`, in `RunScheduler()` after successful refresh, add:

```cpp
    // After conn.Query("REFRESH MATERIALIZED VIEW " + top.view_name):
    try {
        auto &persistence = PipelinePersistence::Get(db);
        auto [database, unqualified] = PipelinePersistence::ResolveQualifiedName(top.view_name);
        persistence.UpdateScheduleLastRun(database, unqualified);
    } catch (...) {
        // Don't crash scheduler on persistence failure
    }
```

- [ ] **Step 3: Build and run tests**

Run: `make release && make test 2>&1 | tail -5`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add src/hortus_pipeline_extension.cpp src/scheduler/scheduler.cpp
git commit -m "feat: hydrate catalog from persisted state on extension load"
```

---

### Task 8: Add pipeline_fires Test and Integration Test

**Files:**
- Create: `test/sql/pipeline_fires.test`
- Modify: `test/sql/integration.test`

- [ ] **Step 1: Create pipeline_fires test**

Create `test/sql/pipeline_fires.test`:

```sql
# name: test/sql/pipeline_fires.test
# group: [hortus_pipeline]

require hortus_pipeline

statement ok
CREATE TABLE fire_data (id INTEGER, val INTEGER);

statement ok
INSERT INTO fire_data VALUES (1, 10), (2, 20);

statement ok
CREATE OR REFRESH MATERIALIZED VIEW fire_test SCHEDULE EVERY 1 HOUR AS SELECT COUNT(*) AS cnt FROM fire_data;

query I
SELECT cnt FROM fire_test;
----
2

statement ok
INSERT INTO fire_data VALUES (3, 30);

# Force fire
query II
SELECT name, status FROM pipeline_fires() WHERE name = 'fire_test';
----
fire_test	refreshed

# Should reflect new data
query I
SELECT cnt FROM fire_test;
----
3

# Run logs should show the fire
query I
SELECT COUNT(*) FROM pipeline_run_logs() WHERE view_name = 'fire_test' AND trigger = 'manual';
----
2

statement ok
DROP MATERIALIZED VIEW fire_test;

statement ok
DROP TABLE fire_data;
```

- [ ] **Step 2: Update integration test to verify persistence tables**

Add to the end of `test/sql/integration.test` (before cleanup):

```sql
# Verify run_logs captured all refreshes
query I
SELECT COUNT(*) FROM pipeline_run_logs() WHERE success = true;
----
(expected count based on number of refreshes in the test)
```

- [ ] **Step 3: Build and run all tests**

Run: `make release && make test 2>&1 | tail -5`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add test/sql/pipeline_fires.test test/sql/integration.test
git commit -m "test: add pipeline_fires and persistence integration tests"
```

---

### Task 9: Update README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update monitoring functions section**

Update the README to:
- Rename `pipeline_check_schedules()` to `pipeline_fires()` everywhere
- Add `pipeline_run_logs()` and `pipeline_expectation_logs()` to the monitoring section
- Add a "Persistence" section explaining `__pipeline__` schema, lazy init, multi-database support
- Update the Table of Contents

Key additions:

```markdown
## Persistence

Pipeline metadata is automatically persisted to a `__pipeline__` schema in the database where the materialized view is created. This happens lazily on first pipeline creation.

- **File-based / S3 / Iceberg:** Metadata persists across restarts. Schedules auto-resume.
- **`:memory:` mode:** Metadata exists during the session but is lost on exit.
- **Multi-database:** Each attached database gets its own `__pipeline__` schema, determined by the view name qualifier.

```sql
-- Default database
CREATE OR REFRESH MATERIALIZED VIEW my_view AS SELECT ...;

-- Iceberg catalog
CREATE OR REFRESH MATERIALIZED VIEW iceberg_catalog.my_view AS SELECT ...;
```

The `__pipeline__` schema contains 5 system tables:

| Table | Purpose |
|-------|---------|
| `__pipeline__.views` | View definitions |
| `__pipeline__.constraints` | Expectation definitions |
| `__pipeline__.schedules` | Schedule configurations |
| `__pipeline__.run_logs` | Run history (append-only) |
| `__pipeline__.expectation_logs` | Per-run constraint results |

All data is cascade-deleted when a view is dropped.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: update README with persistence, pipeline_fires, run_logs"
```
