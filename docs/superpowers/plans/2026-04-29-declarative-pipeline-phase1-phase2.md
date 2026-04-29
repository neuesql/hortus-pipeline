# Hortus Pipeline — Phase 1 & Phase 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a DuckDB C++ extension that adds declarative pipeline semantics — `CREATE OR REFRESH MATERIALIZED VIEW`, DAG resolution, expectations, refresh commands, and a scheduler engine.

**Architecture:** The extension uses DuckDB's `ParserExtension` API (`parser_override`) to intercept custom SQL before DuckDB's parser. Custom statements are parsed into `ExtensionStatement` objects carrying our parse data, then planned via `plan_function` into `TableFunction` calls that manipulate an internal catalog of materialized view definitions, resolve dependencies via topological sort, and materialize results as real DuckDB tables. Phase 2 adds a background scheduler thread (same pattern as Query-farm/cronjob).

**Tech Stack:** C++17, DuckDB extension API, CMake, DuckDB SQL test framework

---

## File Structure

```
hortus-pipeline/
├── CMakeLists.txt                          # Top-level CMake
├── Makefile                                # Convenience build targets
├── extension_config.cmake                  # Extension registration
├── vcpkg.json                              # Dependencies (empty for now)
├── src/
│   ├── hortus_pipeline_extension.cpp       # Extension entry point (LoadInternal)
│   ├── include/
│   │   ├── parser/
│   │   │   ├── pipeline_parser.hpp         # ParserExtension implementation
│   │   │   └── pipeline_parse_data.hpp     # Parse data structs for each statement type
│   │   ├── catalog/
│   │   │   └── materialized_view_catalog.hpp  # In-memory catalog of MV definitions
│   │   ├── executor/
│   │   │   ├── dag_resolver.hpp            # Topological sort / dependency graph
│   │   │   ├── materializer.hpp            # Execute query & store as table
│   │   │   └── expectation_checker.hpp     # Validate rows against constraints
│   │   └── scheduler/
│   │       └── scheduler.hpp               # Background thread scheduler (Phase 2)
│   ├── parser/
│   │   ├── pipeline_parser.cpp             # SQL parsing logic
│   │   └── pipeline_parse_data.cpp         # Parse data constructors
│   ├── catalog/
│   │   └── materialized_view_catalog.cpp   # Catalog CRUD operations
│   ├── executor/
│   │   ├── dag_resolver.cpp                # DAG building & topo sort
│   │   ├── materializer.cpp                # Query execution & table creation
│   │   └── expectation_checker.cpp         # Row validation logic
│   ├── functions/
│   │   ├── create_materialized_view.cpp    # TableFunction for CREATE
│   │   ├── alter_materialized_view.cpp     # TableFunction for ALTER
│   │   ├── drop_materialized_view.cpp      # TableFunction for DROP
│   │   ├── refresh_materialized_view.cpp   # TableFunction for REFRESH
│   │   ├── refresh_all.cpp                 # TableFunction for REFRESH ALL
│   │   └── pipeline_status.cpp             # TableFunction for pipeline_status()
│   └── scheduler/
│       └── scheduler.cpp                   # Scheduler thread implementation (Phase 2)
├── test/
│   └── sql/
│       ├── create_materialized_view.test   # CREATE tests
│       ├── alter_materialized_view.test    # ALTER tests
│       ├── drop_materialized_view.test     # DROP tests
│       ├── refresh_materialized_view.test  # REFRESH tests
│       ├── expectations.test               # Expectation tests
│       ├── dag_resolution.test             # DAG dependency tests
│       ├── pipeline_status.test            # pipeline_status() tests
│       ├── scheduler.test                  # Scheduler tests (Phase 2)
│       └── schedule_syntax.test            # SCHEDULE clause tests (Phase 2)
└── docs/
    └── superpowers/
        ├── specs/
        │   └── 2026-04-29-declarative-pipeline-design.md
        └── plans/
            └── 2026-04-29-declarative-pipeline-phase1-phase2.md
```

---

## Phase 1 Tasks

### Task 1: Bootstrap Extension from Template

**Files:**
- Create: `CMakeLists.txt`, `Makefile`, `extension_config.cmake`, `vcpkg.json`
- Create: `src/hortus_pipeline_extension.cpp`
- Create: `.github/workflows/` (CI from template)

- [ ] **Step 1: Clone the DuckDB extension template and bootstrap**

```bash
cd /Users/qunfei/Projects
# Clone the template into a temp dir
git clone --recurse-submodules https://github.com/duckdb/extension-template.git hortus-pipeline-template
cd hortus-pipeline-template
python3 ./scripts/bootstrap-template.py hortus_pipeline
```

- [ ] **Step 2: Copy bootstrapped files into our repo**

Copy all files from the bootstrapped template into `/Users/qunfei/Projects/hortus-pipeline/`, preserving the git history we already have. Key files:
- `CMakeLists.txt`
- `Makefile`
- `extension_config.cmake`
- `vcpkg.json`
- `src/hortus_pipeline_extension.cpp` (the entry point)
- `src/include/hortus_pipeline_extension.hpp`
- `duckdb/` (submodule)
- `extension-ci-tools/` (submodule)
- `.github/workflows/`

- [ ] **Step 3: Verify it builds**

```bash
cd /Users/qunfei/Projects/hortus-pipeline
make
```

Expected: Build succeeds, produces `./build/release/extension/hortus_pipeline/hortus_pipeline.duckdb_extension`

- [ ] **Step 4: Verify the template's default test passes**

```bash
make test
```

Expected: Default `quack` test passes.

- [ ] **Step 5: Clean up template boilerplate**

Remove the default `quack()` function from `src/hortus_pipeline_extension.cpp`. Replace with an empty `LoadInternal` that just sets the extension name:

```cpp
#define DUCKDB_EXTENSION_MAIN
#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
    // Extension loaded — functions registered in subsequent tasks
}

} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void hortus_pipeline_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    duckdb::ExtensionLoader loader(db_wrapper);
    duckdb::LoadInternal(loader);
}
DUCKDB_EXTENSION_API const char *hortus_pipeline_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}
```

- [ ] **Step 6: Build and verify clean extension loads**

```bash
make
./build/release/duckdb -cmd "LOAD hortus_pipeline;"
```

Expected: Extension loads without errors.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat: bootstrap extension from DuckDB template"
```

---

### Task 2: Materialized View Catalog

**Files:**
- Create: `src/include/catalog/materialized_view_catalog.hpp`
- Create: `src/catalog/materialized_view_catalog.cpp`

- [ ] **Step 1: Define the catalog data structures**

Create `src/include/catalog/materialized_view_catalog.hpp`:

```cpp
#pragma once
#include "duckdb.hpp"
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>

namespace duckdb {

enum class ExpectationAction {
    WARN,       // Keep bad rows, log count
    DROP_ROW,   // Filter out bad rows
    FAIL_UPDATE // Abort pipeline
};

struct Expectation {
    string name;
    string expression; // SQL boolean expression
    ExpectationAction action;
};

struct MaterializedViewDefinition {
    string name;
    string query;                       // The AS query
    string comment;
    vector<Expectation> expectations;
    vector<string> explicit_dependencies; // DEPENDS ON clause (empty = auto-detect)
    bool is_materialized = false;         // Has been executed at least once
};

class MaterializedViewCatalog {
public:
    // CRUD operations
    void CreateOrRefresh(const string &name, const string &query,
                         const string &comment,
                         const vector<Expectation> &expectations,
                         const vector<string> &depends_on);
    void AlterQuery(const string &name, const string &query);
    void AddConstraint(const string &name, const Expectation &expectation);
    void DropConstraint(const string &name, const string &constraint_name);
    void Drop(const string &name);

    // Lookup
    bool Exists(const string &name) const;
    const MaterializedViewDefinition &Get(const string &name) const;
    vector<string> GetAllNames() const;

    // Mark as materialized after successful refresh
    void MarkMaterialized(const string &name);

    // Singleton per database instance
    static MaterializedViewCatalog &Get(DatabaseInstance &db);

private:
    mutable mutex catalog_mutex;
    unordered_map<string, MaterializedViewDefinition> definitions;
};

} // namespace duckdb
```

- [ ] **Step 2: Implement the catalog**

Create `src/catalog/materialized_view_catalog.cpp`:

```cpp
#include "catalog/materialized_view_catalog.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

void MaterializedViewCatalog::CreateOrRefresh(const string &name, const string &query,
                                               const string &comment,
                                               const vector<Expectation> &expectations,
                                               const vector<string> &depends_on) {
    lock_guard<mutex> lock(catalog_mutex);
    MaterializedViewDefinition def;
    def.name = name;
    def.query = query;
    def.comment = comment;
    def.expectations = expectations;
    def.explicit_dependencies = depends_on;
    def.is_materialized = false;
    definitions[name] = std::move(def);
}

void MaterializedViewCatalog::AlterQuery(const string &name, const string &query) {
    lock_guard<mutex> lock(catalog_mutex);
    auto it = definitions.find(name);
    if (it == definitions.end()) {
        throw InvalidInputException("Materialized view '%s' does not exist", name);
    }
    it->second.query = query;
    it->second.is_materialized = false;
}

void MaterializedViewCatalog::AddConstraint(const string &name, const Expectation &expectation) {
    lock_guard<mutex> lock(catalog_mutex);
    auto it = definitions.find(name);
    if (it == definitions.end()) {
        throw InvalidInputException("Materialized view '%s' does not exist", name);
    }
    // Check for duplicate constraint name
    for (auto &e : it->second.expectations) {
        if (e.name == expectation.name) {
            throw InvalidInputException("Constraint '%s' already exists on '%s'", expectation.name, name);
        }
    }
    it->second.expectations.push_back(expectation);
}

void MaterializedViewCatalog::DropConstraint(const string &name, const string &constraint_name) {
    lock_guard<mutex> lock(catalog_mutex);
    auto it = definitions.find(name);
    if (it == definitions.end()) {
        throw InvalidInputException("Materialized view '%s' does not exist", name);
    }
    auto &exps = it->second.expectations;
    auto exp_it = std::find_if(exps.begin(), exps.end(),
        [&](const Expectation &e) { return e.name == constraint_name; });
    if (exp_it == exps.end()) {
        throw InvalidInputException("Constraint '%s' does not exist on '%s'", constraint_name, name);
    }
    exps.erase(exp_it);
}

void MaterializedViewCatalog::Drop(const string &name) {
    lock_guard<mutex> lock(catalog_mutex);
    auto it = definitions.find(name);
    if (it == definitions.end()) {
        throw InvalidInputException("Materialized view '%s' does not exist", name);
    }
    definitions.erase(it);
}

bool MaterializedViewCatalog::Exists(const string &name) const {
    lock_guard<mutex> lock(catalog_mutex);
    return definitions.find(name) != definitions.end();
}

const MaterializedViewDefinition &MaterializedViewCatalog::Get(const string &name) const {
    lock_guard<mutex> lock(catalog_mutex);
    auto it = definitions.find(name);
    if (it == definitions.end()) {
        throw InvalidInputException("Materialized view '%s' does not exist", name);
    }
    return it->second;
}

vector<string> MaterializedViewCatalog::GetAllNames() const {
    lock_guard<mutex> lock(catalog_mutex);
    vector<string> names;
    for (auto &kv : definitions) {
        names.push_back(kv.first);
    }
    return names;
}

void MaterializedViewCatalog::MarkMaterialized(const string &name) {
    lock_guard<mutex> lock(catalog_mutex);
    auto it = definitions.find(name);
    if (it != definitions.end()) {
        it->second.is_materialized = true;
    }
}

// Singleton per database — stored via ObjectCache or similar mechanism
// For now, use a simple static. In production, attach to DatabaseInstance.
static MaterializedViewCatalog global_catalog;

MaterializedViewCatalog &MaterializedViewCatalog::Get(DatabaseInstance &db) {
    return global_catalog;
}

} // namespace duckdb
```

- [ ] **Step 3: Update CMakeLists.txt to include new source files**

Add `src/catalog/materialized_view_catalog.cpp` to the source list in `extension_config.cmake` or `CMakeLists.txt` (depends on template structure).

- [ ] **Step 4: Build to verify compilation**

```bash
make
```

Expected: Builds without errors.

- [ ] **Step 5: Commit**

```bash
git add src/include/catalog/ src/catalog/
git commit -m "feat: add materialized view catalog with CRUD operations"
```

---

### Task 3: SQL Parser Extension

**Files:**
- Create: `src/include/parser/pipeline_parse_data.hpp`
- Create: `src/include/parser/pipeline_parser.hpp`
- Create: `src/parser/pipeline_parse_data.cpp`
- Create: `src/parser/pipeline_parser.cpp`
- Modify: `src/hortus_pipeline_extension.cpp`

- [ ] **Step 1: Define parse data structs**

Create `src/include/parser/pipeline_parse_data.hpp`:

```cpp
#pragma once
#include "duckdb.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "catalog/materialized_view_catalog.hpp"
#include <string>
#include <vector>

namespace duckdb {

enum class PipelineStatementType {
    CREATE_MATERIALIZED_VIEW,
    ALTER_MATERIALIZED_VIEW,
    DROP_MATERIALIZED_VIEW,
    REFRESH_MATERIALIZED_VIEW,
    REFRESH_ALL_MATERIALIZED_VIEWS,
    PIPELINE_STATUS
};

enum class RefreshMode {
    SYNC,
    ASYNC,
    FULL
};

enum class AlterAction {
    SET_QUERY,
    ADD_CONSTRAINT,
    DROP_CONSTRAINT
};

struct PipelineParseData : public ParserExtensionParseData {
    PipelineStatementType statement_type;

    // CREATE fields
    string view_name;
    string query;
    string comment;
    vector<Expectation> expectations;
    vector<string> depends_on;

    // REFRESH fields
    RefreshMode refresh_mode = RefreshMode::SYNC;

    // ALTER fields
    AlterAction alter_action = AlterAction::SET_QUERY;
    Expectation alter_expectation;
    string drop_constraint_name;

    unique_ptr<ParserExtensionParseData> Copy() const override {
        auto result = make_uniq<PipelineParseData>();
        result->statement_type = statement_type;
        result->view_name = view_name;
        result->query = query;
        result->comment = comment;
        result->expectations = expectations;
        result->depends_on = depends_on;
        result->refresh_mode = refresh_mode;
        result->alter_action = alter_action;
        result->alter_expectation = alter_expectation;
        result->drop_constraint_name = drop_constraint_name;
        return std::move(result);
    }

    string ToString() const override {
        return "PipelineStatement";
    }
};

} // namespace duckdb
```

- [ ] **Step 2: Implement the parser**

Create `src/include/parser/pipeline_parser.hpp`:

```cpp
#pragma once
#include "duckdb.hpp"
#include "duckdb/parser/parser_extension.hpp"

namespace duckdb {

class PipelineParserExtension : public ParserExtension {
public:
    PipelineParserExtension();

    static ParserExtensionParseResult ParseStatement(ParserExtensionInfo *info,
                                                      const string &query);
    static ParserExtensionPlanResult PlanStatement(ParserExtensionInfo *info,
                                                    ClientContext &context,
                                                    unique_ptr<ParserExtensionParseData> parse_data);
};

} // namespace duckdb
```

Create `src/parser/pipeline_parser.cpp` with the parsing logic. This is the core parser that tokenizes the SQL string and matches our custom syntax patterns:

- `CREATE OR REFRESH MATERIALIZED VIEW ...`
- `ALTER MATERIALIZED VIEW ...`
- `DROP MATERIALIZED VIEW ...`
- `REFRESH MATERIALIZED VIEW ...`
- `REFRESH ALL MATERIALIZED VIEWS`
- `CALL pipeline_status()`

The parser uses simple string tokenization (split by whitespace, match keyword sequences). For the `AS query` part, it extracts everything after the `AS` keyword as the inner query string.

Key parsing rules:
- Case-insensitive keyword matching
- `CONSTRAINT name EXPECT (expr)` — extract constraint name and expression (content between parens)
- `ON VIOLATION DROP ROW` / `ON VIOLATION FAIL UPDATE` — look ahead after EXPECT clause
- `DEPENDS ON (table1, table2)` — extract comma-separated list between parens
- `COMMENT 'text'` — extract quoted string
- `AS query` — everything after AS is the query

- [ ] **Step 3: Implement PlanStatement to route to TableFunctions**

In `pipeline_parser.cpp`, `PlanStatement` inspects `PipelineParseData::statement_type` and returns a `ParserExtensionPlanResult` that maps to the appropriate `TableFunction`:

```cpp
ParserExtensionPlanResult PipelineParserExtension::PlanStatement(
    ParserExtensionInfo *info, ClientContext &context,
    unique_ptr<ParserExtensionParseData> parse_data) {

    auto &data = dynamic_cast<PipelineParseData &>(*parse_data);
    ParserExtensionPlanResult result;

    switch (data.statement_type) {
    case PipelineStatementType::CREATE_MATERIALIZED_VIEW:
        result.function = CreateMaterializedViewFunction();
        result.parameters.push_back(Value(data.view_name));
        result.parameters.push_back(Value(data.query));
        // ... pack remaining fields
        break;
    case PipelineStatementType::REFRESH_MATERIALIZED_VIEW:
        result.function = RefreshMaterializedViewFunction();
        result.parameters.push_back(Value(data.view_name));
        result.parameters.push_back(Value(static_cast<int>(data.refresh_mode)));
        break;
    // ... other statement types
    }
    return result;
}
```

- [ ] **Step 4: Register the parser in LoadInternal**

Update `src/hortus_pipeline_extension.cpp`:

```cpp
#include "parser/pipeline_parser.hpp"

static void LoadInternal(ExtensionLoader &loader) {
    auto &db = loader.GetDatabaseInstance();
    auto &config = DBConfig::GetConfig(db);

    // Register our custom SQL parser
    PipelineParserExtension parser_ext;
    config.parser_extensions.push_back(parser_ext);
}
```

- [ ] **Step 5: Build to verify compilation**

```bash
make
```

Expected: Builds without errors.

- [ ] **Step 6: Commit**

```bash
git add src/include/parser/ src/parser/ src/hortus_pipeline_extension.cpp
git commit -m "feat: add SQL parser extension for pipeline statements"
```

---

### Task 4: CREATE OR REFRESH MATERIALIZED VIEW

**Files:**
- Create: `src/functions/create_materialized_view.cpp`
- Create: `test/sql/create_materialized_view.test`

- [ ] **Step 1: Write the SQL test**

Create `test/sql/create_materialized_view.test`:

```
# name: test/sql/create_materialized_view.test
# group: [hortus_pipeline]

require hortus_pipeline

# Basic create
statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_test AS SELECT 1 AS id, 'hello' AS name;

# Verify the table was created (materialized views become real tables)
query II
SELECT * FROM mv_test;
----
1	hello

# Create with source data
statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_numbers AS SELECT i AS num FROM range(5) t(i);

query I
SELECT COUNT(*) FROM mv_numbers;
----
5

# Create or refresh overwrites existing
statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_test AS SELECT 2 AS id, 'world' AS name;

query II
SELECT * FROM mv_test;
----
2	world

# Create with COMMENT
statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_commented COMMENT 'test comment' AS SELECT 1 AS x;

query I
SELECT * FROM mv_commented;
----
1

# Create with DEPENDS ON
statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_dep DEPENDS ON (mv_numbers) AS SELECT num * 2 AS doubled FROM mv_numbers;

query I
SELECT COUNT(*) FROM mv_dep;
----
5
```

- [ ] **Step 2: Implement the CREATE TableFunction**

Create `src/functions/create_materialized_view.cpp`:

The function:
1. Receives parsed parameters (view_name, query, comment, expectations, depends_on)
2. Registers the definition in `MaterializedViewCatalog`
3. Executes the query via `context.Query(query)`
4. Creates/replaces a real table with the result: `CREATE OR REPLACE TABLE view_name AS (query)`
5. Marks as materialized in the catalog

```cpp
#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"
#include "catalog/materialized_view_catalog.hpp"

namespace duckdb {

struct CreateMVData : public TableFunctionData {
    string view_name;
    string query;
    string comment;
    vector<Expectation> expectations;
    vector<string> depends_on;
    bool finished = false;
};

static unique_ptr<FunctionData> CreateMVBind(ClientContext &context,
                                              TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types,
                                              vector<string> &names) {
    auto data = make_uniq<CreateMVData>();
    data->view_name = input.inputs[0].GetValue<string>();
    data->query = input.inputs[1].GetValue<string>();
    // Unpack additional params from input.inputs[2..N]

    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("status");
    return std::move(data);
}

static void CreateMVExecute(ClientContext &context, TableFunctionInput &input,
                             DataChunk &output) {
    auto &data = input.bind_data->CastNoConst<CreateMVData>();
    if (data.finished) {
        return; // No more output
    }

    auto &catalog = MaterializedViewCatalog::Get(DatabaseInstance::GetDatabase(context));

    // Register definition
    catalog.CreateOrRefresh(data.view_name, data.query, data.comment,
                            data.expectations, data.depends_on);

    // Materialize: CREATE OR REPLACE TABLE <name> AS (<query>)
    string materialize_sql = "CREATE OR REPLACE TABLE " + data.view_name +
                             " AS (" + data.query + ")";
    context.Query(materialize_sql, false);

    catalog.MarkMaterialized(data.view_name);

    output.SetCardinality(1);
    output.SetValue(0, 0, Value("Materialized view '" + data.view_name + "' created"));
    data.finished = true;
}

TableFunction CreateMaterializedViewFunction() {
    TableFunction func("hortus_create_mv", {LogicalType::VARCHAR, LogicalType::VARCHAR},
                       CreateMVExecute, CreateMVBind);
    return func;
}

} // namespace duckdb
```

- [ ] **Step 3: Register the function and run tests**

Register `CreateMaterializedViewFunction()` in `LoadInternal` via `ExtensionUtil::RegisterFunction`.

```bash
make test
```

Expected: `create_materialized_view.test` passes.

- [ ] **Step 4: Commit**

```bash
git add src/functions/create_materialized_view.cpp test/sql/create_materialized_view.test
git commit -m "feat: implement CREATE OR REFRESH MATERIALIZED VIEW"
```

---

### Task 5: Expectations (CONSTRAINT ... EXPECT)

**Files:**
- Create: `src/include/executor/expectation_checker.hpp`
- Create: `src/executor/expectation_checker.cpp`
- Modify: `src/functions/create_materialized_view.cpp`
- Create: `test/sql/expectations.test`

- [ ] **Step 1: Write the SQL test**

Create `test/sql/expectations.test`:

```
# name: test/sql/expectations.test
# group: [hortus_pipeline]

require hortus_pipeline

# WARN mode — keeps all rows, just logs
statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_warn
  CONSTRAINT positive_id EXPECT (id > 0)
AS SELECT * FROM (VALUES (1, 'a'), (-1, 'b'), (2, 'c')) t(id, name);

query II
SELECT * FROM mv_warn ORDER BY id;
----
-1	b
1	a
2	c

# DROP ROW mode — filters out bad rows
statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_drop
  CONSTRAINT positive_id EXPECT (id > 0) ON VIOLATION DROP ROW
AS SELECT * FROM (VALUES (1, 'a'), (-1, 'b'), (2, 'c')) t(id, name);

query II
SELECT * FROM mv_drop ORDER BY id;
----
1	a
2	c

# FAIL UPDATE mode — entire statement fails on violation
statement error
CREATE OR REFRESH MATERIALIZED VIEW mv_fail
  CONSTRAINT positive_id EXPECT (id > 0) ON VIOLATION FAIL UPDATE
AS SELECT * FROM (VALUES (1, 'a'), (-1, 'b'), (2, 'c')) t(id, name);
----
Expectation 'positive_id' failed

# FAIL UPDATE with no violations — succeeds
statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_fail_ok
  CONSTRAINT positive_id EXPECT (id > 0) ON VIOLATION FAIL UPDATE
AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(id, name);

query II
SELECT * FROM mv_fail_ok ORDER BY id;
----
1	a
2	b
3	c

# Multiple expectations
statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_multi
  CONSTRAINT positive_id EXPECT (id > 0) ON VIOLATION DROP ROW
  CONSTRAINT has_name EXPECT (name IS NOT NULL) ON VIOLATION DROP ROW
AS SELECT * FROM (VALUES (1, 'a'), (-1, 'b'), (2, NULL)) t(id, name);

query II
SELECT * FROM mv_multi ORDER BY id;
----
1	a
```

- [ ] **Step 2: Implement the expectation checker**

Create `src/include/executor/expectation_checker.hpp`:

```cpp
#pragma once
#include "duckdb.hpp"
#include "catalog/materialized_view_catalog.hpp"

namespace duckdb {

struct ExpectationResult {
    string name;
    idx_t total_rows;
    idx_t passed;
    idx_t failed;
    ExpectationAction action;
};

class ExpectationChecker {
public:
    // Returns filtered query SQL for DROP ROW expectations.
    // Throws for FAIL UPDATE if violations found.
    // Logs for WARN.
    // Returns the modified query string with WHERE clauses added for DROP ROW.
    static string ApplyExpectations(ClientContext &context,
                                     const string &base_query,
                                     const vector<Expectation> &expectations,
                                     vector<ExpectationResult> &results);
};

} // namespace duckdb
```

Create `src/executor/expectation_checker.cpp`:

The implementation:
1. For WARN: Run `SELECT COUNT(*) FROM (base_query) WHERE NOT (expr)` to count violations, log them, keep all rows.
2. For DROP ROW: Wrap the query with `SELECT * FROM (base_query) WHERE (expr1) AND (expr2) ...` to filter.
3. For FAIL UPDATE: Run the count check first. If violations > 0, throw an exception.

```cpp
#include "executor/expectation_checker.hpp"

namespace duckdb {

string ExpectationChecker::ApplyExpectations(ClientContext &context,
                                              const string &base_query,
                                              const vector<Expectation> &expectations,
                                              vector<ExpectationResult> &results) {
    if (expectations.empty()) {
        return base_query;
    }

    // First pass: check FAIL_UPDATE expectations
    for (auto &exp : expectations) {
        if (exp.action == ExpectationAction::FAIL_UPDATE) {
            string count_sql = "SELECT COUNT(*) FROM (" + base_query +
                               ") __mv_src WHERE NOT (" + exp.expression + ")";
            auto result = context.Query(count_sql, false);
            if (result->HasError()) {
                throw InvalidInputException("Expectation '%s' expression error: %s",
                                             exp.name, result->GetError());
            }
            auto chunk = result->Fetch();
            int64_t violation_count = chunk->GetValue(0, 0).GetValue<int64_t>();
            if (violation_count > 0) {
                throw InvalidInputException("Expectation '%s' failed: %lld rows violated constraint (%s)",
                                             exp.name, violation_count, exp.expression);
            }
        }
    }

    // Build filtered query for DROP ROW expectations
    vector<string> drop_filters;
    for (auto &exp : expectations) {
        if (exp.action == ExpectationAction::DROP_ROW) {
            drop_filters.push_back("(" + exp.expression + ")");
        }
    }

    string final_query = base_query;
    if (!drop_filters.empty()) {
        final_query = "SELECT * FROM (" + base_query + ") __mv_src WHERE " +
                      StringUtil::Join(drop_filters, " AND ");
    }

    // WARN expectations: count violations and log
    for (auto &exp : expectations) {
        if (exp.action == ExpectationAction::WARN) {
            string count_sql = "SELECT COUNT(*) FROM (" + base_query +
                               ") __mv_src WHERE NOT (" + exp.expression + ")";
            auto result = context.Query(count_sql, false);
            if (!result->HasError()) {
                auto chunk = result->Fetch();
                int64_t violation_count = chunk->GetValue(0, 0).GetValue<int64_t>();
                if (violation_count > 0) {
                    Printer::Print("WARNING: Expectation '" + exp.name + "': " +
                                   to_string(violation_count) + " rows violated (" +
                                   exp.expression + ")");
                }
            }
        }
    }

    return final_query;
}

} // namespace duckdb
```

- [ ] **Step 3: Integrate expectations into CREATE function**

Modify `src/functions/create_materialized_view.cpp` to call `ExpectationChecker::ApplyExpectations` before materializing:

```cpp
// In CreateMVExecute, before the CREATE OR REPLACE TABLE:
vector<ExpectationResult> exp_results;
string final_query = ExpectationChecker::ApplyExpectations(
    context, data.query, data.expectations, exp_results);

string materialize_sql = "CREATE OR REPLACE TABLE " + data.view_name +
                         " AS (" + final_query + ")";
```

- [ ] **Step 4: Run tests**

```bash
make test
```

Expected: `expectations.test` passes.

- [ ] **Step 5: Commit**

```bash
git add src/include/executor/expectation_checker.hpp src/executor/expectation_checker.cpp test/sql/expectations.test
git commit -m "feat: implement data quality expectations (warn/drop/fail)"
```

---

### Task 6: DAG Resolver

**Files:**
- Create: `src/include/executor/dag_resolver.hpp`
- Create: `src/executor/dag_resolver.cpp`
- Create: `test/sql/dag_resolution.test`

- [ ] **Step 1: Write the SQL test**

Create `test/sql/dag_resolution.test`:

```
# name: test/sql/dag_resolution.test
# group: [hortus_pipeline]

require hortus_pipeline

# Define a chain of materialized views
statement ok
CREATE OR REFRESH MATERIALIZED VIEW dag_source AS SELECT i AS id FROM range(3) t(i);

statement ok
CREATE OR REFRESH MATERIALIZED VIEW dag_mid AS SELECT id, id * 2 AS doubled FROM dag_source;

statement ok
CREATE OR REFRESH MATERIALIZED VIEW dag_leaf AS SELECT doubled, doubled + 1 AS plus_one FROM dag_mid;

# Verify chain executed correctly
query II
SELECT * FROM dag_leaf ORDER BY doubled;
----
0	1
2	3
4	5

# REFRESH ALL resolves the DAG and re-materializes in order
statement ok
REFRESH ALL MATERIALIZED VIEWS;

query II
SELECT * FROM dag_leaf ORDER BY doubled;
----
0	1
2	3
4	5
```

- [ ] **Step 2: Implement the DAG resolver**

Create `src/include/executor/dag_resolver.hpp`:

```cpp
#pragma once
#include "duckdb.hpp"
#include "catalog/materialized_view_catalog.hpp"
#include <vector>
#include <string>

namespace duckdb {

class DAGResolver {
public:
    // Returns a topologically sorted list of MV names (dependencies first)
    static vector<string> Resolve(const MaterializedViewCatalog &catalog);

    // Returns sorted list for a single MV and its upstream dependencies
    static vector<string> ResolveFor(const MaterializedViewCatalog &catalog,
                                      const string &target_name);

    // Extract table references from a SQL query string
    static vector<string> ExtractDependencies(const string &query);
};

} // namespace duckdb
```

Create `src/executor/dag_resolver.cpp`:

```cpp
#include "executor/dag_resolver.hpp"
#include "duckdb/common/exception.hpp"
#include <queue>
#include <unordered_set>
#include <unordered_map>
#include <regex>

namespace duckdb {

vector<string> DAGResolver::ExtractDependencies(const string &query) {
    // Extract table references using simple pattern matching:
    // Look for FROM <table>, JOIN <table> patterns
    // Skip function calls like read_csv(...), range(...)
    vector<string> deps;
    // Use DuckDB's parser to extract table references:
    // Parse the query, walk the AST, collect TableRef names
    // For now, use regex as a starting point
    std::regex table_pattern(R"((?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*))",
                              std::regex::icase);
    auto begin = std::sregex_iterator(query.begin(), query.end(), table_pattern);
    auto end = std::sregex_iterator();
    for (auto it = begin; it != end; ++it) {
        string table_name = (*it)[1].str();
        // Skip known function-like sources
        string lower = StringUtil::Lower(table_name);
        if (lower == "range" || lower == "read_csv" || lower == "read_parquet" ||
            lower == "read_json" || lower == "generate_series" || lower == "values") {
            continue;
        }
        deps.push_back(table_name);
    }
    return deps;
}

vector<string> DAGResolver::Resolve(const MaterializedViewCatalog &catalog) {
    auto all_names = catalog.GetAllNames();
    unordered_set<string> mv_set(all_names.begin(), all_names.end());

    // Build adjacency list
    unordered_map<string, vector<string>> adj;     // name -> depends on
    unordered_map<string, int> in_degree;

    for (auto &name : all_names) {
        auto &def = catalog.Get(name);
        in_degree[name] = 0;
        adj[name] = {};
    }

    for (auto &name : all_names) {
        auto &def = catalog.Get(name);
        vector<string> deps;
        if (!def.explicit_dependencies.empty()) {
            deps = def.explicit_dependencies;
        } else {
            deps = ExtractDependencies(def.query);
        }
        for (auto &dep : deps) {
            if (mv_set.count(dep) && dep != name) {
                adj[name].push_back(dep);
                in_degree[name]++;
            }
        }
    }

    // Kahn's algorithm for topological sort
    queue<string> q;
    for (auto &kv : in_degree) {
        if (kv.second == 0) {
            q.push(kv.first);
        }
    }

    vector<string> sorted;
    while (!q.empty()) {
        auto node = q.front();
        q.pop();
        sorted.push_back(node);
        for (auto &name : all_names) {
            auto &dep_list = adj[name];
            auto it = std::find(dep_list.begin(), dep_list.end(), node);
            if (it != dep_list.end()) {
                dep_list.erase(it);
                in_degree[name]--;
                if (in_degree[name] == 0) {
                    q.push(name);
                }
            }
        }
    }

    if (sorted.size() != all_names.size()) {
        throw InvalidInputException("Circular dependency detected in materialized views");
    }

    return sorted;
}

vector<string> DAGResolver::ResolveFor(const MaterializedViewCatalog &catalog,
                                        const string &target_name) {
    // BFS to find all upstream dependencies of target
    auto full_order = Resolve(catalog);
    unordered_set<string> needed;
    needed.insert(target_name);

    // Walk in reverse topological order to find all transitive deps
    auto all_names = catalog.GetAllNames();
    unordered_set<string> mv_set(all_names.begin(), all_names.end());

    bool changed = true;
    while (changed) {
        changed = false;
        for (auto &name : needed) {
            auto &def = catalog.Get(name);
            vector<string> deps;
            if (!def.explicit_dependencies.empty()) {
                deps = def.explicit_dependencies;
            } else {
                deps = ExtractDependencies(def.query);
            }
            for (auto &dep : deps) {
                if (mv_set.count(dep) && !needed.count(dep)) {
                    needed.insert(dep);
                    changed = true;
                }
            }
        }
    }

    // Filter full_order to only include needed
    vector<string> result;
    for (auto &name : full_order) {
        if (needed.count(name)) {
            result.push_back(name);
        }
    }
    return result;
}

} // namespace duckdb
```

- [ ] **Step 3: Run tests**

```bash
make test
```

Expected: `dag_resolution.test` passes.

- [ ] **Step 4: Commit**

```bash
git add src/include/executor/dag_resolver.hpp src/executor/dag_resolver.cpp test/sql/dag_resolution.test
git commit -m "feat: implement DAG resolver with topological sort"
```

---

### Task 7: REFRESH MATERIALIZED VIEW

**Files:**
- Create: `src/functions/refresh_materialized_view.cpp`
- Create: `src/functions/refresh_all.cpp`
- Create: `src/include/executor/materializer.hpp`
- Create: `src/executor/materializer.cpp`
- Create: `test/sql/refresh_materialized_view.test`

- [ ] **Step 1: Write the SQL test**

Create `test/sql/refresh_materialized_view.test`:

```
# name: test/sql/refresh_materialized_view.test
# group: [hortus_pipeline]

require hortus_pipeline

# Setup: create a source table and MV
statement ok
CREATE TABLE raw_data AS SELECT 1 AS id, 'original' AS val;

statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_refresh AS SELECT * FROM raw_data;

query II
SELECT * FROM mv_refresh;
----
1	original

# Modify source data
statement ok
INSERT INTO raw_data VALUES (2, 'added');

# MV still has old data
query I
SELECT COUNT(*) FROM mv_refresh;
----
1

# Refresh to pick up new data
statement ok
REFRESH MATERIALIZED VIEW mv_refresh;

query I
SELECT COUNT(*) FROM mv_refresh;
----
2

# REFRESH FULL — same behavior in Phase 1
statement ok
REFRESH MATERIALIZED VIEW mv_refresh FULL;

query I
SELECT COUNT(*) FROM mv_refresh;
----
2

# REFRESH ALL
statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_derived AS SELECT id * 10 AS big_id FROM mv_refresh;

statement ok
INSERT INTO raw_data VALUES (3, 'third');

statement ok
REFRESH ALL MATERIALIZED VIEWS;

query I
SELECT COUNT(*) FROM mv_derived;
----
3
```

- [ ] **Step 2: Implement the materializer**

Create `src/include/executor/materializer.hpp`:

```cpp
#pragma once
#include "duckdb.hpp"
#include "catalog/materialized_view_catalog.hpp"

namespace duckdb {

class Materializer {
public:
    // Materialize a single MV (re-execute its query, replace the table)
    static void Materialize(ClientContext &context,
                            MaterializedViewCatalog &catalog,
                            const string &view_name);

    // Materialize all MVs in DAG order
    static void MaterializeAll(ClientContext &context,
                                MaterializedViewCatalog &catalog);
};

} // namespace duckdb
```

Create `src/executor/materializer.cpp`:

```cpp
#include "executor/materializer.hpp"
#include "executor/dag_resolver.hpp"
#include "executor/expectation_checker.hpp"

namespace duckdb {

void Materializer::Materialize(ClientContext &context,
                                MaterializedViewCatalog &catalog,
                                const string &view_name) {
    auto &def = catalog.Get(view_name);

    // Apply expectations
    vector<ExpectationResult> exp_results;
    string final_query = ExpectationChecker::ApplyExpectations(
        context, def.query, def.expectations, exp_results);

    // Materialize
    string sql = "CREATE OR REPLACE TABLE " + view_name + " AS (" + final_query + ")";
    auto result = context.Query(sql, false);
    if (result->HasError()) {
        throw InvalidInputException("Failed to materialize '%s': %s",
                                     view_name, result->GetError());
    }

    catalog.MarkMaterialized(view_name);
}

void Materializer::MaterializeAll(ClientContext &context,
                                   MaterializedViewCatalog &catalog) {
    auto order = DAGResolver::Resolve(catalog);
    for (auto &name : order) {
        Materialize(context, catalog, name);
    }
}

} // namespace duckdb
```

- [ ] **Step 3: Implement REFRESH and REFRESH ALL TableFunctions**

Create `src/functions/refresh_materialized_view.cpp` and `src/functions/refresh_all.cpp`.

`refresh_materialized_view.cpp`:
- Takes view_name and refresh_mode as parameters
- Calls `DAGResolver::ResolveFor` to get upstream dependencies
- Calls `Materializer::Materialize` for each in order
- For ASYNC mode: spawn a `std::thread` that does the refresh, return immediately

`refresh_all.cpp`:
- No parameters
- Calls `Materializer::MaterializeAll`

- [ ] **Step 4: Run tests**

```bash
make test
```

Expected: `refresh_materialized_view.test` passes.

- [ ] **Step 5: Commit**

```bash
git add src/include/executor/materializer.hpp src/executor/materializer.cpp \
    src/functions/refresh_materialized_view.cpp src/functions/refresh_all.cpp \
    test/sql/refresh_materialized_view.test
git commit -m "feat: implement REFRESH MATERIALIZED VIEW and REFRESH ALL"
```

---

### Task 8: ALTER MATERIALIZED VIEW

**Files:**
- Create: `src/functions/alter_materialized_view.cpp`
- Create: `test/sql/alter_materialized_view.test`

- [ ] **Step 1: Write the SQL test**

Create `test/sql/alter_materialized_view.test`:

```
# name: test/sql/alter_materialized_view.test
# group: [hortus_pipeline]

require hortus_pipeline

# Setup
statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_alter AS SELECT 1 AS id, 'hello' AS name;

# ALTER query definition (does NOT re-materialize)
statement ok
ALTER MATERIALIZED VIEW mv_alter AS SELECT 2 AS id, 'world' AS name;

# Old data still there until refresh
query II
SELECT * FROM mv_alter;
----
1	hello

# Refresh to apply new definition
statement ok
REFRESH MATERIALIZED VIEW mv_alter;

query II
SELECT * FROM mv_alter;
----
2	world

# ADD CONSTRAINT
statement ok
ALTER MATERIALIZED VIEW mv_alter ADD CONSTRAINT positive_id EXPECT (id > 0);

# DROP CONSTRAINT
statement ok
ALTER MATERIALIZED VIEW mv_alter DROP CONSTRAINT positive_id;

# ALTER non-existent view fails
statement error
ALTER MATERIALIZED VIEW nonexistent AS SELECT 1;
----
does not exist
```

- [ ] **Step 2: Implement ALTER TableFunction**

Create `src/functions/alter_materialized_view.cpp`:

Routes to the appropriate catalog operation based on `AlterAction`:
- `SET_QUERY`: `catalog.AlterQuery(name, new_query)`
- `ADD_CONSTRAINT`: `catalog.AddConstraint(name, expectation)`
- `DROP_CONSTRAINT`: `catalog.DropConstraint(name, constraint_name)`

Does NOT re-materialize — user must `REFRESH` to apply changes.

- [ ] **Step 3: Run tests**

```bash
make test
```

Expected: `alter_materialized_view.test` passes.

- [ ] **Step 4: Commit**

```bash
git add src/functions/alter_materialized_view.cpp test/sql/alter_materialized_view.test
git commit -m "feat: implement ALTER MATERIALIZED VIEW"
```

---

### Task 9: DROP MATERIALIZED VIEW

**Files:**
- Create: `src/functions/drop_materialized_view.cpp`
- Create: `test/sql/drop_materialized_view.test`

- [ ] **Step 1: Write the SQL test**

Create `test/sql/drop_materialized_view.test`:

```
# name: test/sql/drop_materialized_view.test
# group: [hortus_pipeline]

require hortus_pipeline

# Setup
statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_drop AS SELECT 1 AS id;

query I
SELECT * FROM mv_drop;
----
1

# Drop removes both definition and materialized table
statement ok
DROP MATERIALIZED VIEW mv_drop;

# Table no longer exists
statement error
SELECT * FROM mv_drop;
----
does not exist

# Drop non-existent fails
statement error
DROP MATERIALIZED VIEW mv_drop;
----
does not exist
```

- [ ] **Step 2: Implement DROP TableFunction**

Create `src/functions/drop_materialized_view.cpp`:

1. Remove from `MaterializedViewCatalog`
2. Execute `DROP TABLE IF EXISTS view_name`

- [ ] **Step 3: Run tests**

```bash
make test
```

Expected: `drop_materialized_view.test` passes.

- [ ] **Step 4: Commit**

```bash
git add src/functions/drop_materialized_view.cpp test/sql/drop_materialized_view.test
git commit -m "feat: implement DROP MATERIALIZED VIEW"
```

---

### Task 10: pipeline_status() Table Function

**Files:**
- Create: `src/functions/pipeline_status.cpp`
- Create: `test/sql/pipeline_status.test`

- [ ] **Step 1: Write the SQL test**

Create `test/sql/pipeline_status.test`:

```
# name: test/sql/pipeline_status.test
# group: [hortus_pipeline]

require hortus_pipeline

# Empty pipeline
query IIIII
CALL pipeline_status();
----

# Create some MVs
statement ok
CREATE OR REFRESH MATERIALIZED VIEW ps_source AS SELECT 1 AS id;

statement ok
CREATE OR REFRESH MATERIALIZED VIEW ps_derived AS SELECT id * 2 AS x FROM ps_source;

# pipeline_status shows all MVs with their state
query IIIII
CALL pipeline_status();
----
ps_source	SELECT 1 AS id	(empty)	true	(none)
ps_derived	SELECT id * 2 AS x FROM ps_source	ps_source	true	(none)
```

- [ ] **Step 2: Implement pipeline_status**

Create `src/functions/pipeline_status.cpp`:

Returns a table with columns:
- `name` (VARCHAR) — MV name
- `query` (VARCHAR) — the definition query
- `dependencies` (VARCHAR) — comma-separated dependency list
- `is_materialized` (BOOLEAN) — whether it's been materialized
- `comment` (VARCHAR) — the comment

- [ ] **Step 3: Run tests**

```bash
make test
```

Expected: `pipeline_status.test` passes.

- [ ] **Step 4: Commit**

```bash
git add src/functions/pipeline_status.cpp test/sql/pipeline_status.test
git commit -m "feat: implement pipeline_status() table function"
```

---

## Phase 2 Tasks

### Task 11: SCHEDULE Clause Parsing

**Files:**
- Modify: `src/include/parser/pipeline_parse_data.hpp`
- Modify: `src/parser/pipeline_parser.cpp`
- Modify: `src/include/catalog/materialized_view_catalog.hpp`
- Modify: `src/catalog/materialized_view_catalog.cpp`
- Create: `test/sql/schedule_syntax.test`

- [ ] **Step 1: Extend parse data with schedule fields**

Add to `PipelineParseData`:

```cpp
enum class ScheduleType {
    NONE,
    EVERY,       // SCHEDULE EVERY 1 HOUR
    CRON,        // SCHEDULE CRON '...'
    ON_UPDATE    // SCHEDULE TRIGGER ON UPDATE
};

// In PipelineParseData:
ScheduleType schedule_type = ScheduleType::NONE;
int schedule_interval = 0;          // For EVERY: the number
string schedule_interval_unit;       // For EVERY: HOUR, DAY, WEEK, etc.
string schedule_cron_expression;     // For CRON: the cron string
```

- [ ] **Step 2: Extend catalog definition**

Add to `MaterializedViewDefinition`:

```cpp
ScheduleType schedule_type = ScheduleType::NONE;
int schedule_interval = 0;
string schedule_interval_unit;
string schedule_cron_expression;
bool schedule_paused = false;
```

- [ ] **Step 3: Update parser to handle SCHEDULE clause**

In `pipeline_parser.cpp`, after parsing `COMMENT` and `CONSTRAINT` clauses, look for `SCHEDULE`:
- `SCHEDULE EVERY <N> <UNIT>` — parse integer and unit keyword
- `SCHEDULE CRON '<expr>'` — parse quoted cron expression
- `SCHEDULE TRIGGER ON UPDATE` — set type to ON_UPDATE

- [ ] **Step 4: Write test**

Create `test/sql/schedule_syntax.test`:

```
# name: test/sql/schedule_syntax.test
# group: [hortus_pipeline]

require hortus_pipeline

# SCHEDULE EVERY
statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_hourly
  SCHEDULE EVERY 1 HOUR
AS SELECT 1 AS id;

# SCHEDULE CRON
statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_cron
  SCHEDULE CRON '0 6 * * *'
AS SELECT 1 AS id;

# SCHEDULE TRIGGER ON UPDATE
statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_trigger
  SCHEDULE TRIGGER ON UPDATE
AS SELECT 1 AS id;

# Verify schedule info in pipeline_status or pipeline_schedules
query II
CALL pipeline_schedules();
----
mv_hourly	EVERY 1 HOUR
mv_cron	CRON 0 6 * * *
mv_trigger	TRIGGER ON UPDATE
```

- [ ] **Step 5: Run tests**

```bash
make test
```

- [ ] **Step 6: Commit**

```bash
git add src/include/parser/pipeline_parse_data.hpp src/parser/pipeline_parser.cpp \
    src/include/catalog/materialized_view_catalog.hpp src/catalog/materialized_view_catalog.cpp \
    test/sql/schedule_syntax.test
git commit -m "feat: add SCHEDULE clause parsing (EVERY, CRON, TRIGGER ON UPDATE)"
```

---

### Task 12: Scheduler Engine (Background Thread)

**Files:**
- Create: `src/include/scheduler/scheduler.hpp`
- Create: `src/scheduler/scheduler.cpp`
- Modify: `src/hortus_pipeline_extension.cpp`
- Create: `test/sql/scheduler.test`

- [ ] **Step 1: Implement the scheduler**

Create `src/include/scheduler/scheduler.hpp`:

```cpp
#pragma once
#include "duckdb.hpp"
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <chrono>

namespace duckdb {

struct ScheduledTask {
    string view_name;
    std::chrono::system_clock::time_point next_run;

    bool operator>(const ScheduledTask &other) const {
        return next_run > other.next_run;
    }
};

class PipelineScheduler {
public:
    explicit PipelineScheduler(DatabaseInstance &db);
    ~PipelineScheduler();

    // Called when a MV with SCHEDULE is created/altered
    void AddSchedule(const string &view_name);
    void RemoveSchedule(const string &view_name);
    void PauseSchedule(const string &view_name);
    void ResumeSchedule(const string &view_name);

    // Get scheduled tasks info
    vector<pair<string, string>> ListSchedules();

    static PipelineScheduler &Get(DatabaseInstance &db);

private:
    void RunScheduler();
    std::chrono::system_clock::time_point ComputeNextRun(const string &view_name);

    DatabaseInstance &db;
    std::thread scheduler_thread;
    std::mutex mutex;
    std::condition_variable cv;
    bool should_stop = false;

    // Priority queue: smallest next_run first
    std::priority_queue<ScheduledTask, vector<ScheduledTask>, std::greater<ScheduledTask>> task_queue;
    unordered_set<string> paused_views;
};

} // namespace duckdb
```

- [ ] **Step 2: Implement scheduler thread logic**

Create `src/scheduler/scheduler.cpp`:

```cpp
#include "scheduler/scheduler.hpp"
#include "catalog/materialized_view_catalog.hpp"
#include "executor/materializer.hpp"

namespace duckdb {

PipelineScheduler::PipelineScheduler(DatabaseInstance &db_instance) : db(db_instance) {
    scheduler_thread = std::thread(&PipelineScheduler::RunScheduler, this);
}

PipelineScheduler::~PipelineScheduler() {
    {
        lock_guard<std::mutex> lock(mutex);
        should_stop = true;
    }
    cv.notify_one();
    if (scheduler_thread.joinable()) {
        scheduler_thread.join();
    }
}

void PipelineScheduler::RunScheduler() {
    while (true) {
        unique_lock<std::mutex> lock(mutex);

        if (task_queue.empty()) {
            cv.wait(lock, [this] { return should_stop || !task_queue.empty(); });
        } else {
            auto next_time = task_queue.top().next_run;
            cv.wait_until(lock, next_time, [this, &next_time] {
                return should_stop || (!task_queue.empty() && task_queue.top().next_run <= std::chrono::system_clock::now());
            });
        }

        if (should_stop) {
            return;
        }

        // Execute all tasks that are due
        auto now = std::chrono::system_clock::now();
        while (!task_queue.empty() && task_queue.top().next_run <= now) {
            auto task = task_queue.top();
            task_queue.pop();

            if (paused_views.count(task.view_name)) {
                continue;
            }

            lock.unlock();

            // Execute refresh in a new connection
            try {
                DuckDB db_wrapper(db);
                Connection conn(db_wrapper);
                auto result = conn.Query("REFRESH MATERIALIZED VIEW " + task.view_name);
                if (result->HasError()) {
                    Printer::Print("Scheduler error refreshing '" + task.view_name + "': " + result->GetError());
                }
            } catch (std::exception &e) {
                Printer::Print("Scheduler exception refreshing '" + task.view_name + "': " + string(e.what()));
            }

            lock.lock();

            // Reschedule
            auto next = ComputeNextRun(task.view_name);
            if (next != std::chrono::system_clock::time_point{}) {
                task_queue.push({task.view_name, next});
            }
        }
    }
}

void PipelineScheduler::AddSchedule(const string &view_name) {
    lock_guard<std::mutex> lock(mutex);
    auto next = ComputeNextRun(view_name);
    if (next != std::chrono::system_clock::time_point{}) {
        task_queue.push({view_name, next});
        cv.notify_one();
    }
}

void PipelineScheduler::RemoveSchedule(const string &view_name) {
    lock_guard<std::mutex> lock(mutex);
    // Rebuild queue without the removed view
    std::priority_queue<ScheduledTask, vector<ScheduledTask>, std::greater<ScheduledTask>> new_queue;
    while (!task_queue.empty()) {
        auto task = task_queue.top();
        task_queue.pop();
        if (task.view_name != view_name) {
            new_queue.push(task);
        }
    }
    task_queue = std::move(new_queue);
}

void PipelineScheduler::PauseSchedule(const string &view_name) {
    lock_guard<std::mutex> lock(mutex);
    paused_views.insert(view_name);
}

void PipelineScheduler::ResumeSchedule(const string &view_name) {
    lock_guard<std::mutex> lock(mutex);
    paused_views.erase(view_name);
    cv.notify_one();
}

std::chrono::system_clock::time_point PipelineScheduler::ComputeNextRun(const string &view_name) {
    auto &catalog = MaterializedViewCatalog::Get(db);
    if (!catalog.Exists(view_name)) {
        return {};
    }
    auto &def = catalog.Get(view_name);

    auto now = std::chrono::system_clock::now();

    switch (def.schedule_type) {
    case ScheduleType::EVERY: {
        // Convert interval to duration
        std::chrono::seconds duration;
        string unit = StringUtil::Lower(def.schedule_interval_unit);
        if (unit == "second" || unit == "seconds") {
            duration = std::chrono::seconds(def.schedule_interval);
        } else if (unit == "minute" || unit == "minutes") {
            duration = std::chrono::seconds(def.schedule_interval * 60);
        } else if (unit == "hour" || unit == "hours") {
            duration = std::chrono::seconds(def.schedule_interval * 3600);
        } else if (unit == "day" || unit == "days") {
            duration = std::chrono::seconds(def.schedule_interval * 86400);
        } else if (unit == "week" || unit == "weeks") {
            duration = std::chrono::seconds(def.schedule_interval * 604800);
        } else {
            return {};
        }
        return now + duration;
    }
    case ScheduleType::CRON:
        // Cron parsing: use a simple cron parser or library
        // For Phase 2, implement basic cron support
        // Return next matching time based on cron expression
        return now + std::chrono::hours(1); // Placeholder — implement cron parser
    case ScheduleType::ON_UPDATE:
        // ON_UPDATE is event-driven, not time-driven
        // This is handled differently — via hooks on source table changes
        return {};
    default:
        return {};
    }
}

// Singleton — stored as static, similar to cronjob pattern
static unique_ptr<PipelineScheduler> global_scheduler;

PipelineScheduler &PipelineScheduler::Get(DatabaseInstance &db) {
    if (!global_scheduler) {
        global_scheduler = make_uniq<PipelineScheduler>(db);
    }
    return *global_scheduler;
}

} // namespace duckdb
```

- [ ] **Step 3: Start scheduler on extension load**

Update `src/hortus_pipeline_extension.cpp`:

```cpp
#include "scheduler/scheduler.hpp"

static void LoadInternal(ExtensionLoader &loader) {
    auto &db = loader.GetDatabaseInstance();
    // ... existing parser registration ...

    // Start the scheduler thread
    PipelineScheduler::Get(db);
}
```

- [ ] **Step 4: Wire CREATE to notify scheduler when SCHEDULE clause is present**

Modify `src/functions/create_materialized_view.cpp`:
After registering in catalog, if schedule_type != NONE:

```cpp
if (def.schedule_type != ScheduleType::NONE) {
    auto &scheduler = PipelineScheduler::Get(DatabaseInstance::GetDatabase(context));
    scheduler.AddSchedule(data.view_name);
}
```

- [ ] **Step 5: Write test**

Create `test/sql/scheduler.test`:

```
# name: test/sql/scheduler.test
# group: [hortus_pipeline]

require hortus_pipeline

# Create a scheduled MV
statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_scheduled
  SCHEDULE EVERY 1 HOUR
AS SELECT 1 AS id;

# Verify it appears in schedules
query II
CALL pipeline_schedules();
----
mv_scheduled	EVERY 1 HOUR

# Pause
statement ok
ALTER MATERIALIZED VIEW mv_scheduled PAUSE SCHEDULE;

# Resume
statement ok
ALTER MATERIALIZED VIEW mv_scheduled RESUME SCHEDULE;

# Drop removes from scheduler
statement ok
DROP MATERIALIZED VIEW mv_scheduled;

query II
CALL pipeline_schedules();
----
```

- [ ] **Step 6: Run tests**

```bash
make test
```

- [ ] **Step 7: Commit**

```bash
git add src/include/scheduler/scheduler.hpp src/scheduler/scheduler.cpp \
    src/hortus_pipeline_extension.cpp test/sql/scheduler.test
git commit -m "feat: implement background scheduler engine with priority queue"
```

---

### Task 13: pipeline_schedules() and pipeline_check_schedules()

**Files:**
- Create: `src/functions/pipeline_schedules.cpp`
- Modify: `test/sql/scheduler.test`

- [ ] **Step 1: Implement pipeline_schedules() table function**

Returns a table with columns:
- `name` (VARCHAR)
- `schedule` (VARCHAR) — e.g., "EVERY 1 HOUR", "CRON 0 6 * * *"
- `next_run` (TIMESTAMP)
- `paused` (BOOLEAN)

- [ ] **Step 2: Implement pipeline_check_schedules() function**

A convenience function that checks what's overdue and runs it immediately. Useful for external schedulers as a fallback.

- [ ] **Step 3: Register both functions**

Add to `LoadInternal` via `ExtensionUtil::RegisterFunction`.

- [ ] **Step 4: Run tests**

```bash
make test
```

- [ ] **Step 5: Commit**

```bash
git add src/functions/pipeline_schedules.cpp
git commit -m "feat: implement pipeline_schedules() and pipeline_check_schedules()"
```

---

### Task 14: Expectation Metrics (pipeline_expectations())

**Files:**
- Create: `src/functions/pipeline_expectations.cpp`
- Modify: `src/include/catalog/materialized_view_catalog.hpp`
- Create: `test/sql/expectation_metrics.test`

- [ ] **Step 1: Add metrics storage to catalog**

Add to `MaterializedViewDefinition`:

```cpp
struct ExpectationMetric {
    string constraint_name;
    idx_t total_rows;
    idx_t passed;
    idx_t failed;
    ExpectationAction action;
    std::chrono::system_clock::time_point last_checked;
};

// In MaterializedViewDefinition:
vector<ExpectationMetric> last_expectation_metrics;
```

Update `ExpectationChecker::ApplyExpectations` to populate these metrics and store them back in the catalog after each refresh.

- [ ] **Step 2: Implement pipeline_expectations() table function**

Returns a table with columns:
- `view_name` (VARCHAR)
- `constraint_name` (VARCHAR)
- `total_rows` (BIGINT)
- `passed` (BIGINT)
- `failed` (BIGINT)
- `action` (VARCHAR) — "WARN", "DROP ROW", "FAIL UPDATE"
- `last_checked` (TIMESTAMP)

- [ ] **Step 3: Write test**

Create `test/sql/expectation_metrics.test`:

```
# name: test/sql/expectation_metrics.test
# group: [hortus_pipeline]

require hortus_pipeline

statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_metrics
  CONSTRAINT positive_id EXPECT (id > 0)
  CONSTRAINT has_name EXPECT (name IS NOT NULL) ON VIOLATION DROP ROW
AS SELECT * FROM (VALUES (1, 'a'), (-1, 'b'), (2, NULL)) t(id, name);

# Check metrics
query IIIIII
SELECT view_name, constraint_name, total_rows, passed, failed, action
FROM pipeline_expectations()
ORDER BY constraint_name;
----
mv_metrics	has_name	3	2	1	DROP ROW
mv_metrics	positive_id	3	2	1	WARN
```

- [ ] **Step 4: Run tests**

```bash
make test
```

- [ ] **Step 5: Commit**

```bash
git add src/functions/pipeline_expectations.cpp test/sql/expectation_metrics.test
git commit -m "feat: implement pipeline_expectations() with metrics tracking"
```

---

### Task 15: Best-Effort Failure Mode

**Files:**
- Modify: `src/executor/materializer.cpp`
- Modify: `src/functions/refresh_all.cpp`
- Create: `test/sql/best_effort.test`

- [ ] **Step 1: Write test**

Create `test/sql/best_effort.test`:

```
# name: test/sql/best_effort.test
# group: [hortus_pipeline]

require hortus_pipeline

# Create a chain where middle node fails
statement ok
CREATE OR REFRESH MATERIALIZED VIEW be_source AS SELECT 1 AS id;

statement ok
CREATE OR REFRESH MATERIALIZED VIEW be_broken AS SELECT * FROM nonexistent_table;

statement ok
CREATE OR REFRESH MATERIALIZED VIEW be_independent AS SELECT 42 AS val;

# Default fail-fast: REFRESH ALL fails on be_broken
statement error
REFRESH ALL MATERIALIZED VIEWS;
----
nonexistent_table

# Best-effort mode: skip failures, continue independent branches
statement ok
REFRESH ALL MATERIALIZED VIEWS WITH (on_failure = 'best_effort');

# be_independent should still be refreshed
query I
SELECT * FROM be_independent;
----
42
```

- [ ] **Step 2: Implement best-effort mode**

Modify `Materializer::MaterializeAll` to accept a failure mode parameter. In best-effort mode:
- Wrap each `Materialize` call in try/catch
- Collect errors
- Skip dependents of failed nodes
- Continue with independent branches
- Report all errors at the end

- [ ] **Step 3: Run tests**

```bash
make test
```

- [ ] **Step 4: Commit**

```bash
git add src/executor/materializer.cpp src/functions/refresh_all.cpp test/sql/best_effort.test
git commit -m "feat: implement best-effort failure mode for REFRESH ALL"
```

---

### Task 16: EXPLAIN CREATE MATERIALIZED VIEW

**Files:**
- Modify: `src/parser/pipeline_parser.cpp`
- Create: `src/functions/explain_materialized_view.cpp`
- Create: `test/sql/explain.test`

- [ ] **Step 1: Write test**

Create `test/sql/explain.test`:

```
# name: test/sql/explain.test
# group: [hortus_pipeline]

require hortus_pipeline

statement ok
CREATE OR REFRESH MATERIALIZED VIEW ex_source AS SELECT i AS id FROM range(10) t(i);

statement ok
CREATE OR REFRESH MATERIALIZED VIEW ex_derived AS SELECT id * 2 AS x FROM ex_source;

# EXPLAIN shows the query plan + dependency info
query I
EXPLAIN CREATE MATERIALIZED VIEW ex_derived AS SELECT id * 2 AS x FROM ex_source;
----
(contains query plan and dependency information)
```

- [ ] **Step 2: Implement EXPLAIN**

The function:
1. Parses the same as CREATE but doesn't execute
2. Runs `EXPLAIN` on the inner query
3. Runs `DAGResolver::ExtractDependencies` to show dependencies
4. Returns combined output

- [ ] **Step 3: Run tests**

```bash
make test
```

- [ ] **Step 4: Commit**

```bash
git add src/functions/explain_materialized_view.cpp src/parser/pipeline_parser.cpp \
    test/sql/explain.test
git commit -m "feat: implement EXPLAIN CREATE MATERIALIZED VIEW"
```

---

### Task 17: Integration Test

**Files:**
- Create: `test/sql/integration.test`

- [ ] **Step 1: Write end-to-end integration test**

Create `test/sql/integration.test`:

```
# name: test/sql/integration.test
# group: [hortus_pipeline]

require hortus_pipeline

# Full pipeline: source → filter → aggregate with expectations

# 1. Create source MV from inline data
statement ok
CREATE OR REFRESH MATERIALIZED VIEW orders
  CONSTRAINT valid_amount EXPECT (amount > 0)
  CONSTRAINT valid_region EXPECT (region IS NOT NULL) ON VIOLATION DROP ROW
AS SELECT * FROM (VALUES
  (1, 'US', 100.0),
  (2, 'EU', 200.0),
  (3, NULL, 50.0),
  (4, 'US', -10.0),
  (5, 'EU', 300.0)
) t(id, region, amount);

# NULL region row was dropped
query I
SELECT COUNT(*) FROM orders;
----
4

# 2. Create derived MV
statement ok
CREATE OR REFRESH MATERIALIZED VIEW us_orders AS
  SELECT * FROM orders WHERE region = 'US';

query I
SELECT COUNT(*) FROM us_orders;
----
2

# 3. Create aggregate MV
statement ok
CREATE OR REFRESH MATERIALIZED VIEW revenue_by_region
  DEPENDS ON (orders)
AS SELECT region, SUM(amount) AS total FROM orders GROUP BY region;

query II
SELECT * FROM revenue_by_region ORDER BY region;
----
EU	500.0
US	90.0

# 4. Check DAG
query IIIII
CALL pipeline_status();
----
(shows all 3 MVs with dependencies)

# 5. ALTER a definition
statement ok
ALTER MATERIALIZED VIEW us_orders AS SELECT * FROM orders WHERE region = 'US' AND amount > 0;

# 6. REFRESH to apply
statement ok
REFRESH MATERIALIZED VIEW us_orders;

query I
SELECT COUNT(*) FROM us_orders;
----
1

# 7. REFRESH ALL
statement ok
REFRESH ALL MATERIALIZED VIEWS;

# 8. DROP
statement ok
DROP MATERIALIZED VIEW revenue_by_region;
DROP MATERIALIZED VIEW us_orders;
DROP MATERIALIZED VIEW orders;
```

- [ ] **Step 2: Run full test suite**

```bash
make test
```

Expected: All tests pass.

- [ ] **Step 3: Commit**

```bash
git add test/sql/integration.test
git commit -m "test: add end-to-end integration test for full pipeline"
```
