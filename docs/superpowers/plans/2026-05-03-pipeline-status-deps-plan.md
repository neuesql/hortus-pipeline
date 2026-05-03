# `pipeline_status()` Auto-Detected Dependencies (Lazy) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `pipeline_status()` displays the same MV-filtered dependencies that `DAGResolver::Resolve` uses at refresh time. Auto-detected dependencies (extracted from FROM/JOIN clauses) become visible without any schema change.

**Architecture:** Extract the explicit-vs-auto resolution rule into a small static helper `DAGResolver::ResolveEffectiveDeps`. `Resolve()` and `ResolveFor()` are refactored to call it (eliminating two duplicate either-or blocks). `pipeline_status()` is restructured from a single UNION ALL to per-database iteration so we know each row's database, then calls the helper to compute deps lazily and serialize to a comma-separated string.

**Tech Stack:** C++ (DuckDB extension), SQLLogic tests.

**Spec:** `docs/superpowers/specs/2026-05-03-pipeline-status-deps-design.md`

**Sequencing note:** This plan assumes the rename (`docs/superpowers/plans/2026-05-03-rename-system-tables-plan.md`) has already landed. The internal table is `__pipeline__.materialized_views`. If you are executing this plan against pre-rename code, substitute `materialized_views` → `views` in the SQL strings.

---

## File Structure

| File | Change |
|---|---|
| `src/include/executor/dag_resolver.hpp` | Add `ResolveEffectiveDeps` declaration |
| `src/executor/dag_resolver.cpp` | Implement helper; refactor `Resolve()` and `ResolveFor()` to call it |
| `src/functions/pipeline_status.cpp` | Restructure init from single UNION ALL to per-database iteration; compute and serialize effective deps |
| `test/sql/pipeline_status_auto_deps.test` | New test |

No public API changes other than the new static helper.

---

### Task 1: Write the failing test reproducing the user's example

**Files:**
- Create: `test/sql/pipeline_status_auto_deps.test`

- [ ] **Step 1: Create the test file**

```
# name: test/sql/pipeline_status_auto_deps.test
# group: [sql]

require hortus_pipeline

# Setup: source table.
statement ok
CREATE TABLE raw_orders(amount DOUBLE, region VARCHAR);

# 4-MV chain with auto-detected deps.
statement ok
CREATE OR REFRESH MATERIALIZED VIEW orders AS
  SELECT * FROM raw_orders WHERE amount > 0 AND region IS NOT NULL;

statement ok
CREATE OR REFRESH MATERIALIZED VIEW us_orders AS
  SELECT * FROM orders WHERE region = 'US';

statement ok
CREATE OR REFRESH MATERIALIZED VIEW eu_orders AS
  SELECT * FROM orders WHERE region = 'EU';

statement ok
CREATE OR REFRESH MATERIALIZED VIEW us_revenue AS
  SELECT SUM(amount) AS total FROM us_orders;

# pipeline_status() must show MV-filtered deps for each view.
query II
SELECT name, dependencies FROM pipeline_status() ORDER BY name;
----
eu_orders	orders
orders	(empty)
us_orders	orders
us_revenue	us_orders
```

(In sqllogic, an empty value renders as `(empty)` — verify by running the test once and observing the actual rendering; if the runner uses an empty string instead, replace `(empty)` with the empty string. This format detail is purely test-cosmetic.)

- [ ] **Step 2: Add three more sub-cases as separate query blocks**

Append to the same file:

```
# Test: filter non-MV table refs — raw_orders is a regular table, must not appear.
query I
SELECT COUNT(*) FROM pipeline_status() WHERE dependencies LIKE '%raw_orders%';
----
0

# Test: explicit DEPENDS ON takes precedence over auto-detect.
statement ok
CREATE OR REFRESH MATERIALIZED VIEW dep_explicit
  DEPENDS ON (orders)
AS SELECT * FROM us_orders;

query I
SELECT dependencies FROM pipeline_status() WHERE name = 'dep_explicit';
----
orders

# Test: explicit DEPENDS ON listing a non-MV is filtered out.
statement ok
CREATE OR REFRESH MATERIALIZED VIEW dep_dangling
  DEPENDS ON (not_a_view)
AS SELECT 1 AS x;

query I
SELECT dependencies FROM pipeline_status() WHERE name = 'dep_dangling';
----
(empty)

# Cleanup
statement ok
DROP MATERIALIZED VIEW dep_dangling;

statement ok
DROP MATERIALIZED VIEW dep_explicit;

statement ok
DROP MATERIALIZED VIEW us_revenue;

statement ok
DROP MATERIALIZED VIEW eu_orders;

statement ok
DROP MATERIALIZED VIEW us_orders;

statement ok
DROP MATERIALIZED VIEW orders;
```

- [ ] **Step 3: Run the test to verify it fails**

```bash
build/release/test/unittest --test-dir . "[sql]" '*pipeline_status_auto_deps*'
```

Expected: FAIL — first query returns rows with empty `dependencies` for all four views, because `pipeline_status()` reads the stored column directly and auto-detected deps aren't stored.

- [ ] **Step 4: Commit the failing test**

```bash
git add test/sql/pipeline_status_auto_deps.test
git commit -m "test: failing test for pipeline_status auto-detected dependencies"
```

---

### Task 2: Add `DAGResolver::ResolveEffectiveDeps` helper

**Files:**
- Modify: `src/include/executor/dag_resolver.hpp`
- Modify: `src/executor/dag_resolver.cpp`

- [ ] **Step 1: Declare the helper in the header**

In `dag_resolver.hpp`, add to the `DAGResolver` class (under `public:`):

```cpp
	static vector<string> ResolveEffectiveDeps(const MaterializedViewDefinition &def,
	                                            const unordered_set<string> &mv_set);
```

The header should now read:
```cpp
class DAGResolver {
public:
	static vector<string> Resolve(DatabaseInstance &db, const string &database = "");
	static vector<string> ResolveFor(DatabaseInstance &db, const string &target, const string &database = "");
	static vector<string> ExtractDependencies(const string &query);
	static vector<string> ResolveEffectiveDeps(const MaterializedViewDefinition &def,
	                                            const unordered_set<string> &mv_set);
};
```

(`MaterializedViewDefinition` and `unordered_set` are already available via existing includes in this header — `pipeline_persistence.hpp` is included, and `<unordered_set>` is a transitive include via DuckDB headers; if the build complains, add `#include <unordered_set>` to the header.)

- [ ] **Step 2: Implement the helper in the cpp**

In `dag_resolver.cpp`, after the `ExtractDependencies` definition (around line 38), add:

```cpp
vector<string> DAGResolver::ResolveEffectiveDeps(const MaterializedViewDefinition &def,
                                                  const unordered_set<string> &mv_set) {
	vector<string> raw;
	if (!def.explicit_dependencies.empty()) {
		raw = def.explicit_dependencies;
	} else {
		raw = ExtractDependencies(def.query);
	}
	vector<string> result;
	for (auto &dep : raw) {
		if (mv_set.count(dep) > 0 && dep != def.name) {
			result.push_back(dep);
		}
	}
	return result;
}
```

- [ ] **Step 3: Refactor `Resolve()` to call the helper**

Find the block (lines 53-66 of the current file):
```cpp
	for (auto &name : all_names) {
		auto def = persistence.GetView(db, database, name);
		vector<string> deps;
		if (!def.explicit_dependencies.empty()) {
			deps = def.explicit_dependencies;
		} else {
			deps = ExtractDependencies(def.query);
		}

		for (auto &dep : deps) {
			if (mv_set.count(dep) > 0 && dep != name) {
				dependents[dep].push_back(name);
				in_degree[name]++;
			}
		}
	}
```

Replace with:
```cpp
	for (auto &name : all_names) {
		auto def = persistence.GetView(db, database, name);
		auto deps = ResolveEffectiveDeps(def, mv_set);
		for (auto &dep : deps) {
			dependents[dep].push_back(name);
			in_degree[name]++;
		}
	}
```

- [ ] **Step 4: Refactor `ResolveFor()` to call the helper**

Find the block (lines 107-122):
```cpp
	unordered_map<string, vector<string>> dep_map;
	for (auto &name : all_names) {
		auto def = persistence.GetView(db, database, name);
		vector<string> deps;
		if (!def.explicit_dependencies.empty()) {
			deps = def.explicit_dependencies;
		} else {
			deps = ExtractDependencies(def.query);
		}
		vector<string> mv_deps;
		for (auto &d : deps) {
			if (mv_set.count(d) > 0 && d != name) {
				mv_deps.push_back(d);
			}
		}
		dep_map[name] = mv_deps;
	}
```

Replace with:
```cpp
	unordered_map<string, vector<string>> dep_map;
	for (auto &name : all_names) {
		auto def = persistence.GetView(db, database, name);
		dep_map[name] = ResolveEffectiveDeps(def, mv_set);
	}
```

- [ ] **Step 5: Build and run existing tests**

```bash
make
make test
```

Expected: all existing tests still pass. The refactor is behaviorally identical to the previous either-or blocks.

- [ ] **Step 6: Commit**

```bash
git add src/include/executor/dag_resolver.hpp src/executor/dag_resolver.cpp
git commit -m "refactor: extract ResolveEffectiveDeps helper in DAGResolver"
```

---

### Task 3: Restructure `pipeline_status()` to compute effective deps lazily

**Files:**
- Modify: `src/functions/pipeline_status.cpp` (the global state struct, init function, and emit function)

The current init builds one big UNION ALL query. We replace that with per-database iteration that retains the database-of-origin so we can compute deps against the correct MV set.

- [ ] **Step 1: Replace the global state struct**

Around line 13-16, find:
```cpp
struct PipelineStatusGlobalState : public GlobalTableFunctionState {
	idx_t offset = 0;
	unique_ptr<MaterializedQueryResult> result;
};
```

Replace with:
```cpp
struct PipelineStatusRow {
	string name;
	string query;
	string dependencies;
	bool is_materialized;
	string comment;
};

struct PipelineStatusGlobalState : public GlobalTableFunctionState {
	idx_t offset = 0;
	vector<PipelineStatusRow> rows;
};
```

- [ ] **Step 2: Rewrite `PipelineStatusInit`**

Find the entire `PipelineStatusInit` function (around lines 36-63). Replace its body with:

```cpp
static unique_ptr<GlobalTableFunctionState> PipelineStatusInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<PipelineStatusGlobalState>();

	auto &db = DatabaseInstance::GetDatabase(context);
	auto &persistence = PipelinePersistence::Get();

	auto databases = persistence.GetAllPipelineDatabases(db);
	if (databases.empty()) {
		persistence.EnsureInitialized(db);
		databases.push_back("");
	}

	for (auto &raw_db : databases) {
		string database = (raw_db == "memory") ? "" : raw_db;
		auto all_names = persistence.GetAllNames(db, database);
		unordered_set<string> mv_set(all_names.begin(), all_names.end());

		for (auto &name : all_names) {
			auto def = persistence.GetView(db, database, name);
			auto deps = DAGResolver::ResolveEffectiveDeps(def, mv_set);

			string deps_str;
			for (idx_t i = 0; i < deps.size(); i++) {
				if (i > 0) deps_str += ",";
				deps_str += deps[i];
			}

			PipelineStatusRow row;
			row.name = def.name;
			row.query = def.query;
			row.dependencies = deps_str;
			row.is_materialized = def.is_materialized;
			row.comment = def.comment;
			state->rows.push_back(std::move(row));
		}
	}

	return std::move(state);
}
```

- [ ] **Step 3: Rewrite `PipelineStatusFunc` to read from `state->rows`**

Find `PipelineStatusFunc` (around lines 65-84). Replace with:

```cpp
static void PipelineStatusFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<PipelineStatusGlobalState>();

	if (state.offset >= state.rows.size()) {
		return;
	}

	idx_t count = 0;
	idx_t max_count = STANDARD_VECTOR_SIZE;

	while (state.offset < state.rows.size() && count < max_count) {
		auto &row = state.rows[state.offset];
		output.SetValue(0, count, Value(row.name));
		output.SetValue(1, count, Value(row.query));
		output.SetValue(2, count, Value(row.dependencies));
		output.SetValue(3, count, Value::BOOLEAN(row.is_materialized));
		output.SetValue(4, count, row.comment.empty() ? Value(LogicalType::VARCHAR) : Value(row.comment));
		state.offset++;
		count++;
	}

	output.SetCardinality(count);
}
```

(For the `comment` column we emit a NULL if empty — matches the previous behavior of reading directly from the table where `comment VARCHAR` permits NULL. If the previous behavior was to emit empty-string for empty comment, simplify to `Value(row.comment)`.)

- [ ] **Step 4: Verify includes**

At the top of `pipeline_status.cpp`, the file already includes `executor/dag_resolver.hpp` (line 7) and `persistence/pipeline_persistence.hpp` (line 6). Add `<unordered_set>` if it isn't transitively included:

```cpp
#include <unordered_set>
```

(Place it near the top with the other includes; if the build complains about `unordered_set` it is needed.)

- [ ] **Step 5: Build**

```bash
make
```

Expected: success.

- [ ] **Step 6: Run the new test**

```bash
build/release/test/unittest --test-dir . "[sql]" '*pipeline_status_auto_deps*'
```

Expected: PASS — all assertions in `test/sql/pipeline_status_auto_deps.test` succeed.

- [ ] **Step 7: Run the full test suite**

```bash
make test
```

Expected: all tests pass. Confirm `pipeline_status.test` (the existing one) still passes — its assertions about explicit DEPENDS ON should be unaffected since the helper reproduces the prior behavior for that case.

- [ ] **Step 8: Commit**

```bash
git add src/functions/pipeline_status.cpp
git commit -m "feat: pipeline_status() shows auto-detected dependencies (lazy)"
```

---

## Self-Review

- **Spec coverage:**
  - "Shared helper" → Task 2 Steps 1-2.
  - "Resolve and ResolveFor refactored to call helper" → Task 2 Steps 3-4.
  - "pipeline_status restructured to per-database iteration" → Task 3 Step 2.
  - "Output schema unchanged" → Task 3 Steps 1-3 keep the same five-column emit (name, query, dependencies, is_materialized, comment).
  - "Comma-separated, no spaces" format → Task 3 Step 2 uses `","` separator with no spaces.
  - "Order follows helper" → helper preserves the order it computes (regex match order or explicit-list order).
  - Edge cases (non-MV refs filtered, explicit takes precedence, dangling explicit dep filtered) → Task 1 Step 2 includes assertions for each.
- **Placeholders:** none.
- **Type consistency:** `ResolveEffectiveDeps(const MaterializedViewDefinition &, const unordered_set<string> &)` matches between header (Task 2 Step 1) and cpp (Task 2 Step 2). The struct `PipelineStatusRow` is defined and consumed only inside `pipeline_status.cpp` (Tasks 3 Steps 1-3) — no cross-file type leakage. The signature `Materializer::Materialize` is not used in this plan; relevant only that `MaterializedViewDefinition` is the existing type from `pipeline_persistence.hpp`.
- **Refactor risk:** Task 2 Step 5 runs the existing test suite *before* Task 3 changes pipeline_status — this catches any behavior drift in `Resolve` or `ResolveFor` early.
