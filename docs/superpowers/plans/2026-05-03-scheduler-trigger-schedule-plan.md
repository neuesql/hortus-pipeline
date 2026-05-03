# Scheduler Trigger='schedule' Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Background scheduler thread writes `trigger='schedule'` to `__pipeline__.run_logs` instead of `'manual'`.

**Architecture:** Replace the SQL roundtrip (`conn.Query("REFRESH MATERIALIZED VIEW <name>")`) inside `PipelineScheduler::RunScheduler` with a direct `Materializer::Materialize` call that passes `"schedule"` as the trigger string. `Connection` already exposes a `ClientContext` via its `context` member, satisfying `Materializer`'s parameter type without any new public API.

**Tech Stack:** C++ (DuckDB extension), SQLLogic tests (`.test` files run by `make test`).

**Spec:** `docs/superpowers/specs/2026-05-03-scheduler-trigger-schedule-design.md`

---

## File Structure

| File | Change |
|---|---|
| `src/scheduler/scheduler.cpp` | Replace SQL refresh block (lines 144-150) with direct Materializer call; add 2 includes |
| `test/sql/scheduler_trigger_value.test` | New test file |

No header changes. No new symbols.

---

### Task 1: Write the failing test

**Files:**
- Create: `test/sql/scheduler_trigger_value.test`

- [ ] **Step 1: Create the test file**

```
# name: test/sql/scheduler_trigger_value.test
# group: [sql]

require hortus_pipeline

# Background scheduler should log trigger='schedule' for fired refreshes.

statement ok
CREATE OR REFRESH MATERIALIZED VIEW sched_trig_test SCHEDULE EVERY 2 SECOND AS SELECT 1 AS id;

# Wait for the scheduler thread to fire at least once.
sleep 4 seconds

# At least one row in run_logs for this view must have trigger='schedule'.
query I
SELECT COUNT(*) FROM pipeline_run_logs() WHERE view_name = 'sched_trig_test' AND trigger = 'schedule';
----
1

# A separate manual REFRESH still logs trigger='manual'.
statement ok
REFRESH MATERIALIZED VIEW sched_trig_test;

query I
SELECT COUNT(*) FROM pipeline_run_logs() WHERE view_name = 'sched_trig_test' AND trigger = 'manual';
----
1

# Cleanup
statement ok
DROP MATERIALIZED VIEW sched_trig_test;
```

(`COUNT(*) = 1` for the schedule branch is a lower bound — the test does not assert exactness because the scheduler may fire multiple times in the 4-second window. The test just requires at least one schedule-tagged row exists.)

> Note: `COUNT(*) = 1` would fail if the scheduler fires twice. Use `>= 1` semantics by checking the count is at least 1. Adjust:

Replace the first `query I ... ---- 1` block with:

```
query I
SELECT COUNT(*) >= 1 FROM pipeline_run_logs() WHERE view_name = 'sched_trig_test' AND trigger = 'schedule';
----
true
```

And the second block similarly:

```
query I
SELECT COUNT(*) >= 1 FROM pipeline_run_logs() WHERE view_name = 'sched_trig_test' AND trigger = 'manual';
----
true
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
make test_release ARGS='test/sql/scheduler_trigger_value.test'
```

If `ARGS` is not honored, run the test binary directly:

```bash
build/release/test/unittest --test-dir . "[sql]" '*scheduler_trigger_value*'
```

Expected: FAIL — the schedule-trigger row count returns 0 because the scheduler currently logs `trigger='manual'`. (The first assertion will fail because no row has `trigger='schedule'`.)

- [ ] **Step 3: Commit the failing test**

```bash
git add test/sql/scheduler_trigger_value.test
git commit -m "test: failing test for scheduler trigger='schedule'"
```

---

### Task 2: Replace SQL refresh with direct Materializer call

**Files:**
- Modify: `src/scheduler/scheduler.cpp:1-5` (add includes)
- Modify: `src/scheduler/scheduler.cpp:144-150` (replace block)

- [ ] **Step 1: Add the two includes at the top of `scheduler.cpp`**

After the existing `#include "duckdb/main/connection.hpp"` line, insert:

```cpp
#include "executor/materializer.hpp"
#include "executor/dag_resolver.hpp"
```

The top of the file will then read:

```cpp
#include "scheduler/scheduler.hpp"
#include "persistence/pipeline_persistence.hpp"
#include "duckdb/main/connection.hpp"
#include "executor/materializer.hpp"
#include "executor/dag_resolver.hpp"
```

- [ ] **Step 2: Replace the refresh block (lines 144-150)**

Find this exact block in `RunScheduler`:

```cpp
		// Execute refresh
		try {
			Connection conn(db);
			conn.Query("REFRESH MATERIALIZED VIEW " + top.view_name);
			persistence.UpdateScheduleLastRun(db, top.database, top.name);
		} catch (...) {
		}
```

Replace it with:

```cpp
		// Execute refresh
		try {
			Connection conn(db);
			auto order = DAGResolver::ResolveFor(db, top.name, top.database);
			for (auto &dep_name : order) {
				Materializer::Materialize(*conn.context, dep_name, "schedule");
			}
			persistence.UpdateScheduleLastRun(db, top.database, top.name);
		} catch (...) {
		}
```

- [ ] **Step 3: Build the extension (release)**

```bash
make
```

Expected: the build completes successfully with no compiler errors. (First-time builds may take several minutes.)

- [ ] **Step 4: Run the new test and verify it passes**

```bash
build/release/test/unittest --test-dir . "[sql]" '*scheduler_trigger_value*'
```

Expected: PASS.

- [ ] **Step 5: Run the full test suite**

```bash
make test
```

Expected: all tests pass. The change is invisible to non-scheduler tests; only behavior change is the trigger string in scheduled runs.

- [ ] **Step 6: Commit**

```bash
git add src/scheduler/scheduler.cpp
git commit -m "fix: scheduler logs trigger='schedule' for background refreshes"
```

---

## Self-Review

- **Spec coverage:** "Goal" → Tasks 1-2; "Edge cases — Materialize throws" → preserved by existing `catch (...)` in Task 2 Step 2; "Trigger semantics across paths" → manual REFRESH still uses `"manual"` (untouched in `refresh_materialized_view.cpp`), `pipeline_fires()` still uses `"schedule"` (untouched in `pipeline_schedules.cpp`). Test 2's manual-REFRESH assertion guards the first; no test changes needed for the second.
- **Placeholders:** none.
- **Type consistency:** `Materializer::Materialize(ClientContext &, const string &, const string &)` matches the declaration in `src/include/executor/materializer.hpp`. `DAGResolver::ResolveFor(DatabaseInstance &, const string &target, const string &database)` matches `src/include/executor/dag_resolver.hpp` (default `database=""`).
