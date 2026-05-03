# Scheduler logs `trigger='schedule'` for background-fired refreshes

**Date:** 2026-05-03
**Scope:** `PipelineScheduler::RunScheduler` only.

## Problem

The background scheduler thread runs scheduled materialized-view refreshes via SQL:

```cpp
// src/scheduler/scheduler.cpp:146-147
Connection conn(db);
conn.Query("REFRESH MATERIALIZED VIEW " + top.view_name);
```

The `REFRESH MATERIALIZED VIEW` table function (`refresh_materialized_view.cpp:51`) calls `Materializer::Materialize(context, name, "manual")`. The trigger string is hard-coded to `"manual"`, so background scheduled runs are logged in `__pipeline__.run_logs` indistinguishably from user-typed manual refreshes.

`pipeline_fires()` (the manual force-fire entry point) already passes `"schedule"` correctly (`pipeline_schedules.cpp:168`), so only the background scheduler thread is affected.

## Goal

A refresh fired by the background scheduler writes `trigger='schedule'` to `__pipeline__.run_logs`. Manual user invocations continue to write `trigger='manual'`. `pipeline_fires()` continues to write `trigger='schedule'`.

## Non-goals

- Changing `RefreshMVFunc` (manual `REFRESH MATERIALIZED VIEW foo` should remain `'manual'`).
- Changing the `run_logs` schema or any other trigger-string consumers.
- Changing the scheduler's threading, queue, or pause semantics.

## Design

Replace the SQL roundtrip in `PipelineScheduler::RunScheduler` with a direct call to `Materializer::Materialize`, mirroring the dependency-resolution behavior of `RefreshMVFunc` but with `trigger="schedule"`.

`Materializer::Materialize` takes a `ClientContext &`. `Connection` already exposes one as `conn.context` (a `shared_ptr<ClientContext>`), so no new public API is needed.

### Code change

In `src/scheduler/scheduler.cpp`, replace lines 144-150:

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

This preserves:

- Dependency resolution (so that scheduling a leaf view still refreshes its prerequisites).
- The existing `try/catch (...)` that prevents scheduler-thread exceptions from killing the loop.
- The `UpdateScheduleLastRun` call.

Add `#include "executor/materializer.hpp"` and `#include "executor/dag_resolver.hpp"` to `scheduler.cpp`.

### Trigger semantics across paths

| Caller | Trigger string |
|---|---|
| User runs `REFRESH MATERIALIZED VIEW foo` | `"manual"` (unchanged) |
| User runs `REFRESH ALL MATERIALIZED VIEWS` | `"refresh_all"` (unchanged, default of `MaterializeAll`) |
| User calls `SELECT * FROM pipeline_fires()` | `"schedule"` (unchanged) |
| Background scheduler thread | `"schedule"` (changed from `"manual"`) |
| Initial materialization in `CREATE OR REFRESH MATERIALIZED VIEW` | `"manual"` (unchanged — default of `Materializer::Materialize`) |

## Edge cases

| Case | Behavior |
|---|---|
| Scheduled view depends on another MV | All MVs in the resolved chain log `trigger='schedule'` for that fire. The scheduler caused the cascade, so this is correct. |
| View dropped between fire and execute | Existing `persistence.Exists` check at `scheduler.cpp:129-132` skips; unchanged. |
| Materialize throws | Caught by existing `catch (...)`. `Materializer` is responsible for writing the failure run-log row via `InsertRunLog`. |
| Schedule paused mid-fire | Existing pause check at `scheduler.cpp:134-140` runs before this block; unchanged. |

## Files touched

- `src/scheduler/scheduler.cpp` — swap SQL refresh for direct Materializer call; add includes.
- `test/sql/scheduler_trigger_value.test` — new test file.

## Tests

New `test/sql/scheduler_trigger_value.test`:

1. **Background scheduler logs `'schedule'`** — create an MV scheduled `EVERY 1 SECOND`, sleep ~2s, query `pipeline_run_logs()`, assert at least one row for that view has `trigger='schedule'`.
2. **Manual REFRESH still logs `'manual'`** — invoke `REFRESH MATERIALIZED VIEW foo` directly; assert the resulting run-log row has `trigger='manual'`.
3. **pipeline_fires() regression** — call `SELECT * FROM pipeline_fires()`, assert resulting run-log row has `trigger='schedule'` (was already correct; guard against accidental future change).

Existing scheduler tests must continue to pass.
