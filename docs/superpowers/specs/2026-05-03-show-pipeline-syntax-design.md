# `SHOW pipeline_*()` strict syntactic sugar for monitoring functions

**Date:** 2026-05-03
**Scope:** Parser extension only. Rewrites `SHOW <name>()` to `SELECT * FROM <name>()` for five whitelisted monitoring functions.

## Goal

`SHOW pipeline_status()` is exactly equivalent to `SELECT * FROM pipeline_status()`. The same applies to four other monitoring functions. The existing `SELECT * FROM ...` form continues to work unchanged.

The SHOW form is strict: nothing is allowed after the closing paren. If the user wants `WHERE`, `LIMIT`, `ORDER BY`, etc., they use the `SELECT` form.

## Whitelist

| Function | SHOW form |
|---|---|
| `pipeline_status()` | `SHOW pipeline_status()` |
| `pipeline_expectations()` | `SHOW pipeline_expectations()` |
| `pipeline_schedules()` | `SHOW pipeline_schedules()` |
| `pipeline_run_logs()` | `SHOW pipeline_run_logs()` |
| `pipeline_expectation_logs()` | `SHOW pipeline_expectation_logs()` |

`pipeline_fires()` (a side-effect function that fires schedules) is intentionally not in this set — `SHOW` is intended for read-only inspection, so firing requires the explicit `SELECT * FROM pipeline_fires()` form.

## Non-goals

- `WHERE`, `LIMIT`, `ORDER BY`, or any other clause after the function call.
- Schema-qualified names like `SHOW main.pipeline_status()`.
- Quoted identifiers like `SHOW "pipeline_status"()`.
- Adding `SHOW <name>` (no parens) leniency — strict form requires `()`.
- Anything other than the 5 whitelisted functions.

Any of these falls through to DuckDB's native SHOW parser, which will produce its standard error.

## Approach

Hook into the existing `PipelineParserOverride` (`src/parser/pipeline_parser.cpp:815`) — same place that intercepts `DROP MATERIALIZED VIEW`, `ALTER`, and `EXPLAIN` for our extension. Detect the exact 4-token shape `SHOW <name> ( )`, rewrite to `SELECT * FROM <name>()`, and re-parse the rewritten string with DuckDB's stock `Parser::ParseQuery`. Return the resulting statements via `ParserOverrideResult`.

This is preferable to using `parse_function` (which only fires when DuckDB's native parser rejects a query) because:

- The override runs first, deterministically.
- The rewritten SELECT goes through DuckDB's normal binder/optimizer, so we don't reimplement anything.

## Code change

Inside `PipelineParserOverride` in `src/parser/pipeline_parser.cpp`, before the existing `is_ours` block:

```cpp
if (StringUtil::StartsWith(upper, "SHOW PIPELINE_")) {
    static const unordered_set<string> show_targets = {
        "PIPELINE_STATUS",
        "PIPELINE_EXPECTATIONS",
        "PIPELINE_SCHEDULES",
        "PIPELINE_RUN_LOGS",
        "PIPELINE_EXPECTATION_LOGS",
    };
    auto tokens = Tokenize(trimmed);
    // Strict shape: ["SHOW", <name>, "(", ")"] — exactly 4 tokens
    if (tokens.size() == 4 &&
        StringUtil::Upper(tokens[0]) == "SHOW" &&
        show_targets.count(StringUtil::Upper(tokens[1])) &&
        tokens[2] == "(" && tokens[3] == ")") {
        string rewritten = "SELECT * FROM " + tokens[1] + "()";
        try {
            Parser parser;
            parser.ParseQuery(rewritten);
            return ParserOverrideResult(std::move(parser.statements));
        } catch (std::exception &e) {
            return ParserOverrideResult(e);
        }
    }
    // Any other SHOW pipeline_* shape — fall through to native parser
}
```

Add `#include "duckdb/parser/parser.hpp"`.

The existing `Tokenize` helper (lines 24-98 of the same file) already strips trailing semicolons and handles whitespace, so `SHOW pipeline_status();`, `SHOW  pipeline_status ( )`, and similar all resolve to the same 4 tokens.

## Behavior matrix

| Form | Behavior |
|---|---|
| `SELECT * FROM pipeline_status()` | unchanged |
| `SELECT * FROM pipeline_status() WHERE name = 'x'` | unchanged |
| `SHOW pipeline_status()` | **new** — rewritten to `SELECT * FROM pipeline_status()` |
| `Show Pipeline_Status()` (mixed case) | matched (case-insensitive) — same rewrite |
| `SHOW pipeline_status();` | matched — trailing `;` already stripped by `Tokenize` |
| `SHOW pipeline_status() WHERE name = 'x'` | not matched (extra tokens) → DuckDB native error |
| `SHOW pipeline_status` | not matched (missing parens) → DuckDB native error |
| `SHOW pipeline_fires()` | not in whitelist → DuckDB native error |
| `SHOW main.pipeline_status()` | not matched (schema-qualified token doesn't match whitelist) → DuckDB native error |
| `SHOW TABLES`, `SHOW <table>` | unchanged — DuckDB native SHOW |

## Files touched

- `src/parser/pipeline_parser.cpp` — add the SHOW intercept block in `PipelineParserOverride`; add `#include "duckdb/parser/parser.hpp"`.
- `test/sql/show_monitoring.test` — new test.

## Tests

New `test/sql/show_monitoring.test`:

1. **Equivalence for each whitelisted function** — set up a small pipeline, then assert that `SHOW <name>()` returns the same rows (and same column count) as `SELECT * FROM <name>()`. Repeat for all 5.
2. **Mixed case** — `Show Pipeline_Status()` works.
3. **Trailing semicolon** — `SHOW pipeline_status();` works.
4. **Strictness — extra clause errors** — `SHOW pipeline_status() WHERE name = 'x'` produces an error (not silent success).
5. **Strictness — no parens errors** — `SHOW pipeline_status` produces an error.
6. **Non-whitelisted function falls through** — `SHOW pipeline_fires()` produces an error.
7. **Native SHOW unaffected** — `SHOW TABLES` still works.
