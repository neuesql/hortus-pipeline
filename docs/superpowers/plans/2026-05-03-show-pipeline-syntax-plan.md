# `SHOW pipeline_*()` Strict Syntactic Sugar Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `SHOW pipeline_status()` (and four other monitoring functions) work as exact equivalents of `SELECT * FROM <name>()`. The strict shape is `SHOW <name>()` only — no `WHERE`, `LIMIT`, or other clauses are accepted.

**Architecture:** Add a new branch in `PipelineParserExtension::PipelineParserOverride` ahead of the existing override block. When the input matches the strict 4-token shape `["SHOW", <whitelisted_name>, "(", ")"]`, rewrite to `SELECT * FROM <name>()` and re-parse with DuckDB's stock `Parser::ParseQuery`. Anything else falls through to DuckDB's native parser, which produces its standard error.

**Tech Stack:** C++ (DuckDB extension), SQLLogic tests.

**Spec:** `docs/superpowers/specs/2026-05-03-show-pipeline-syntax-design.md`

---

## File Structure

| File | Change |
|---|---|
| `src/parser/pipeline_parser.cpp` | Add SHOW intercept block in `PipelineParserOverride`; add `#include "duckdb/parser/parser.hpp"` |
| `test/sql/show_monitoring.test` | New test |

No header changes. No new public symbols.

---

### Task 1: Write the failing test

**Files:**
- Create: `test/sql/show_monitoring.test`

- [ ] **Step 1: Create the test file**

```
# name: test/sql/show_monitoring.test
# group: [sql]

require hortus_pipeline

# Setup: a tiny pipeline so the monitoring tables have something to show.
statement ok
CREATE OR REFRESH MATERIALIZED VIEW mv_show_test
  CONSTRAINT positive_id EXPECT (id > 0)
  SCHEDULE EVERY 1 HOUR
AS SELECT 1 AS id;

# 1. SHOW pipeline_status() returns the same row count as SELECT * FROM pipeline_status().
query I
SELECT (SELECT COUNT(*) FROM pipeline_status()) = (SELECT COUNT(*) FROM (SHOW pipeline_status()));
----
true

# 2. Same for the other four whitelisted functions.
query I
SELECT (SELECT COUNT(*) FROM pipeline_expectations()) = (SELECT COUNT(*) FROM (SHOW pipeline_expectations()));
----
true

query I
SELECT (SELECT COUNT(*) FROM pipeline_schedules()) = (SELECT COUNT(*) FROM (SHOW pipeline_schedules()));
----
true

query I
SELECT (SELECT COUNT(*) FROM pipeline_run_logs()) = (SELECT COUNT(*) FROM (SHOW pipeline_run_logs()));
----
true

query I
SELECT (SELECT COUNT(*) FROM pipeline_expectation_logs()) = (SELECT COUNT(*) FROM (SHOW pipeline_expectation_logs()));
----
true

# 3. Mixed case works.
query I
SELECT COUNT(*) > 0 FROM (Show Pipeline_Status());
----
true

# 4. Trailing semicolon works (sqllogic strips the trailing ; on its own; this verifies the parser handles whitespace).
statement ok
SHOW pipeline_status();

# 5. Strictness: extra clause is rejected.
statement error
SHOW pipeline_status() WHERE name = 'mv_show_test';
----

# 6. Strictness: missing parens is rejected.
statement error
SHOW pipeline_status;
----

# 7. Non-whitelisted pipeline function is rejected.
statement error
SHOW pipeline_fires();
----

# 8. Native SHOW still works.
statement ok
SHOW TABLES;

# Cleanup
statement ok
DROP MATERIALIZED VIEW mv_show_test;
```

(`(SHOW pipeline_status())` as a subquery may not be valid sqllogic syntax — DuckDB's SHOW returns a result set so it should parse. If the test runner rejects this idiom for the equivalence check, replace lines 1-2 of each comparison with two separate `query I` blocks comparing row counts independently. The functional test (just running `SHOW pipeline_status()` and getting rows) is already covered by the `statement ok` at point 4.)

- [ ] **Step 2: Run the test to verify it fails**

```bash
build/release/test/unittest --test-dir . "[sql]" '*show_monitoring*'
```

Expected: FAIL — DuckDB's native parser does not recognize `SHOW pipeline_status()`. The first `query I ... ---- true` block raises a parse error.

- [ ] **Step 3: Commit the failing test**

```bash
git add test/sql/show_monitoring.test
git commit -m "test: failing test for SHOW pipeline_*() strict sugar"
```

---

### Task 2: Add the SHOW intercept block in `PipelineParserOverride`

**Files:**
- Modify: `src/parser/pipeline_parser.cpp` — top-of-file include + `PipelineParserOverride` body

- [ ] **Step 1: Add the parser include**

Near the top of `pipeline_parser.cpp` (after the existing `#include "duckdb/parser/statement/extension_statement.hpp"`), add:

```cpp
#include "duckdb/parser/parser.hpp"
```

- [ ] **Step 2: Add the SHOW intercept block in `PipelineParserOverride`**

Find the start of `PipelineParserOverride` (around line 815):

```cpp
ParserOverrideResult PipelineParserExtension::PipelineParserOverride(ParserExtensionInfo *info, const string &query,
                                                                     ParserOptions &options) {
	// Only intercept DROP MATERIALIZED VIEW and ALTER MATERIALIZED VIEW
	// These are needed because DuckDB's native parser accepts them but fails at transform
	string trimmed = query;
	StringUtil::Trim(trimmed);
	while (!trimmed.empty() && trimmed.back() == ';') {
		trimmed.pop_back();
	}
	StringUtil::Trim(trimmed);
	string upper = StringUtil::Upper(trimmed);
```

Immediately after the line `string upper = StringUtil::Upper(trimmed);`, insert the SHOW block:

```cpp
	// Strict sugar: SHOW pipeline_<name>()  ->  SELECT * FROM pipeline_<name>()
	if (StringUtil::StartsWith(upper, "SHOW PIPELINE_")) {
		static const unordered_set<string> show_targets = {
		    "PIPELINE_STATUS",       "PIPELINE_EXPECTATIONS",     "PIPELINE_SCHEDULES",
		    "PIPELINE_RUN_LOGS",     "PIPELINE_EXPECTATION_LOGS",
		};
		auto tokens = Tokenize(trimmed);
		if (tokens.size() == 4 && StringUtil::Upper(tokens[0]) == "SHOW" &&
		    show_targets.count(StringUtil::Upper(tokens[1])) > 0 && tokens[2] == "(" && tokens[3] == ")") {
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

(`Tokenize` is already defined at the top of the file; `unordered_set` is available via existing DuckDB headers — if the build complains, add `#include <unordered_set>` near the top of the file.)

- [ ] **Step 3: Build**

```bash
make
```

Expected: success.

- [ ] **Step 4: Run the new test**

```bash
build/release/test/unittest --test-dir . "[sql]" '*show_monitoring*'
```

Expected: PASS.

- [ ] **Step 5: Run the full test suite**

```bash
make test
```

Expected: all tests pass. The override only fires for `SHOW PIPELINE_<whitelisted>` shapes; native DuckDB SHOW is untouched.

- [ ] **Step 6: Commit**

```bash
git add src/parser/pipeline_parser.cpp
git commit -m "feat: add SHOW pipeline_*() strict sugar for monitoring functions"
```

---

## Self-Review

- **Spec coverage:**
  - "Goal: equivalence" → Task 1 Step 1 (5 equivalence assertions, one per whitelisted function).
  - "Strict shape" → Task 2 Step 2 enforces exactly 4 tokens.
  - "Behavior matrix":
    - mixed case → Task 1 Step 1 (`Show Pipeline_Status()`).
    - trailing semicolon → Task 1 Step 1 (`SHOW pipeline_status();` as `statement ok`).
    - extra `WHERE` → Task 1 Step 1 (point 5, `statement error`).
    - missing parens → Task 1 Step 1 (point 6, `statement error`).
    - non-whitelisted (`pipeline_fires`) → Task 1 Step 1 (point 7, `statement error`).
    - native SHOW unaffected → Task 1 Step 1 (point 8, `SHOW TABLES`).
- **Placeholders:** none.
- **Type consistency:** `Parser parser; parser.ParseQuery(rewritten); ParserOverrideResult(std::move(parser.statements));` matches the DuckDB API in `duckdb/src/include/duckdb/parser/parser.hpp` (verified). `Tokenize` is the existing static helper at the top of `pipeline_parser.cpp`. `StringUtil::Upper`/`StringUtil::StartsWith` are already used elsewhere in the same file.
- **Whitelist size:** 5 entries, matching the spec exactly.
