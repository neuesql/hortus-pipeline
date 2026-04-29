#pragma once
#include "duckdb.hpp"
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>

namespace duckdb {

enum class ScheduleType;  // Forward declaration

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

struct ExpectationMetric {
    string constraint_name;
    int64_t total_rows;
    int64_t passed;
    int64_t failed;
    string action; // "WARN", "DROP ROW", "FAIL UPDATE"
};

struct MaterializedViewDefinition {
    string name;
    string query;                       // The AS query
    string comment;
    vector<Expectation> expectations;
    vector<string> explicit_dependencies; // DEPENDS ON clause (empty = auto-detect)
    bool is_materialized = false;         // Has been executed at least once
    int schedule_type = 0;                // 0=NONE, 1=EVERY, 2=CRON, 3=ON_UPDATE
    int schedule_interval = 0;
    string schedule_interval_unit;
    string schedule_cron_expression;
    bool schedule_paused = false;
    vector<ExpectationMetric> last_expectation_metrics;
};

class MaterializedViewCatalog {
public:
    void CreateOrRefresh(const string &name, const string &query,
                         const string &comment,
                         const vector<Expectation> &expectations,
                         const vector<string> &depends_on);
    void AlterQuery(const string &name, const string &query);
    void AddConstraint(const string &name, const Expectation &expectation);
    void DropConstraint(const string &name, const string &constraint_name);
    void Drop(const string &name);
    bool Exists(const string &name) const;
    const MaterializedViewDefinition &Get(const string &name) const;
    vector<string> GetAllNames() const;
    void MarkMaterialized(const string &name);
    void PauseSchedule(const string &name);
    void ResumeSchedule(const string &name);
    void SetExpectationMetrics(const string &name, const vector<ExpectationMetric> &metrics);
    static MaterializedViewCatalog &Get(DatabaseInstance &db);

private:
    mutable mutex catalog_mutex;
    unordered_map<string, MaterializedViewDefinition> definitions;
};

} // namespace duckdb
