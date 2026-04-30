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

    void EnsureInitialized(const string &database = "");

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

    int64_t InsertRunLog(const string &database, const string &view_name,
                         const string &trigger);
    void CompleteRunLog(const string &database, int64_t run_id, bool success,
                        const string &error_message, int64_t rows_affected);
    void InsertExpectationLog(const string &database, int64_t run_id,
                              const string &view_name, const string &constraint_name,
                              int64_t total_rows, int64_t passed, int64_t failed,
                              const string &action);

    void HydrateFromDatabase(const string &database, MaterializedViewCatalog &catalog);

    static pair<string, string> ResolveQualifiedName(const string &qualified_name);

    static PipelinePersistence &Get(DatabaseInstance &db);

private:
    DatabaseInstance &db;
    std::mutex mutex;
    unordered_set<string> initialized_databases;

    void CreateSchema(const string &database);
    string QualifyTable(const string &database, const string &table);
};

} // namespace duckdb
