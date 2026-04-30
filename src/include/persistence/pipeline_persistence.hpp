#pragma once
#include "duckdb.hpp"
#include <string>
#include <mutex>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Data Transfer Objects (moved from materialized_view_catalog.hpp)
//===--------------------------------------------------------------------===//

enum class ExpectationAction { WARN, DROP_ROW, FAIL_UPDATE };

struct Expectation {
	string name;
	string expression;
	ExpectationAction action;
};

struct ExpectationMetric {
	string constraint_name;
	int64_t total_rows;
	int64_t passed;
	int64_t failed;
	string action;
};

struct MaterializedViewDefinition {
	string name;
	string query;
	string comment;
	vector<Expectation> expectations;
	vector<string> explicit_dependencies;
	bool is_materialized = false;
	int schedule_type = 0;
	int schedule_interval = 0;
	string schedule_interval_unit;
	string schedule_cron_expression;
	bool schedule_paused = false;
};

//===--------------------------------------------------------------------===//
// PipelinePersistence — the single source of truth
//===--------------------------------------------------------------------===//

class PipelinePersistence {
public:
	// Schema lifecycle
	void EnsureInitialized(DatabaseInstance &db, const string &database = "");

	// Write methods
	void PersistView(DatabaseInstance &db, const string &database, const string &name, const string &query,
	                 const string &comment, const vector<Expectation> &expectations, const vector<string> &depends_on);
	void PersistSchedule(DatabaseInstance &db, const string &database, const string &name, int schedule_type,
	                     int interval_value, const string &interval_unit, const string &cron_expression);
	void UpdateViewQuery(DatabaseInstance &db, const string &database, const string &name, const string &query);
	void UpdateViewMaterialized(DatabaseInstance &db, const string &database, const string &name);
	void AddConstraint(DatabaseInstance &db, const string &database, const string &name, const string &constraint_name,
	                   const string &expression, const string &action);
	void DropConstraint(DatabaseInstance &db, const string &database, const string &name,
	                    const string &constraint_name);
	void UpdateSchedulePaused(DatabaseInstance &db, const string &database, const string &name, bool paused);
	void UpdateScheduleLastRun(DatabaseInstance &db, const string &database, const string &name);
	void CascadeDelete(DatabaseInstance &db, const string &database, const string &name);

	// Run logging
	int64_t InsertRunLog(DatabaseInstance &db, const string &database, const string &view_name, const string &trigger);
	void CompleteRunLog(DatabaseInstance &db, const string &database, int64_t run_id, bool success,
	                    const string &error_message, int64_t rows_affected);
	void InsertExpectationLog(DatabaseInstance &db, const string &database, int64_t run_id, const string &view_name,
	                          const string &constraint_name, int64_t total_rows, int64_t passed, int64_t failed,
	                          const string &action);

	// Read methods
	bool Exists(DatabaseInstance &db, const string &database, const string &name);
	MaterializedViewDefinition GetView(DatabaseInstance &db, const string &database, const string &name);
	vector<string> GetAllNames(DatabaseInstance &db, const string &database = "");

	// Multi-database: find all databases that have a __pipeline__ schema
	vector<string> GetAllPipelineDatabases(DatabaseInstance &db);

	// Utilities
	static pair<string, string> ResolveQualifiedName(const string &qualified_name);
	static string QualifyTable(const string &database, const string &table);

	static PipelinePersistence &Get();

private:
	std::mutex mutex;

	void CreateSchema(DatabaseInstance &db, const string &database);
};

} // namespace duckdb
