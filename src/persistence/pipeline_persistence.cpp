#include "persistence/pipeline_persistence.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

static PipelinePersistence global_persistence_instance; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

PipelinePersistence &PipelinePersistence::Get() {
	return global_persistence_instance;
}

string PipelinePersistence::QualifyTable(const string &database, const string &table) {
	if (database.empty()) {
		return "__pipeline__." + table;
	}
	return database + ".__pipeline__." + table;
}

pair<string, string> PipelinePersistence::ResolveQualifiedName(const string &qualified_name) {
	auto dot_pos = qualified_name.find('.');
	if (dot_pos == string::npos) {
		return {"", qualified_name};
	}
	return {qualified_name.substr(0, dot_pos), qualified_name.substr(dot_pos + 1)};
}

void PipelinePersistence::EnsureInitialized(DatabaseInstance &db, const string &database) {
	lock_guard<std::mutex> lock(mutex);

	// Fast path: check if __pipeline__ schema already exists with a lightweight query
	Connection check_conn(db);
	string schema_prefix = database.empty() ? "" : database + ".";
	auto result = check_conn.Query("SELECT 1 FROM information_schema.schemata WHERE schema_name = '__pipeline__'" +
	                               (database.empty() ? string("") : " AND catalog_name = '" + database + "'"));
	if (!result->HasError() && result->RowCount() > 0) {
		return;
	}

	CreateSchema(db, database);
}

void PipelinePersistence::CreateSchema(DatabaseInstance &db, const string &database) {
	Connection conn(db);

	string schema_prefix = database.empty() ? "" : database + ".";

	auto schema_result = conn.Query("CREATE SCHEMA IF NOT EXISTS " + schema_prefix + "__pipeline__");
	if (schema_result->HasError()) {
		throw InvalidInputException("Failed to create __pipeline__ schema: %s", schema_result->GetError());
	}

	conn.Query("CREATE TABLE IF NOT EXISTS " + QualifyTable(database, "materialized_views") +
	           " ("
	           "name VARCHAR PRIMARY KEY, "
	           "query VARCHAR NOT NULL, "
	           "comment VARCHAR, "
	           "dependencies VARCHAR, "
	           "is_materialized BOOLEAN DEFAULT false, "
	           "created_at TIMESTAMP DEFAULT current_timestamp, "
	           "updated_at TIMESTAMP DEFAULT current_timestamp)");

	conn.Query("CREATE TABLE IF NOT EXISTS " + QualifyTable(database, "expectations") +
	           " ("
	           "view_name VARCHAR NOT NULL, "
	           "expectation_name VARCHAR NOT NULL, "
	           "expression VARCHAR NOT NULL, "
	           "action VARCHAR NOT NULL, "
	           "PRIMARY KEY (view_name, expectation_name))");

	conn.Query("CREATE TABLE IF NOT EXISTS " + QualifyTable(database, "schedules") +
	           " ("
	           "view_name VARCHAR PRIMARY KEY, "
	           "schedule_type INTEGER NOT NULL, "
	           "interval_value INTEGER, "
	           "interval_unit VARCHAR, "
	           "cron_expression VARCHAR, "
	           "paused BOOLEAN DEFAULT false, "
	           "last_run_at TIMESTAMP)");

	conn.Query("CREATE SEQUENCE IF NOT EXISTS " + QualifyTable(database, "run_seq"));

	conn.Query("CREATE TABLE IF NOT EXISTS " + QualifyTable(database, "run_logs") +
	           " ("
	           "run_id BIGINT DEFAULT nextval('" +
	           QualifyTable(database, "run_seq") +
	           "'), "
	           "view_name VARCHAR NOT NULL, "
	           "started_at TIMESTAMP DEFAULT current_timestamp, "
	           "finished_at TIMESTAMP, "
	           "success BOOLEAN, "
	           "error_message VARCHAR, "
	           "\"trigger\" VARCHAR, "
	           "rows_affected BIGINT)");

	conn.Query("CREATE TABLE IF NOT EXISTS " + QualifyTable(database, "expectation_logs") +
	           " ("
	           "run_id BIGINT NOT NULL, "
	           "view_name VARCHAR NOT NULL, "
	           "expectation_name VARCHAR NOT NULL, "
	           "total_rows BIGINT, "
	           "passed BIGINT, "
	           "failed BIGINT, "
	           "action VARCHAR)");
}

static string EscapeSQL(const string &val) {
	return StringUtil::Replace(val, "'", "''");
}

void PipelinePersistence::PersistView(DatabaseInstance &db, const string &database, const string &name,
                                      const string &query, const string &comment,
                                      const vector<Expectation> &expectations, const vector<string> &depends_on) {
	EnsureInitialized(db, database);
	Connection conn(db);

	string deps;
	for (idx_t i = 0; i < depends_on.size(); i++) {
		if (i > 0) {
			deps += ",";
		}
		deps += depends_on[i];
	}

	conn.Query("BEGIN TRANSACTION");

	// Check if view already exists to preserve created_at
	string views_table = QualifyTable(database, "materialized_views");
	auto existing = conn.Query("SELECT created_at FROM " + views_table + " WHERE name = '" + EscapeSQL(name) + "'");
	bool is_update = !existing->HasError() && existing->RowCount() > 0;

	conn.Query("DELETE FROM " + views_table + " WHERE name = '" + EscapeSQL(name) + "'");

	if (is_update) {
		// Preserve original created_at
		string created_at = existing->GetValue(0, 0).ToString();
		conn.Query("INSERT INTO " + views_table +
		           " (name, query, comment, dependencies, is_materialized, created_at, updated_at) VALUES ('" +
		           EscapeSQL(name) + "', '" + EscapeSQL(query) + "', '" + EscapeSQL(comment) + "', '" +
		           EscapeSQL(deps) + "', false, '" + created_at + "', current_timestamp)");
	} else {
		conn.Query("INSERT INTO " + views_table + " (name, query, comment, dependencies, is_materialized) VALUES ('" +
		           EscapeSQL(name) + "', '" + EscapeSQL(query) + "', '" + EscapeSQL(comment) + "', '" +
		           EscapeSQL(deps) + "', false)");
	}

	conn.Query("DELETE FROM " + QualifyTable(database, "expectations") + " WHERE view_name = '" + EscapeSQL(name) + "'");
	for (auto &exp : expectations) {
		string action_str;
		switch (exp.action) {
		case ExpectationAction::WARN:
			action_str = "WARN";
			break;
		case ExpectationAction::DROP_ROW:
			action_str = "DROP ROW";
			break;
		case ExpectationAction::FAIL_UPDATE:
			action_str = "FAIL UPDATE";
			break;
		default:
			break;
		}
		conn.Query("INSERT INTO " + QualifyTable(database, "expectations") +
		           " (view_name, expectation_name, expression, action) VALUES ('" + EscapeSQL(name) + "', '" +
		           EscapeSQL(exp.name) + "', '" + EscapeSQL(exp.expression) + "', '" + EscapeSQL(action_str) + "')");
	}

	conn.Query("COMMIT");
}

void PipelinePersistence::PersistSchedule(DatabaseInstance &db, const string &database, const string &name,
                                          int schedule_type, int interval_value, const string &interval_unit,
                                          const string &cron_expression) {
	EnsureInitialized(db, database);
	Connection conn(db);

	conn.Query("DELETE FROM " + QualifyTable(database, "schedules") + " WHERE view_name = '" + EscapeSQL(name) + "'");
	conn.Query("INSERT INTO " + QualifyTable(database, "schedules") +
	           " (view_name, schedule_type, interval_value, interval_unit, cron_expression) VALUES ('" +
	           EscapeSQL(name) + "', " + std::to_string(schedule_type) + ", " + std::to_string(interval_value) + ", '" +
	           EscapeSQL(interval_unit) + "', '" + EscapeSQL(cron_expression) + "')");
}

void PipelinePersistence::UpdateViewQuery(DatabaseInstance &db, const string &database, const string &name,
                                          const string &query) {
	EnsureInitialized(db, database);
	Connection conn(db);

	conn.Query("UPDATE " + QualifyTable(database, "materialized_views") + " SET query = '" + EscapeSQL(query) +
	           "', updated_at = current_timestamp"
	           " WHERE name = '" +
	           EscapeSQL(name) + "'");
}

void PipelinePersistence::UpdateViewMaterialized(DatabaseInstance &db, const string &database, const string &name) {
	EnsureInitialized(db, database);
	Connection conn(db);

	conn.Query("UPDATE " + QualifyTable(database, "materialized_views") +
	           " SET is_materialized = true, updated_at = current_timestamp"
	           " WHERE name = '" +
	           EscapeSQL(name) + "'");
}

void PipelinePersistence::AddExpectation(DatabaseInstance &db, const string &database, const string &name,
                                         const string &expectation_name, const string &expression,
                                         const string &action) {
	EnsureInitialized(db, database);
	Connection conn(db);

	conn.Query("INSERT INTO " + QualifyTable(database, "expectations") +
	           " (view_name, expectation_name, expression, action) VALUES ('" + EscapeSQL(name) + "', '" +
	           EscapeSQL(expectation_name) + "', '" + EscapeSQL(expression) + "', '" + EscapeSQL(action) + "')");
}

void PipelinePersistence::DropExpectation(DatabaseInstance &db, const string &database, const string &name,
                                          const string &expectation_name) {
	EnsureInitialized(db, database);
	Connection conn(db);

	conn.Query("DELETE FROM " + QualifyTable(database, "expectations") + " WHERE view_name = '" + EscapeSQL(name) +
	           "' AND expectation_name = '" + EscapeSQL(expectation_name) + "'");
}

void PipelinePersistence::UpdateSchedulePaused(DatabaseInstance &db, const string &database, const string &name,
                                               bool paused) {
	EnsureInitialized(db, database);
	Connection conn(db);

	conn.Query("UPDATE " + QualifyTable(database, "schedules") + " SET paused = " + (paused ? "true" : "false") +
	           " WHERE view_name = '" + EscapeSQL(name) + "'");
}

void PipelinePersistence::UpdateScheduleLastRun(DatabaseInstance &db, const string &database, const string &name) {
	EnsureInitialized(db, database);
	Connection conn(db);

	conn.Query("UPDATE " + QualifyTable(database, "schedules") +
	           " SET last_run_at = current_timestamp"
	           " WHERE view_name = '" +
	           EscapeSQL(name) + "'");
}

void PipelinePersistence::CascadeDelete(DatabaseInstance &db, const string &database, const string &name) {
	EnsureInitialized(db, database);
	Connection conn(db);

	conn.Query("BEGIN TRANSACTION");
	conn.Query("DELETE FROM " + QualifyTable(database, "expectation_logs") + " WHERE view_name = '" + EscapeSQL(name) +
	           "'");
	conn.Query("DELETE FROM " + QualifyTable(database, "run_logs") + " WHERE view_name = '" + EscapeSQL(name) + "'");
	conn.Query("DELETE FROM " + QualifyTable(database, "expectations") + " WHERE view_name = '" + EscapeSQL(name) + "'");
	conn.Query("DELETE FROM " + QualifyTable(database, "schedules") + " WHERE view_name = '" + EscapeSQL(name) + "'");
	conn.Query("DELETE FROM " + QualifyTable(database, "materialized_views") + " WHERE name = '" + EscapeSQL(name) + "'");
	conn.Query("COMMIT");
}

int64_t PipelinePersistence::InsertRunLog(DatabaseInstance &db, const string &database, const string &view_name,
                                          const string &trigger) {
	EnsureInitialized(db, database);
	Connection conn(db);

	auto result =
	    conn.Query("INSERT INTO " + QualifyTable(database, "run_logs") + " (view_name, \"trigger\") VALUES ('" +
	               EscapeSQL(view_name) + "', '" + EscapeSQL(trigger) + "') RETURNING run_id");
	if (result->HasError() || result->RowCount() == 0) {
		return -1;
	}
	return result->GetValue(0, 0).GetValue<int64_t>();
}

void PipelinePersistence::CompleteRunLog(DatabaseInstance &db, const string &database, int64_t run_id, bool success,
                                         const string &error_message, int64_t rows_affected) {
	EnsureInitialized(db, database);
	Connection conn(db);

	conn.Query("UPDATE " + QualifyTable(database, "run_logs") +
	           " SET finished_at = current_timestamp, success = " + (success ? string("true") : string("false")) +
	           ", error_message = " + (error_message.empty() ? string("NULL") : "'" + EscapeSQL(error_message) + "'") +
	           ", rows_affected = " + std::to_string(rows_affected) + " WHERE run_id = " + std::to_string(run_id));
}

void PipelinePersistence::InsertExpectationLog(DatabaseInstance &db, const string &database, int64_t run_id,
                                               const string &view_name, const string &expectation_name,
                                               int64_t total_rows, int64_t passed, int64_t failed,
                                               const string &action) {
	EnsureInitialized(db, database);
	Connection conn(db);

	conn.Query("INSERT INTO " + QualifyTable(database, "expectation_logs") +
	           " (run_id, view_name, expectation_name, total_rows, passed, failed, action) VALUES (" +
	           std::to_string(run_id) + ", '" + EscapeSQL(view_name) + "', '" + EscapeSQL(expectation_name) + "', " +
	           std::to_string(total_rows) + ", " + std::to_string(passed) + ", " + std::to_string(failed) + ", '" +
	           EscapeSQL(action) + "')");
}

bool PipelinePersistence::Exists(DatabaseInstance &db, const string &database, const string &name) {
	EnsureInitialized(db, database);
	Connection conn(db);
	auto result = conn.Query("SELECT COUNT(*) FROM " + QualifyTable(database, "materialized_views") + " WHERE name = '" +
	                         EscapeSQL(name) + "'");
	if (result->HasError() || result->RowCount() == 0) {
		return false;
	}
	return result->GetValue(0, 0).GetValue<int64_t>() > 0;
}

MaterializedViewDefinition PipelinePersistence::GetView(DatabaseInstance &db, const string &database,
                                                        const string &name) {
	EnsureInitialized(db, database);
	Connection conn(db);

	auto result = conn.Query("SELECT name, query, comment, dependencies, is_materialized FROM " +
	                         QualifyTable(database, "materialized_views") + " WHERE name = '" + EscapeSQL(name) + "'");
	if (result->HasError() || result->RowCount() == 0) {
		throw InvalidInputException("Materialized view '%s' not found", name);
	}

	MaterializedViewDefinition def;
	def.name = result->GetValue(0, 0).ToString();
	def.query = result->GetValue(1, 0).ToString();
	def.comment = result->GetValue(2, 0).IsNull() ? "" : result->GetValue(2, 0).ToString();
	string deps_str = result->GetValue(3, 0).IsNull() ? "" : result->GetValue(3, 0).ToString();
	def.is_materialized = result->GetValue(4, 0).GetValue<bool>();

	// Parse dependencies
	if (!deps_str.empty()) {
		idx_t start = 0;
		for (idx_t i = 0; i <= deps_str.size(); i++) {
			if (i == deps_str.size() || deps_str[i] == ',') {
				if (i > start) {
					def.explicit_dependencies.push_back(deps_str.substr(start, i - start));
				}
				start = i + 1;
			}
		}
	}

	// Load expectations
	auto expectations_result =
	    conn.Query("SELECT expectation_name, expression, action FROM " + QualifyTable(database, "expectations") +
	               " WHERE view_name = '" + EscapeSQL(name) + "'");
	if (!expectations_result->HasError()) {
		for (idx_t row = 0; row < expectations_result->RowCount(); row++) {
			Expectation exp;
			exp.name = expectations_result->GetValue(0, row).ToString();
			exp.expression = expectations_result->GetValue(1, row).ToString();
			string action_str = expectations_result->GetValue(2, row).ToString();
			if (action_str == "DROP ROW") {
				exp.action = ExpectationAction::DROP_ROW;
			} else if (action_str == "FAIL UPDATE") {
				exp.action = ExpectationAction::FAIL_UPDATE;
			} else {
				exp.action = ExpectationAction::WARN;
			}
			def.expectations.push_back(std::move(exp));
		}
	}

	// Load schedule
	auto sched_result =
	    conn.Query("SELECT schedule_type, interval_value, interval_unit, cron_expression, paused FROM " +
	               QualifyTable(database, "schedules") + " WHERE view_name = '" + EscapeSQL(name) + "'");
	if (!sched_result->HasError() && sched_result->RowCount() > 0) {
		def.schedule_type = sched_result->GetValue(0, 0).GetValue<int>();
		def.schedule_interval =
		    sched_result->GetValue(1, 0).IsNull() ? 0 : sched_result->GetValue(1, 0).GetValue<int>();
		def.schedule_interval_unit =
		    sched_result->GetValue(2, 0).IsNull() ? "" : sched_result->GetValue(2, 0).ToString();
		def.schedule_cron_expression =
		    sched_result->GetValue(3, 0).IsNull() ? "" : sched_result->GetValue(3, 0).ToString();
		def.schedule_paused = sched_result->GetValue(4, 0).GetValue<bool>();
	}

	return def;
}

vector<string> PipelinePersistence::GetAllNames(DatabaseInstance &db, const string &database) {
	EnsureInitialized(db, database);
	Connection conn(db);
	vector<string> names;

	auto result = conn.Query("SELECT name FROM " + QualifyTable(database, "materialized_views"));
	if (!result->HasError()) {
		for (idx_t row = 0; row < result->RowCount(); row++) {
			names.push_back(result->GetValue(0, row).ToString());
		}
	}
	return names;
}

vector<string> PipelinePersistence::GetAllPipelineDatabases(DatabaseInstance &db) {
	Connection conn(db);
	vector<string> databases;

	auto result = conn.Query("SELECT DISTINCT catalog_name FROM information_schema.schemata "
	                         "WHERE schema_name = '__pipeline__'");
	if (!result->HasError()) {
		for (idx_t row = 0; row < result->RowCount(); row++) {
			string db_name = result->GetValue(0, row).ToString();
			// DuckDB uses "memory" for the default in-memory catalog or the file name
			// Treat the default catalog as empty string for our QualifyTable logic
			databases.push_back(db_name);
		}
	}
	return databases;
}

} // namespace duckdb
