#include "persistence/pipeline_persistence.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

static PipelinePersistence global_persistence_instance;

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
    // Always call CreateSchema — it uses IF NOT EXISTS so it's idempotent and cheap.
    // We cannot cache by database name alone because the global singleton survives
    // across different DatabaseInstance lifetimes (e.g. in test suites).
    CreateSchema(db, database);
}

void PipelinePersistence::CreateSchema(DatabaseInstance &db, const string &database) {
    Connection conn(db);

    string schema_prefix = database.empty() ? "" : database + ".";

    conn.Query("CREATE SCHEMA IF NOT EXISTS " + schema_prefix + "__pipeline__");

    conn.Query("CREATE TABLE IF NOT EXISTS " + QualifyTable(database, "views") + " ("
               "name VARCHAR PRIMARY KEY, "
               "query VARCHAR, "
               "comment VARCHAR, "
               "dependencies VARCHAR, "
               "is_materialized BOOLEAN DEFAULT false, "
               "created_at TIMESTAMP DEFAULT current_timestamp, "
               "updated_at TIMESTAMP DEFAULT current_timestamp)");

    conn.Query("CREATE TABLE IF NOT EXISTS " + QualifyTable(database, "constraints") + " ("
               "view_name VARCHAR, "
               "constraint_name VARCHAR, "
               "expression VARCHAR, "
               "action VARCHAR, "
               "PRIMARY KEY (view_name, constraint_name))");

    conn.Query("CREATE TABLE IF NOT EXISTS " + QualifyTable(database, "schedules") + " ("
               "view_name VARCHAR PRIMARY KEY, "
               "schedule_type INTEGER, "
               "interval_value INTEGER, "
               "interval_unit VARCHAR, "
               "cron_expression VARCHAR, "
               "paused BOOLEAN DEFAULT false, "
               "last_run_at TIMESTAMP)");

    conn.Query("CREATE SEQUENCE IF NOT EXISTS " + QualifyTable(database, "run_seq"));

    conn.Query("CREATE TABLE IF NOT EXISTS " + QualifyTable(database, "run_logs") + " ("
               "run_id BIGINT DEFAULT nextval('" + QualifyTable(database, "run_seq") + "'), "
               "view_name VARCHAR, "
               "started_at TIMESTAMP DEFAULT current_timestamp, "
               "finished_at TIMESTAMP, "
               "success BOOLEAN, "
               "error_message VARCHAR, "
               "\"trigger\" VARCHAR, "
               "rows_affected BIGINT)");

    conn.Query("CREATE TABLE IF NOT EXISTS " + QualifyTable(database, "expectation_logs") + " ("
               "run_id BIGINT, "
               "view_name VARCHAR, "
               "constraint_name VARCHAR, "
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
                                      const vector<Expectation> &expectations,
                                      const vector<string> &depends_on) {
    EnsureInitialized(db, database);
    Connection conn(db);

    string deps;
    for (idx_t i = 0; i < depends_on.size(); i++) {
        if (i > 0) deps += ",";
        deps += depends_on[i];
    }

    conn.Query("DELETE FROM " + QualifyTable(database, "views") + " WHERE name = '" + EscapeSQL(name) + "'");
    conn.Query("INSERT INTO " + QualifyTable(database, "views") +
               " (name, query, comment, dependencies, is_materialized) VALUES ('" +
               EscapeSQL(name) + "', '" + EscapeSQL(query) + "', '" + EscapeSQL(comment) +
               "', '" + EscapeSQL(deps) + "', false)");

    conn.Query("DELETE FROM " + QualifyTable(database, "constraints") + " WHERE view_name = '" + EscapeSQL(name) + "'");
    for (auto &exp : expectations) {
        string action_str;
        switch (exp.action) {
        case ExpectationAction::WARN: action_str = "WARN"; break;
        case ExpectationAction::DROP_ROW: action_str = "DROP ROW"; break;
        case ExpectationAction::FAIL_UPDATE: action_str = "FAIL UPDATE"; break;
        }
        conn.Query("INSERT INTO " + QualifyTable(database, "constraints") +
                   " (view_name, constraint_name, expression, action) VALUES ('" +
                   EscapeSQL(name) + "', '" + EscapeSQL(exp.name) + "', '" +
                   EscapeSQL(exp.expression) + "', '" + EscapeSQL(action_str) + "')");
    }
}

void PipelinePersistence::PersistSchedule(DatabaseInstance &db, const string &database, const string &name,
                                          int schedule_type, int interval_value,
                                          const string &interval_unit, const string &cron_expression) {
    EnsureInitialized(db, database);
    Connection conn(db);

    conn.Query("DELETE FROM " + QualifyTable(database, "schedules") + " WHERE view_name = '" + EscapeSQL(name) + "'");
    conn.Query("INSERT INTO " + QualifyTable(database, "schedules") +
               " (view_name, schedule_type, interval_value, interval_unit, cron_expression) VALUES ('" +
               EscapeSQL(name) + "', " + std::to_string(schedule_type) + ", " +
               std::to_string(interval_value) + ", '" + EscapeSQL(interval_unit) + "', '" +
               EscapeSQL(cron_expression) + "')");
}

void PipelinePersistence::UpdateViewQuery(DatabaseInstance &db, const string &database,
                                          const string &name, const string &query) {
    EnsureInitialized(db, database);
    Connection conn(db);

    conn.Query("UPDATE " + QualifyTable(database, "views") +
               " SET query = '" + EscapeSQL(query) + "', updated_at = current_timestamp"
               " WHERE name = '" + EscapeSQL(name) + "'");
}

void PipelinePersistence::UpdateViewMaterialized(DatabaseInstance &db, const string &database, const string &name) {
    EnsureInitialized(db, database);
    Connection conn(db);

    conn.Query("UPDATE " + QualifyTable(database, "views") +
               " SET is_materialized = true, updated_at = current_timestamp"
               " WHERE name = '" + EscapeSQL(name) + "'");
}

void PipelinePersistence::AddConstraint(DatabaseInstance &db, const string &database, const string &name,
                                        const string &constraint_name, const string &expression,
                                        const string &action) {
    EnsureInitialized(db, database);
    Connection conn(db);

    conn.Query("INSERT INTO " + QualifyTable(database, "constraints") +
               " (view_name, constraint_name, expression, action) VALUES ('" +
               EscapeSQL(name) + "', '" + EscapeSQL(constraint_name) + "', '" +
               EscapeSQL(expression) + "', '" + EscapeSQL(action) + "')");
}

void PipelinePersistence::DropConstraint(DatabaseInstance &db, const string &database, const string &name,
                                         const string &constraint_name) {
    EnsureInitialized(db, database);
    Connection conn(db);

    conn.Query("DELETE FROM " + QualifyTable(database, "constraints") +
               " WHERE view_name = '" + EscapeSQL(name) + "' AND constraint_name = '" +
               EscapeSQL(constraint_name) + "'");
}

void PipelinePersistence::UpdateSchedulePaused(DatabaseInstance &db, const string &database,
                                               const string &name, bool paused) {
    EnsureInitialized(db, database);
    Connection conn(db);

    conn.Query("UPDATE " + QualifyTable(database, "schedules") +
               " SET paused = " + (paused ? "true" : "false") +
               " WHERE view_name = '" + EscapeSQL(name) + "'");
}

void PipelinePersistence::UpdateScheduleLastRun(DatabaseInstance &db, const string &database, const string &name) {
    EnsureInitialized(db, database);
    Connection conn(db);

    conn.Query("UPDATE " + QualifyTable(database, "schedules") +
               " SET last_run_at = current_timestamp"
               " WHERE view_name = '" + EscapeSQL(name) + "'");
}

void PipelinePersistence::CascadeDelete(DatabaseInstance &db, const string &database, const string &name) {
    EnsureInitialized(db, database);
    Connection conn(db);

    conn.Query("DELETE FROM " + QualifyTable(database, "expectation_logs") +
               " WHERE view_name = '" + EscapeSQL(name) + "'");
    conn.Query("DELETE FROM " + QualifyTable(database, "run_logs") +
               " WHERE view_name = '" + EscapeSQL(name) + "'");
    conn.Query("DELETE FROM " + QualifyTable(database, "constraints") +
               " WHERE view_name = '" + EscapeSQL(name) + "'");
    conn.Query("DELETE FROM " + QualifyTable(database, "schedules") +
               " WHERE view_name = '" + EscapeSQL(name) + "'");
    conn.Query("DELETE FROM " + QualifyTable(database, "views") +
               " WHERE name = '" + EscapeSQL(name) + "'");
}

int64_t PipelinePersistence::InsertRunLog(DatabaseInstance &db, const string &database,
                                          const string &view_name, const string &trigger) {
    EnsureInitialized(db, database);
    Connection conn(db);

    auto result = conn.Query("INSERT INTO " + QualifyTable(database, "run_logs") +
                            " (view_name, \"trigger\") VALUES ('" +
                            EscapeSQL(view_name) + "', '" + EscapeSQL(trigger) +
                            "') RETURNING run_id");
    if (result->HasError() || result->RowCount() == 0) {
        return -1;
    }
    return result->GetValue(0, 0).GetValue<int64_t>();
}

void PipelinePersistence::CompleteRunLog(DatabaseInstance &db, const string &database, int64_t run_id,
                                         bool success, const string &error_message, int64_t rows_affected) {
    EnsureInitialized(db, database);
    Connection conn(db);

    conn.Query("UPDATE " + QualifyTable(database, "run_logs") +
               " SET finished_at = current_timestamp, success = " +
               (success ? string("true") : string("false")) +
               ", error_message = " +
               (error_message.empty() ? string("NULL") : "'" + EscapeSQL(error_message) + "'") +
               ", rows_affected = " + std::to_string(rows_affected) +
               " WHERE run_id = " + std::to_string(run_id));
}

void PipelinePersistence::InsertExpectationLog(DatabaseInstance &db, const string &database, int64_t run_id,
                                               const string &view_name, const string &constraint_name,
                                               int64_t total_rows, int64_t passed, int64_t failed,
                                               const string &action) {
    EnsureInitialized(db, database);
    Connection conn(db);

    conn.Query("INSERT INTO " + QualifyTable(database, "expectation_logs") +
               " (run_id, view_name, constraint_name, total_rows, passed, failed, action) VALUES (" +
               std::to_string(run_id) + ", '" + EscapeSQL(view_name) + "', '" +
               EscapeSQL(constraint_name) + "', " + std::to_string(total_rows) + ", " +
               std::to_string(passed) + ", " + std::to_string(failed) + ", '" +
               EscapeSQL(action) + "')");
}

void PipelinePersistence::HydrateFromDatabase(DatabaseInstance &db, const string &database,
                                               MaterializedViewCatalog &catalog) {
    Connection conn(db);

    string schema_prefix = database.empty() ? "" : database + ".";

    auto schema_check = conn.Query(
        "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '__pipeline__'");
    if (schema_check->HasError() || schema_check->RowCount() == 0 ||
        schema_check->GetValue(0, 0).GetValue<int64_t>() == 0) {
        return;
    }

    auto views_result = conn.Query("SELECT name, query, comment, dependencies, is_materialized FROM " +
                                   QualifyTable(database, "views"));
    if (!views_result->HasError()) {
        for (idx_t row = 0; row < views_result->RowCount(); row++) {
            string name = views_result->GetValue(0, row).ToString();
            string query = views_result->GetValue(1, row).ToString();
            string comment = views_result->GetValue(2, row).ToString();
            string deps_str = views_result->GetValue(3, row).ToString();
            bool is_materialized = views_result->GetValue(4, row).GetValue<bool>();

            vector<string> depends_on;
            if (!deps_str.empty()) {
                idx_t start = 0;
                for (idx_t i = 0; i <= deps_str.size(); i++) {
                    if (i == deps_str.size() || deps_str[i] == ',') {
                        if (i > start) {
                            depends_on.push_back(deps_str.substr(start, i - start));
                        }
                        start = i + 1;
                    }
                }
            }

            vector<Expectation> expectations;
            auto constraints_result = conn.Query(
                "SELECT constraint_name, expression, action FROM " +
                QualifyTable(database, "constraints") +
                " WHERE view_name = '" + EscapeSQL(name) + "'");
            if (!constraints_result->HasError()) {
                for (idx_t crow = 0; crow < constraints_result->RowCount(); crow++) {
                    Expectation exp;
                    exp.name = constraints_result->GetValue(0, crow).ToString();
                    exp.expression = constraints_result->GetValue(1, crow).ToString();
                    string action_str = constraints_result->GetValue(2, crow).ToString();
                    if (action_str == "DROP ROW") {
                        exp.action = ExpectationAction::DROP_ROW;
                    } else if (action_str == "FAIL UPDATE") {
                        exp.action = ExpectationAction::FAIL_UPDATE;
                    } else {
                        exp.action = ExpectationAction::WARN;
                    }
                    expectations.push_back(std::move(exp));
                }
            }

            catalog.CreateOrRefresh(name, query, comment, expectations, depends_on);
            if (is_materialized) {
                catalog.MarkMaterialized(name);
            }
        }
    }
}

} // namespace duckdb
