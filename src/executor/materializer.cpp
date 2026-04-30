#include "executor/materializer.hpp"
#include "executor/expectation_checker.hpp"
#include "executor/dag_resolver.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include <unordered_set>

namespace duckdb {

void Materializer::Materialize(ClientContext &context, const string &view_name, const string &trigger) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &persistence = PipelinePersistence::Get();
	auto resolved = PipelinePersistence::ResolveQualifiedName(view_name);
	auto &database = resolved.first;
	auto &unqualified_name = resolved.second;

	auto def = persistence.GetView(db, database, unqualified_name);

	// Start run log
	int64_t run_id = persistence.InsertRunLog(db, database, unqualified_name, trigger);

	try {
		// Apply expectations (may throw for FAIL_UPDATE)
		vector<ExpectationMetric> metrics;
		string final_query = ExpectationChecker::ApplyExpectations(context, def.query, def.expectations, metrics);

		// Execute CREATE OR REPLACE TABLE
		Connection conn(db);
		string create_sql = "CREATE OR REPLACE TABLE " + view_name + " AS (" + final_query + ")";
		auto result = conn.Query(create_sql);
		if (result->HasError()) {
			throw InvalidInputException("Failed to materialize view '%s': %s", view_name, result->GetError());
		}

		// Count rows
		auto count_result = conn.Query("SELECT COUNT(*) FROM " + view_name);
		int64_t row_count = 0;
		if (!count_result->HasError()) {
			row_count = count_result->GetValue(0, 0).GetValue<int64_t>();
		}

		// Write expectation logs
		for (auto &m : metrics) {
			persistence.InsertExpectationLog(db, database, run_id, unqualified_name, m.constraint_name, m.total_rows,
			                                 m.passed, m.failed, m.action);
		}

		// Complete run log
		persistence.CompleteRunLog(db, database, run_id, true, "", row_count);
		persistence.UpdateViewMaterialized(db, database, unqualified_name);

	} catch (std::exception &e) {
		persistence.CompleteRunLog(db, database, run_id, false, e.what(), 0);
		throw;
	}
}

void Materializer::MaterializeAll(ClientContext &context, bool best_effort, const string &trigger) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto order = DAGResolver::Resolve(db);

	if (!best_effort) {
		for (auto &name : order) {
			Materialize(context, name, trigger);
		}
		return;
	}

	// Best-effort mode
	auto &persistence = PipelinePersistence::Get();
	auto all_names = persistence.GetAllNames(db);
	unordered_set<string> mv_set(all_names.begin(), all_names.end());
	unordered_set<string> failed_set;
	vector<string> errors;

	unordered_map<string, vector<string>> dep_map;
	for (auto &name : all_names) {
		auto def = persistence.GetView(db, "", name);
		vector<string> deps;
		if (!def.explicit_dependencies.empty()) {
			deps = def.explicit_dependencies;
		} else {
			deps = DAGResolver::ExtractDependencies(def.query);
		}
		vector<string> mv_deps;
		for (auto &d : deps) {
			if (mv_set.count(d) > 0 && d != name) {
				mv_deps.push_back(d);
			}
		}
		dep_map[name] = mv_deps;
	}

	for (auto &name : order) {
		bool skip = false;
		for (auto &dep : dep_map[name]) {
			if (failed_set.count(dep) > 0) {
				skip = true;
				break;
			}
		}
		if (skip) {
			failed_set.insert(name);
			errors.push_back("Skipped '" + name + "' due to failed dependency");
			continue;
		}

		try {
			Materialize(context, name, trigger);
		} catch (std::exception &e) {
			failed_set.insert(name);
			errors.push_back("Failed to materialize '" + name + "': " + string(e.what()));
		}
	}

	if (!errors.empty()) {
		string msg = "Best-effort refresh completed with errors:";
		for (auto &err : errors) {
			msg += "\n  - " + err;
		}
		Printer::Print(msg);
	}
}

} // namespace duckdb
