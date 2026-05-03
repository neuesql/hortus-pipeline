#include "executor/expectation_checker.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

static string ActionToString(ExpectationAction action) {
	switch (action) {
	case ExpectationAction::WARN:
		return "WARN";
	case ExpectationAction::DROP_ROW:
		return "DROP ROW";
	case ExpectationAction::FAIL_UPDATE:
		return "FAIL UPDATE";
	default:
		break;
	}
	return "WARN";
}

string ExpectationChecker::ApplyExpectations(ClientContext &context, const string &base_query,
                                             const vector<Expectation> &expectations) {
	vector<ExpectationMetric> unused_metrics;
	return ApplyExpectations(context, base_query, expectations, unused_metrics);
}

string ExpectationChecker::ApplyExpectations(ClientContext &context, const string &base_query,
                                             const vector<Expectation> &expectations,
                                             vector<ExpectationMetric> &out_metrics) {
	out_metrics.clear();

	if (expectations.empty()) {
		return base_query;
	}

	auto &db = DatabaseInstance::GetDatabase(context);

	// Get total row count
	int64_t total_rows = 0;
	{
		string count_query = "SELECT COUNT(*) FROM (" + base_query + ") __mv_total";
		Connection conn(db);
		auto result = conn.Query(count_query);
		if (!result->HasError()) {
			total_rows = result->GetValue(0, 0).GetValue<int64_t>();
		}
	}

	// Phase 1: Check FAIL_UPDATE expectations first (abort early)
	for (auto &exp : expectations) {
		if (exp.action != ExpectationAction::FAIL_UPDATE) {
			continue;
		}
		string count_query = "SELECT COUNT(*) FROM (" + base_query + ") __mv_src WHERE NOT (" + exp.expression + ")";
		Connection conn(db);
		auto result = conn.Query(count_query);
		if (result->HasError()) {
			throw InvalidInputException("Expectation '%s': failed to evaluate expression: %s", exp.name,
			                            result->GetError());
		}
		auto failed_count = result->GetValue(0, 0).GetValue<int64_t>();

		ExpectationMetric metric;
		metric.expectation_name = exp.name;
		metric.total_rows = total_rows;
		metric.failed = failed_count;
		metric.passed = total_rows - failed_count;
		metric.action = ActionToString(exp.action);
		out_metrics.push_back(metric);

		if (failed_count > 0) {
			throw InvalidInputException("Expectation '%s' failed: %lld rows violated constraint (%s)", exp.name,
			                            static_cast<int64_t>(failed_count), exp.expression);
		}
	}

	// Phase 2: Build DROP ROW filters
	vector<string> drop_filters;
	for (auto &exp : expectations) {
		if (exp.action == ExpectationAction::DROP_ROW) {
			drop_filters.push_back("(" + exp.expression + ")");

			// Count violations for metrics
			string count_query =
			    "SELECT COUNT(*) FROM (" + base_query + ") __mv_src WHERE NOT (" + exp.expression + ")";
			Connection conn(db);
			auto result = conn.Query(count_query);
			int64_t failed_count = 0;
			if (!result->HasError()) {
				failed_count = result->GetValue(0, 0).GetValue<int64_t>();
			}

			ExpectationMetric metric;
			metric.expectation_name = exp.name;
			metric.total_rows = total_rows;
			metric.failed = failed_count;
			metric.passed = total_rows - failed_count;
			metric.action = ActionToString(exp.action);
			out_metrics.push_back(metric);
		}
	}

	// Phase 3: Run WARN checks (just count and print, keep all rows)
	for (auto &exp : expectations) {
		if (exp.action != ExpectationAction::WARN) {
			continue;
		}
		string count_query = "SELECT COUNT(*) FROM (" + base_query + ") __mv_src WHERE NOT (" + exp.expression + ")";
		Connection conn(db);
		auto result = conn.Query(count_query);
		int64_t failed_count = 0;
		if (result->HasError()) {
			Printer::Print("WARNING: Expectation '" + exp.name + "': failed to evaluate: " + result->GetError());
		} else {
			failed_count = result->GetValue(0, 0).GetValue<int64_t>();
			if (failed_count > 0) {
				Printer::Print("WARNING: Expectation '" + exp.name + "': " + std::to_string(failed_count) +
				               " rows violated constraint (" + exp.expression + ")");
			}
		}

		ExpectationMetric metric;
		metric.expectation_name = exp.name;
		metric.total_rows = total_rows;
		metric.failed = failed_count;
		metric.passed = total_rows - failed_count;
		metric.action = ActionToString(exp.action);
		out_metrics.push_back(metric);
	}

	// Build final query
	if (!drop_filters.empty()) {
		string filter = StringUtil::Join(drop_filters, " AND ");
		return "SELECT * FROM (" + base_query + ") __mv_src WHERE " + filter;
	}

	return base_query;
}

} // namespace duckdb
