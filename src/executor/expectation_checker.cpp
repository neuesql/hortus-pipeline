#include "executor/expectation_checker.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

string ExpectationChecker::ApplyExpectations(ClientContext &context,
                                              const string &base_query,
                                              const vector<Expectation> &expectations) {
    if (expectations.empty()) {
        return base_query;
    }

    auto &db = DatabaseInstance::GetDatabase(context);

    // Phase 1: Check FAIL_UPDATE expectations first (abort early)
    for (auto &exp : expectations) {
        if (exp.action != ExpectationAction::FAIL_UPDATE) {
            continue;
        }
        string count_query = "SELECT COUNT(*) FROM (" + base_query + ") __mv_src WHERE NOT (" + exp.expression + ")";
        Connection conn(db);
        auto result = conn.Query(count_query);
        if (result->HasError()) {
            throw InvalidInputException("Expectation '%s': failed to evaluate expression: %s",
                                        exp.name, result->GetError());
        }
        auto count = result->GetValue(0, 0).GetValue<int64_t>();
        if (count > 0) {
            throw InvalidInputException("Expectation '%s' failed: %lld rows violated constraint (%s)",
                                        exp.name, (long long)count, exp.expression);
        }
    }

    // Phase 2: Build DROP ROW filters
    vector<string> drop_filters;
    for (auto &exp : expectations) {
        if (exp.action == ExpectationAction::DROP_ROW) {
            drop_filters.push_back("(" + exp.expression + ")");
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
        if (result->HasError()) {
            Printer::Print("WARNING: Expectation '" + exp.name + "': failed to evaluate: " + result->GetError());
            continue;
        }
        auto count = result->GetValue(0, 0).GetValue<int64_t>();
        if (count > 0) {
            Printer::Print("WARNING: Expectation '" + exp.name + "': " + std::to_string(count) +
                           " rows violated constraint (" + exp.expression + ")");
        }
    }

    // Build final query
    if (!drop_filters.empty()) {
        string filter = StringUtil::Join(drop_filters, " AND ");
        return "SELECT * FROM (" + base_query + ") __mv_src WHERE " + filter;
    }

    return base_query;
}

} // namespace duckdb
