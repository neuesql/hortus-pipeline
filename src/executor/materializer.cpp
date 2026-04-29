#include "executor/materializer.hpp"
#include "executor/expectation_checker.hpp"
#include "executor/dag_resolver.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include <unordered_set>

namespace duckdb {

void Materializer::Materialize(ClientContext &context, MaterializedViewCatalog &catalog, const string &view_name) {
    auto &def = catalog.Get(view_name);

    // Apply expectations (may throw for FAIL_UPDATE)
    vector<ExpectationMetric> metrics;
    string final_query = ExpectationChecker::ApplyExpectations(context, def.query, def.expectations, metrics);

    // Execute CREATE OR REPLACE TABLE using a separate connection
    auto &db = DatabaseInstance::GetDatabase(context);
    Connection conn(db);
    string create_sql = "CREATE OR REPLACE TABLE " + view_name + " AS (" + final_query + ")";
    auto result = conn.Query(create_sql);
    if (result->HasError()) {
        throw InvalidInputException("Failed to materialize view '%s': %s", view_name, result->GetError());
    }

    // Store metrics
    if (!metrics.empty()) {
        catalog.SetExpectationMetrics(view_name, metrics);
    }

    catalog.MarkMaterialized(view_name);
}

void Materializer::MaterializeAll(ClientContext &context, MaterializedViewCatalog &catalog, bool best_effort) {
    auto order = DAGResolver::Resolve(catalog);

    if (!best_effort) {
        for (auto &name : order) {
            Materialize(context, catalog, name);
        }
        return;
    }

    // Best-effort mode: skip dependents of failed nodes, continue with independent branches
    unordered_set<string> failed_set;
    vector<string> errors;

    // Build dependency map to check if any ancestor failed
    auto all_names = catalog.GetAllNames();
    unordered_set<string> mv_set(all_names.begin(), all_names.end());
    unordered_map<string, vector<string>> dep_map;
    for (auto &name : all_names) {
        auto &def = catalog.Get(name);
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
        // Check if any dependency has failed
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
            Materialize(context, catalog, name);
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
