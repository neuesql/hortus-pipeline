#include "executor/materializer.hpp"
#include "executor/expectation_checker.hpp"
#include "executor/dag_resolver.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

void Materializer::Materialize(ClientContext &context, MaterializedViewCatalog &catalog, const string &view_name) {
    auto &def = catalog.Get(view_name);

    // Apply expectations (may throw for FAIL_UPDATE)
    string final_query = ExpectationChecker::ApplyExpectations(context, def.query, def.expectations);

    // Execute CREATE OR REPLACE TABLE using a separate connection
    auto &db = DatabaseInstance::GetDatabase(context);
    Connection conn(db);
    string create_sql = "CREATE OR REPLACE TABLE " + view_name + " AS (" + final_query + ")";
    auto result = conn.Query(create_sql);
    if (result->HasError()) {
        throw InvalidInputException("Failed to materialize view '%s': %s", view_name, result->GetError());
    }

    catalog.MarkMaterialized(view_name);
}

void Materializer::MaterializeAll(ClientContext &context, MaterializedViewCatalog &catalog) {
    auto order = DAGResolver::Resolve(catalog);
    for (auto &name : order) {
        Materialize(context, catalog, name);
    }
}

} // namespace duckdb
