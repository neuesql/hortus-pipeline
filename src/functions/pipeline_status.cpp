#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/string_util.hpp"
#include "catalog/materialized_view_catalog.hpp"
#include "executor/dag_resolver.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// pipeline_status() TableFunction
//===--------------------------------------------------------------------===//

struct PipelineStatusBindData : public TableFunctionData {};

struct PipelineStatusGlobalState : public GlobalTableFunctionState {
    idx_t offset = 0;
    vector<string> names;
};

static unique_ptr<FunctionData> PipelineStatusBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
    auto data = make_uniq<PipelineStatusBindData>();

    names.emplace_back("name");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("query");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("dependencies");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("is_materialized");
    return_types.emplace_back(LogicalType::BOOLEAN);

    names.emplace_back("comment");
    return_types.emplace_back(LogicalType::VARCHAR);

    return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> PipelineStatusInit(ClientContext &context, TableFunctionInitInput &input) {
    auto state = make_uniq<PipelineStatusGlobalState>();

    auto &db = DatabaseInstance::GetDatabase(context);
    auto &catalog = MaterializedViewCatalog::Get(db);
    state->names = catalog.GetAllNames();

    return std::move(state);
}

static void PipelineStatusFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<PipelineStatusGlobalState>();

    if (state.offset >= state.names.size()) {
        return;
    }

    auto &db = DatabaseInstance::GetDatabase(context);
    auto &catalog = MaterializedViewCatalog::Get(db);

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (state.offset < state.names.size() && count < max_count) {
        auto &name = state.names[state.offset];
        auto &def = catalog.Get(name);

        // Determine dependencies: prefer explicit, fall back to auto-detect
        vector<string> deps;
        if (!def.explicit_dependencies.empty()) {
            deps = def.explicit_dependencies;
        } else {
            deps = DAGResolver::ExtractDependencies(def.query);
        }

        string deps_str;
        for (idx_t i = 0; i < deps.size(); i++) {
            if (i > 0) {
                deps_str += ", ";
            }
            deps_str += deps[i];
        }

        output.SetValue(0, count, Value(name));
        output.SetValue(1, count, Value(def.query));
        output.SetValue(2, count, Value(deps_str));
        output.SetValue(3, count, Value::BOOLEAN(def.is_materialized));
        output.SetValue(4, count, Value(def.comment));

        state.offset++;
        count++;
    }

    output.SetCardinality(count);
}

TableFunction GetPipelineStatusFunction() {
    TableFunction func("pipeline_status", {},
                       PipelineStatusFunc, PipelineStatusBind, PipelineStatusInit);
    return func;
}

} // namespace duckdb
