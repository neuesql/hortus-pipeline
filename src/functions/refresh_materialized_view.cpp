#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/string_util.hpp"
#include "persistence/pipeline_persistence.hpp"
#include "executor/materializer.hpp"
#include "executor/dag_resolver.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// REFRESH MATERIALIZED VIEW TableFunction
//===--------------------------------------------------------------------===//

struct RefreshMVBindData : public TableFunctionData {
    string view_name;
    string refresh_mode;
};

struct RefreshMVGlobalState : public GlobalTableFunctionState {
    bool done = false;
};

static unique_ptr<FunctionData> RefreshMVBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
    auto data = make_uniq<RefreshMVBindData>();
    data->view_name = StringValue::Get(input.inputs[0]);
    data->refresh_mode = StringValue::Get(input.inputs[1]);

    names.emplace_back("status");
    return_types.emplace_back(LogicalType::VARCHAR);
    return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> RefreshMVInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<RefreshMVGlobalState>();
}

static void RefreshMVFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<RefreshMVBindData>();
    auto &state = data_p.global_state->Cast<RefreshMVGlobalState>();
    if (state.done) {
        return;
    }

    auto &db = DatabaseInstance::GetDatabase(context);

    // Resolve dependencies and materialize in order
    auto order = DAGResolver::ResolveFor(db, bind_data.view_name);
    for (auto &name : order) {
        Materializer::Materialize(context, name, "manual");
    }

    output.SetValue(0, 0, Value("Refreshed materialized view '" + bind_data.view_name + "' successfully"));
    output.SetCardinality(1);
    state.done = true;
}

TableFunction GetRefreshMaterializedViewFunction() {
    TableFunction func("pipeline_refresh_mv",
                       {LogicalType::VARCHAR, LogicalType::VARCHAR},
                       RefreshMVFunc, RefreshMVBind, RefreshMVInit);
    return func;
}

//===--------------------------------------------------------------------===//
// REFRESH ALL MATERIALIZED VIEWS TableFunction
//===--------------------------------------------------------------------===//

struct RefreshAllMVBindData : public TableFunctionData {
    bool best_effort = false;
};

struct RefreshAllMVGlobalState : public GlobalTableFunctionState {
    bool done = false;
};

static unique_ptr<FunctionData> RefreshAllMVBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
    auto data = make_uniq<RefreshAllMVBindData>();
    if (!input.inputs.empty()) {
        string mode = StringValue::Get(input.inputs[0]);
        data->best_effort = (mode == "best_effort");
    }
    names.emplace_back("status");
    return_types.emplace_back(LogicalType::VARCHAR);
    return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> RefreshAllMVInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<RefreshAllMVGlobalState>();
}

static void RefreshAllMVFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<RefreshAllMVBindData>();
    auto &state = data_p.global_state->Cast<RefreshAllMVGlobalState>();
    if (state.done) {
        return;
    }

    Materializer::MaterializeAll(context, bind_data.best_effort);

    output.SetValue(0, 0, Value("All materialized views refreshed successfully"));
    output.SetCardinality(1);
    state.done = true;
}

TableFunction GetRefreshAllMaterializedViewsFunction() {
    TableFunction func("pipeline_refresh_all_mvs", {LogicalType::VARCHAR},
                       RefreshAllMVFunc, RefreshAllMVBind, RefreshAllMVInit);
    return func;
}

} // namespace duckdb
