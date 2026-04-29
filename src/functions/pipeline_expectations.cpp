#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/string_util.hpp"
#include "catalog/materialized_view_catalog.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// pipeline_expectations() TableFunction
//===--------------------------------------------------------------------===//

struct PipelineExpectationsBindData : public TableFunctionData {};

struct PipelineExpectationsGlobalState : public GlobalTableFunctionState {
    idx_t view_offset = 0;
    idx_t metric_offset = 0;
    vector<string> names;
};

static unique_ptr<FunctionData> PipelineExpectationsBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
    auto data = make_uniq<PipelineExpectationsBindData>();

    names.emplace_back("view_name");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("constraint_name");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("total_rows");
    return_types.emplace_back(LogicalType::BIGINT);

    names.emplace_back("passed");
    return_types.emplace_back(LogicalType::BIGINT);

    names.emplace_back("failed");
    return_types.emplace_back(LogicalType::BIGINT);

    names.emplace_back("action");
    return_types.emplace_back(LogicalType::VARCHAR);

    return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> PipelineExpectationsInit(ClientContext &context, TableFunctionInitInput &input) {
    auto state = make_uniq<PipelineExpectationsGlobalState>();

    auto &db = DatabaseInstance::GetDatabase(context);
    auto &catalog = MaterializedViewCatalog::Get(db);
    state->names = catalog.GetAllNames();

    return std::move(state);
}

static void PipelineExpectationsFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<PipelineExpectationsGlobalState>();

    auto &db = DatabaseInstance::GetDatabase(context);
    auto &catalog = MaterializedViewCatalog::Get(db);

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (state.view_offset < state.names.size() && count < max_count) {
        auto &name = state.names[state.view_offset];
        auto &def = catalog.Get(name);
        auto &metrics = def.last_expectation_metrics;

        if (metrics.empty()) {
            state.view_offset++;
            state.metric_offset = 0;
            continue;
        }

        while (state.metric_offset < metrics.size() && count < max_count) {
            auto &m = metrics[state.metric_offset];

            output.SetValue(0, count, Value(name));
            output.SetValue(1, count, Value(m.constraint_name));
            output.SetValue(2, count, Value::BIGINT(m.total_rows));
            output.SetValue(3, count, Value::BIGINT(m.passed));
            output.SetValue(4, count, Value::BIGINT(m.failed));
            output.SetValue(5, count, Value(m.action));

            state.metric_offset++;
            count++;
        }

        if (state.metric_offset >= metrics.size()) {
            state.view_offset++;
            state.metric_offset = 0;
        }
    }

    output.SetCardinality(count);
}

TableFunction GetPipelineExpectationsFunction() {
    TableFunction func("pipeline_expectations", {},
                       PipelineExpectationsFunc, PipelineExpectationsBind, PipelineExpectationsInit);
    return func;
}

} // namespace duckdb
