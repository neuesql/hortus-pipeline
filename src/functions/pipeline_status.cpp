#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/string_util.hpp"
#include "persistence/pipeline_persistence.hpp"
#include "executor/dag_resolver.hpp"

namespace duckdb {

struct PipelineStatusBindData : public TableFunctionData {};

struct PipelineStatusGlobalState : public GlobalTableFunctionState {
    idx_t offset = 0;
    unique_ptr<MaterializedQueryResult> result;
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
    auto &persistence = PipelinePersistence::Get();
    persistence.EnsureInitialized(db);

    Connection conn(db);
    state->result = conn.Query("SELECT name, query, dependencies, is_materialized, comment FROM __pipeline__.views");

    return std::move(state);
}

static void PipelineStatusFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<PipelineStatusGlobalState>();

    if (!state.result || state.result->HasError() || state.offset >= state.result->RowCount()) {
        return;
    }

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (state.offset < state.result->RowCount() && count < max_count) {
        for (idx_t col = 0; col < 5; col++) {
            output.SetValue(col, count, state.result->GetValue(col, state.offset));
        }
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
