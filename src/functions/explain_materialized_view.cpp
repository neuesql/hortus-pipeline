#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/string_util.hpp"
#include "persistence/pipeline_persistence.hpp"
#include "executor/dag_resolver.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// EXPLAIN CREATE MATERIALIZED VIEW TableFunction
//===--------------------------------------------------------------------===//

struct ExplainMVBindData : public TableFunctionData {
    string view_name;
    string query;
};

struct ExplainMVGlobalState : public GlobalTableFunctionState {
    bool done = false;
    vector<string> lines;
    idx_t offset = 0;
};

static unique_ptr<FunctionData> ExplainMVBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
    auto data = make_uniq<ExplainMVBindData>();
    data->view_name = StringValue::Get(input.inputs[0]);
    data->query = StringValue::Get(input.inputs[1]);

    names.emplace_back("plan_info");
    return_types.emplace_back(LogicalType::VARCHAR);

    return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> ExplainMVInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<ExplainMVGlobalState>();
}

static void ExplainMVFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<ExplainMVBindData>();
    auto &state = data_p.global_state->Cast<ExplainMVGlobalState>();

    if (!state.done) {
        state.done = true;

        auto &db = DatabaseInstance::GetDatabase(context);

        // Line 1: Header
        state.lines.push_back("=== EXPLAIN CREATE MATERIALIZED VIEW " + bind_data.view_name + " ===");

        // Run EXPLAIN on the inner query
        {
            Connection conn(db);
            string explain_sql = "EXPLAIN " + bind_data.query;
            auto result = conn.Query(explain_sql);
            if (result->HasError()) {
                state.lines.push_back("Query plan: ERROR - " + result->GetError());
            } else {
                state.lines.push_back("--- Query Plan ---");
                for (idx_t row = 0; row < result->RowCount(); row++) {
                    string line;
                    for (idx_t col = 0; col < result->ColumnCount(); col++) {
                        if (col > 0) line += " | ";
                        line += result->GetValue(col, row).ToString();
                    }
                    state.lines.push_back(line);
                }
            }
        }

        // Extract and show dependencies
        auto deps = DAGResolver::ExtractDependencies(bind_data.query);
        state.lines.push_back("--- Dependencies ---");
        if (deps.empty()) {
            state.lines.push_back("(none detected)");
        } else {
            for (auto &dep : deps) {
                state.lines.push_back("  -> " + dep);
            }
        }
    }

    if (state.offset >= state.lines.size()) {
        return;
    }

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (state.offset < state.lines.size() && count < max_count) {
        output.SetValue(0, count, Value(state.lines[state.offset]));
        state.offset++;
        count++;
    }

    output.SetCardinality(count);
}

TableFunction GetExplainMaterializedViewFunction() {
    TableFunction func("pipeline_explain_mv",
                       {LogicalType::VARCHAR, LogicalType::VARCHAR},
                       ExplainMVFunc, ExplainMVBind, ExplainMVInit);
    return func;
}

} // namespace duckdb
