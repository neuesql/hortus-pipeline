#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "persistence/pipeline_persistence.hpp"

namespace duckdb {

struct PipelineRunLogsBindData : public TableFunctionData {};

struct PipelineRunLogsGlobalState : public GlobalTableFunctionState {
	idx_t offset = 0;
	unique_ptr<MaterializedQueryResult> result;
};

static unique_ptr<FunctionData> PipelineRunLogsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<PipelineRunLogsBindData>();

	names.emplace_back("run_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("view_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("started_at");
	return_types.emplace_back(LogicalType::TIMESTAMP);
	names.emplace_back("finished_at");
	return_types.emplace_back(LogicalType::TIMESTAMP);
	names.emplace_back("success");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("error_message");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("trigger");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("rows_affected");
	return_types.emplace_back(LogicalType::BIGINT);

	return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> PipelineRunLogsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<PipelineRunLogsGlobalState>();

	auto &db = DatabaseInstance::GetDatabase(context);
	auto &persistence = PipelinePersistence::Get();

	auto databases = persistence.GetAllPipelineDatabases(db);
	if (databases.empty()) {
		persistence.EnsureInitialized(db);
		databases.push_back("");
	}

	string query;
	for (idx_t i = 0; i < databases.size(); i++) {
		if (i > 0) {
			query += " UNION ALL ";
		}
		string table = PipelinePersistence::QualifyTable(databases[i] == "memory" ? "" : databases[i], "run_logs");
		query += "SELECT run_id, view_name, started_at, finished_at, success, error_message, \"trigger\", "
		         "rows_affected FROM " +
		         table;
	}
	query += " ORDER BY run_id";

	Connection conn(db);
	state->result = conn.Query(query);

	return std::move(state);
}

static void PipelineRunLogsFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<PipelineRunLogsGlobalState>();

	if (!state.result || state.result->HasError() || state.offset >= state.result->RowCount()) {
		return;
	}

	idx_t count = 0;
	idx_t max_count = STANDARD_VECTOR_SIZE;

	while (state.offset < state.result->RowCount() && count < max_count) {
		for (idx_t col = 0; col < 8; col++) {
			output.SetValue(col, count, state.result->GetValue(col, state.offset));
		}
		state.offset++;
		count++;
	}

	output.SetCardinality(count);
}

TableFunction GetPipelineRunLogsFunction() {
	TableFunction func("pipeline_run_logs", {}, PipelineRunLogsFunc, PipelineRunLogsBind, PipelineRunLogsInit);
	return func;
}

} // namespace duckdb
