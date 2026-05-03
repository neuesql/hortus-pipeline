#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "persistence/pipeline_persistence.hpp"

namespace duckdb {

struct PipelineExpectationLogsBindData : public TableFunctionData {};

struct PipelineExpectationLogsGlobalState : public GlobalTableFunctionState {
	idx_t offset = 0;
	unique_ptr<MaterializedQueryResult> result;
};

static unique_ptr<FunctionData> PipelineExpectationLogsBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<PipelineExpectationLogsBindData>();

	names.emplace_back("run_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("view_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("expectation_name");
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

static unique_ptr<GlobalTableFunctionState> PipelineExpectationLogsInit(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	auto state = make_uniq<PipelineExpectationLogsGlobalState>();

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
		string table =
		    PipelinePersistence::QualifyTable(databases[i] == "memory" ? "" : databases[i], "expectation_logs");
		query += "SELECT run_id, view_name, expectation_name, total_rows, passed, failed, action FROM " + table;
	}
	query += " ORDER BY run_id";

	Connection conn(db);
	state->result = conn.Query(query);

	return std::move(state);
}

static void PipelineExpectationLogsFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<PipelineExpectationLogsGlobalState>();

	if (!state.result || state.result->HasError() || state.offset >= state.result->RowCount()) {
		return;
	}

	idx_t count = 0;
	idx_t max_count = STANDARD_VECTOR_SIZE;

	while (state.offset < state.result->RowCount() && count < max_count) {
		for (idx_t col = 0; col < 7; col++) {
			output.SetValue(col, count, state.result->GetValue(col, state.offset));
		}
		state.offset++;
		count++;
	}

	output.SetCardinality(count);
}

TableFunction GetPipelineExpectationLogsFunction() {
	TableFunction func("pipeline_expectation_logs", {}, PipelineExpectationLogsFunc, PipelineExpectationLogsBind,
	                   PipelineExpectationLogsInit);
	return func;
}

} // namespace duckdb
