#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/string_util.hpp"
#include "persistence/pipeline_persistence.hpp"

namespace duckdb {

struct PipelineExpectationsBindData : public TableFunctionData {};

struct PipelineExpectationsGlobalState : public GlobalTableFunctionState {
	idx_t offset = 0;
	unique_ptr<MaterializedQueryResult> result;
};

static unique_ptr<FunctionData> PipelineExpectationsBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<PipelineExpectationsBindData>();

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

static unique_ptr<GlobalTableFunctionState> PipelineExpectationsInit(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto state = make_uniq<PipelineExpectationsGlobalState>();

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
		string db_prefix = databases[i] == "memory" ? "" : databases[i];
		string el = PipelinePersistence::QualifyTable(db_prefix, "expectation_logs");
		string rl = PipelinePersistence::QualifyTable(db_prefix, "run_logs");
		query += "SELECT e.view_name, e.expectation_name, e.total_rows, e.passed, e.failed, e.action "
		         "FROM " +
		         el +
		         " e "
		         "INNER JOIN (SELECT view_name, MAX(run_id) AS max_run_id "
		         "            FROM " +
		         rl +
		         " GROUP BY view_name) r "
		         "ON e.view_name = r.view_name AND e.run_id = r.max_run_id";
	}

	Connection conn(db);
	state->result = conn.Query(query);

	return std::move(state);
}

static void PipelineExpectationsFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<PipelineExpectationsGlobalState>();

	if (!state.result || state.result->HasError() || state.offset >= state.result->RowCount()) {
		return;
	}

	idx_t count = 0;
	idx_t max_count = STANDARD_VECTOR_SIZE;

	while (state.offset < state.result->RowCount() && count < max_count) {
		for (idx_t col = 0; col < 6; col++) {
			output.SetValue(col, count, state.result->GetValue(col, state.offset));
		}
		state.offset++;
		count++;
	}

	output.SetCardinality(count);
}

TableFunction GetPipelineExpectationsFunction() {
	TableFunction func("pipeline_expectations", {}, PipelineExpectationsFunc, PipelineExpectationsBind,
	                   PipelineExpectationsInit);
	return func;
}

} // namespace duckdb
