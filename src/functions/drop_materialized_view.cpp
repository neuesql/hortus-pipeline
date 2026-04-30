#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/string_util.hpp"
#include "persistence/pipeline_persistence.hpp"
#include "scheduler/scheduler.hpp"

namespace duckdb {

struct DropMVBindData : public TableFunctionData {
	string view_name;
};

struct DropMVGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> DropMVBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<DropMVBindData>();
	data->view_name = StringValue::Get(input.inputs[0]);

	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);
	return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> DropMVInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<DropMVGlobalState>();
}

static void DropMVFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<DropMVBindData>();
	auto &state = data_p.global_state->Cast<DropMVGlobalState>();
	if (state.done) {
		return;
	}

	auto &db = DatabaseInstance::GetDatabase(context);
	auto &persistence = PipelinePersistence::Get();
	auto resolved = PipelinePersistence::ResolveQualifiedName(bind_data.view_name);
	auto &database = resolved.first;
	auto &unqualified_name = resolved.second;

	// Check existence
	if (!persistence.Exists(db, database, unqualified_name)) {
		throw InvalidInputException("Materialized view '%s' not found", bind_data.view_name);
	}

	// Remove from scheduler
	PipelineScheduler::Get(db).RemoveSchedule(bind_data.view_name);

	// Cascade delete from __pipeline__ tables
	persistence.CascadeDelete(db, database, unqualified_name);

	// Drop the underlying materialized table
	Connection conn(db);
	auto result = conn.Query("DROP TABLE IF EXISTS " + bind_data.view_name);
	if (result->HasError()) {
		throw InternalException("Failed to drop materialized table: %s", result->GetError());
	}

	output.SetValue(0, 0, Value("Dropped materialized view '" + bind_data.view_name + "'"));
	output.SetCardinality(1);
	state.done = true;
}

TableFunction GetDropMaterializedViewFunction() {
	TableFunction func("pipeline_drop_mv", {LogicalType::VARCHAR}, DropMVFunc, DropMVBind, DropMVInit);
	return func;
}

} // namespace duckdb
