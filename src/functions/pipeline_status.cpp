#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/string_util.hpp"
#include "persistence/pipeline_persistence.hpp"
#include "executor/dag_resolver.hpp"
#include <unordered_set>

namespace duckdb {

struct PipelineStatusBindData : public TableFunctionData {};

struct PipelineStatusRow {
	string name;
	string query;
	string dependencies;
	bool is_materialized;
	string comment;
};

struct PipelineStatusGlobalState : public GlobalTableFunctionState {
	idx_t offset = 0;
	vector<PipelineStatusRow> rows;
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

	auto databases = persistence.GetAllPipelineDatabases(db);
	if (databases.empty()) {
		persistence.EnsureInitialized(db);
		databases.push_back("");
	}

	for (auto &raw_db : databases) {
		string database = (raw_db == "memory") ? "" : raw_db;
		auto all_names = persistence.GetAllNames(db, database);
		unordered_set<string> mv_set(all_names.begin(), all_names.end());

		for (auto &name : all_names) {
			auto def = persistence.GetView(db, database, name);
			auto deps = DAGResolver::ResolveEffectiveDeps(def, mv_set);

			string deps_str = StringUtil::Join(deps, ",");

			PipelineStatusRow row;
			row.name = def.name;
			row.query = def.query;
			row.dependencies = deps_str;
			row.is_materialized = def.is_materialized;
			row.comment = def.comment;
			state->rows.push_back(std::move(row));
		}
	}

	return std::move(state);
}

static void PipelineStatusFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<PipelineStatusGlobalState>();

	if (state.offset >= state.rows.size()) {
		return;
	}

	idx_t count = 0;
	idx_t max_count = STANDARD_VECTOR_SIZE;

	while (state.offset < state.rows.size() && count < max_count) {
		auto &row = state.rows[state.offset];
		output.SetValue(0, count, Value(row.name));
		output.SetValue(1, count, Value(row.query));
		output.SetValue(2, count, Value(row.dependencies));
		output.SetValue(3, count, Value::BOOLEAN(row.is_materialized));
		output.SetValue(4, count, row.comment.empty() ? Value(LogicalType::VARCHAR) : Value(row.comment));
		state.offset++;
		count++;
	}

	output.SetCardinality(count);
}

TableFunction GetPipelineStatusFunction() {
	TableFunction func("pipeline_status", {}, PipelineStatusFunc, PipelineStatusBind, PipelineStatusInit);
	return func;
}

} // namespace duckdb
