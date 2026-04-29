#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/string_util.hpp"
#include "catalog/materialized_view_catalog.hpp"
#include "executor/materializer.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Deserialization helpers
//===--------------------------------------------------------------------===//

static vector<Expectation> DeserializeExpectations(const string &serialized) {
    vector<Expectation> result;
    if (serialized.empty()) {
        return result;
    }
    // Format: name:::expression:::action|||name:::expression:::action
    auto entries = StringUtil::Split(serialized, "|||");
    for (auto &entry : entries) {
        auto parts = StringUtil::Split(entry, ":::");
        if (parts.size() != 3) {
            continue;
        }
        Expectation exp;
        exp.name = parts[0];
        exp.expression = parts[1];
        if (parts[2] == "WARN") {
            exp.action = ExpectationAction::WARN;
        } else if (parts[2] == "DROP_ROW") {
            exp.action = ExpectationAction::DROP_ROW;
        } else if (parts[2] == "FAIL_UPDATE") {
            exp.action = ExpectationAction::FAIL_UPDATE;
        }
        result.push_back(std::move(exp));
    }
    return result;
}

static vector<string> DeserializeDependsOn(const string &serialized) {
    vector<string> result;
    if (serialized.empty()) {
        return result;
    }
    return StringUtil::Split(serialized, ",");
}

//===--------------------------------------------------------------------===//
// CREATE MATERIALIZED VIEW TableFunction
//===--------------------------------------------------------------------===//

struct CreateMVBindData : public TableFunctionData {
    string view_name;
    string query;
    string comment;
    string serialized_expectations;
    string serialized_deps;
};

struct CreateMVGlobalState : public GlobalTableFunctionState {
    bool done = false;
};

static unique_ptr<FunctionData> CreateMVBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
    auto data = make_uniq<CreateMVBindData>();
    data->view_name = StringValue::Get(input.inputs[0]);
    data->query = StringValue::Get(input.inputs[1]);
    data->comment = StringValue::Get(input.inputs[2]);
    data->serialized_expectations = StringValue::Get(input.inputs[3]);
    data->serialized_deps = StringValue::Get(input.inputs[4]);

    names.emplace_back("status");
    return_types.emplace_back(LogicalType::VARCHAR);
    return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> CreateMVInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<CreateMVGlobalState>();
}

static void CreateMVFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<CreateMVBindData>();
    auto &state = data_p.global_state->Cast<CreateMVGlobalState>();
    if (state.done) {
        return;
    }

    auto expectations = DeserializeExpectations(bind_data.serialized_expectations);
    auto deps = DeserializeDependsOn(bind_data.serialized_deps);

    auto &db = DatabaseInstance::GetDatabase(context);
    auto &catalog = MaterializedViewCatalog::Get(db);

    // Register in catalog
    catalog.CreateOrRefresh(bind_data.view_name, bind_data.query,
                            bind_data.comment, expectations, deps);

    // Materialize
    Materializer::Materialize(context, catalog, bind_data.view_name);

    output.SetValue(0, 0, Value("Materialized view '" + bind_data.view_name + "' created successfully"));
    output.SetCardinality(1);
    state.done = true;
}

TableFunction GetCreateMaterializedViewFunction() {
    TableFunction func("pipeline_create_mv",
                       {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                        LogicalType::VARCHAR, LogicalType::VARCHAR},
                       CreateMVFunc, CreateMVBind, CreateMVInit);
    return func;
}

} // namespace duckdb
