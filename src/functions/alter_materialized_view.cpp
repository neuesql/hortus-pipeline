#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/string_util.hpp"
#include "persistence/pipeline_persistence.hpp"
#include "scheduler/scheduler.hpp"

namespace duckdb {

struct AlterMVBindData : public TableFunctionData {
    string view_name;
    string alter_action;
    string new_query;
    string constraint_name;
    string expression;
    string action_string;
    string drop_constraint_name;
};

struct AlterMVGlobalState : public GlobalTableFunctionState {
    bool done = false;
};

static unique_ptr<FunctionData> AlterMVBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
    auto data = make_uniq<AlterMVBindData>();
    data->view_name = StringValue::Get(input.inputs[0]);
    data->alter_action = StringValue::Get(input.inputs[1]);

    if (data->alter_action == "SET_QUERY") {
        data->new_query = StringValue::Get(input.inputs[2]);
    } else if (data->alter_action == "ADD_CONSTRAINT") {
        data->constraint_name = StringValue::Get(input.inputs[2]);
        data->expression = StringValue::Get(input.inputs[3]);
        data->action_string = StringValue::Get(input.inputs[4]);
    } else if (data->alter_action == "DROP_CONSTRAINT") {
        data->drop_constraint_name = StringValue::Get(input.inputs[2]);
    }

    names.emplace_back("status");
    return_types.emplace_back(LogicalType::VARCHAR);
    return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> AlterMVInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<AlterMVGlobalState>();
}

static void AlterMVFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<AlterMVBindData>();
    auto &state = data_p.global_state->Cast<AlterMVGlobalState>();
    if (state.done) {
        return;
    }

    auto &db = DatabaseInstance::GetDatabase(context);
    auto &persistence = PipelinePersistence::Get();
    auto [database, unqualified_name] = PipelinePersistence::ResolveQualifiedName(bind_data.view_name);

    // Check existence
    if (!persistence.Exists(db, database, unqualified_name)) {
        throw InvalidInputException("Materialized view '%s' not found", bind_data.view_name);
    }

    string status;

    if (bind_data.alter_action == "SET_QUERY") {
        persistence.UpdateViewQuery(db, database, unqualified_name, bind_data.new_query);
        status = "Altered materialized view '" + bind_data.view_name + "' query (refresh needed to apply)";
    } else if (bind_data.alter_action == "ADD_CONSTRAINT") {
        string action_str;
        if (bind_data.action_string == "DROP_ROW") action_str = "DROP ROW";
        else if (bind_data.action_string == "FAIL_UPDATE") action_str = "FAIL UPDATE";
        else action_str = "WARN";
        persistence.AddConstraint(db, database, unqualified_name,
                                   bind_data.constraint_name, bind_data.expression, action_str);
        status = "Added constraint '" + bind_data.constraint_name + "' to materialized view '" + bind_data.view_name + "'";
    } else if (bind_data.alter_action == "DROP_CONSTRAINT") {
        persistence.DropConstraint(db, database, unqualified_name, bind_data.drop_constraint_name);
        status = "Dropped constraint '" + bind_data.drop_constraint_name + "' from materialized view '" + bind_data.view_name + "'";
    } else if (bind_data.alter_action == "PAUSE_SCHEDULE") {
        persistence.UpdateSchedulePaused(db, database, unqualified_name, true);
        PipelineScheduler::Get(db).PauseSchedule(bind_data.view_name);
        status = "Paused schedule for materialized view '" + bind_data.view_name + "'";
    } else if (bind_data.alter_action == "RESUME_SCHEDULE") {
        persistence.UpdateSchedulePaused(db, database, unqualified_name, false);
        PipelineScheduler::Get(db).ResumeSchedule(bind_data.view_name);
        status = "Resumed schedule for materialized view '" + bind_data.view_name + "'";
    }

    output.SetValue(0, 0, Value(status));
    output.SetCardinality(1);
    state.done = true;
}

TableFunction GetAlterMaterializedViewFunction() {
    TableFunction func("pipeline_alter_mv",
                       {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                        LogicalType::VARCHAR, LogicalType::VARCHAR},
                       AlterMVFunc, AlterMVBind, AlterMVInit);
    return func;
}

} // namespace duckdb
