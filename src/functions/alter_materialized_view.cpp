#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/string_util.hpp"
#include "catalog/materialized_view_catalog.hpp"
#include "scheduler/scheduler.hpp"
#include "persistence/pipeline_persistence.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// ALTER MATERIALIZED VIEW TableFunction
//===--------------------------------------------------------------------===//

struct AlterMVBindData : public TableFunctionData {
    string view_name;
    string alter_action;
    // SET_QUERY
    string new_query;
    // ADD_CONSTRAINT
    string constraint_name;
    string expression;
    string action_string;
    // DROP_CONSTRAINT
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
    } else if (data->alter_action == "PAUSE_SCHEDULE" || data->alter_action == "RESUME_SCHEDULE") {
        // No additional params needed
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
    auto &catalog = MaterializedViewCatalog::Get(db);

    auto resolved = PipelinePersistence::ResolveQualifiedName(bind_data.view_name);
    auto &database = resolved.first;
    auto &unqualified_name = resolved.second;
    auto &persistence = PipelinePersistence::Get();

    string status;

    if (bind_data.alter_action == "SET_QUERY") {
        catalog.AlterQuery(bind_data.view_name, bind_data.new_query);
        persistence.UpdateViewQuery(db, database, unqualified_name, bind_data.new_query);
        status = "Altered materialized view '" + bind_data.view_name + "' query (refresh needed to apply)";
    } else if (bind_data.alter_action == "ADD_CONSTRAINT") {
        Expectation exp;
        exp.name = bind_data.constraint_name;
        exp.expression = bind_data.expression;
        if (bind_data.action_string == "DROP_ROW") {
            exp.action = ExpectationAction::DROP_ROW;
        } else if (bind_data.action_string == "FAIL_UPDATE") {
            exp.action = ExpectationAction::FAIL_UPDATE;
        } else {
            exp.action = ExpectationAction::WARN;
        }
        catalog.AddConstraint(bind_data.view_name, exp);
        string action_str;
        if (bind_data.action_string == "DROP_ROW") action_str = "DROP ROW";
        else if (bind_data.action_string == "FAIL_UPDATE") action_str = "FAIL UPDATE";
        else action_str = "WARN";
        persistence.AddConstraint(db, database, unqualified_name, bind_data.constraint_name, bind_data.expression, action_str);
        status = "Added constraint '" + bind_data.constraint_name + "' to materialized view '" + bind_data.view_name + "'";
    } else if (bind_data.alter_action == "DROP_CONSTRAINT") {
        catalog.DropConstraint(bind_data.view_name, bind_data.drop_constraint_name);
        persistence.DropConstraint(db, database, unqualified_name, bind_data.drop_constraint_name);
        status = "Dropped constraint '" + bind_data.drop_constraint_name + "' from materialized view '" + bind_data.view_name + "'";
    } else if (bind_data.alter_action == "PAUSE_SCHEDULE") {
        catalog.PauseSchedule(bind_data.view_name);
        PipelineScheduler::Get(db).PauseSchedule(bind_data.view_name);
        persistence.UpdateSchedulePaused(db, database, unqualified_name, true);
        status = "Paused schedule for materialized view '" + bind_data.view_name + "'";
    } else if (bind_data.alter_action == "RESUME_SCHEDULE") {
        catalog.ResumeSchedule(bind_data.view_name);
        PipelineScheduler::Get(db).ResumeSchedule(bind_data.view_name);
        persistence.UpdateSchedulePaused(db, database, unqualified_name, false);
        status = "Resumed schedule for materialized view '" + bind_data.view_name + "'";
    }

    output.SetValue(0, 0, Value(status));
    output.SetCardinality(1);
    state.done = true;
}

TableFunction GetAlterMaterializedViewFunction() {
    // We use 5 VARCHAR params to cover all cases. Unused params get empty strings.
    TableFunction func("pipeline_alter_mv",
                       {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                        LogicalType::VARCHAR, LogicalType::VARCHAR},
                       AlterMVFunc, AlterMVBind, AlterMVInit);
    return func;
}

} // namespace duckdb
