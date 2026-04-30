#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/string_util.hpp"
#include "persistence/pipeline_persistence.hpp"
#include "executor/materializer.hpp"
#include "scheduler/scheduler.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Deserialization helpers
//===--------------------------------------------------------------------===//

static vector<Expectation> DeserializeExpectations(const string &serialized) {
    vector<Expectation> result;
    if (serialized.empty()) {
        return result;
    }
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
    string serialized_schedule;
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
    data->serialized_schedule = StringValue::Get(input.inputs[5]);

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
    auto &persistence = PipelinePersistence::Get();
    auto [database, unqualified_name] = PipelinePersistence::ResolveQualifiedName(bind_data.view_name);

    // Persist view definition to __pipeline__ tables
    persistence.PersistView(db, database, unqualified_name, bind_data.query,
                            bind_data.comment, expectations, deps);

    // Store schedule info if present
    if (!bind_data.serialized_schedule.empty()) {
        int sched_type = 0;
        int sched_interval = 0;
        string sched_unit, sched_cron;

        if (StringUtil::StartsWith(bind_data.serialized_schedule, "EVERY:")) {
            auto parts = StringUtil::Split(bind_data.serialized_schedule, ":");
            if (parts.size() >= 3) {
                sched_type = 1;
                sched_interval = std::stoi(parts[1]);
                sched_unit = parts[2];
            }
        } else if (StringUtil::StartsWith(bind_data.serialized_schedule, "CRON:")) {
            sched_type = 2;
            sched_cron = bind_data.serialized_schedule.substr(5);
        } else if (bind_data.serialized_schedule == "ON_UPDATE") {
            sched_type = 3;
        }

        persistence.PersistSchedule(db, database, unqualified_name,
                                     sched_type, sched_interval, sched_unit, sched_cron);

        // Register with scheduler
        auto &scheduler = PipelineScheduler::Get(db);
        scheduler.RemoveSchedule(bind_data.view_name);
        scheduler.AddSchedule(bind_data.view_name);
    }

    // Materialize
    Materializer::Materialize(context, bind_data.view_name);

    output.SetValue(0, 0, Value("Materialized view '" + bind_data.view_name + "' created successfully"));
    output.SetCardinality(1);
    state.done = true;
}

TableFunction GetCreateMaterializedViewFunction() {
    TableFunction func("pipeline_create_mv",
                       {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                        LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                       CreateMVFunc, CreateMVBind, CreateMVInit);
    return func;
}

} // namespace duckdb
