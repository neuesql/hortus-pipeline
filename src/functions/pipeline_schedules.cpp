#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/string_util.hpp"
#include "persistence/pipeline_persistence.hpp"
#include "executor/materializer.hpp"
#include "scheduler/scheduler.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// pipeline_schedules() TableFunction
//===--------------------------------------------------------------------===//

struct PipelineSchedulesBindData : public TableFunctionData {};

struct PipelineSchedulesGlobalState : public GlobalTableFunctionState {
    idx_t offset = 0;
    unique_ptr<MaterializedQueryResult> result;
};

static unique_ptr<FunctionData> PipelineSchedulesBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
    auto data = make_uniq<PipelineSchedulesBindData>();

    names.emplace_back("name");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("schedule");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("paused");
    return_types.emplace_back(LogicalType::BOOLEAN);

    return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> PipelineSchedulesInit(ClientContext &context, TableFunctionInitInput &input) {
    auto state = make_uniq<PipelineSchedulesGlobalState>();

    auto &db = DatabaseInstance::GetDatabase(context);
    auto &persistence = PipelinePersistence::Get();
    persistence.EnsureInitialized(db);

    Connection conn(db);
    state->result = conn.Query(
        "SELECT view_name, schedule_type, interval_value, interval_unit, cron_expression, paused "
        "FROM __pipeline__.schedules");

    return std::move(state);
}

static void PipelineSchedulesFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<PipelineSchedulesGlobalState>();

    if (!state.result || state.result->HasError() || state.offset >= state.result->RowCount()) {
        return;
    }

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (state.offset < state.result->RowCount() && count < max_count) {
        string name = state.result->GetValue(0, state.offset).ToString();
        int stype = state.result->GetValue(1, state.offset).GetValue<int>();
        string schedule_desc;
        switch (stype) {
        case 1:
            schedule_desc = "EVERY " + state.result->GetValue(2, state.offset).ToString() + " " +
                            state.result->GetValue(3, state.offset).ToString();
            break;
        case 2:
            schedule_desc = "CRON " + state.result->GetValue(4, state.offset).ToString();
            break;
        case 3:
            schedule_desc = "TRIGGER ON UPDATE";
            break;
        }
        bool paused = state.result->GetValue(5, state.offset).GetValue<bool>();

        output.SetValue(0, count, Value(name));
        output.SetValue(1, count, Value(schedule_desc));
        output.SetValue(2, count, Value::BOOLEAN(paused));
        state.offset++;
        count++;
    }

    output.SetCardinality(count);
}

TableFunction GetPipelineSchedulesFunction() {
    TableFunction func("pipeline_schedules", {},
                       PipelineSchedulesFunc, PipelineSchedulesBind, PipelineSchedulesInit);
    return func;
}

//===--------------------------------------------------------------------===//
// pipeline_fires() TableFunction (renamed from pipeline_check_schedules)
//===--------------------------------------------------------------------===//

struct FiresBindData : public TableFunctionData {};

struct FiresGlobalState : public GlobalTableFunctionState {
    idx_t offset = 0;
    vector<pair<string, string>> results;
};

static unique_ptr<FunctionData> FiresBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
    auto data = make_uniq<FiresBindData>();

    names.emplace_back("name");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("status");
    return_types.emplace_back(LogicalType::VARCHAR);

    return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> FiresInit(ClientContext &context, TableFunctionInitInput &input) {
    auto state = make_uniq<FiresGlobalState>();

    auto &db = DatabaseInstance::GetDatabase(context);
    auto &persistence = PipelinePersistence::Get();
    persistence.EnsureInitialized(db);

    Connection conn(db);
    auto sched_result = conn.Query("SELECT view_name, paused FROM __pipeline__.schedules");
    if (!sched_result->HasError()) {
        for (idx_t i = 0; i < sched_result->RowCount(); i++) {
            string name = sched_result->GetValue(0, i).ToString();
            bool paused = sched_result->GetValue(1, i).GetValue<bool>();

            if (paused) {
                state->results.push_back({name, "paused"});
                continue;
            }
            try {
                Materializer::Materialize(context, name, "schedule");
                state->results.push_back({name, "refreshed"});
            } catch (std::exception &e) {
                state->results.push_back({name, string("error: ") + e.what()});
            }
        }
    }

    return std::move(state);
}

static void FiresFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<FiresGlobalState>();

    if (state.offset >= state.results.size()) {
        return;
    }

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (state.offset < state.results.size() && count < max_count) {
        auto &r = state.results[state.offset];
        output.SetValue(0, count, Value(r.first));
        output.SetValue(1, count, Value(r.second));
        state.offset++;
        count++;
    }

    output.SetCardinality(count);
}

TableFunction GetPipelineFiresFunction() {
    TableFunction func("pipeline_fires", {},
                       FiresFunc, FiresBind, FiresInit);
    return func;
}

} // namespace duckdb
