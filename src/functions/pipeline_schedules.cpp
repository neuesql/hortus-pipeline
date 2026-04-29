#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/string_util.hpp"
#include "catalog/materialized_view_catalog.hpp"
#include "scheduler/scheduler.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// pipeline_schedules() TableFunction
//===--------------------------------------------------------------------===//

struct PipelineSchedulesBindData : public TableFunctionData {};

struct PipelineSchedulesGlobalState : public GlobalTableFunctionState {
    idx_t offset = 0;
    vector<PipelineScheduler::ScheduleInfo> schedules;
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
    auto &scheduler = PipelineScheduler::Get(db);
    state->schedules = scheduler.ListSchedules();
    return std::move(state);
}

static void PipelineSchedulesFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<PipelineSchedulesGlobalState>();

    if (state.offset >= state.schedules.size()) {
        return;
    }

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (state.offset < state.schedules.size() && count < max_count) {
        auto &info = state.schedules[state.offset];
        output.SetValue(0, count, Value(info.name));
        output.SetValue(1, count, Value(info.schedule_description));
        output.SetValue(2, count, Value::BOOLEAN(info.paused));
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
// pipeline_check_schedules() TableFunction
//===--------------------------------------------------------------------===//

struct CheckSchedulesBindData : public TableFunctionData {};

struct CheckSchedulesGlobalState : public GlobalTableFunctionState {
    idx_t offset = 0;
    vector<pair<string, string>> results; // name, status
};

static unique_ptr<FunctionData> CheckSchedulesBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
    auto data = make_uniq<CheckSchedulesBindData>();

    names.emplace_back("name");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("status");
    return_types.emplace_back(LogicalType::VARCHAR);

    return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> CheckSchedulesInit(ClientContext &context, TableFunctionInitInput &input) {
    auto state = make_uniq<CheckSchedulesGlobalState>();

    auto &db = DatabaseInstance::GetDatabase(context);
    auto &catalog = MaterializedViewCatalog::Get(db);
    auto names = catalog.GetAllNames();

    for (auto &name : names) {
        auto &def = catalog.Get(name);
        if (def.schedule_type == 0) {
            continue;
        }
        if (def.schedule_paused) {
            state->results.push_back({name, "paused"});
            continue;
        }
        // For check_schedules, we refresh all scheduled views immediately
        try {
            Connection conn(db);
            auto result = conn.Query("REFRESH MATERIALIZED VIEW " + name);
            if (result->HasError()) {
                state->results.push_back({name, "error: " + result->GetError()});
            } else {
                state->results.push_back({name, "refreshed"});
            }
        } catch (std::exception &e) {
            state->results.push_back({name, string("error: ") + e.what()});
        }
    }

    return std::move(state);
}

static void CheckSchedulesFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<CheckSchedulesGlobalState>();

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

TableFunction GetPipelineCheckSchedulesFunction() {
    TableFunction func("pipeline_check_schedules", {},
                       CheckSchedulesFunc, CheckSchedulesBind, CheckSchedulesInit);
    return func;
}

} // namespace duckdb
