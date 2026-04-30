#define DUCKDB_EXTENSION_MAIN

#include "hortus_pipeline_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "parser/pipeline_parser.hpp"
#include "persistence/pipeline_persistence.hpp"
#include "scheduler/scheduler.hpp"

namespace duckdb {

// Forward declarations
TableFunction GetPipelineStatusFunction();
TableFunction GetPipelineSchedulesFunction();
TableFunction GetPipelineFiresFunction();
TableFunction GetPipelineExpectationsFunction();
TableFunction GetPipelineRunLogsFunction();
TableFunction GetPipelineExpectationLogsFunction();

static void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	ParserExtension::Register(config, PipelineParserExtension());

	Connection conn(db);
	conn.Query("SET allow_parser_override_extension='fallback'");

	// Register table functions
	loader.RegisterFunction(GetPipelineStatusFunction());
	loader.RegisterFunction(GetPipelineSchedulesFunction());
	loader.RegisterFunction(GetPipelineFiresFunction());
	loader.RegisterFunction(GetPipelineExpectationsFunction());
	loader.RegisterFunction(GetPipelineRunLogsFunction());
	loader.RegisterFunction(GetPipelineExpectationLogsFunction());

	// Hydrate scheduler from persisted schedules
	auto &persistence = PipelinePersistence::Get();
	// Check if __pipeline__ schema exists (from a previous session)
	{
		Connection check_conn(db);
		auto schema_check = check_conn.Query(
		    "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '__pipeline__'");
		if (!schema_check->HasError() && schema_check->RowCount() > 0 &&
		    schema_check->GetValue(0, 0).GetValue<int64_t>() > 0) {
			// Schema exists -- load schedule info and register with scheduler
			auto names = persistence.GetAllNames(db);
			auto &scheduler = PipelineScheduler::Get(db);
			for (auto &name : names) {
				auto def = persistence.GetView(db, "", name);
				if (def.schedule_type != 0 && !def.schedule_paused) {
					scheduler.AddSchedule(name);
				}
			}
		}
	}

	// Start the background scheduler
	PipelineScheduler::Get(db);
}

void HortusPipelineExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string HortusPipelineExtension::Name() {
	return "hortus_pipeline";
}

std::string HortusPipelineExtension::Version() const {
#ifdef EXT_VERSION_HORTUS_PIPELINE
	return EXT_VERSION_HORTUS_PIPELINE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(hortus_pipeline, loader) {
	duckdb::LoadInternal(loader);
}
}
