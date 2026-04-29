#define DUCKDB_EXTENSION_MAIN

#include "hortus_pipeline_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "parser/pipeline_parser.hpp"
#include "scheduler/scheduler.hpp"

namespace duckdb {

// Forward declarations
TableFunction GetPipelineStatusFunction();
TableFunction GetPipelineSchedulesFunction();
TableFunction GetPipelineCheckSchedulesFunction();
TableFunction GetPipelineExpectationsFunction();

static void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	ParserExtension::Register(config, PipelineParserExtension());

	// Enable parser override so DROP/ALTER MATERIALIZED VIEW can be intercepted
	// before DuckDB's native parser (which accepts the syntax but fails at transform)
	Connection conn(db);
	conn.Query("SET allow_parser_override_extension='fallback'");

	// Register table functions
	loader.RegisterFunction(GetPipelineStatusFunction());
	loader.RegisterFunction(GetPipelineSchedulesFunction());
	loader.RegisterFunction(GetPipelineCheckSchedulesFunction());
	loader.RegisterFunction(GetPipelineExpectationsFunction());

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
