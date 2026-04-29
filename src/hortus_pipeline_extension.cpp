#define DUCKDB_EXTENSION_MAIN

#include "hortus_pipeline_extension.hpp"
#include "duckdb.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Extension functions will be registered here
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
