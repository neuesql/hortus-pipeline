#pragma once
#include "duckdb.hpp"
#include "persistence/pipeline_persistence.hpp"

namespace duckdb {

class Materializer {
public:
	static void Materialize(ClientContext &context, const string &view_name, const string &trigger = "manual");
	static void MaterializeAll(ClientContext &context, bool best_effort = false, const string &trigger = "refresh_all");
};

} // namespace duckdb
