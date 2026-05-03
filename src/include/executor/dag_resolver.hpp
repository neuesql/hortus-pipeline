#pragma once
#include "duckdb.hpp"
#include "persistence/pipeline_persistence.hpp"
#include <unordered_set>

namespace duckdb {

class DAGResolver {
public:
	static vector<string> Resolve(DatabaseInstance &db, const string &database = "");
	static vector<string> ResolveFor(DatabaseInstance &db, const string &target, const string &database = "");
	static vector<string> ExtractDependencies(const string &query);
	static vector<string> ResolveEffectiveDeps(const MaterializedViewDefinition &def,
	                                            const unordered_set<string> &mv_set);
};

} // namespace duckdb
