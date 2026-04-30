#pragma once
#include "duckdb.hpp"
#include "persistence/pipeline_persistence.hpp"

namespace duckdb {

class DAGResolver {
public:
    static vector<string> Resolve(DatabaseInstance &db, const string &database = "");
    static vector<string> ResolveFor(DatabaseInstance &db, const string &target, const string &database = "");
    static vector<string> ExtractDependencies(const string &query);
};

} // namespace duckdb
