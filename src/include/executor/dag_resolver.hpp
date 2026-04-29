#pragma once
#include "duckdb.hpp"
#include "catalog/materialized_view_catalog.hpp"

namespace duckdb {

class DAGResolver {
public:
    // Topological sort of all MVs
    static vector<string> Resolve(const MaterializedViewCatalog &catalog);
    // Topological sort of target + its upstream deps only
    static vector<string> ResolveFor(const MaterializedViewCatalog &catalog, const string &target);
    // Extract table references from SQL (FROM/JOIN table patterns, skip functions)
    static vector<string> ExtractDependencies(const string &query);
};

} // namespace duckdb
