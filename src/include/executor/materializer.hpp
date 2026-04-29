#pragma once
#include "duckdb.hpp"
#include "catalog/materialized_view_catalog.hpp"

namespace duckdb {

class Materializer {
public:
    static void Materialize(ClientContext &context, MaterializedViewCatalog &catalog, const string &view_name);
    static void MaterializeAll(ClientContext &context, MaterializedViewCatalog &catalog);
};

} // namespace duckdb
