#pragma once
#include "duckdb.hpp"
#include "catalog/materialized_view_catalog.hpp"

namespace duckdb {

class ExpectationChecker {
public:
    // Apply expectations to a base query. May throw for FAIL_UPDATE violations.
    // Returns the (possibly wrapped) final query string.
    static string ApplyExpectations(ClientContext &context,
                                    const string &base_query,
                                    const vector<Expectation> &expectations);
};

} // namespace duckdb
