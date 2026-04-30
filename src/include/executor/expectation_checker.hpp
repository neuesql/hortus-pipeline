#pragma once
#include "duckdb.hpp"
#include "persistence/pipeline_persistence.hpp"

namespace duckdb {

class ExpectationChecker {
public:
	// Apply expectations to a base query. May throw for FAIL_UPDATE violations.
	// Returns the (possibly wrapped) final query string.
	static string ApplyExpectations(ClientContext &context, const string &base_query,
	                                const vector<Expectation> &expectations);

	// Apply expectations and also output metrics for each expectation.
	static string ApplyExpectations(ClientContext &context, const string &base_query,
	                                const vector<Expectation> &expectations, vector<ExpectationMetric> &out_metrics);
};

} // namespace duckdb
