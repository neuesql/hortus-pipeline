#pragma once
#include "duckdb.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "catalog/materialized_view_catalog.hpp"

namespace duckdb {

enum class PipelineStatementType {
	CREATE_MATERIALIZED_VIEW,
	ALTER_MATERIALIZED_VIEW,
	DROP_MATERIALIZED_VIEW,
	REFRESH_MATERIALIZED_VIEW,
	REFRESH_ALL_MATERIALIZED_VIEWS
};

enum class RefreshMode { SYNC, ASYNC, FULL };
enum class AlterAction { SET_QUERY, ADD_CONSTRAINT, DROP_CONSTRAINT };

struct PipelineParseData : public ParserExtensionParseData {
	PipelineStatementType statement_type;
	string view_name;
	string query;
	string comment;
	vector<Expectation> expectations;
	vector<string> depends_on;
	RefreshMode refresh_mode = RefreshMode::SYNC;
	AlterAction alter_action = AlterAction::SET_QUERY;
	Expectation alter_expectation;
	string drop_constraint_name;

	unique_ptr<ParserExtensionParseData> Copy() const override {
		auto copy = make_uniq<PipelineParseData>();
		copy->statement_type = statement_type;
		copy->view_name = view_name;
		copy->query = query;
		copy->comment = comment;
		copy->expectations = expectations;
		copy->depends_on = depends_on;
		copy->refresh_mode = refresh_mode;
		copy->alter_action = alter_action;
		copy->alter_expectation = alter_expectation;
		copy->drop_constraint_name = drop_constraint_name;
		return std::move(copy);
	}

	string ToString() const override {
		switch (statement_type) {
		case PipelineStatementType::CREATE_MATERIALIZED_VIEW:
			return "CREATE OR REFRESH MATERIALIZED VIEW " + view_name;
		case PipelineStatementType::ALTER_MATERIALIZED_VIEW:
			return "ALTER MATERIALIZED VIEW " + view_name;
		case PipelineStatementType::DROP_MATERIALIZED_VIEW:
			return "DROP MATERIALIZED VIEW " + view_name;
		case PipelineStatementType::REFRESH_MATERIALIZED_VIEW:
			return "REFRESH MATERIALIZED VIEW " + view_name;
		case PipelineStatementType::REFRESH_ALL_MATERIALIZED_VIEWS:
			return "REFRESH ALL MATERIALIZED VIEWS";
		default:
			return "UNKNOWN PIPELINE STATEMENT";
		}
	}
};

} // namespace duckdb
