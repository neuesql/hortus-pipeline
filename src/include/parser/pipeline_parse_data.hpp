#pragma once
#include "duckdb.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "persistence/pipeline_persistence.hpp"

namespace duckdb {

enum class PipelineStatementType {
	CREATE_MATERIALIZED_VIEW,
	ALTER_MATERIALIZED_VIEW,
	DROP_MATERIALIZED_VIEW,
	REFRESH_MATERIALIZED_VIEW,
	REFRESH_ALL_MATERIALIZED_VIEWS,
	EXPLAIN_MATERIALIZED_VIEW
};

enum class RefreshMode { SYNC, ASYNC, FULL };
enum class AlterAction { SET_QUERY, ADD_CONSTRAINT, DROP_CONSTRAINT, PAUSE_SCHEDULE, RESUME_SCHEDULE };

enum class ScheduleType {
	NONE,
	EVERY,    // SCHEDULE EVERY 1 HOUR
	CRON,     // SCHEDULE CRON '...'
	ON_UPDATE // SCHEDULE TRIGGER ON UPDATE
};

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
	string drop_expectation_name;
	ScheduleType schedule_type = ScheduleType::NONE;
	int schedule_interval = 0;
	string schedule_interval_unit;
	string schedule_cron_expression;
	bool best_effort = false;

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
		copy->drop_expectation_name = drop_expectation_name;
		copy->schedule_type = schedule_type;
		copy->schedule_interval = schedule_interval;
		copy->schedule_interval_unit = schedule_interval_unit;
		copy->schedule_cron_expression = schedule_cron_expression;
		copy->best_effort = best_effort;
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
		case PipelineStatementType::EXPLAIN_MATERIALIZED_VIEW:
			return "EXPLAIN CREATE MATERIALIZED VIEW " + view_name;
		default:
			return "UNKNOWN PIPELINE STATEMENT";
		}
	}
};

} // namespace duckdb
