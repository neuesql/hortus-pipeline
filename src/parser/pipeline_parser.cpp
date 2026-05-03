#include "parser/pipeline_parser.hpp"
#include "parser/pipeline_parse_data.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckdb/parser/parser.hpp"

// Forward declarations for table functions defined in src/functions/
namespace duckdb {
TableFunction GetCreateMaterializedViewFunction();
TableFunction GetRefreshMaterializedViewFunction();
TableFunction GetRefreshAllMaterializedViewsFunction();
TableFunction GetAlterMaterializedViewFunction();
TableFunction GetDropMaterializedViewFunction();
TableFunction GetExplainMaterializedViewFunction();
} // namespace duckdb

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helper: Tokenizer
//===--------------------------------------------------------------------===//

// Simple tokenizer that splits on whitespace but respects parentheses and single-quoted strings
static vector<string> Tokenize(const string &input) {
	vector<string> tokens;
	string current;
	idx_t i = 0;
	while (i < input.size()) {
		char c = input[i];
		if (c == '\'') {
			// Single-quoted string: consume everything until closing quote
			current += c;
			i++;
			while (i < input.size()) {
				current += input[i];
				if (input[i] == '\'') {
					i++;
					break;
				}
				i++;
			}
		} else if (c == '(') {
			if (!current.empty()) {
				tokens.push_back(current);
				current.clear();
			}
			tokens.push_back("(");
			i++;
		} else if (c == ')') {
			if (!current.empty()) {
				tokens.push_back(current);
				current.clear();
			}
			tokens.push_back(")");
			i++;
		} else if (c == ',') {
			if (!current.empty()) {
				tokens.push_back(current);
				current.clear();
			}
			tokens.push_back(",");
			i++;
		} else if (c == '=') {
			// Keep >= <= != as single tokens
			if (!current.empty() && (current.back() == '>' || current.back() == '<' || current.back() == '!')) {
				current += c;
				i++;
			} else {
				if (!current.empty()) {
					tokens.push_back(current);
					current.clear();
				}
				tokens.push_back("=");
				i++;
			}
		} else if (c == ';') {
			if (!current.empty()) {
				tokens.push_back(current);
				current.clear();
			}
			// Ignore trailing semicolons
			i++;
		} else if (std::isspace(c)) {
			if (!current.empty()) {
				tokens.push_back(current);
				current.clear();
			}
			i++;
		} else {
			current += c;
			i++;
		}
	}
	if (!current.empty()) {
		tokens.push_back(current);
	}
	return tokens;
}

// Check if tokens starting at pos match the given keywords (case-insensitive)
static bool MatchKeywords(const vector<string> &tokens, idx_t pos, const vector<string> &keywords) {
	if (pos + keywords.size() > tokens.size()) {
		return false;
	}
	for (idx_t i = 0; i < keywords.size(); i++) {
		if (!StringUtil::CIEquals(tokens[pos + i], keywords[i])) {
			return false;
		}
	}
	return true;
}

// Find the position of the last AS keyword that's not inside parentheses
static idx_t FindLastAS(const string &input) {
	// Search from the end for " AS " (case-insensitive) that's not inside parens
	string upper = StringUtil::Upper(input);
	int paren_depth = 0;
	// We scan from end to start to find the LAST occurrence
	for (int i = (int)upper.size() - 4; i >= 0; i--) {
		char c = upper[i];
		if (c == ')') {
			paren_depth++;
		} else if (c == '(') {
			paren_depth--;
		} else if (paren_depth == 0 && upper.substr(i, 4) == " AS ") {
			return (idx_t)(i + 4); // Position after " AS "
		}
	}
	return DConstants::INVALID_INDEX;
}

// Find the first top-level AS keyword after a starting position (skipping content inside parens)
static idx_t FindFirstASAfter(const string &input, idx_t start_pos) {
	string upper = StringUtil::Upper(input);
	int paren_depth = 0;
	for (idx_t i = start_pos; i + 2 < upper.size(); i++) {
		char c = upper[i];
		if (c == '(') {
			paren_depth++;
		} else if (c == ')') {
			paren_depth--;
		} else if (paren_depth == 0 && std::isspace(static_cast<unsigned char>(upper[i])) && i + 3 < upper.size() &&
		           upper[i + 1] == 'A' && upper[i + 2] == 'S' &&
		           std::isspace(static_cast<unsigned char>(upper[i + 3]))) {
			return (idx_t)(i + 4); // Position after "<ws>AS<ws>"
		}
	}
	return DConstants::INVALID_INDEX;
}

// Extract content inside parentheses from token stream starting at pos
// Returns the index after the closing paren
static idx_t ExtractParenContent(const vector<string> &tokens, idx_t pos, string &content) {
	if (pos >= tokens.size() || tokens[pos] != "(") {
		return pos;
	}
	pos++; // skip "("
	int depth = 1;
	content.clear();
	while (pos < tokens.size() && depth > 0) {
		if (tokens[pos] == "(") {
			depth++;
			content += "(";
		} else if (tokens[pos] == ")") {
			depth--;
			if (depth > 0) {
				content += ")";
			}
		} else if (tokens[pos] == ",") {
			if (depth == 1) {
				content += ",";
			} else {
				content += ",";
			}
		} else {
			if (!content.empty() && content.back() != '(' && content.back() != ',') {
				content += " ";
			}
			content += tokens[pos];
		}
		pos++;
	}
	return pos;
}

// Extract a single-quoted string, removing surrounding quotes
static string ExtractQuotedString(const string &token) {
	if (token.size() >= 2 && token.front() == '\'' && token.back() == '\'') {
		return token.substr(1, token.size() - 2);
	}
	return token;
}

//===--------------------------------------------------------------------===//
// Parse: CREATE OR REFRESH MATERIALIZED VIEW
//===--------------------------------------------------------------------===//
static unique_ptr<PipelineParseData> ParseCreateOrRefresh(const string &raw_query, const vector<string> &tokens,
                                                          idx_t pos) {
	// tokens[0..4] = CREATE OR REFRESH MATERIALIZED VIEW
	// pos points after VIEW
	auto data = make_uniq<PipelineParseData>();
	data->statement_type = PipelineStatementType::CREATE_MATERIALIZED_VIEW;

	if (pos >= tokens.size()) {
		throw ParserException("Expected view name after CREATE OR REFRESH MATERIALIZED VIEW");
	}
	data->view_name = tokens[pos++];

	// Parse optional clauses before AS: CONSTRAINT, DEPENDS ON, COMMENT
	while (pos < tokens.size()) {
		if (StringUtil::CIEquals(tokens[pos], "CONSTRAINT")) {
			pos++;
			if (pos >= tokens.size()) {
				throw ParserException("Expected constraint name after CONSTRAINT");
			}
			Expectation exp;
			exp.name = tokens[pos++];

			if (pos >= tokens.size() || !StringUtil::CIEquals(tokens[pos], "EXPECT")) {
				throw ParserException("Expected EXPECT after constraint name");
			}
			pos++;

			// Extract expression inside parens
			string expr;
			pos = ExtractParenContent(tokens, pos, expr);
			exp.expression = expr;
			exp.action = ExpectationAction::WARN; // default

			// Check for ON VIOLATION
			if (pos + 1 < tokens.size() && StringUtil::CIEquals(tokens[pos], "ON") &&
			    StringUtil::CIEquals(tokens[pos + 1], "VIOLATION")) {
				pos += 2;
				if (pos + 1 < tokens.size() && StringUtil::CIEquals(tokens[pos], "DROP") &&
				    StringUtil::CIEquals(tokens[pos + 1], "ROW")) {
					exp.action = ExpectationAction::DROP_ROW;
					pos += 2;
				} else if (pos + 1 < tokens.size() && StringUtil::CIEquals(tokens[pos], "FAIL") &&
				           StringUtil::CIEquals(tokens[pos + 1], "UPDATE")) {
					exp.action = ExpectationAction::FAIL_UPDATE;
					pos += 2;
				} else {
					throw ParserException("Expected DROP ROW or FAIL UPDATE after ON VIOLATION");
				}
			}

			data->expectations.push_back(std::move(exp));
		} else if (StringUtil::CIEquals(tokens[pos], "DEPENDS") && pos + 1 < tokens.size() &&
		           StringUtil::CIEquals(tokens[pos + 1], "ON")) {
			pos += 2;
			// Extract comma-separated list inside parens
			if (pos >= tokens.size() || tokens[pos] != "(") {
				throw ParserException("Expected ( after DEPENDS ON");
			}
			pos++; // skip (
			while (pos < tokens.size() && tokens[pos] != ")") {
				if (tokens[pos] != ",") {
					data->depends_on.push_back(tokens[pos]);
				}
				pos++;
			}
			if (pos < tokens.size()) {
				pos++; // skip )
			}
		} else if (StringUtil::CIEquals(tokens[pos], "COMMENT")) {
			pos++;
			if (pos >= tokens.size()) {
				throw ParserException("Expected quoted string after COMMENT");
			}
			data->comment = ExtractQuotedString(tokens[pos]);
			pos++;
		} else if (StringUtil::CIEquals(tokens[pos], "SCHEDULE")) {
			pos++;
			if (pos >= tokens.size()) {
				throw ParserException("Expected EVERY, CRON, or TRIGGER after SCHEDULE");
			}
			if (StringUtil::CIEquals(tokens[pos], "EVERY")) {
				data->schedule_type = ScheduleType::EVERY;
				pos++;
				if (pos >= tokens.size()) {
					throw ParserException("Expected interval number after EVERY");
				}
				data->schedule_interval = std::stoi(tokens[pos]);
				pos++;
				if (pos >= tokens.size()) {
					throw ParserException("Expected time unit after interval number");
				}
				string unit = StringUtil::Upper(tokens[pos]);
				// Normalize plural forms
				if (unit == "SECONDS") {
					unit = "SECOND";
				} else if (unit == "MINUTES") {
					unit = "MINUTE";
				} else if (unit == "HOURS") {
					unit = "HOUR";
				} else if (unit == "DAYS") {
					unit = "DAY";
				} else if (unit == "WEEKS") {
					unit = "WEEK";
				}
				data->schedule_interval_unit = unit;
				pos++;
			} else if (StringUtil::CIEquals(tokens[pos], "CRON")) {
				data->schedule_type = ScheduleType::CRON;
				pos++;
				if (pos >= tokens.size()) {
					throw ParserException("Expected cron expression after CRON");
				}
				data->schedule_cron_expression = ExtractQuotedString(tokens[pos]);
				pos++;
			} else if (StringUtil::CIEquals(tokens[pos], "TRIGGER")) {
				pos++;
				if (pos + 1 >= tokens.size() || !StringUtil::CIEquals(tokens[pos], "ON") ||
				    !StringUtil::CIEquals(tokens[pos + 1], "UPDATE")) {
					throw ParserException("Expected ON UPDATE after TRIGGER");
				}
				data->schedule_type = ScheduleType::ON_UPDATE;
				pos += 2;
			} else {
				throw ParserException("Expected EVERY, CRON, or TRIGGER after SCHEDULE");
			}
		} else if (StringUtil::CIEquals(tokens[pos], "AS")) {
			// The rest is the inner query - extract from raw_query
			// Find the position of this specific AS keyword by searching forward
			// from the view name position in the raw string
			string upper_raw = StringUtil::Upper(raw_query);
			// Find the view name in the raw query to know where to start searching
			string upper_view = StringUtil::Upper(data->view_name);
			idx_t view_pos = upper_raw.find(upper_view);
			idx_t search_start = (view_pos != string::npos) ? view_pos + upper_view.size() : 0;
			idx_t as_pos = FindFirstASAfter(raw_query, search_start);
			if (as_pos == DConstants::INVALID_INDEX) {
				throw ParserException("Could not find AS clause in CREATE OR REFRESH MATERIALIZED VIEW");
			}
			string inner = raw_query.substr(as_pos);
			StringUtil::Trim(inner);
			// Remove trailing semicolons
			while (!inner.empty() && inner.back() == ';') {
				inner.pop_back();
			}
			StringUtil::Trim(inner);
			data->query = inner;
			break;
		} else {
			throw ParserException("Unexpected token '%s' in CREATE OR REFRESH MATERIALIZED VIEW", tokens[pos]);
		}
	}

	return data;
}

//===--------------------------------------------------------------------===//
// Parse: ALTER MATERIALIZED VIEW
//===--------------------------------------------------------------------===//
static unique_ptr<PipelineParseData> ParseAlter(const string &raw_query, const vector<string> &tokens, idx_t pos) {
	// tokens[0..2] = ALTER MATERIALIZED VIEW
	auto data = make_uniq<PipelineParseData>();
	data->statement_type = PipelineStatementType::ALTER_MATERIALIZED_VIEW;

	if (pos >= tokens.size()) {
		throw ParserException("Expected view name after ALTER MATERIALIZED VIEW");
	}
	data->view_name = tokens[pos++];

	if (pos >= tokens.size()) {
		throw ParserException("Expected AS, ADD, or DROP after view name");
	}

	if (StringUtil::CIEquals(tokens[pos], "AS")) {
		// ALTER MATERIALIZED VIEW name AS query
		data->alter_action = AlterAction::SET_QUERY;
		// Find the first AS after the view name (not the last AS, which could be inside the query)
		string upper_raw = StringUtil::Upper(raw_query);
		string upper_view = StringUtil::Upper(data->view_name);
		idx_t view_pos = upper_raw.find(upper_view);
		idx_t search_start = (view_pos != string::npos) ? view_pos + upper_view.size() : 0;
		idx_t as_pos = FindFirstASAfter(raw_query, search_start);
		if (as_pos == DConstants::INVALID_INDEX) {
			throw ParserException("Could not find AS clause in ALTER MATERIALIZED VIEW");
		}
		string inner = raw_query.substr(as_pos);
		StringUtil::Trim(inner);
		while (!inner.empty() && inner.back() == ';') {
			inner.pop_back();
		}
		StringUtil::Trim(inner);
		data->query = inner;
	} else if (StringUtil::CIEquals(tokens[pos], "ADD") && pos + 1 < tokens.size() &&
	           StringUtil::CIEquals(tokens[pos + 1], "CONSTRAINT")) {
		// ALTER MATERIALIZED VIEW name ADD CONSTRAINT cname EXPECT (expr) [ON VIOLATION ...]
		data->alter_action = AlterAction::ADD_CONSTRAINT;
		pos += 2;

		if (pos >= tokens.size()) {
			throw ParserException("Expected constraint name after ADD CONSTRAINT");
		}
		data->alter_expectation.name = tokens[pos++];

		if (pos >= tokens.size() || !StringUtil::CIEquals(tokens[pos], "EXPECT")) {
			throw ParserException("Expected EXPECT after constraint name");
		}
		pos++;

		string expr;
		pos = ExtractParenContent(tokens, pos, expr);
		data->alter_expectation.expression = expr;
		data->alter_expectation.action = ExpectationAction::WARN;

		// Check for ON VIOLATION
		if (pos + 1 < tokens.size() && StringUtil::CIEquals(tokens[pos], "ON") &&
		    StringUtil::CIEquals(tokens[pos + 1], "VIOLATION")) {
			pos += 2;
			if (pos + 1 < tokens.size() && StringUtil::CIEquals(tokens[pos], "DROP") &&
			    StringUtil::CIEquals(tokens[pos + 1], "ROW")) {
				data->alter_expectation.action = ExpectationAction::DROP_ROW;
			} else if (pos + 1 < tokens.size() && StringUtil::CIEquals(tokens[pos], "FAIL") &&
			           StringUtil::CIEquals(tokens[pos + 1], "UPDATE")) {
				data->alter_expectation.action = ExpectationAction::FAIL_UPDATE;
			} else {
				throw ParserException("Expected DROP ROW or FAIL UPDATE after ON VIOLATION");
			}
		}
	} else if (StringUtil::CIEquals(tokens[pos], "DROP") && pos + 1 < tokens.size() &&
	           StringUtil::CIEquals(tokens[pos + 1], "CONSTRAINT")) {
		// ALTER MATERIALIZED VIEW name DROP CONSTRAINT cname
		data->alter_action = AlterAction::DROP_CONSTRAINT;
		pos += 2;
		if (pos >= tokens.size()) {
			throw ParserException("Expected constraint name after DROP CONSTRAINT");
		}
		data->drop_expectation_name = tokens[pos];
	} else if (StringUtil::CIEquals(tokens[pos], "PAUSE") && pos + 1 < tokens.size() &&
	           StringUtil::CIEquals(tokens[pos + 1], "SCHEDULE")) {
		data->alter_action = AlterAction::PAUSE_SCHEDULE;
	} else if (StringUtil::CIEquals(tokens[pos], "RESUME") && pos + 1 < tokens.size() &&
	           StringUtil::CIEquals(tokens[pos + 1], "SCHEDULE")) {
		data->alter_action = AlterAction::RESUME_SCHEDULE;
	} else {
		throw ParserException("Expected AS, ADD CONSTRAINT, DROP CONSTRAINT, PAUSE SCHEDULE, or RESUME SCHEDULE after "
		                      "ALTER MATERIALIZED VIEW name");
	}

	return data;
}

//===--------------------------------------------------------------------===//
// Parse: DROP MATERIALIZED VIEW
//===--------------------------------------------------------------------===//
static unique_ptr<PipelineParseData> ParseDrop(const vector<string> &tokens, idx_t pos) {
	auto data = make_uniq<PipelineParseData>();
	data->statement_type = PipelineStatementType::DROP_MATERIALIZED_VIEW;

	if (pos >= tokens.size()) {
		throw ParserException("Expected view name after DROP MATERIALIZED VIEW");
	}
	data->view_name = tokens[pos];
	return data;
}

//===--------------------------------------------------------------------===//
// Parse: REFRESH MATERIALIZED VIEW / REFRESH ALL MATERIALIZED VIEWS
//===--------------------------------------------------------------------===//
static unique_ptr<PipelineParseData> ParseRefresh(const vector<string> &tokens, idx_t pos) {
	// Check for REFRESH ALL MATERIALIZED VIEWS
	if (tokens.size() >= 2 && StringUtil::CIEquals(tokens[1], "ALL")) {
		// tokens: REFRESH ALL MATERIALIZED VIEWS [WITH (on_failure = 'best_effort')]
		auto data = make_uniq<PipelineParseData>();
		data->statement_type = PipelineStatementType::REFRESH_ALL_MATERIALIZED_VIEWS;

		// Check for WITH clause after VIEWS (pos = 4)
		if (pos < tokens.size() && StringUtil::CIEquals(tokens[pos], "WITH")) {
			pos++;
			if (pos < tokens.size() && tokens[pos] == "(") {
				pos++;
				// Parse key = value pairs
				while (pos < tokens.size() && tokens[pos] != ")") {
					if (StringUtil::CIEquals(tokens[pos], "on_failure")) {
						pos++; // skip key
						// skip '=' if present as a separate token
						if (pos < tokens.size() && tokens[pos] == "=") {
							pos++;
						}
						if (pos < tokens.size()) {
							string val = ExtractQuotedString(tokens[pos]);
							if (StringUtil::CIEquals(val, "best_effort")) {
								data->best_effort = true;
							}
							pos++;
						}
					} else {
						pos++;
					}
				}
				if (pos < tokens.size() && tokens[pos] == ")") {
					pos++;
				}
			}
		}
		return data;
	}

	// REFRESH MATERIALIZED VIEW name [ASYNC|FULL]
	auto data = make_uniq<PipelineParseData>();
	data->statement_type = PipelineStatementType::REFRESH_MATERIALIZED_VIEW;

	if (pos >= tokens.size()) {
		throw ParserException("Expected view name after REFRESH MATERIALIZED VIEW");
	}
	data->view_name = tokens[pos++];

	// Check for optional refresh mode
	if (pos < tokens.size()) {
		if (StringUtil::CIEquals(tokens[pos], "ASYNC")) {
			data->refresh_mode = RefreshMode::ASYNC;
		} else if (StringUtil::CIEquals(tokens[pos], "FULL")) {
			data->refresh_mode = RefreshMode::FULL;
		}
	}

	return data;
}

//===--------------------------------------------------------------------===//
// Placeholder TableFunction for plan_function
//===--------------------------------------------------------------------===//

struct PlaceholderBindData : public TableFunctionData {
	string message;
};

struct PlaceholderGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> PlaceholderBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<PlaceholderBindData>();
	data->message = StringValue::Get(input.inputs[0]);
	names.emplace_back("message");
	return_types.emplace_back(LogicalType::VARCHAR);
	return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> PlaceholderInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<PlaceholderGlobalState>();
}

static void PlaceholderFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<PlaceholderBindData>();
	auto &state = data_p.global_state->Cast<PlaceholderGlobalState>();
	if (state.done) {
		return;
	}
	output.SetValue(0, 0, Value(bind_data.message));
	output.SetCardinality(1);
	state.done = true;
}

static TableFunction GetPlaceholderFunction() {
	TableFunction func("pipeline_placeholder", {LogicalType::VARCHAR}, PlaceholderFunc, PlaceholderBind,
	                   PlaceholderInit);
	return func;
}

//===--------------------------------------------------------------------===//
// Serialize helpers for plan_function
//===--------------------------------------------------------------------===//

static string SerializeExpectations(const vector<Expectation> &expectations) {
	string result;
	for (idx_t i = 0; i < expectations.size(); i++) {
		if (i > 0) {
			result += "|||";
		}
		auto &exp = expectations[i];
		string action_str;
		switch (exp.action) {
		case ExpectationAction::WARN:
			action_str = "WARN";
			break;
		case ExpectationAction::DROP_ROW:
			action_str = "DROP_ROW";
			break;
		case ExpectationAction::FAIL_UPDATE:
			action_str = "FAIL_UPDATE";
			break;
		default:
			break;
		}
		result += exp.name + ":::" + exp.expression + ":::" + action_str;
	}
	return result;
}

static string SerializeDependsOn(const vector<string> &deps) {
	string result;
	for (idx_t i = 0; i < deps.size(); i++) {
		if (i > 0) {
			result += ",";
		}
		result += deps[i];
	}
	return result;
}

//===--------------------------------------------------------------------===//
// PipelineParserExtension
//===--------------------------------------------------------------------===//

PipelineParserExtension::PipelineParserExtension() {
	parse_function = PipelineParseFunction;
	plan_function = PipelinePlanFunction;
	parser_override = PipelineParserOverride;
}

ParserExtensionParseResult PipelineParserExtension::PipelineParseFunction(ParserExtensionInfo *info,
                                                                          const string &query) {
	// Trim and get an uppercase version for keyword matching
	string trimmed = query;
	StringUtil::Trim(trimmed);

	// Remove trailing semicolons for matching
	string match_str = trimmed;
	while (!match_str.empty() && match_str.back() == ';') {
		match_str.pop_back();
	}
	StringUtil::Trim(match_str);

	string upper = StringUtil::Upper(match_str);

	// Check if this is one of our statements
	bool is_ours = false;

	if (StringUtil::StartsWith(upper, "CREATE OR REFRESH MATERIALIZED VIEW") ||
	    StringUtil::StartsWith(upper, "ALTER MATERIALIZED VIEW") ||
	    StringUtil::StartsWith(upper, "DROP MATERIALIZED VIEW") ||
	    StringUtil::StartsWith(upper, "REFRESH MATERIALIZED VIEW") ||
	    StringUtil::StartsWith(upper, "REFRESH ALL MATERIALIZED VIEWS") ||
	    StringUtil::StartsWith(upper, "EXPLAIN CREATE MATERIALIZED VIEW") ||
	    StringUtil::StartsWith(upper, "EXPLAIN CREATE OR REFRESH MATERIALIZED VIEW")) {
		is_ours = true;
	}

	if (!is_ours) {
		// Not our statement - let DuckDB show its own error
		return ParserExtensionParseResult();
	}

	try {
		auto tokens = Tokenize(match_str);
		if (tokens.empty()) {
			return ParserExtensionParseResult();
		}

		unique_ptr<PipelineParseData> data;

		if (MatchKeywords(tokens, 0, {"EXPLAIN", "CREATE", "OR", "REFRESH", "MATERIALIZED", "VIEW"})) {
			data = ParseCreateOrRefresh(match_str, tokens, 6);
			data->statement_type = PipelineStatementType::EXPLAIN_MATERIALIZED_VIEW;
		} else if (MatchKeywords(tokens, 0, {"EXPLAIN", "CREATE", "MATERIALIZED", "VIEW"})) {
			data = ParseCreateOrRefresh(match_str, tokens, 4);
			data->statement_type = PipelineStatementType::EXPLAIN_MATERIALIZED_VIEW;
		} else if (MatchKeywords(tokens, 0, {"CREATE", "OR", "REFRESH", "MATERIALIZED", "VIEW"})) {
			data = ParseCreateOrRefresh(match_str, tokens, 5);
		} else if (MatchKeywords(tokens, 0, {"ALTER", "MATERIALIZED", "VIEW"})) {
			data = ParseAlter(match_str, tokens, 3);
		} else if (MatchKeywords(tokens, 0, {"DROP", "MATERIALIZED", "VIEW"})) {
			data = ParseDrop(tokens, 3);
		} else if (MatchKeywords(tokens, 0, {"REFRESH", "ALL", "MATERIALIZED", "VIEWS"})) {
			data = ParseRefresh(tokens, 4);
		} else if (MatchKeywords(tokens, 0, {"REFRESH", "MATERIALIZED", "VIEW"})) {
			data = ParseRefresh(tokens, 3);
		} else {
			return ParserExtensionParseResult();
		}

		return ParserExtensionParseResult(std::move(data));
	} catch (const Exception &e) {
		return ParserExtensionParseResult(e.what());
	}
}

ParserExtensionPlanResult
PipelineParserExtension::PipelinePlanFunction(ParserExtensionInfo *info, ClientContext &context,
                                              unique_ptr<ParserExtensionParseData> parse_data) {
	auto &data = dynamic_cast<PipelineParseData &>(*parse_data);

	ParserExtensionPlanResult result;
	result.requires_valid_transaction = true;
	result.return_type = StatementReturnType::QUERY_RESULT;

	switch (data.statement_type) {
	case PipelineStatementType::CREATE_MATERIALIZED_VIEW: {
		result.function = GetCreateMaterializedViewFunction();
		result.parameters.push_back(Value(data.view_name));
		result.parameters.push_back(Value(data.query));
		result.parameters.push_back(Value(data.comment));
		result.parameters.push_back(Value(SerializeExpectations(data.expectations)));
		result.parameters.push_back(Value(SerializeDependsOn(data.depends_on)));
		// Serialize schedule info
		string schedule_str;
		switch (data.schedule_type) {
		case ScheduleType::EVERY:
			schedule_str = "EVERY:" + std::to_string(data.schedule_interval) + ":" + data.schedule_interval_unit;
			break;
		case ScheduleType::CRON:
			schedule_str = "CRON:" + data.schedule_cron_expression;
			break;
		case ScheduleType::ON_UPDATE:
			schedule_str = "ON_UPDATE";
			break;
		default:
			break;
		}
		result.parameters.push_back(Value(schedule_str));
		break;
	}
	case PipelineStatementType::ALTER_MATERIALIZED_VIEW: {
		result.function = GetAlterMaterializedViewFunction();
		result.parameters.push_back(Value(data.view_name));
		switch (data.alter_action) {
		case AlterAction::SET_QUERY:
			result.parameters.push_back(Value("SET_QUERY"));
			result.parameters.push_back(Value(data.query));
			result.parameters.push_back(Value(""));
			result.parameters.push_back(Value(""));
			break;
		case AlterAction::ADD_CONSTRAINT: {
			result.parameters.push_back(Value("ADD_CONSTRAINT"));
			result.parameters.push_back(Value(data.alter_expectation.name));
			result.parameters.push_back(Value(data.alter_expectation.expression));
			string action_str;
			switch (data.alter_expectation.action) {
			case ExpectationAction::WARN:
				action_str = "WARN";
				break;
			case ExpectationAction::DROP_ROW:
				action_str = "DROP_ROW";
				break;
			case ExpectationAction::FAIL_UPDATE:
				action_str = "FAIL_UPDATE";
				break;
			default:
				break;
			}
			result.parameters.push_back(Value(action_str));
			break;
		}
		case AlterAction::DROP_CONSTRAINT:
			result.parameters.push_back(Value("DROP_CONSTRAINT"));
			result.parameters.push_back(Value(data.drop_expectation_name));
			result.parameters.push_back(Value(""));
			result.parameters.push_back(Value(""));
			break;
		case AlterAction::PAUSE_SCHEDULE:
			result.parameters.push_back(Value("PAUSE_SCHEDULE"));
			result.parameters.push_back(Value(""));
			result.parameters.push_back(Value(""));
			result.parameters.push_back(Value(""));
			break;
		case AlterAction::RESUME_SCHEDULE:
			result.parameters.push_back(Value("RESUME_SCHEDULE"));
			result.parameters.push_back(Value(""));
			result.parameters.push_back(Value(""));
			result.parameters.push_back(Value(""));
			break;
		default:
			break;
		}
		break;
	}
	case PipelineStatementType::DROP_MATERIALIZED_VIEW: {
		result.function = GetDropMaterializedViewFunction();
		result.parameters.push_back(Value(data.view_name));
		break;
	}
	case PipelineStatementType::REFRESH_MATERIALIZED_VIEW: {
		result.function = GetRefreshMaterializedViewFunction();
		string mode_str;
		switch (data.refresh_mode) {
		case RefreshMode::SYNC:
			mode_str = "SYNC";
			break;
		case RefreshMode::ASYNC:
			mode_str = "ASYNC";
			break;
		case RefreshMode::FULL:
			mode_str = "FULL";
			break;
		default:
			break;
		}
		result.parameters.push_back(Value(data.view_name));
		result.parameters.push_back(Value(mode_str));
		break;
	}
	case PipelineStatementType::REFRESH_ALL_MATERIALIZED_VIEWS: {
		result.function = GetRefreshAllMaterializedViewsFunction();
		result.parameters.push_back(Value(data.best_effort ? "best_effort" : "normal"));
		break;
	}
	case PipelineStatementType::EXPLAIN_MATERIALIZED_VIEW: {
		result.function = GetExplainMaterializedViewFunction();
		result.parameters.push_back(Value(data.view_name));
		result.parameters.push_back(Value(data.query));
		break;
	}
	default:
		break;
	}

	return result;
}

ParserOverrideResult PipelineParserExtension::PipelineParserOverride(ParserExtensionInfo *info, const string &query,
                                                                     ParserOptions &options) {
	// Intercept statements DuckDB's native parser would either reject or
	// mis-transform: DROP/ALTER MATERIALIZED VIEW, EXPLAIN CREATE [OR REFRESH]
	// MATERIALIZED VIEW, and the strict SHOW pipeline_<name>() sugar below.
	string trimmed = query;
	StringUtil::Trim(trimmed);
	while (!trimmed.empty() && trimmed.back() == ';') {
		trimmed.pop_back();
	}
	StringUtil::Trim(trimmed);
	string upper = StringUtil::Upper(trimmed);

	// Strict sugar: SHOW pipeline_<name>()  ->  SELECT * FROM pipeline_<name>()
	// Top-level only. The strict 4-token shape ensures we never rewrite SHOW
	// occurrences inside string literals, comments, or expression contexts.
	if (StringUtil::StartsWith(upper, "SHOW PIPELINE_")) {
		static const unordered_set<string> show_targets = {
		    "PIPELINE_STATUS",   "PIPELINE_EXPECTATIONS",     "PIPELINE_SCHEDULES",
		    "PIPELINE_RUN_LOGS", "PIPELINE_EXPECTATION_LOGS",
		};
		auto tokens = Tokenize(trimmed);
		if (tokens.size() == 4 && StringUtil::Upper(tokens[0]) == "SHOW" &&
		    show_targets.count(StringUtil::Upper(tokens[1])) > 0 && tokens[2] == "(" && tokens[3] == ")") {
			string rewritten = "SELECT * FROM " + tokens[1] + "()";
			try {
				Parser parser;
				parser.ParseQuery(rewritten);
				return ParserOverrideResult(std::move(parser.statements));
			} catch (std::exception &e) {
				return ParserOverrideResult(e);
			}
		}
		// Any other SHOW pipeline_* shape — fall through to native parser
	}

	bool is_ours = StringUtil::StartsWith(upper, "DROP MATERIALIZED VIEW") ||
	               StringUtil::StartsWith(upper, "ALTER MATERIALIZED VIEW") ||
	               StringUtil::StartsWith(upper, "EXPLAIN CREATE MATERIALIZED VIEW") ||
	               StringUtil::StartsWith(upper, "EXPLAIN CREATE OR REFRESH MATERIALIZED VIEW");

	if (!is_ours) {
		return ParserOverrideResult();
	}

	// Parse using our parse function
	auto parse_result = PipelineParseFunction(info, query);
	if (parse_result.type != ParserExtensionResultType::PARSE_SUCCESSFUL) {
		try {
			throw ParserException("%s", parse_result.error);
		} catch (std::exception &e) {
			return ParserOverrideResult(e);
		}
	}

	// Create an ExtensionStatement wrapping our parse data
	PipelineParserExtension ext;
	auto statement = make_uniq<ExtensionStatement>(ext, std::move(parse_result.parse_data));
	statement->stmt_length = query.size();
	statement->stmt_location = 0;

	vector<unique_ptr<SQLStatement>> statements;
	statements.push_back(std::move(statement));
	return ParserOverrideResult(std::move(statements));
}

} // namespace duckdb
