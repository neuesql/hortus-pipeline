#include "parser/pipeline_parser.hpp"
#include "parser/pipeline_parse_data.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"

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
		} else if (StringUtil::CIEquals(tokens[pos], "AS")) {
			// The rest is the inner query - extract from raw_query
			idx_t as_pos = FindLastAS(raw_query);
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
		idx_t as_pos = FindLastAS(raw_query);
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
		data->drop_constraint_name = tokens[pos];
	} else {
		throw ParserException("Expected AS, ADD CONSTRAINT, or DROP CONSTRAINT after ALTER MATERIALIZED VIEW name");
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
		// tokens: REFRESH ALL MATERIALIZED VIEWS
		auto data = make_uniq<PipelineParseData>();
		data->statement_type = PipelineStatementType::REFRESH_ALL_MATERIALIZED_VIEWS;
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
	    StringUtil::StartsWith(upper, "REFRESH ALL MATERIALIZED VIEWS")) {
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

		if (MatchKeywords(tokens, 0, {"CREATE", "OR", "REFRESH", "MATERIALIZED", "VIEW"})) {
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

ParserExtensionPlanResult PipelineParserExtension::PipelinePlanFunction(ParserExtensionInfo *info,
                                                                        ClientContext &context,
                                                                        unique_ptr<ParserExtensionParseData> parse_data) {
	auto &data = dynamic_cast<PipelineParseData &>(*parse_data);

	ParserExtensionPlanResult result;
	result.function = GetPlaceholderFunction();
	result.requires_valid_transaction = true;
	result.return_type = StatementReturnType::QUERY_RESULT;

	string message;
	switch (data.statement_type) {
	case PipelineStatementType::CREATE_MATERIALIZED_VIEW:
		message = "CREATE OR REFRESH MATERIALIZED VIEW " + data.view_name + " - not yet implemented";
		break;
	case PipelineStatementType::ALTER_MATERIALIZED_VIEW:
		message = "ALTER MATERIALIZED VIEW " + data.view_name + " - not yet implemented";
		break;
	case PipelineStatementType::DROP_MATERIALIZED_VIEW:
		message = "DROP MATERIALIZED VIEW " + data.view_name + " - not yet implemented";
		break;
	case PipelineStatementType::REFRESH_MATERIALIZED_VIEW:
		message = "REFRESH MATERIALIZED VIEW " + data.view_name + " - not yet implemented";
		break;
	case PipelineStatementType::REFRESH_ALL_MATERIALIZED_VIEWS:
		message = "REFRESH ALL MATERIALIZED VIEWS - not yet implemented";
		break;
	}

	result.parameters.push_back(Value(message));
	return result;
}

} // namespace duckdb
