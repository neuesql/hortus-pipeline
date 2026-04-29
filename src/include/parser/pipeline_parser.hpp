#pragma once
#include "duckdb.hpp"
#include "duckdb/parser/parser_extension.hpp"

namespace duckdb {

class PipelineParserExtension : public ParserExtension {
public:
	PipelineParserExtension();

	static ParserExtensionParseResult PipelineParseFunction(ParserExtensionInfo *info, const string &query);
	static ParserExtensionPlanResult PipelinePlanFunction(ParserExtensionInfo *info, ClientContext &context,
	                                                      unique_ptr<ParserExtensionParseData> parse_data);
	static ParserOverrideResult PipelineParserOverride(ParserExtensionInfo *info, const string &query,
	                                                   ParserOptions &options);
};

} // namespace duckdb
