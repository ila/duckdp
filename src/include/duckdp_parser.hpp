#ifndef DUCKDB_DP_PARSER_HPP
#define DUCKDB_DP_PARSER_HPP

#pragma once

#include "duckdb.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Parser extension
//===--------------------------------------------------------------------===//

struct DPParseData : ParserExtensionParseData {

	string query;
	float epsilon;
	float lower;
	float upper;
	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq_base<ParserExtensionParseData, DPParseData>(query);
	}

	string ToString() const override {
		return query;
	}

	explicit DPParseData(const string &query) : query(query) {
	}
};

class DPParserExtension : public ParserExtension {
public:
	DPParserExtension();

	static ParserExtensionParseResult DPParseFunction(ParserExtensionInfo *info, const string &query);
	static ParserExtensionPlanResult DPPlanFunction(ParserExtensionInfo *info, ClientContext &context,
	                                                  duckdb::unique_ptr<ParserExtensionParseData> parse_data);

	string Name();
	static string path;
	static string db;
};


} // namespace duckdb

#endif // DUCKDB_DP_PARSER_HPP
