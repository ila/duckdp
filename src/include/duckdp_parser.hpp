#ifndef DUCKDB_DP_PARSER_HPP
#define DUCKDB_DP_PARSER_HPP

#pragma once

#include "duckdb.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Parser extension
//===--------------------------------------------------------------------===//

/*
struct DPParseData : ParserExtensionParseData {

	string query;
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

class DPFunction : public TableFunction {
public:
	DPFunction();

	struct DPBindData : TableFunctionData {
		explicit DPBindData(bool result) : result(result) {
		}
		bool result;
	};

	struct DPGlobalData : GlobalTableFunctionState {
		DPGlobalData() : offset(0) {
		}
		idx_t offset;
	};

	static unique_ptr<FunctionData> DPBind(ClientContext &context, TableFunctionBindInput &input,
	                                         vector<LogicalType> &return_types, vector<string> &names);
	static unique_ptr<GlobalTableFunctionState> DPInit(ClientContext &context, TableFunctionInitInput &input);
	static void DPFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
};

BoundStatement DPBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement);

 */

} // namespace duckdb

#endif // DUCKDB_DP_PARSER_HPP
