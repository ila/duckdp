#include "include/duckdp_parser.hpp"

#include <iostream>
#include <string>

namespace duckdb {

// DPParserExtension constructor implementation
DPParserExtension::DPParserExtension() {
	parse_function = DPParseFunction;
	plan_function = DPPlanFunction;
}

// DPFunction constructor implementation
DPFunction::DPFunction() {
	name = "DP function";
	arguments.push_back(LogicalType(LogicalTypeId::BOOLEAN)); // parsing successful
	bind = DPBind;
	init_global = DPInit;
	function = DPFunc;
}

// DPFunction static method implementations
unique_ptr<FunctionData> DPFunction::DPBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("DP OBJECT CREATION");
	return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
	bool result = false;
	if (IntegerValue::Get(input.inputs[0]) == 1) {
		result = true; // explicit creation of the result since the input is an integer value for some reason
	}
	return make_uniq<DPFunction::DPBindData>(result);
}

unique_ptr<GlobalTableFunctionState> DPFunction::DPInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<DPFunction::DPGlobalData>();
}

void DPFunction::DPFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<DPFunction::DPGlobalData>();
	auto &bind_data = data_p.bind_data->Cast<DPFunction::DPBindData>();

	if (state.offset == 0) {
		output.SetCardinality(1);
		output.SetValue(0, 0, Value::BOOLEAN(bind_data.result));
		state.offset = 1;
	} else {
		output.SetCardinality(0);
	}
}

// Parser extension methods
ParserExtensionParseResult DPParserExtension::DPParseFunction(ParserExtensionInfo *info, const string &query) {
	return ParserExtensionParseResult();
}

ParserExtensionPlanResult DPParserExtension::DPPlanFunction(ParserExtensionInfo *info, ClientContext &context,
                                                                unique_ptr<ParserExtensionParseData> parse_data) {
	auto query = dynamic_cast<DPParseData *>(parse_data.get())->query;

	ParserExtensionPlanResult result;
	result.function = static_cast<TableFunction>(DPFunction());
	result.parameters.push_back(true);
	result.modified_databases = {};
	result.requires_valid_transaction = false;
	result.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

BoundStatement DPBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement) {
	return BoundStatement();
}

std::string DPParserExtension::Name() {
	return "dp_parser_extension";
}
} // namespace duckdb
