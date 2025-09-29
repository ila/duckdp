#define DUCKDB_EXTENSION_MAIN

#include "include/duckdp_extension.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/main/config.hpp"
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <regex>
#include <string.h>
#include <sys/stat.h>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <signal.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/function/table/table_scan.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <duckdb/planner/operator/logical_get.hpp>

#include <absl/status/status.h>
#include <absl/status/statusor.h>
#include <algorithms/bounded-mean.h>
#include <algorithms/bounded-sum.h>

namespace duckdb {


class DPSumFunction : public TableFunction {
public:
	DPSumFunction() {
		name = "dp_sum";
		// Arguments: input column, epsilon, lower, upper
		arguments.push_back(LogicalType::DOUBLE); // input column
		arguments.push_back(LogicalType::DOUBLE); // epsilon
		arguments.push_back(LogicalType::DOUBLE); // lower
		arguments.push_back(LogicalType::DOUBLE); // upper
		bind = DPSumBind;
		init_global = DPSumInit;
		function = DPSumFunc;
	}

	struct DPSumBindData : public TableFunctionData {
		DPSumBindData(double epsilon, double lower, double upper) : epsilon(epsilon), lower(lower), upper(upper) {}
		double epsilon;
		double lower;
		double upper;
	};

	struct DPSumGlobalData : public GlobalTableFunctionState {
		DPSumGlobalData(double epsilon, double lower, double upper) {}
		absl::StatusOr<std::unique_ptr<differential_privacy::BoundedSum<double>>> dp_sum;
		idx_t offset = 0;
	};

	static duckdb::unique_ptr<FunctionData> DPSumBind(ClientContext &context, TableFunctionBindInput &input,
													  vector<LogicalType> &return_types, vector<string> &names) {
		names.emplace_back("dp_sum");
		return_types.emplace_back(LogicalType::DOUBLE);
		double epsilon = input.inputs[1].GetValue<double>();
		double lower = input.inputs[2].GetValue<double>();
		double upper = input.inputs[3].GetValue<double>();
		return make_uniq<DPSumBindData>(epsilon, lower, upper);
	}

	static duckdb::unique_ptr<GlobalTableFunctionState> DPSumInit(ClientContext &context,
																  TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<DPSumBindData>();
		return make_uniq<DPSumGlobalData>(bind_data.epsilon, bind_data.lower, bind_data.upper);
	}

	static void DPSumFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &input) {
		auto &state = data_p.global_state->Cast<DPSumGlobalData>();
		// If input chunk has rows, add them to the DP sum
		/*
		if (input.size() > 0) {
			for (idx_t i = 0; i < input.size(); ++i) {
				if (!input.GetValue(0, i).IsNull()) {
					double value = input.GetValue(0, i).GetValue<double>();
					if (state.dp_sum.ok() && state.dp_sum.value()) {
						state.dp_sum.value()->AddEntry(value);
					}
				}
			}
			return; // Wait for DuckDB to call with empty chunk
		}
		// If input chunk is empty and we haven't output yet, output the result
		if (state.offset == 0) {
			double dp_result = 0.0;
			Output PartialResult();
			if (state.dp_sum.value()) {
				auto result = state.dp_sum.value().get()
			}
			input.SetCardinality(1);
			input.SetValue(0, 0, Value::DOUBLE(dp_result));
			state.offset = 1;
		} */

		// Build the BoundedSum object
		std::vector<int> data = {1, 2, 3, 4, 5};

		auto bounded_sum_builder = differential_privacy::BoundedSum<int>::Builder();
		bounded_sum_builder.SetEpsilon(1.0);
		bounded_sum_builder.SetLower(0);
		bounded_sum_builder.SetUpper(10);

		auto bounded_sum_result = bounded_sum_builder.Build();
		if (bounded_sum_result.ok()) {
			std::cout << "ok";
			bounded_sum_result.value()->AddEntry(5);
			bounded_sum_result.value()->AddEntry(4);
			bounded_sum_result.value()->AddEntry(3);
		}

		auto result = bounded_sum_result.value()->Result(data.begin(), data.end());
		std::cout << differential_privacy::GetValue<int64_t>(result.value());



	}
};

static void LoadInternal(ExtensionLoader &loader) {

	/*
	auto &instance = loader.GetDatabaseInstance();
	DuckDB db(instance);
	Connection con(db);
	auto &client_context = *con.context;
	auto &catalog = Catalog::GetSystemCatalog(client_context);


	// add a parser extension
	auto &db_config = DBConfig::GetConfig(instance);
	//auto dp_parser = DPParserExtension();
	//db_config.parser_extensions.push_back(dp_parser);
	 */

	DPSumFunction dp_sum_function;
	CreateTableFunctionInfo dp_sum_info(dp_sum_function);
	loader.RegisterFunction(dp_sum_info);

	// the first argument is the name of the view to flush
	// the second is the database - duckdb, postgres, etc.
	//auto flush = PragmaFunction::PragmaCall("flush", FlushFunction, {LogicalType::VARCHAR}, {LogicalType::VARCHAR});
	//loader.RegisterFunction(flush);

}

void DuckdpExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}
string DuckdpExtension::Name() {
	return "DP";
}
std::string DuckdpExtension::Version() const {
	return "0.1.0"; // minimal version string
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API const char *server_version() {
	return duckdb::DuckDB::LibraryVersion();
}

DUCKDB_CPP_EXTENSION_ENTRY(dp, loader) {
	duckdb::LoadInternal(loader);
}

DUCKDB_EXTENSION_API void duckdp_duckdb_cpp_init(duckdb::ExtensionLoader &loader) {
	duckdb::DuckdpExtension extension;
	extension.Load(loader);
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif