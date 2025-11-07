#define DUCKDB_EXTENSION_MAIN

#include "include/duckdp_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "include/dp_functions.hpp"
#include "include/dp_benchmark.hpp"

namespace duckdb {

void DuckdpExtension::Load(ExtensionLoader &loader) {
    // Register DP functions grouped by feature
    RegisterDPSum(loader);
    RegisterDPLaplace(loader);
    RegisterDPGaussian(loader);

    // Register benchmark pragma: PRAGMA dp_sum_benchmark(... named params ...)
    auto pragma = PragmaFunction::PragmaCall("dp_sum_benchmark", DPSumBenchmarkPragma, {});
    // Named parameters
    pragma.named_parameters["num_clients"] = LogicalType::ANY;
    pragma.named_parameters["max_steps"] = LogicalType::ANY;
    pragma.named_parameters["max_records_per_day"] = LogicalType::ANY;
    pragma.named_parameters["num_days"] = LogicalType::ANY;
    pragma.named_parameters["epsilon"] = LogicalType::ANY;
    pragma.named_parameters["mechanism"] = LogicalType::VARCHAR;
    pragma.named_parameters["delta"] = LogicalType::ANY;
    pragma.named_parameters["seed"] = LogicalType::ANY;
    pragma.named_parameters["lower_bound"] = LogicalType::ANY;
    pragma.named_parameters["upper_bound"] = LogicalType::ANY;
    pragma.named_parameters["percentage_outliers"] = LogicalType::ANY;
    // CSV export + fairness diagnostics
    pragma.named_parameters["export_csv"] = LogicalType::BOOLEAN;
    pragma.named_parameters["csv_path"] = LogicalType::VARCHAR;
    pragma.named_parameters["csv_delimiter"] = LogicalType::VARCHAR;
    pragma.named_parameters["fairness"] = LogicalType::BOOLEAN;

    loader.RegisterFunction(pragma);

    // Register wrapper pragma: PRAGMA dp_sum_wrapper(epsilon_min=..., epsilon_max=..., epsilon_step=..., seeds=...)
    auto wrapper = PragmaFunction::PragmaCall("dp_sum_wrapper", DPSumWrapperPragma, {});
    // Sweep parameters
    wrapper.named_parameters["epsilon_min"] = LogicalType::ANY;
    wrapper.named_parameters["epsilon_max"] = LogicalType::ANY;
    wrapper.named_parameters["epsilon_step"] = LogicalType::ANY;
    wrapper.named_parameters["seeds"] = LogicalType::ANY; // number of repeats per epsilon
    // Forwarded benchmark parameters (same as dp_sum_benchmark)
    wrapper.named_parameters["num_clients"] = LogicalType::ANY;
    wrapper.named_parameters["max_steps"] = LogicalType::ANY;
    wrapper.named_parameters["max_records_per_day"] = LogicalType::ANY;
    wrapper.named_parameters["num_days"] = LogicalType::ANY;
    wrapper.named_parameters["mechanism"] = LogicalType::VARCHAR;
    wrapper.named_parameters["delta"] = LogicalType::ANY;
    wrapper.named_parameters["seed"] = LogicalType::ANY; // base seed
    wrapper.named_parameters["lower_bound"] = LogicalType::ANY;
    wrapper.named_parameters["upper_bound"] = LogicalType::ANY;
    wrapper.named_parameters["percentage_outliers"] = LogicalType::ANY;
    // CSV export + fairness diagnostics
    wrapper.named_parameters["export_csv"] = LogicalType::BOOLEAN;
    wrapper.named_parameters["csv_path"] = LogicalType::VARCHAR;
    wrapper.named_parameters["csv_delimiter"] = LogicalType::VARCHAR;
    wrapper.named_parameters["fairness"] = LogicalType::BOOLEAN;

    loader.RegisterFunction(wrapper);
}

std::string DuckdpExtension::Name() { return "duckdp"; }
std::string DuckdpExtension::Version() const { return "0.1.0"; }

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void duckdp_duckdb_cpp_init(duckdb::ExtensionLoader &loader) {
    duckdb::DuckdpExtension extension;
    extension.Load(loader);
}

DUCKDB_EXTENSION_API const char *duckdp_version() { return duckdb::DuckDB::LibraryVersion(); }
}
