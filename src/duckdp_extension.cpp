#define DUCKDB_EXTENSION_MAIN

#include "include/duckdp_extension.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

extern "C" {
#include "include/duckdp_c_wrapper.h"  // C wrapper for DP library
}

namespace duckdb {

// ------------------------ DPSumFunction ------------------------

struct DPSumBindData : public TableFunctionData {
    DPSumBindData(double epsilon, double lower, double upper)
        : epsilon(epsilon), lower(lower), upper(upper) {}
    double epsilon;
    double lower;
    double upper;
};

struct DPSumGlobalData : public GlobalTableFunctionState {
    DPSumGlobalData(double epsilon, double lower, double upper) {
        handle = dp_bounded_sum_create(epsilon, lower, upper);
    }
    ~DPSumGlobalData() {
        dp_bounded_sum_destroy(handle);
    }
    DP_BoundedSumHandle handle;
    idx_t offset = 0;
};

static unique_ptr<FunctionData> DPSumBind(ClientContext &context,
                                           TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types,
                                           vector<string> &names) {
    names.emplace_back("dp_sum");
    return_types.emplace_back(LogicalType(LogicalTypeId::DOUBLE));
    double epsilon = input.inputs[1].GetValue<double>();
    double lower = input.inputs[2].GetValue<double>();
    double upper = input.inputs[3].GetValue<double>();
    return make_uniq<DPSumBindData>(epsilon, lower, upper);
}

static unique_ptr<GlobalTableFunctionState> DPSumInit(ClientContext &context,
                                                      TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<DPSumBindData>();
    return make_uniq<DPSumGlobalData>(bind_data.epsilon, bind_data.lower, bind_data.upper);
}

static void DPSumFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<DPSumGlobalData>();

    // If we haven't output the result yet
    if (state.offset == 0) {
        double dp_result = dp_bounded_sum_result(state.handle);
        output.SetCardinality(1);
        output.SetValue(0, 0, Value::DOUBLE(dp_result));
        state.offset = 1;
    } else {
        output.SetCardinality(0);
    }
}

static void LoadInternal(ExtensionLoader &loader) {
    // Register DP table function
    TableFunction dp_sum_function("dp_sum",
        {LogicalType(LogicalTypeId::DOUBLE), LogicalType(LogicalTypeId::DOUBLE), LogicalType(LogicalTypeId::DOUBLE), LogicalType(LogicalTypeId::DOUBLE)},
        DPSumFunc, DPSumBind, DPSumInit);
    loader.RegisterFunction(dp_sum_function);
}

void DuckdpExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}

std::string DuckdpExtension::Name() {
    return "duckdp";
}

std::string DuckdpExtension::Version() const {
#ifdef EXT_VERSION_DUCKDP
    return EXT_VERSION_DUCKDP;
#else
    return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(duckdp, loader) {
    duckdb::LoadInternal(loader);
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
