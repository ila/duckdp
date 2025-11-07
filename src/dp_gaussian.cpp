#include "include/dp_functions.hpp"

#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

// Google DP
#include <algorithms/numerical-mechanisms.h>

namespace duckdb {

static void DPGaussianNoiseFunction(DataChunk &args, ExpressionState &, Vector &result) {
	// value DOUBLE, epsilon DOUBLE, delta DOUBLE, sensitivity DOUBLE
	auto &value_vec = args.data[0];
	auto &epsilon_vec = args.data[1];
	auto &delta_vec = args.data[2];
	auto &sens_vec = args.data[3];

	UnifiedVectorFormat value_data, eps_data, delta_data, sens_data;
	value_vec.ToUnifiedFormat(args.size(), value_data);
	epsilon_vec.ToUnifiedFormat(args.size(), eps_data);
	delta_vec.ToUnifiedFormat(args.size(), delta_data);
	sens_vec.ToUnifiedFormat(args.size(), sens_data);

	auto value_ptr = UnifiedVectorFormat::GetData<double>(value_data);
	auto eps_ptr = UnifiedVectorFormat::GetData<double>(eps_data);
	auto del_ptr = UnifiedVectorFormat::GetData<double>(delta_data);
	auto sens_ptr = UnifiedVectorFormat::GetData<double>(sens_data);
	auto out_ptr = FlatVector::GetData<double>(result);
	auto &out_valid = FlatVector::Validity(result);

	for (idx_t i = 0; i < args.size(); i++) {
		auto vi = value_data.sel->get_index(i);
		auto ei = eps_data.sel->get_index(i);
		auto di = delta_data.sel->get_index(i);
		auto si = sens_data.sel->get_index(i);

		if (!value_data.validity.RowIsValid(vi) || !eps_data.validity.RowIsValid(ei) ||
		    !delta_data.validity.RowIsValid(di) || !sens_data.validity.RowIsValid(si)) {
			out_valid.SetInvalid(i);
			continue;
		}

		double val = value_ptr[vi];
		double epsilon = eps_ptr[ei];
		double delta = del_ptr[di];
		double sensitivity = sens_ptr[si];

		if (epsilon <= 0 || delta <= 0 || sensitivity < 0) {
			out_valid.SetInvalid(i);
			continue;
		}

		auto builder = differential_privacy::GaussianMechanism::Builder();
		builder.SetEpsilon(epsilon);
		builder.SetDelta(delta);
		builder.SetL2Sensitivity(sensitivity);
		auto mech_or = builder.Build();
		if (!mech_or.ok()) {
			out_valid.SetInvalid(i);
			continue;
		}
		auto mech = std::move(mech_or).value();
		out_ptr[i] = mech->AddNoise(val);
	}
}

void RegisterDPGaussian(ExtensionLoader &loader) {
	ScalarFunction fn(
	    "dp_gaussian_noise",
	    {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	    LogicalType::DOUBLE,
	    DPGaussianNoiseFunction);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
