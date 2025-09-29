#define DUCKDB_EXTENSION_MAIN

#include "include/duckdp_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

// DP library headers
#include <absl/status/statusor.h>
#include <algorithms/bounded-sum.h>
#include <proto/util.h>

#include <vector>
#include <limits>
#include <type_traits>
#include <memory>

namespace duckdb {

// Bind data with fixed (for now) epsilon/lower/upper
struct DPSumBindData : public FunctionData {
	double epsilon;
	double lower;
	double upper;

	DPSumBindData(double epsilon_p = 1.0, double lower_p = 0.0, double upper_p = 10.0)
	    : epsilon(epsilon_p), lower(lower_p), upper(upper_p) {}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<DPSumBindData>(epsilon, lower, upper);
	}

	bool Equals(const FunctionData &other) const override {
		auto *o = dynamic_cast<const DPSumBindData *>(&other);
		return o && o->epsilon == epsilon && o->lower == lower && o->upper == upper;
	}
};

// Per-type state storing collected values via unique_ptr (LEGACY aggregate pattern)
template <class T>
struct DPSumState {
	std::unique_ptr<std::vector<T>> values; // lazily allocated
};

// Operation implementation templated on input type T
// Produces a DOUBLE result.
template <class T>
struct DPSumOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		// Release any existing (defensive, though newly allocated state should be null-initialized)
		state.values.release();
		state.values = nullptr;
	}

	static bool IgnoreNull() { return true; }

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		if (state.values) {
			state.values = nullptr; // unique_ptr destructor frees
		}
	}

	template <class A_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &idata) {
		if (!state.values) {
			state.values = std::make_unique<std::vector<T>>();
			state.values->reserve(STANDARD_VECTOR_SIZE);
		}
		state.values->push_back(static_cast<T>(a_data));
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &idata, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, idata);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.values || source.values->empty()) {
			return;
		}
		if (!target.values) {
			// Deep copy source vector into new target vector
			target.values = std::make_unique<std::vector<T>>(*source.values);
			return;
		}
		// Append with reserve to minimize reallocations
		target.values->reserve(target.values->size() + source.values->size());
		target.values->insert(target.values->end(), source.values->begin(), source.values->end());
	}

	template <class TARGET_TYPE, class STATE>
	static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data) {
		auto &bind = finalize_data.input.bind_data->Cast<DPSumBindData>();
		using DPType = typename std::conditional<std::is_floating_point<T>::value, double, int64_t>::type;
		using DPBoundedSum = differential_privacy::BoundedSum<DPType>;
		typename DPBoundedSum::Builder builder;
		builder.SetEpsilon(bind.epsilon);
		builder.SetLower(static_cast<DPType>(bind.lower));
		builder.SetUpper(static_cast<DPType>(bind.upper));
		auto mech_status = builder.Build();
		if (!mech_status.ok()) {
			finalize_data.ReturnNull();
			return;
		}
		auto ptr = std::move(mech_status).value();

		if (!state.values || state.values->empty()) {
			std::vector<DPType> empty;
			auto res = ptr->Result(empty.begin(), empty.end());
			if (!res.ok()) { finalize_data.ReturnNull(); return; }
			target = static_cast<double>(differential_privacy::GetValue<DPType>(res.value()));
			return;
		}

		if constexpr (!std::is_same<DPType, T>::value) {
			std::vector<DPType> converted;
			converted.reserve(state.values->size());
			for (auto &v : *state.values) converted.push_back(static_cast<DPType>(v));
			auto res = ptr->Result(converted.begin(), converted.end());
			if (!res.ok()) { finalize_data.ReturnNull(); return; }
			target = static_cast<double>(differential_privacy::GetValue<DPType>(res.value()));
		} else {
			auto res = ptr->Result(state.values->begin(), state.values->end());
			if (!res.ok()) { finalize_data.ReturnNull(); return; }
			target = static_cast<double>(differential_privacy::GetValue<DPType>(res.value()));
		}
	}
};

// Bind: currently single argument. (Later extend to optional epsilon/lower/upper args.)
unique_ptr<FunctionData> DPSumBind(ClientContext &, AggregateFunction &, vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 1) {
		throw BinderException("dp_sum currently supports exactly one argument: dp_sum(value)");
	}
	// For now fixed epsilon/lower/upper. TODO: parse optional parameters.
	return make_uniq<DPSumBindData>(1.0, 0.0, 10.0);
}

template <typename T>
static AggregateFunction DPSumCreateAggregate(const LogicalType &input_type) {
	return AggregateFunction::UnaryAggregateDestructor<
	    DPSumState<T>, T, double, DPSumOperation<T>, AggregateDestructorType::LEGACY>(
	    input_type, LogicalType::DOUBLE);
}

void DuckdpExtension::Load(ExtensionLoader &loader) {
	AggregateFunctionSet dp_sum_set("dp_sum");

	// Register different numeric input types
	{
		auto f = DPSumCreateAggregate<int32_t>(LogicalType::INTEGER);
		f.bind = DPSumBind;
		dp_sum_set.AddFunction(f);
	}
	{
		auto f = DPSumCreateAggregate<int64_t>(LogicalType::BIGINT);
		f.bind = DPSumBind;
		dp_sum_set.AddFunction(f);
	}
	{
		auto f = DPSumCreateAggregate<float>(LogicalType::FLOAT);
		f.bind = DPSumBind;
		dp_sum_set.AddFunction(f);
	}
	{
		auto f = DPSumCreateAggregate<double>(LogicalType::DOUBLE);
		f.bind = DPSumBind;
		dp_sum_set.AddFunction(f);
	}

	loader.RegisterFunction(dp_sum_set);
}

string DuckdpExtension::Name() { return "duckdp"; }
std::string DuckdpExtension::Version() const { return "0.1.0"; }

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void duckdp_duckdb_cpp_init(duckdb::ExtensionLoader &loader) {
	duckdb::DuckdpExtension extension;
	extension.Load(loader);
}

DUCKDB_EXTENSION_API const char *duckdp_version() { return duckdb::DuckDB::LibraryVersion(); }
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif