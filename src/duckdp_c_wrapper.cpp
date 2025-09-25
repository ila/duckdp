//
// Created by ila on 9/24/25.
//
#include "include/duckdp_c_wrapper.h"
#include "algorithms/bounded-sum.h"  // Google DP header
#include <memory>

extern "C" {

struct DP_BoundedSumWrapper {
    std::unique_ptr<differential_privacy::BoundedSum<double>> sum;
};

DP_BoundedSumHandle dp_bounded_sum_create(double epsilon, double lower, double upper) {
    auto builder = differential_privacy::BoundedSum<double>::Builder();
    builder.SetEpsilon(epsilon);
    builder.SetLower(lower);
    builder.SetUpper(upper);
    auto result = builder.Build();
    if (!result.ok()) return nullptr;
    auto wrapper = new DP_BoundedSumWrapper();
    wrapper->sum = std::move(result.value());
    return wrapper;
}

void dp_bounded_sum_add(DP_BoundedSumHandle handle, double value) {
    if (!handle) return;
    auto wrapper = static_cast<DP_BoundedSumWrapper*>(handle);
    wrapper->sum->AddEntry(value);
}

double dp_bounded_sum_result(DP_BoundedSumHandle handle) {
    if (!handle) return 0.0;
    auto wrapper = static_cast<DP_BoundedSumWrapper*>(handle);
    auto res = wrapper->sum->PartialResult(); // Compute DP result
    if (!res.ok()) return 0.0;
    return differential_privacy::GetValue<double>(res.value());
}

void dp_bounded_sum_destroy(DP_BoundedSumHandle handle) {
    if (!handle) return;
    auto wrapper = static_cast<DP_BoundedSumWrapper*>(handle);
    delete wrapper;
}

} // extern "C"
