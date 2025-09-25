//
// Created by ila on 9/24/25.
//

#ifndef DUCKDB_DP_C_WRAPPER_H
#define DUCKDB_DP_C_WRAPPER_H

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

// Simplified API for bounded sum
typedef void* DP_BoundedSumHandle;

// Create a BoundedSum object
DP_BoundedSumHandle dp_bounded_sum_create(double epsilon, double lower, double upper);

// Add a single entry
void dp_bounded_sum_add(DP_BoundedSumHandle handle, double value);

// Compute result
double dp_bounded_sum_result(DP_BoundedSumHandle handle);

// Destroy object
void dp_bounded_sum_destroy(DP_BoundedSumHandle handle);

#ifdef __cplusplus
}
#endif


#endif // DUCKDB_DP_C_WRAPPER_H
