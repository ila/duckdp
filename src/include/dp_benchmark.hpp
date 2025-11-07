#pragma once

#include "duckdb.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// DP Sum Benchmark - Accuracy Comparison
//===--------------------------------------------------------------------===//
// This benchmark compares the accuracy of three DP application strategies:
// 1) Raw DP at the record level, then aggregate
// 2) Local DP at client/day aggregates, then aggregate across clients
// 3) Global DP at the final daily aggregate
// It generates synthetic data and computes error metrics (MAE, MRE, RMSE, Max).
//===--------------------------------------------------------------------===//

struct DPBenchmarkConfig {
	idx_t num_clients;           // N: Number of clients/users
	idx_t max_steps_per_record;  // X: Maximum steps per individual record (1..X)
	idx_t max_records_per_day;   // Y: Maximum records per client per day (1..Y)
	idx_t num_days;              // Z: Number of days to simulate
	double epsilon;              // Single privacy budget (used for all mechanisms)
	bool use_laplace;            // true = Laplace, false = Gaussian
	double delta;                // Delta for Gaussian
	uint32_t seed;               // Random seed
	double lower_bound;          // Optional lower bound (default 1)
	double upper_bound;          // Optional upper bound (default = max_steps_per_record)
	double percentage_outliers;  // Fraction in [0,1] of values above upper_bound

	DPBenchmarkConfig()
	    : num_clients(10), max_steps_per_record(10000), max_records_per_day(10), num_days(7), epsilon(1.0),
	      use_laplace(true), delta(1e-5), seed(42), lower_bound(1.0), upper_bound(0.0), percentage_outliers(0.0) {}
};

//===--------------------------------------------------------------------===//
// Benchmark Results Structure
//===--------------------------------------------------------------------===//
struct DPBenchmarkResults {
	struct ErrorMetrics {
		double mae = 0.0;       // Mean Absolute Error
		double mre = 0.0;       // Mean Relative Error (percentage)
		double rmse = 0.0;      // Root Mean Squared Error
		double max_error = 0.0; // Maximum error across all days
	};

	ErrorMetrics raw_dp;
	ErrorMetrics local_dp;
	ErrorMetrics global_dp;

	// Observed outlier share (percentage of rows above upper bound)
	double actual_percentage_outliers = 0.0; // [0,100]

	// Echo the configuration used
	DPBenchmarkConfig config;
};

//===--------------------------------------------------------------------===//
// Main Benchmark Function (runs benchmark and returns results)
//===--------------------------------------------------------------------===//
DPBenchmarkResults RunDPSumBenchmark(ClientContext &context, const DPBenchmarkConfig &config);

//===--------------------------------------------------------------------===//
// Pragma Function Handlers
//===--------------------------------------------------------------------===//
void DPSumBenchmarkPragma(ClientContext &context, const FunctionParameters &parameters);

// Wrapper pragma that sweeps epsilon over a range and repeats per epsilon
void DPSumWrapperPragma(ClientContext &context, const FunctionParameters &parameters);

} // namespace duckdb
