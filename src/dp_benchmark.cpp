#include "include/dp_benchmark.hpp"
#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include <cmath>
#include <sstream>
#include <stdexcept>
#include <vector>
#include <string>

namespace duckdb {

//===--------------------------------------------------------------------===//
// SQL Helpers
//===--------------------------------------------------------------------===//
// ExecOrThrow: convenience wrapper to run a single SQL statement and throw a
// helpful exception if the execution fails.
static inline void ExecOrThrow(Connection &con, const string &sql) {
	auto res = con.Query(sql);
	if (res->HasError()) {
		throw std::runtime_error("dp_sum_benchmark SQL error: " + res->GetError());
	}
}

// FetchMetrics: convenience function that given the name of a scenario table
// (with schema: day, sum) computes MAE, MRE, RMSE and Max error vs true_daily.
static inline DPBenchmarkResults::ErrorMetrics FetchMetrics(Connection &con, const string &scenario_table) {
	// Compute metrics by joining ground truth with the scenario table
	std::stringstream ss;
	ss << "WITH j AS (\n"
	   << "  SELECT t.day, t.sum AS true_sum, s.sum AS noisy_sum\n"
	   << "  FROM true_daily t JOIN " << scenario_table << " s USING(day)\n"
	   << ")\n"
	   << "SELECT\n"
	   << "  AVG(ABS(true_sum - noisy_sum)) AS mae,\n"
	   << "  AVG(CASE WHEN true_sum > 1e-12 THEN ABS(true_sum - noisy_sum)/true_sum ELSE 0 END) * 100.0 AS mre,\n"
	   << "  SQRT(AVG(POWER(true_sum - noisy_sum, 2))) AS rmse,\n"
	   << "  MAX(ABS(true_sum - noisy_sum)) AS max_error\n"
	   << "FROM j;";

	auto res = con.Query(ss.str());
	if (res->HasError()) {
		throw std::runtime_error("dp_sum_benchmark metrics SQL error: " + res->GetError());
	}
	DPBenchmarkResults::ErrorMetrics m;
	if (res->RowCount() > 0) {
		// Use Value API directly to preserve floating precision (avoid int64 truncation in templated GetValue)
		m.mae = res->GetValue(0, 0).GetValue<double>();
		m.mre = res->GetValue(1, 0).GetValue<double>();
		m.rmse = res->GetValue(2, 0).GetValue<double>();
		m.max_error = res->GetValue(3, 0).GetValue<double>();
	}
	return m;
}

// Helper to build a DP function call string for either Laplace or Gaussian.
// Replace the "%VAL%" placeholder with the expression the noise should be
// applied to (e.g., CAST(steps AS DOUBLE) or CAST(SUM(steps) AS DOUBLE)).
static inline string DpCall(const string &dpfn, bool use_laplace, double epsilon, double delta_or_sens, double sens_if_gauss) {
	std::stringstream ss;
	ss.setf(std::ios::fixed);
	ss.precision(12);
	if (use_laplace) {
		// dp_laplace_noise(value, epsilon, sensitivity)
		ss << dpfn << "(%VAL%, " << epsilon << ", " << delta_or_sens << ")";
	} else {
		// dp_gaussian_noise(value, epsilon, delta, sensitivity)
		ss << dpfn << "(%VAL%, " << epsilon << ", " << delta_or_sens << ", " << sens_if_gauss << ")";
	}
	return ss.str();
}

//===--------------------------------------------------------------------===//
// Main Benchmark Implementation (SQL-only data + metrics)
//===--------------------------------------------------------------------===//
DPBenchmarkResults RunDPSumBenchmark(ClientContext &context, const DPBenchmarkConfig &input_cfg) {
	DPBenchmarkResults results;
	results.config = input_cfg;

	// Normalize configuration to safe minimums to avoid corner-cases that would
	// otherwise generate empty tables, invalid ranges, or divide-by-zero:
	// - ensure N, Z, Y, X >= 1
	// - default upper_bound to X if not set
	// - ensure upper_bound > lower_bound
	auto cfg = input_cfg;
	if (cfg.num_clients == 0) cfg.num_clients = 1;
	if (cfg.num_days == 0) cfg.num_days = 1;
	if (cfg.max_records_per_day == 0) cfg.max_records_per_day = 1;
	if (cfg.max_steps_per_record == 0) cfg.max_steps_per_record = 1;
	if (cfg.upper_bound <= 0.0) cfg.upper_bound = static_cast<double>(cfg.max_steps_per_record);
	if (cfg.upper_bound <= cfg.lower_bound) cfg.upper_bound = cfg.lower_bound + 1.0;
	// p_out is the probability (fraction in [0,1]) that a generated record is
	// marked as outlier and bumped above the upper bound.
	double p_out = std::max(0.0, std::min(1.0, cfg.percentage_outliers));

	// Precompute integer bounds for the random step generator.
	// We sample integer steps uniformly in [lb_i .. ub_i], hence we:
	// - map the (potentially fractional) bounds to integers using floor
	// - enforce ub_i > lb_i to avoid zero-width ranges or modulo-by-zero in SQL
	//   randomization expressions
	idx_t lb_i = static_cast<idx_t>(std::llround(std::max(0.0, std::floor(cfg.lower_bound))));
	idx_t ub_i = static_cast<idx_t>(std::llround(std::floor(cfg.upper_bound)));
	if (ub_i <= lb_i) ub_i = lb_i + 1;
	// Notes:
	// - lb_i/ub_i are used in SQL to generate steps as:
	//     CAST(FLOOR(random() * (ub_i - lb_i + 1)) + lb_i AS BIGINT)
	//   which yields integers in [lb_i, ub_i].
	// - We also use ub_i as the threshold for flagging outliers (steps > ub_i).

	// Sensitivity and epsilon allocation (single epsilon budget):
	// Adjacency assumption (from user choice): change ONE record anywhere.
	// Therefore sensitivity of any sum (per record, per day, global) is just X (max_steps_per_record).
	// We split epsilon to make composed mechanisms comparable:
	//   - Raw DP: noise added per record, each record gets epsilon_raw_record = epsilon / (num_days * max_records_per_day)
	//   - Local DP: noise added once per client/day, epsilon_local_day = epsilon / num_days
	//   - Global DP: noise added once to final daily aggregate across all clients, uses full epsilon
	// This makes Raw DP the noisiest (lots of tiny-epsilon noises summed), then Local, then Global (least noise).
	// NOTE: For Gaussian mechanism we keep delta constant for all calls.
	double per_record_sens = static_cast<double>(cfg.max_steps_per_record); // X
	double denom_records = static_cast<double>(cfg.num_days) * static_cast<double>(cfg.max_records_per_day);
	if (denom_records < 1.0) denom_records = 1.0; // safety
	double epsilon_raw_record = cfg.epsilon / denom_records;
	if (epsilon_raw_record <= 0) epsilon_raw_record = cfg.epsilon; // fallback if epsilon tiny
	double epsilon_local_day = cfg.epsilon / static_cast<double>(cfg.num_days > 0 ? cfg.num_days : 1);
	if (epsilon_local_day <= 0) epsilon_local_day = cfg.epsilon;
	// Global DP uses full epsilon

	// Choose DP UDF based on mechanism (unchanged)
	const bool use_laplace = cfg.use_laplace;
	const string dpfn = use_laplace ? "dp_laplace_noise" : "dp_gaussian_noise";

	// Create a connection on the same in-memory database as the calling context
	Connection con(*context.db);

	// Seed the SQL RNG so that calls to random() are reproducible per seed.
	// setseed expects a value in [0,1), so we map the integer seed to a fraction.
	{
		double seed_frac = (static_cast<double>(cfg.seed % 1000000u)) / 1000000.0; // in [0,1)
		std::stringstream ss;
		ss.setf(std::ios::fixed);
		ss.precision(12);
		ss << "SELECT setseed(" << seed_frac << ");";
		ExecOrThrow(con, ss.str());
	}

	// Create per-client raw tables with schema:
	//   raw_data_i(day INTEGER, steps BIGINT, dp_steps DOUBLE)
	// steps is randomly generated; dp_steps applies DP to the record-level value.
	for (idx_t client = 1; client <= cfg.num_clients; client++) {
		// Define the schema explicitly so the insert-only query can stream rows in.
		{
			std::stringstream cs;
			cs << "CREATE TEMP TABLE raw_data_" << client << " (day INTEGER, steps BIGINT, dp_steps DOUBLE);";
			ExecOrThrow(con, cs.str());
		}
		// Prepare the DP call for record-level noise: substitute %VAL% with the steps expression.
		// Laplace: dp_laplace_noise(val, epsilon_raw_record, sensitivity=X)
		// Gaussian: dp_gaussian_noise(val, epsilon_raw_record, delta, sensitivity=X)
		string raw_call = DpCall(dpfn, use_laplace, epsilon_raw_record, use_laplace ? per_record_sens : cfg.delta, per_record_sens);

		// Populate raw_data_i: generate random number of records per day in [1..Y],
		// sample steps in [lb_i..ub_i], flip a biased coin for outliers, then compute
		// dp_steps by applying DP to the steps value.
		std::stringstream ss;
		ss.setf(std::ios::fixed);
		ss.precision(12);
		ss << "WITH days AS (SELECT range AS day FROM range(1, " << cfg.num_days << ")),\n"
		   << "     per_day_counts AS (\n"
		   << "       SELECT day, 1 + CAST(FLOOR(random() * " << cfg.max_records_per_day << ") AS BIGINT) AS num_records\n"
		   << "       FROM days\n"
		   << "     ),\n"
		   << "     raw AS (\n"
		   << "       SELECT p.day, r.range AS rec_index,\n"
		   << "              CAST(FLOOR(random() * " << (ub_i - lb_i + 1) << ") + " << lb_i << " AS BIGINT) AS base_steps,\n"
		   << "              (random() < " << p_out << ") AS is_outlier\n"
		   << "       FROM per_day_counts p\n"
		   << "       JOIN LATERAL range(1, p.num_records) r ON TRUE\n"
		   << "     ),\n"
		   << "     vals AS (\n"
		   << "       SELECT day,\n"
		   << "              CASE WHEN is_outlier THEN base_steps + GREATEST(1, CAST(" << ub_i << " AS BIGINT)) ELSE base_steps END AS steps\n"
		   << "       FROM raw\n"
		   << "     )\n";
		// Replace %VAL% with CAST(steps AS DOUBLE) in the dp call, then insert rows.
		auto dp_raw = raw_call;
		auto pos = dp_raw.find("%VAL%");
		dp_raw.replace(pos, 5, "CAST(steps AS DOUBLE)");
		ss << "INSERT INTO raw_data_" << client << "\n"
		   << "SELECT day, steps, " << dp_raw << " AS dp_steps\n"
		   << "FROM vals;";
		ExecOrThrow(con, ss.str());
	}

	// Compute the actually observed outlier fraction over all generated rows by
	// checking how many steps exceeded the upper integer bound ub_i.
	{
		std::stringstream ss;
		ss << "SELECT 100.0 * AVG(CASE WHEN steps > " << ub_i << " THEN 1.0 ELSE 0.0 END) AS pct FROM (\n";
		for (idx_t client = 1; client <= cfg.num_clients; client++) {
			ss << (client == 1 ? "" : "UNION ALL ") << "SELECT steps FROM raw_data_" << client << "\n";
		}
		ss << ") t;";
		auto res = con.Query(ss.str());
		if (!res->HasError() && res->RowCount() > 0) {
			results.actual_percentage_outliers = res->GetValue<double>(0, 0);
		}
	}

	// For each client, create aggregated_client_i(day, true_sum, raw_dp_sum, local_dp_sum):
	// - true_sum      = SUM(steps)
	// - raw_dp_sum    = SUM(dp_steps)                  [DP at record-level]
	// - local_dp_sum  = DP(SUM(steps)) with sens=Y*X   [DP at client/day aggregate]
	for (idx_t client = 1; client <= cfg.num_clients; client++) {
		{
			std::stringstream cs;
			cs << "CREATE TEMP TABLE aggregated_client_" << client
			   << " (day INTEGER, true_sum DOUBLE, raw_dp_sum DOUBLE, local_dp_sum DOUBLE);";
			ExecOrThrow(con, cs.str());
		}
		// Build the local DP call applied after summing the client's steps per day
		// Uses epsilon_local_day and same sensitivity X
		string local_call = DpCall(dpfn, use_laplace, epsilon_local_day, use_laplace ? per_record_sens : cfg.delta, per_record_sens);
		auto local_call_sub = local_call;
		auto pos2 = local_call_sub.find("%VAL%");
		local_call_sub.replace(pos2, 5, "CAST(SUM(steps) AS DOUBLE)");

		std::stringstream ss;
		ss << "INSERT INTO aggregated_client_" << client << "\n"
		   << "SELECT day,\n"
		   << "       CAST(SUM(steps) AS DOUBLE) AS true_sum,\n"
		   << "       SUM(CAST(dp_steps AS DOUBLE)) AS raw_dp_sum,\n"
		   << "       " << local_call_sub << " AS local_dp_sum\n"
		   << "FROM raw_data_" << client << "\n"
		   << "GROUP BY day\n"
		   << "ORDER BY day;";
		ExecOrThrow(con, ss.str());
	}

	// Build final_daily(day, true_sum, raw_dp_sum, local_dp_sum, global_dp_sum):
	// - true_sum/raw_dp_sum/local_dp_sum are summed across clients
	// - global_dp_sum = DP(true_sum) with sensitivity X (record-level adjacency)
	{
		ExecOrThrow(con, "CREATE TEMP TABLE final_daily (day INTEGER, true_sum DOUBLE, raw_dp_sum DOUBLE, local_dp_sum DOUBLE, global_dp_sum DOUBLE);");

		std::stringstream union_ss;
		for (idx_t client = 1; client <= cfg.num_clients; client++) {
			union_ss << (client == 1 ? "" : "UNION ALL ")
			         << "SELECT day, true_sum, raw_dp_sum, local_dp_sum FROM aggregated_client_" << client << "\n";
		}

		// Apply global DP to the true daily totals across all clients
		// Uses full epsilon and same sensitivity X (record-level adjacency)
		string global_call = DpCall(dpfn, use_laplace, cfg.epsilon, use_laplace ? per_record_sens : cfg.delta, per_record_sens);
		auto global_call_sub = global_call;
		auto posg = global_call_sub.find("%VAL%");
		global_call_sub.replace(posg, 5, "true_sum");

		std::stringstream final_ss;
		final_ss << "WITH totals AS (\n"
		        << "  SELECT day,\n"
		        << "         SUM(true_sum) AS true_sum,\n"
		        << "         SUM(raw_dp_sum) AS raw_dp_sum,\n"
		        << "         SUM(local_dp_sum) AS local_dp_sum\n"
		        << "  FROM (\n" << union_ss.str() << "  ) u\n"
		        << "  GROUP BY day\n"
		        << ")\n"
		        << "INSERT INTO final_daily\n"
		        << "SELECT day, true_sum, raw_dp_sum, local_dp_sum, " << global_call_sub << " AS global_dp_sum\n"
		        << "FROM totals\n"
		        << "ORDER BY day;";
		ExecOrThrow(con, final_ss.str());
	}

	// Per-day metrics comparing raw/local/global to the true sums.
	// MRE is reported as a percentage; a small threshold avoids division by 0.
	{
		std::stringstream ss;
		ss << "CREATE TEMP TABLE dp_benchmark_daily AS\n"
		   << "SELECT day, true_sum, raw_dp_sum, local_dp_sum, global_dp_sum,\n"
		   << "       ABS(true_sum - raw_dp_sum) AS raw_mae,\n"
		   << "       CASE WHEN true_sum > 1e-12 THEN ABS(true_sum - raw_dp_sum)/true_sum * 100.0 ELSE 0 END AS raw_mre,\n"
		   << "       ABS(true_sum - local_dp_sum) AS local_mae,\n"
		   << "       CASE WHEN true_sum > 1e-12 THEN ABS(true_sum - local_dp_sum)/true_sum * 100.0 ELSE 0 END AS local_mre,\n"
		   << "       ABS(true_sum - global_dp_sum) AS global_mae,\n"
		   << "       CASE WHEN true_sum > 1e-12 THEN ABS(true_sum - global_dp_sum)/true_sum * 100.0 ELSE 0 END AS global_mre\n"
		   << "FROM final_daily\n"
		   << "ORDER BY day;";
		ExecOrThrow(con, ss.str());
	}

	// Scenario summary (1 row per scenario) aggregating the per-day errors.
	{
		std::stringstream ss;
		ss << "CREATE TEMP TABLE dp_benchmark_summary AS\n"
		   << "WITH agg AS (\n"
		   << "  SELECT\n"
		   << "    AVG(raw_mae) AS raw_mae,\n"
		   << "    AVG(raw_mre) AS raw_mre,\n"
		   << "    SQRT(AVG(POWER(raw_mae, 2))) AS raw_rmse,\n"
		   << "    MAX(raw_mae) AS raw_max,\n"
		   << "    AVG(local_mae) AS local_mae,\n"
		   << "    AVG(local_mre) AS local_mre,\n"
		   << "    SQRT(AVG(POWER(local_mae, 2))) AS local_rmse,\n"
		   << "    MAX(local_mae) AS local_max,\n"
		   << "    AVG(global_mae) AS global_mae,\n"
		   << "    AVG(global_mre) AS global_mre,\n"
		   << "    SQRT(AVG(POWER(global_mae, 2))) AS global_rmse,\n"
		   << "    MAX(global_mae) AS global_max\n"
		   << "  FROM dp_benchmark_daily\n"
		   << ")\n"
		   << "SELECT 'raw_dp' AS scenario, raw_mae AS mae, raw_mre AS mre, raw_rmse AS rmse, raw_max AS max_error FROM agg\n"
		   << "UNION ALL\n"
		   << "SELECT 'local_dp', local_mae, local_mre, local_rmse, local_max FROM agg\n"
		   << "UNION ALL\n"
		   << "SELECT 'global_dp', global_mae, global_mre, global_rmse, global_max FROM agg;";
		ExecOrThrow(con, ss.str());
	}

	// Copy the summary metrics into the result struct so the pragma can print them.
	{
		auto res = con.Query("SELECT scenario, mae, mre, rmse, max_error FROM dp_benchmark_summary ORDER BY scenario;");
		if (!res->HasError() && res->RowCount() >= 3) {
			for (idx_t i = 0; i < res->RowCount(); i++) {
				auto scen = res->GetValue(0, i).ToString();
				double mae = res->GetValue(1, i).GetValue<double>();
				double mre = res->GetValue(2, i).GetValue<double>();
				double rmse = res->GetValue(3, i).GetValue<double>();
				double maxe = res->GetValue(4, i).GetValue<double>();
				if (scen == "raw_dp") {
					results.raw_dp.mae = mae; results.raw_dp.mre = mre; results.raw_dp.rmse = rmse; results.raw_dp.max_error = maxe;
				} else if (scen == "local_dp") {
					results.local_dp.mae = mae; results.local_dp.mre = mre; results.local_dp.rmse = rmse; results.local_dp.max_error = maxe;
				} else if (scen == "global_dp") {
					results.global_dp.mae = mae; results.global_dp.mre = mre; results.global_dp.rmse = rmse; results.global_dp.max_error = maxe;
				}
			}
		}
	}

	return results;
}

//===--------------------------------------------------------------------===//
// Pragma Function Handler (prints summary + leaves temp tables for inspection)
//===--------------------------------------------------------------------===//
void DPSumBenchmarkPragma(ClientContext &context, const FunctionParameters &parameters) {
	// Parse parameters (all optional with defaults)
	DPBenchmarkConfig config;

	// Expected format: PRAGMA dp_sum_benchmark(num_clients=10, max_steps=10000, ...)
	auto &named_params = parameters.named_parameters;

	if (named_params.count("num_clients")) {
		config.num_clients = named_params.at("num_clients").GetValue<idx_t>();
	}
	if (named_params.count("max_steps")) {
		config.max_steps_per_record = named_params.at("max_steps").GetValue<idx_t>();
	}
	if (named_params.count("max_records_per_day")) {
		config.max_records_per_day = named_params.at("max_records_per_day").GetValue<idx_t>();
	}
	if (named_params.count("num_days")) {
		config.num_days = named_params.at("num_days").GetValue<idx_t>();
	}
	if (named_params.count("epsilon")) {
		config.epsilon = named_params.at("epsilon").GetValue<double>();
	}
	if (named_params.count("mechanism")) {
		auto mech = named_params.at("mechanism").ToString();
		config.use_laplace = (mech == "laplace");
	}
	if (named_params.count("delta")) {
		config.delta = named_params.at("delta").GetValue<double>();
	}
	if (named_params.count("seed")) {
		config.seed = named_params.at("seed").GetValue<uint32_t>();
	}
	if (named_params.count("lower_bound")) {
		config.lower_bound = named_params.at("lower_bound").GetValue<double>();
	}
	if (named_params.count("upper_bound")) {
		config.upper_bound = named_params.at("upper_bound").GetValue<double>();
	}
	if (named_params.count("percentage_outliers")) {
		double po = named_params.at("percentage_outliers").GetValue<double>();
		// Accept either fraction [0,1] or percentage [0,100]
		if (po > 1.0 && po <= 100.0) po /= 100.0;
		po = std::max(0.0, std::min(1.0, po));
		config.percentage_outliers = po;
	}

	// Normalize bounds before running (same logic as in RunDPSumBenchmark)
	if (config.upper_bound <= 0.0) {
		config.upper_bound = static_cast<double>(config.max_steps_per_record);
	}
	if (config.upper_bound <= config.lower_bound) {
		config.upper_bound = config.lower_bound + 1.0;
	}

	// Run the benchmark (SQL-based)
	auto results = RunDPSumBenchmark(context, config);

	// Print results to console (pragma output)
	Printer::Print("DP Sum Benchmark Results");
	Printer::Print("========================");
	Printer::Print("Configuration:");
	Printer::Print("  Clients: " + std::to_string(config.num_clients));
	Printer::Print("  Max Steps/Record: " + std::to_string(config.max_steps_per_record));
	Printer::Print("  Max Records/Day: " + std::to_string(config.max_records_per_day));
	Printer::Print("  Days: " + std::to_string(config.num_days));
	Printer::Print("  Epsilon: " + std::to_string(config.epsilon));
	Printer::Print("  Mechanism: " + string(config.use_laplace ? "Laplace" : "Gaussian"));
	if (!config.use_laplace) {
		Printer::Print("  Delta: " + std::to_string(config.delta));
	}
	Printer::Print("  Bounds: [" + std::to_string(config.lower_bound) + ", " + std::to_string(config.upper_bound) + "]");
	Printer::Print("  Target Outliers: " + std::to_string(static_cast<int>(std::round(config.percentage_outliers * 100.0))) + "%");
	Printer::Print("  Seed: " + std::to_string(config.seed));
	Printer::Print("");
	Printer::Print("Observed Outliers: " + std::to_string(results.actual_percentage_outliers) + "% (share of records with steps > upper bound)");
	Printer::Print("");
	Printer::Print("Error Metrics:");
	// Use expanded names instead of acronyms in the header (unchanged)
	Printer::Print("Approach              | Mean Absolute Error | Mean Relative Error (%) | Root Mean Squared Error | Max Absolute Error");
	Printer::Print("---------------------|---------------------|--------------------------|-------------------------|--------------------");

	vector<string> approaches = {"Raw DP", "Local DP (Client)", "Global DP (Final)"};
	vector<DPBenchmarkResults::ErrorMetrics> metrics = {results.raw_dp, results.local_dp, results.global_dp};
	for (idx_t i = 0; i < approaches.size(); i++) {
		char buffer[512];
		snprintf(buffer, sizeof(buffer), "%-20s | %19.2f | %24.2f | %23.2f | %20.2f", approaches[i].c_str(), metrics[i].mae,
		         metrics[i].mre, metrics[i].rmse, metrics[i].max_error);
		Printer::Print(buffer);
	}

	Printer::Print("");
	Printer::Print("Temp tables created for inspection:");
	Printer::Print("  - raw_data_1..N");
	Printer::Print("  - aggregated_client_1..N");
	Printer::Print("  - final_daily");
	Printer::Print("  - dp_benchmark_daily (per-day metrics)");
	Printer::Print("  - dp_benchmark_summary (scenario summary)");

	bool export_csv = false;
	string csv_path;
	string csv_delim = ",";
	bool fairness = false;
	if (named_params.count("export_csv")) {
		export_csv = named_params.at("export_csv").GetValue<bool>();
	}
	if (named_params.count("csv_path")) {
		csv_path = named_params.at("csv_path").ToString();
	}
	if (named_params.count("csv_delimiter")) {
		csv_delim = named_params.at("csv_delimiter").ToString();
	}
	if (named_params.count("fairness")) {
		fairness = named_params.at("fairness").GetValue<bool>();
	}

	// Fairness diagnostics (optional): verify composition splits
	if (fairness) {
		Printer::Print("Fairness Diagnostics:");
		double denom_records = (double)config.num_days * (double)config.max_records_per_day;
		if (denom_records < 1.0) denom_records = 1.0;
		double eps_raw_record = config.epsilon / denom_records;
		double recomposed = eps_raw_record * denom_records;
		char buf[256];
		snprintf(buf, sizeof(buf), "  Raw per-record epsilon: %.6f; recomposed: %.6f (target %.6f)", eps_raw_record, recomposed, config.epsilon);
		Printer::Print(buf);
		double eps_local_day = config.epsilon / (config.num_days > 0 ? config.num_days : 1);
		snprintf(buf, sizeof(buf), "  Local per-day epsilon: %.6f; recomposed: %.6f (days=%llu)", eps_local_day, eps_local_day * (double)config.num_days, (unsigned long long)config.num_days);
		Printer::Print(buf);
		snprintf(buf, sizeof(buf), "  Global epsilon (full budget): %.6f", config.epsilon);
		Printer::Print(buf);
		Printer::Print("");
	}
	// CSV export (optional)
	if (export_csv) {
		if (csv_path.empty()) {
			csv_path = "dp_sum_benchmark.csv"; // default
		}
		Connection con(*context.db);
		std::stringstream ins;
		ins.setf(std::ios::fixed); ins.precision(12);
		ins << "CREATE OR REPLACE TEMP TABLE dp_benchmark_export AS SELECT "
		    << config.epsilon << " AS epsilon, "
		    << config.num_clients << " AS num_clients, " << config.num_days << " AS num_days, "
		    << config.max_records_per_day << " AS max_records_per_day, " << config.max_steps_per_record << " AS max_steps_per_record, '"
		    << (config.use_laplace ? "laplace" : "gaussian") << "' AS mechanism, " << config.delta << " AS delta, "
		    << config.seed << " AS seed, " << (config.percentage_outliers * 100.0) << " AS target_outliers_pct, "
		    << results.actual_percentage_outliers << " AS observed_outliers_pct, scenario, mae, mre, rmse, max_error FROM dp_benchmark_summary;";
		ExecOrThrow(con, ins.str());
		std::stringstream cp;
		cp << "COPY (SELECT * FROM dp_benchmark_export) TO '" << csv_path << "' (FORMAT CSV, HEADER TRUE, DELIMITER '" << csv_delim << "');";
		auto copy_res = con.Query(cp.str());
		if (copy_res->HasError()) {
			Printer::Print("CSV export failed: " + copy_res->GetError());
		} else {
			Printer::Print("CSV exported to " + csv_path);
		}
	}
}

// Wrapper pragma: sweeps epsilon range and repeats per epsilon; optional CSV and fairness diagnostics.
void DPSumWrapperPragma(ClientContext &context, const FunctionParameters &parameters) {
	auto &named_params = parameters.named_parameters;
	double e_min=0.0,e_max=0.0,e_step=0.0; idx_t seeds=1;
	if (!named_params.count("epsilon_min") || !named_params.count("epsilon_max") || !named_params.count("epsilon_step")) {
		throw std::runtime_error("dp_sum_wrapper requires epsilon_min, epsilon_max, epsilon_step");
	}
	e_min = named_params.at("epsilon_min").GetValue<double>(); e_max = named_params.at("epsilon_max").GetValue<double>(); e_step = named_params.at("epsilon_step").GetValue<double>();
	if (named_params.count("seeds")) {
		seeds = named_params.at("seeds").GetValue<idx_t>();
	}
	if (e_step <= 0.0) {
		throw std::runtime_error("epsilon_step must be > 0");
	}
	if (e_min <= 0.0 || e_max <= 0.0) {
		throw std::runtime_error("epsilon_min and epsilon_max must be > 0");
	}
	if (e_max < e_min) {
		throw std::runtime_error("epsilon_max must be >= epsilon_min");
	}
	double span = e_max - e_min; double steps_exact = span / e_step; idx_t steps_count = (idx_t)llround(steps_exact); if (fabs((double)steps_count * e_step - span) > 1e-9) throw std::runtime_error("(epsilon_max - epsilon_min) must be an integer multiple of epsilon_step");
	bool export_csv=false; string csv_path; string csv_delim=","; bool fairness=false; if (named_params.count("export_csv")) export_csv = named_params.at("export_csv").GetValue<bool>(); if (named_params.count("csv_path")) csv_path = named_params.at("csv_path").ToString(); if (named_params.count("csv_delimiter")) csv_delim = named_params.at("csv_delimiter").ToString(); if (named_params.count("fairness")) fairness = named_params.at("fairness").GetValue<bool>();
	DPBenchmarkConfig base; if (named_params.count("num_clients")) base.num_clients = named_params.at("num_clients").GetValue<idx_t>(); if (named_params.count("max_steps")) base.max_steps_per_record = named_params.at("max_steps").GetValue<idx_t>(); if (named_params.count("max_records_per_day")) base.max_records_per_day = named_params.at("max_records_per_day").GetValue<idx_t>(); if (named_params.count("num_days")) base.num_days = named_params.at("num_days").GetValue<idx_t>(); if (named_params.count("mechanism")) { auto mech = named_params.at("mechanism").ToString(); base.use_laplace = (mech == "laplace"); } if (named_params.count("delta")) base.delta = named_params.at("delta").GetValue<double>(); uint32_t seed_base = base.seed; if (named_params.count("seed")) seed_base = named_params.at("seed").GetValue<uint32_t>(); if (named_params.count("lower_bound")) base.lower_bound = named_params.at("lower_bound").GetValue<double>(); if (named_params.count("upper_bound")) base.upper_bound = named_params.at("upper_bound").GetValue<double>(); if (named_params.count("percentage_outliers")) { double po = named_params.at("percentage_outliers").GetValue<double>(); if (po > 1.0 && po <= 100.0) po /= 100.0; po = std::max(0.0, std::min(1.0, po)); base.percentage_outliers = po; }
	Connection con(*context.db);
	ExecOrThrow(con, "CREATE TEMP TABLE dp_sum_wrapper_runs (epsilon DOUBLE, seed BIGINT, repeat_idx BIGINT, scenario VARCHAR, mae DOUBLE, mre DOUBLE, rmse DOUBLE, max_error DOUBLE);");
	for (idx_t step_i=0; step_i<=steps_count; step_i++) {
		double eps_cur = e_min + (double)step_i * e_step;
		for (idx_t rep=0; rep<seeds; rep++) {
			DPBenchmarkConfig cfg = base;
			cfg.epsilon = eps_cur;
			cfg.seed = seed_base + (uint32_t)rep;
			auto res = RunDPSumBenchmark(context, cfg);
			std::stringstream ins;
			ins.setf(std::ios::fixed); ins.precision(12);
			ins << "INSERT INTO dp_sum_wrapper_runs VALUES(" << eps_cur << ", " << cfg.seed << ", " << rep << ", 'raw_dp', " << res.raw_dp.mae << ", " << res.raw_dp.mre << ", " << res.raw_dp.rmse << ", " << res.raw_dp.max_error << ");";
			ins << "INSERT INTO dp_sum_wrapper_runs VALUES(" << eps_cur << ", " << cfg.seed << ", " << rep << ", 'local_dp', " << res.local_dp.mae << ", " << res.local_dp.mre << ", " << res.local_dp.rmse << ", " << res.local_dp.max_error << ");";
			ins << "INSERT INTO dp_sum_wrapper_runs VALUES(" << eps_cur << ", " << cfg.seed << ", " << rep << ", 'global_dp', " << res.global_dp.mae << ", " << res.global_dp.mre << ", " << res.global_dp.rmse << ", " << res.global_dp.max_error << ");";
			ExecOrThrow(con, ins.str());
		}
	}
	ExecOrThrow(con, "CREATE TEMP TABLE dp_sum_wrapper_summary AS SELECT epsilon, scenario, AVG(mae) AS mae, AVG(mre) AS mre, AVG(rmse) AS rmse, AVG(max_error) AS max_error, COUNT(*)/3 AS repeats FROM dp_sum_wrapper_runs GROUP BY epsilon, scenario ORDER BY epsilon, scenario;");
	Printer::Print("DP Sum Wrapper Results");
	Printer::Print("======================");
	Printer::Print("Configuration (shared except epsilon sweep):");
	Printer::Print("  Epsilon Range: " + std::to_string(e_min) + " to " + std::to_string(e_max) + " step " + std::to_string(e_step));
	Printer::Print("  Repeats per epsilon: " + std::to_string(seeds));
	Printer::Print("  Clients: " + std::to_string(base.num_clients));
	Printer::Print("  Days: " + std::to_string(base.num_days));
	Printer::Print("  Max Records/Day: " + std::to_string(base.max_records_per_day));
	Printer::Print("  Max Steps/Record: " + std::to_string(base.max_steps_per_record));
	Printer::Print("  Mechanism: " + string(base.use_laplace ? "Laplace" : "Gaussian"));
	if (!base.use_laplace) Printer::Print("  Delta: " + std::to_string(base.delta));
	Printer::Print("");
	if (fairness) {
		Printer::Print("Fairness Diagnostics (per epsilon):");
		for (idx_t step_i=0; step_i<=steps_count; step_i++) {
			double eps_cur = e_min + (double)step_i * e_step;
			double denom_records = (double)base.num_days * (double)base.max_records_per_day; if (denom_records < 1.0) denom_records = 1.0;
			double eps_raw_record = eps_cur / denom_records;
			char fb[256]; snprintf(fb, sizeof(fb), "  eps=%.6f raw_per_record=%.6f local_per_day=%.6f global=%.6f", eps_cur, eps_raw_record, eps_cur / (double)base.num_days, eps_cur); Printer::Print(fb);
		}
		Printer::Print("");
	}
	Printer::Print("Aggregated Error Metrics (averaged across repeats):");
	Printer::Print("Epsilon  | Scenario   | MAE        | MRE (%)   | RMSE       | Max Error");
	Printer::Print("---------|------------|------------|-----------|------------|-----------");
	auto summ_res = con.Query("SELECT epsilon, scenario, mae, mre, rmse, max_error FROM dp_sum_wrapper_summary ORDER BY epsilon, scenario;");
	if (!summ_res->HasError()) {
		for (idx_t i = 0; i < summ_res->RowCount(); i++) {
			char line[256];
			snprintf(line, sizeof(line), "%8.4f | %-10s | %10.2f | %9.2f | %10.2f | %9.2f", summ_res->GetValue(0,i).GetValue<double>(), summ_res->GetValue(1,i).ToString().c_str(), summ_res->GetValue(2,i).GetValue<double>(), summ_res->GetValue(3,i).GetValue<double>(), summ_res->GetValue(4,i).GetValue<double>(), summ_res->GetValue(5,i).GetValue<double>());
			Printer::Print(line);
		}
	}
	Printer::Print("");
	Printer::Print("Temp tables created for inspection:");
	Printer::Print("  - dp_sum_wrapper_runs (per epsilon & repeat)");
	Printer::Print("  - dp_sum_wrapper_summary (aggregated)");
	// CSV export
	if (export_csv) {
		if (csv_path.empty()) csv_path = "dp_sum_wrapper.csv";
		std::stringstream cp;
		cp << "COPY (SELECT * FROM dp_sum_wrapper_summary) TO '" << csv_path << "' (FORMAT CSV, HEADER TRUE, DELIMITER '" << csv_delim << "');";
		auto copy_res = con.Query(cp.str());
		if (copy_res->HasError()) {
			Printer::Print("CSV export failed: " + copy_res->GetError());
		} else {
			Printer::Print("CSV exported to " + csv_path);
		}
	}
}

} // namespace duckdb

