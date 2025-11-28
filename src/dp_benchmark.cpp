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
#include <chrono>
#include <ctime>
#include <iomanip>
#include <fstream>
#include <filesystem>
#include <algorithm>

namespace duckdb {

// Helper: sanitize a string to be safe for filenames (keep alnum, '-', '_' and '.')
static inline string SanitizeForFilename(const string &s) {
    string out;
    out.reserve(s.size());
    for (char c : s) {
        if (('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') || c == '-' || c == '_' || c == '.') {
            out.push_back(c);
        } else if (c == ' ') {
            out.push_back('_');
        } else {
            out.push_back('_');
        }
    }
    return out;
}

// Forward declaration: FormatDoubleTrim is defined later but used by MakeCSVFilename's lambda
static inline string FormatDoubleTrim(double v);

// Build a CSV filename under `folder` from benchmark parameters. Folder must be a directory path.
static inline string MakeCSVFilename(const string &folder, const DPBenchmarkConfig &cfg, double e_min, double e_max, double e_step, idx_t runs, uint32_t seed_base, bool use_laplace) {
    // Format double without unnecessary trailing zeros (preserve dot)
    auto fmt = [](double v) {
        return FormatDoubleTrim(v);
    };
    string mech = use_laplace ? string("laplace") : string("gaussian");
    std::stringstream ss;
    // Build components similar to what the R script expects for cfg_suffix
    ss << "dp_sum_";
    ss << "clients" << cfg.num_clients;
    ss << "_days" << cfg.num_days;
    ss << "_maxrec" << cfg.max_records_per_day;
    ss << "_maxsteps" << cfg.max_steps_per_record;
    ss << "_mech" << mech;
    ss << "_seed" << seed_base;
    // Epsilon formatting: single value or range
    if (e_step <= 0.0 || fabs(e_min - e_max) < 1e-18) {
        ss << "_eps" << fmt(e_min);
    } else {
        ss << "_eps" << fmt(e_min) << "-" << fmt(e_max) << "_step" << fmt(e_step);
    }
    ss << "_runs" << (unsigned long long)runs;
    ss << ".csv";
    string fname = ss.str();
    fname = SanitizeForFilename(fname);
    std::filesystem::path p(folder);
    p /= fname;
    return p.string();
}

// Helper: format a double with up to 12 fractional digits but trim trailing zeros
static inline string FormatDoubleTrim(double v) {
    std::ostringstream ss;
    ss.setf(std::ios::fixed);
    ss.precision(12);
    ss << v;
    auto s = ss.str();
    // Trim trailing zeros and possible trailing decimal point
    auto pos = s.find('.');
    if (pos != string::npos) {
        while (!s.empty() && s.back() == '0') s.pop_back();
        if (!s.empty() && s.back() == '.') s.pop_back();
    }
    return s;
}

// Helper: current timestamp as YYYY-MM-DD HH:MM:SS
static inline string CurrentTimestamp() {
    using namespace std::chrono;
    auto now = system_clock::now();
    std::time_t t = system_clock::to_time_t(now);
    char buf[64];
    std::tm tm;
    localtime_r(&t, &tm);
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
    return string(buf);
}

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

// Helper: normalize outlier percentage (accepts 0-1 fraction or 0-100 percentage)
static inline double NormalizeOutlierPercentage(double po) {
	if (po > 1.0 && po <= 100.0) po /= 100.0;
	return std::max(0.0, std::min(1.0, po));
}

// Helper: extract folder path from csv_path (handles both file and directory paths)
static inline string GetCSVFolder(const string &csv_path) {
	if (csv_path.empty()) return ".";
	std::filesystem::path p(csv_path);
	if (p.extension() == ".csv") {
		string folder = p.parent_path().string();
		return folder.empty() ? "." : folder;
	}
	return csv_path;
}

//===--------------------------------------------------------------------===//
// Main Benchmark Implementation (SQL-only data + metrics)
//===--------------------------------------------------------------------===//
DPBenchmarkResults RunDPSumBenchmark(Connection &con, const DPBenchmarkConfig &input_cfg) {
	DPBenchmarkResults results;
	results.config = input_cfg;

	// Drop leftover tables from prior runs
	ExecOrThrow(con, "DROP TABLE IF EXISTS raw_data;");
	ExecOrThrow(con, "DROP TABLE IF EXISTS aggregated_client;");
	ExecOrThrow(con, "DROP TABLE IF EXISTS final_daily;");
	ExecOrThrow(con, "DROP TABLE IF EXISTS dp_benchmark_daily;");
	ExecOrThrow(con, "DROP TABLE IF EXISTS dp_benchmark_summary;");

	// Normalize configuration to safe minimums
	auto cfg = input_cfg;
	if (cfg.num_clients == 0) cfg.num_clients = 1;
	if (cfg.num_days == 0) cfg.num_days = 1;
	if (cfg.min_records_per_day == 0) cfg.min_records_per_day = 1;
	if (cfg.max_records_per_day == 0) cfg.max_records_per_day = 1;
	if (cfg.max_records_per_day < cfg.min_records_per_day) cfg.max_records_per_day = cfg.min_records_per_day;
	if (cfg.max_steps_per_record == 0) cfg.max_steps_per_record = 1;
	if (cfg.upper_bound <= 0.0) cfg.upper_bound = static_cast<double>(cfg.max_steps_per_record);
	if (cfg.upper_bound <= cfg.lower_bound) cfg.upper_bound = cfg.lower_bound + 1.0;
	double p_out = std::max(0.0, std::min(1.0, cfg.percentage_outliers));

	// Precompute integer bounds for the random step generator
	idx_t lb_i = static_cast<idx_t>(std::llround(std::max(0.0, std::floor(cfg.lower_bound))));
	idx_t ub_i = static_cast<idx_t>(std::llround(std::floor(cfg.upper_bound)));
	if (ub_i <= lb_i) ub_i = lb_i + 1;

	// Epsilon allocation and sensitivity calculations
	//
	// CORRECTED DP MODE DEFINITIONS:
	//
	// Raw DP: Each user's total epsilon budget is split across ALL their records
	//   - A user contributes records across num_days, each day up to max_records_per_day
	//   - Per-user total records = num_days × max_records_per_day
	//   - Epsilon per record = total_epsilon / (num_days × max_records_per_day)
	//   - Sensitivity per record = upper_bound
	//   - After adding noise to each record, we sum them up
	//
	// Local DP: Each user sends multiple reports (one per day). Each report is privatized independently.
	//   - A user contributes num_days reports (client-day aggregates)
	//   - Epsilon per report = total_epsilon / num_days
	//   - Sensitivity per report = max_records_per_day × upper_bound
	//   - Each client-day sum gets independent noise, then we sum across clients
	//
	// Global DP: Aggregate first, then add noise once per day to the global sum
	//   - Full epsilon applied to the day-level aggregate
	//   - Epsilon per day aggregate = total_epsilon (no split, single query per day)
	//   - Sensitivity = max_records_per_day × upper_bound × refresh (refresh accounts for multiple updates)

	// Raw DP: per-record epsilon (user's budget split across their records)
	// Use max_records_per_day for worst-case epsilon calculation
	double denom_records = static_cast<double>(cfg.num_days) * static_cast<double>(cfg.max_records_per_day);
	if (denom_records < 1.0) denom_records = 1.0;
	double epsilon_raw_record = cfg.epsilon / denom_records;
	if (epsilon_raw_record <= 0) epsilon_raw_record = cfg.epsilon;
	double raw_sensitivity = cfg.upper_bound;  // Sensitivity per individual record

	// Local DP: per-client-day epsilon (user's budget split across days)
	double epsilon_local_day = cfg.epsilon / static_cast<double>(cfg.num_days);
	if (epsilon_local_day <= 0) epsilon_local_day = cfg.epsilon;
	// Use max_records_per_day for worst-case sensitivity
	double local_sensitivity = static_cast<double>(cfg.max_records_per_day) * cfg.upper_bound;

	// Global DP: full epsilon per day (no split, single global aggregate per day)
	double epsilon_global_day = cfg.epsilon;
	// Use max_records_per_day for worst-case sensitivity
	double global_sensitivity = static_cast<double>(cfg.max_records_per_day) * cfg.upper_bound;

	const bool use_laplace = cfg.use_laplace;
	const string dpfn = use_laplace ? "dp_laplace_noise" : "dp_gaussian_noise";

	// Seed the SQL RNG
	{
		double seed_frac = (static_cast<double>(cfg.seed % 1000000u)) / 1000000.0;
		std::stringstream ss;
		ss.setf(std::ios::fixed);
		ss.precision(12);
		ss << "SELECT setseed(" << seed_frac << ");";
		ExecOrThrow(con, ss.str());
	}

	// Create ONE raw_data table with client_id column
	ExecOrThrow(con, "CREATE TEMP TABLE raw_data (client_id INTEGER, day INTEGER, steps BIGINT, dp_steps DOUBLE);");

	// Prepare the DP call for record-level noise
	string raw_call = DpCall(dpfn, use_laplace, epsilon_raw_record, use_laplace ? raw_sensitivity : cfg.delta, raw_sensitivity);
	auto dp_raw = raw_call;
	auto pos = dp_raw.find("%VAL%");
	dp_raw.replace(pos, 5, "CAST(steps AS DOUBLE)");

	// Generate data for all clients in ONE query using CROSS JOIN
	std::stringstream ss;
	ss.setf(std::ios::fixed);
	ss.precision(12);
	ss << "INSERT INTO raw_data\n"
	   << "WITH clients AS (SELECT range AS client_id FROM range(1, " << (cfg.num_clients + 1) << ")),\n"
	   << "     days AS (SELECT range AS day FROM range(1, " << (cfg.num_days + 1) << ")),\n"
	   << "     client_days AS (SELECT c.client_id, d.day FROM clients c CROSS JOIN days d),\n"
	   << "     per_day_counts AS (\n"
	   << "       SELECT client_id, day, " << cfg.min_records_per_day << " + CAST(FLOOR(random() * " << (cfg.max_records_per_day - cfg.min_records_per_day + 1) << ") AS BIGINT) AS num_records\n"
	   << "       FROM client_days\n"
	   << "     ),\n"
	   << "     raw AS (\n"
	   << "       SELECT p.client_id, p.day, r.range AS rec_index,\n"
	   << "              CAST(FLOOR(random() * " << (ub_i - lb_i + 1) << ") + " << lb_i << " AS BIGINT) AS base_steps,\n"
	   << "              (random() < " << p_out << ") AS is_outlier\n"
	   << "       FROM per_day_counts p\n"
	   << "       JOIN LATERAL range(1, p.num_records) r ON TRUE\n"
	   << "     ),\n"
	   << "     vals AS (\n"
	   << "       SELECT client_id, day,\n"
	   << "              CASE WHEN is_outlier THEN base_steps + GREATEST(1, CAST(" << ub_i << " AS BIGINT)) ELSE base_steps END AS steps\n"
	   << "       FROM raw\n"
	   << "     )\n"
	   << "SELECT client_id, day, steps, " << dp_raw << " AS dp_steps\n"
	   << "FROM vals;";
	ExecOrThrow(con, ss.str());

	// Compute observed outlier percentage
	{
		std::stringstream outlier_ss;
		outlier_ss << "SELECT 100.0 * AVG(CASE WHEN steps > " << ub_i << " THEN 1.0 ELSE 0.0 END) AS pct FROM raw_data;";
		auto res = con.Query(outlier_ss.str());
		if (!res->HasError() && res->RowCount() > 0) {
			results.actual_percentage_outliers = res->GetValue<double>(0, 0);
		}
	}

	// Create aggregated_client table with Local DP applied
	ExecOrThrow(con, "CREATE TEMP TABLE aggregated_client (client_id INTEGER, day INTEGER, true_sum DOUBLE, raw_dp_sum DOUBLE, local_dp_sum DOUBLE);");

	string local_call = DpCall(dpfn, use_laplace, epsilon_local_day, use_laplace ? local_sensitivity : cfg.delta, local_sensitivity);
	auto local_call_sub = local_call;
	auto pos2 = local_call_sub.find("%VAL%");
	local_call_sub.replace(pos2, 5, "CAST(SUM(steps) AS DOUBLE)");

	std::stringstream agg_ss;
	agg_ss << "INSERT INTO aggregated_client\n"
	       << "SELECT client_id, day,\n"
	       << "       CAST(SUM(steps) AS DOUBLE) AS true_sum,\n"
	       << "       SUM(CAST(dp_steps AS DOUBLE)) AS raw_dp_sum,\n"
	       << "       " << local_call_sub << " AS local_dp_sum\n"
	       << "FROM raw_data\n"
	       << "GROUP BY client_id, day\n"
	       << "ORDER BY client_id, day;";
	ExecOrThrow(con, agg_ss.str());

	// Create final_daily by aggregating across clients and applying Global DP
	ExecOrThrow(con, "CREATE TEMP TABLE final_daily (day INTEGER, true_sum DOUBLE, raw_dp_sum DOUBLE, local_dp_sum DOUBLE, global_dp_sum DOUBLE);");

	string global_call = DpCall(dpfn, use_laplace, epsilon_global_day, use_laplace ? global_sensitivity : cfg.delta, global_sensitivity);
	auto global_call_sub = global_call;
	auto posg = global_call_sub.find("%VAL%");
	global_call_sub.replace(posg, 5, "SUM(true_sum)");

	std::stringstream final_ss;
	final_ss << "INSERT INTO final_daily\n"
	         << "SELECT day,\n"
	         << "       SUM(true_sum) AS true_sum,\n"
	         << "       SUM(raw_dp_sum) AS raw_dp_sum,\n"
	         << "       SUM(local_dp_sum) AS local_dp_sum,\n"
	         << "       " << global_call_sub << " AS global_dp_sum\n"
	         << "FROM aggregated_client\n"
	         << "GROUP BY day\n"
	         << "ORDER BY day;";
	ExecOrThrow(con, final_ss.str());

	// Per-day metrics
	{
		std::stringstream ss;
		ss << "CREATE TEMP TABLE dp_benchmark_daily AS\n"
		   << "SELECT day, true_sum, raw_dp_sum, local_dp_sum, global_dp_sum,\n"
		   << "       ABS(true_sum - raw_dp_sum) AS raw_error,\n"
		   << "       ABS(true_sum - local_dp_sum) AS local_error,\n"
		   << "       ABS(true_sum - global_dp_sum) AS global_error\n"
		   << "FROM final_daily\n"
		   << "ORDER BY day;";
		ExecOrThrow(con, ss.str());
	}

	// Scenario summary
	{
		std::stringstream ss;
		ss << "CREATE TEMP TABLE dp_benchmark_summary AS\n"
		   << "WITH agg AS (\n"
		   << "  SELECT\n"
		   << "    COALESCE(AVG(raw_error), 0.0) AS raw_mae,\n"
		   << "    COALESCE(STDDEV_POP(raw_error), 0.0) AS raw_std,\n"
		   << "    COALESCE(AVG(local_error), 0.0) AS local_mae,\n"
		   << "    COALESCE(STDDEV_POP(local_error), 0.0) AS local_std,\n"
		   << "    COALESCE(AVG(global_error), 0.0) AS global_mae,\n"
		   << "    COALESCE(STDDEV_POP(global_error), 0.0) AS global_std\n"
		   << "  FROM dp_benchmark_daily\n"
		   << ")\n"
		   << "SELECT 'raw_dp' AS scenario, raw_mae AS mae, raw_std AS std_dev FROM agg\n"
		   << "UNION ALL\n"
		   << "SELECT 'local_dp', local_mae, local_std FROM agg\n"
		   << "UNION ALL\n"
		   << "SELECT 'global_dp', global_mae, global_std FROM agg;";
		ExecOrThrow(con, ss.str());
	}

	// Copy metrics into result struct
	{
		auto res = con.Query("SELECT scenario, mae, std_dev FROM dp_benchmark_summary ORDER BY scenario;");
		if (!res->HasError() && res->RowCount() >= 3) {
			for (idx_t i = 0; i < res->RowCount(); i++) {
				auto scen = res->GetValue(0, i).ToString();
				double mae = res->GetValue(1, i).GetValue<double>();
				double std_dev = res->GetValue(2, i).GetValue<double>();
				if (scen == "raw_dp") {
					results.raw_dp.mae = mae; results.raw_dp.std_dev = std_dev;
				} else if (scen == "local_dp") {
					results.local_dp.mae = mae; results.local_dp.std_dev = std_dev;
				} else if (scen == "global_dp") {
					results.global_dp.mae = mae; results.global_dp.std_dev = std_dev;
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
	if (named_params.count("min_records_per_day")) {
		config.min_records_per_day = named_params.at("min_records_per_day").GetValue<idx_t>();
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
		config.percentage_outliers = NormalizeOutlierPercentage(named_params.at("percentage_outliers").GetValue<double>());
	}

	// Run the benchmark (SQL-based)
	Connection shared_con(*context.db);
	auto results = RunDPSumBenchmark(shared_con, config);

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
	Printer::Print("Approach              | Mean Absolute Error | Std Dev");
	Printer::Print("---------------------|---------------------|-------------------");

	vector<string> approaches = {"Raw DP", "Local DP (Client)", "Global DP (Final)"};
	vector<DPBenchmarkResults::ErrorMetrics> metrics = {results.raw_dp, results.local_dp, results.global_dp};
	for (idx_t i = 0; i < approaches.size(); i++) {
		char buffer[256];
		snprintf(buffer, sizeof(buffer), "%-20s | %19.2f | %19.2f", approaches[i].c_str(), metrics[i].mae, metrics[i].std_dev);
		Printer::Print(buffer);
	}

	Printer::Print("");
	Printer::Print("Temp tables created for inspection:");
	Printer::Print("  - raw_data");
	Printer::Print("  - aggregated_client");
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
	// CSV export (optional) for single-run benchmark
	if (export_csv) {
		string folder = GetCSVFolder(csv_path);
		string out_csv = MakeCSVFilename(folder, config, config.epsilon, config.epsilon, 0.0, 1, config.seed, config.use_laplace);
		std::ofstream ofs(out_csv, std::ios::out | std::ios::trunc);
		// header
		ofs << "epsilon" << csv_delim << "num_clients" << csv_delim << "num_days" << csv_delim << "max_records_per_day" << csv_delim << "max_steps_per_record" << csv_delim << "mechanism" << csv_delim << "delta" << csv_delim << "seed" << csv_delim << "target_outliers_pct" << csv_delim << "observed_outliers_pct" << csv_delim << "scenario" << csv_delim << "mae" << csv_delim << "std_dev" << "\n";
		// write rows from dp_benchmark_summary
		auto q = shared_con.Query("SELECT scenario, mae, std_dev FROM dp_benchmark_summary ORDER BY scenario;");
		if (!q->HasError()) {
			for (idx_t i = 0; i < q->RowCount(); i++) {
				string scen = q->GetValue(0,i).ToString();
				double mae = q->GetValue(1,i).GetValue<double>();
				double std_dev = q->GetValue(2,i).GetValue<double>();
				ofs.setf(std::ios::fixed); ofs.precision(12);
				ofs << config.epsilon << csv_delim << config.num_clients << csv_delim << config.num_days << csv_delim << config.max_records_per_day << csv_delim << config.max_steps_per_record << csv_delim << (config.use_laplace ? "laplace" : "gaussian") << csv_delim << config.delta << csv_delim << config.seed << csv_delim << (config.percentage_outliers * 100.0) << csv_delim << results.actual_percentage_outliers << csv_delim << scen << csv_delim << mae << csv_delim << std_dev << "\n";
			}
		}
		ofs.close();
		// Print absolute path
		try {
			Printer::Print("Results saved in: " + std::filesystem::absolute(out_csv).string());
		} catch (...) {
			Printer::Print("Results saved in: " + out_csv);
		}
	}

}

// Wrapper pragma: sweeps epsilon range and runs per epsilon; optional CSV and fairness diagnostics.
void DPSumWrapperPragma(ClientContext &context, const FunctionParameters &parameters) {
    auto &named_params = parameters.named_parameters;
    double e_min = 0.0, e_max = 0.0, e_step = 0.0;
    idx_t runs = 1; // number of independent executions per epsilon

    // Client sweep parameters (optional)
    idx_t c_min = 0, c_max = 0, c_step = 0;
    bool sweep_clients = false;

    bool epsilon_step_exp = false;
    bool clients_step_exp = false;

    if (!named_params.count("epsilon_min") || !named_params.count("epsilon_max") || !named_params.count("epsilon_step")) {
        throw std::runtime_error("dp_sum_wrapper requires epsilon_min, epsilon_max, epsilon_step");
    }

    e_min = named_params.at("epsilon_min").GetValue<double>();
    e_max = named_params.at("epsilon_max").GetValue<double>();
    // epsilon_step may be numeric or the literal string "exp" which means exponential doubling
    if (named_params.at("epsilon_step").ToString() == "exp") {
        epsilon_step_exp = true;
    } else {
        e_step = named_params.at("epsilon_step").GetValue<double>();
    }

    // Check if client sweep parameters are provided
    if (named_params.count("num_clients_min") && named_params.count("num_clients_max") && named_params.count("num_clients_step")) {
        sweep_clients = true;
        c_min = named_params.at("num_clients_min").GetValue<idx_t>();
        c_max = named_params.at("num_clients_max").GetValue<idx_t>();
        // num_clients_step may be numeric or the literal string "exp"
        if (named_params.at("num_clients_step").ToString() == "exp") {
            clients_step_exp = true;
        } else {
            c_step = named_params.at("num_clients_step").GetValue<idx_t>();
        }

        if (!clients_step_exp && c_step <= 0) {
            throw std::runtime_error("num_clients_step must be > 0");
        }
        if (c_min <= 0 || c_max <= 0) {
            throw std::runtime_error("num_clients_min and num_clients_max must be > 0");
        }
        if (c_max < c_min) {
            throw std::runtime_error("num_clients_max must be >= num_clients_min");
        }
    }

    // Number of independent executions (runs) for each epsilon value.
    // 'runs' is the number of independent executions per epsilon.
    if (named_params.count("runs")) {
        runs = named_params.at("runs").GetValue<idx_t>();
    }

    if (!epsilon_step_exp && e_step <= 0.0) {
        throw std::runtime_error("epsilon_step must be > 0");
    }
    if (e_min <= 0.0 || e_max <= 0.0) {
        throw std::runtime_error("epsilon_min and epsilon_max must be > 0");
    }
    if (e_max < e_min) {
        throw std::runtime_error("epsilon_max must be >= epsilon_min");
    }

    // Build explicit lists of epsilon values and client counts depending on linear or exponential stepping.
    std::vector<double> eps_values;
    if (epsilon_step_exp) {
        // Exponential (doubling) steps: e_min, e_min*2, ... up to <= e_max
        double cur = e_min;
        // guard against infinite loops: require e_min > 0 already satisfied
        while (cur <= e_max * (1.0 + 1e-12)) {
            eps_values.push_back(cur);
            double next = cur * 2.0;
            if (next <= cur) break; // overflow or no progress
            cur = next;
        }
    } else {
        double span = e_max - e_min;
        double steps_exact = span / e_step;
        idx_t steps_count = (idx_t)llround(steps_exact);
        if (fabs((double)steps_count * e_step - span) > 1e-9) {
            throw std::runtime_error("(epsilon_max - epsilon_min) must be an integer multiple of epsilon_step when using linear stepping");
        }
        for (idx_t i = 0; i <= steps_count; i++) {
            eps_values.push_back(e_min + (double)i * e_step);
        }
    }

    // Build client values vector if sweeping, otherwise single value from base config
    std::vector<idx_t> client_values;
    if (sweep_clients) {
        if (clients_step_exp) {
            idx_t cur = c_min;
            while (cur <= c_max) {
                client_values.push_back(cur);
                // Doubling; guard against overflow
                if (cur > (std::numeric_limits<idx_t>::max() / 2)) break;
                idx_t next = cur * 2;
                if (next <= cur) break;
                cur = next;
            }
        } else {
            if ((c_max - c_min) % c_step != 0) {
                throw std::runtime_error("(num_clients_max - num_clients_min) must be an integer multiple of num_clients_step when using linear stepping");
            }
            idx_t client_steps_count = (c_max - c_min) / c_step;
            for (idx_t i = 0; i <= client_steps_count; i++) {
                client_values.push_back(c_min + i * c_step);
            }
        }
    } else {
        // will be filled later from base.num_clients if not sweeping
    }

    bool export_csv = false; string csv_path; string csv_delim = ","; bool fairness = false;
    if (named_params.count("export_csv")) export_csv = named_params.at("export_csv").GetValue<bool>();
    if (named_params.count("csv_path")) csv_path = named_params.at("csv_path").ToString();
    if (named_params.count("csv_delimiter")) csv_delim = named_params.at("csv_delimiter").ToString();
    if (named_params.count("fairness")) fairness = named_params.at("fairness").GetValue<bool>();

    DPBenchmarkConfig base;
    if (named_params.count("num_clients")) base.num_clients = named_params.at("num_clients").GetValue<idx_t>();
    if (named_params.count("max_steps")) base.max_steps_per_record = named_params.at("max_steps").GetValue<idx_t>();
    if (named_params.count("min_records_per_day")) base.min_records_per_day = named_params.at("min_records_per_day").GetValue<idx_t>();
    if (named_params.count("max_records_per_day")) base.max_records_per_day = named_params.at("max_records_per_day").GetValue<idx_t>();
    if (named_params.count("num_days")) base.num_days = named_params.at("num_days").GetValue<idx_t>();
    if (named_params.count("mechanism")) { auto mech = named_params.at("mechanism").ToString(); base.use_laplace = (mech == "laplace"); }
    if (named_params.count("delta")) base.delta = named_params.at("delta").GetValue<double>();
    uint32_t seed_base = base.seed; if (named_params.count("seed")) seed_base = named_params.at("seed").GetValue<uint32_t>();
    if (named_params.count("lower_bound")) base.lower_bound = named_params.at("lower_bound").GetValue<double>();
    if (named_params.count("upper_bound")) base.upper_bound = named_params.at("upper_bound").GetValue<double>();
    if (named_params.count("percentage_outliers")) {
        base.percentage_outliers = NormalizeOutlierPercentage(named_params.at("percentage_outliers").GetValue<double>());
    }

    Connection con(*context.db);
    ExecOrThrow(con, "CREATE TEMP TABLE dp_sum_wrapper_runs (epsilon DOUBLE, num_clients BIGINT, seed BIGINT, repeat_idx BIGINT, scenario VARCHAR, mae DOUBLE, std_dev DOUBLE);");

    // If export_csv is enabled, build a deterministic filename from parameters and create header.
    if (export_csv) {
        string folder = GetCSVFolder(csv_path);
        // Construct canonical filename and overwrite any existing file (start fresh)
        if (sweep_clients) {
            // Create filename with client sweep info
            std::stringstream fname_ss;
            fname_ss << "dp_sum_clients" << c_min << "-" << c_max << "_step" << (clients_step_exp ? string("exp") : std::to_string(c_step));
            fname_ss << "_days" << base.num_days << "_maxrec" << base.max_records_per_day;
            fname_ss << "_maxsteps" << base.max_steps_per_record;
            fname_ss << "_mech" << (base.use_laplace ? "laplace" : "gaussian");
            fname_ss << "_seed" << seed_base;
            fname_ss << "_eps" << FormatDoubleTrim(e_min) << "-" << FormatDoubleTrim(e_max) << "_step" << (epsilon_step_exp ? string("exp") : FormatDoubleTrim(e_step));
            fname_ss << "_runs" << runs << ".csv";
            std::filesystem::path p(folder);
            p /= fname_ss.str();
            csv_path = p.string();
        } else {
            csv_path = MakeCSVFilename(folder, base, e_min, e_max, e_step, runs, seed_base, base.use_laplace);
        }
        std::ofstream ofs(csv_path, std::ios::out | std::ios::trunc);
        ofs << "epsilon" << csv_delim << "num_clients" << csv_delim << "seed" << csv_delim << "repeat_idx" << csv_delim << "scenario" << csv_delim << "mae" << csv_delim << "std_dev" << "\n";
        ofs.close();
    }

    // Progress accounting
    idx_t total_eps = (idx_t)eps_values.size();
    idx_t total_clients = sweep_clients ? (idx_t)client_values.size() : 1;
    idx_t total_runs = total_eps * total_clients * (runs > 0 ? runs : 1);
    char hdr[512];
    if (sweep_clients) {
        snprintf(hdr, sizeof(hdr), "Starting dp_sum_wrapper: epsilons=%llu, clients=%llu, runs=%llu, total runs=%llu",
                 (unsigned long long)total_eps, (unsigned long long)total_clients, (unsigned long long)runs, (unsigned long long)total_runs);
    } else {
        snprintf(hdr, sizeof(hdr), "Starting dp_sum_wrapper: epsilons=%llu, runs=%llu, total runs=%llu",
                 (unsigned long long)total_eps, (unsigned long long)runs, (unsigned long long)total_runs);
    }
    Printer::Print(hdr);

    // Ensure at least 1 run to avoid no-op loops
    if (runs == 0) runs = 1;

    idx_t run_idx = 0;
    // Iterate runs on the outside so each run sweeps the full parameter space.
    for (idx_t rep = 0; rep < runs; rep++) {
        uint32_t run_seed = seed_base + (uint32_t)rep;

        // Iterate over client counts if sweeping
        if (sweep_clients) {
            for (idx_t ci = 0; ci < client_values.size(); ++ci) {
                idx_t num_clients_cur = client_values[ci];

                for (idx_t ei = 0; ei < eps_values.size(); ++ei) {
                    double eps_cur = eps_values[ei];
                    run_idx++;
                    // Per-run progress line
                    {
                        auto eps_str = FormatDoubleTrim(eps_cur);
                        auto ts = CurrentTimestamp();
                        std::stringstream plss;
                        plss << ts << " [Run " << (unsigned long long)(rep + 1) << "/" << (unsigned long long)runs << "] ";
                        plss << "clients=" << (unsigned long long)num_clients_cur << " ";
                        plss << "epsilon=" << eps_str << " (" << (unsigned long long)run_idx << "/" << (unsigned long long)total_runs << ")";
                        Printer::Print(plss.str());
                    }

                    DPBenchmarkConfig cfg = base;
                    cfg.epsilon = eps_cur;
                    cfg.num_clients = num_clients_cur;
                    cfg.seed = run_seed;
                    auto res = RunDPSumBenchmark(con, cfg);

                    std::stringstream ins;
                    ins.setf(std::ios::fixed); ins.precision(12);
                    idx_t repeat_idx_db = rep + 1;
                    ins << "INSERT INTO dp_sum_wrapper_runs VALUES(" << eps_cur << ", " << num_clients_cur << ", " << cfg.seed << ", " << repeat_idx_db << ", 'raw_dp', " << res.raw_dp.mae << ", " << res.raw_dp.std_dev << ");";
                    ins << "INSERT INTO dp_sum_wrapper_runs VALUES(" << eps_cur << ", " << num_clients_cur << ", " << cfg.seed << ", " << repeat_idx_db << ", 'local_dp', " << res.local_dp.mae << ", " << res.local_dp.std_dev << ");";
                    ins << "INSERT INTO dp_sum_wrapper_runs VALUES(" << eps_cur << ", " << num_clients_cur << ", " << cfg.seed << ", " << repeat_idx_db << ", 'global_dp', " << res.global_dp.mae << ", " << res.global_dp.std_dev << ");";
                    ExecOrThrow(con, ins.str());

                    // Append per-run CSV lines immediately
                    if (export_csv) {
                        std::ofstream ofs(csv_path, std::ios::app);
                        ofs.setf(std::ios::fixed);
                        ofs.precision(12);
                        ofs << eps_cur << csv_delim << num_clients_cur << csv_delim << cfg.seed << csv_delim << repeat_idx_db << csv_delim << "raw_dp" << csv_delim << res.raw_dp.mae << csv_delim << res.raw_dp.std_dev << "\n";
                        ofs << eps_cur << csv_delim << num_clients_cur << csv_delim << cfg.seed << csv_delim << repeat_idx_db << csv_delim << "local_dp" << csv_delim << res.local_dp.mae << csv_delim << res.local_dp.std_dev << "\n";
                        ofs << eps_cur << csv_delim << num_clients_cur << csv_delim << cfg.seed << csv_delim << repeat_idx_db << csv_delim << "global_dp" << csv_delim << res.global_dp.mae << csv_delim << res.global_dp.std_dev << "\n";
                        ofs.close();
                    }
                }
            }
        } else {
            idx_t num_clients_cur = base.num_clients;
            for (idx_t ei = 0; ei < eps_values.size(); ++ei) {
                double eps_cur = eps_values[ei];
                run_idx++;
                // Per-run progress line
                {
                    auto eps_str = FormatDoubleTrim(eps_cur);
                    auto ts = CurrentTimestamp();
                    std::stringstream plss;
                    plss << ts << " [Run " << (unsigned long long)(rep + 1) << "/" << (unsigned long long)runs << "] ";
                    plss << "epsilon=" << eps_str << " (" << (unsigned long long)run_idx << "/" << (unsigned long long)total_runs << ")";
                    Printer::Print(plss.str());
                }

                DPBenchmarkConfig cfg = base;
                cfg.epsilon = eps_cur;
                cfg.num_clients = num_clients_cur;
                cfg.seed = run_seed;
                auto res = RunDPSumBenchmark(con, cfg);

                std::stringstream ins;
                ins.setf(std::ios::fixed); ins.precision(12);
                idx_t repeat_idx_db = rep + 1;
                ins << "INSERT INTO dp_sum_wrapper_runs VALUES(" << eps_cur << ", " << num_clients_cur << ", " << cfg.seed << ", " << repeat_idx_db << ", 'raw_dp', " << res.raw_dp.mae << ", " << res.raw_dp.std_dev << ");";
                ins << "INSERT INTO dp_sum_wrapper_runs VALUES(" << eps_cur << ", " << num_clients_cur << ", " << cfg.seed << ", " << repeat_idx_db << ", 'local_dp', " << res.local_dp.mae << ", " << res.local_dp.std_dev << ");";
                ins << "INSERT INTO dp_sum_wrapper_runs VALUES(" << eps_cur << ", " << num_clients_cur << ", " << cfg.seed << ", " << repeat_idx_db << ", 'global_dp', " << res.global_dp.mae << ", " << res.global_dp.std_dev << ");";
                ExecOrThrow(con, ins.str());

                // Append per-run CSV lines immediately
                if (export_csv) {
                    std::ofstream ofs(csv_path, std::ios::app);
                    ofs.setf(std::ios::fixed);
                    ofs.precision(12);
                    ofs << eps_cur << csv_delim << num_clients_cur << csv_delim << cfg.seed << csv_delim << repeat_idx_db << csv_delim << "raw_dp" << csv_delim << res.raw_dp.mae << csv_delim << res.raw_dp.std_dev << "\n";
                    ofs << eps_cur << csv_delim << num_clients_cur << csv_delim << cfg.seed << csv_delim << repeat_idx_db << csv_delim << "local_dp" << csv_delim << res.local_dp.mae << csv_delim << res.local_dp.std_dev << "\n";
                    ofs << eps_cur << csv_delim << num_clients_cur << csv_delim << cfg.seed << csv_delim << repeat_idx_db << csv_delim << "global_dp" << csv_delim << res.global_dp.mae << csv_delim << res.global_dp.std_dev << "\n";
                    ofs.close();
                }
            }
        }
    }
    Printer::Print("dp_sum_wrapper: all runs complete.");

    // If wrapper export_csv was enabled, print the absolute CSV path
    if (export_csv) {
        try {
            Printer::Print("Results saved in: " + std::filesystem::absolute(csv_path).string());
        } catch (...) {
            Printer::Print("Results saved in: " + csv_path);
        }
    }

    // Create summary table - aggregate by all dimensions
    if (sweep_clients) {
        ExecOrThrow(con, "CREATE TEMP TABLE dp_sum_wrapper_summary AS SELECT epsilon, num_clients, scenario, AVG(mae) AS mae, AVG(std_dev) AS std_dev, COUNT(*)/3 AS runs FROM dp_sum_wrapper_runs GROUP BY epsilon, num_clients, scenario ORDER BY num_clients, epsilon, scenario;");
    } else {
        ExecOrThrow(con, "CREATE TEMP TABLE dp_sum_wrapper_summary AS SELECT epsilon, scenario, AVG(mae) AS mae, AVG(std_dev) AS std_dev, COUNT(*)/3 AS runs FROM dp_sum_wrapper_runs GROUP BY epsilon, scenario ORDER BY epsilon, scenario;");
    }

    Printer::Print("DP Sum Wrapper Results");
    Printer::Print("======================");
    Printer::Print("Configuration (shared except sweep parameters):");
    Printer::Print("  Epsilon Range: " + std::to_string(e_min) + " to " + std::to_string(e_max) + " step " + (epsilon_step_exp ? string("exp") : std::to_string(e_step)));
    if (sweep_clients) {
        Printer::Print("  Clients Range: " + std::to_string(c_min) + " to " + std::to_string(c_max) + " step " + (clients_step_exp ? string("exp") : std::to_string(c_step)));
    } else {
        Printer::Print("  Clients: " + std::to_string(base.num_clients));
    }
    Printer::Print("  Runs per configuration: " + std::to_string(runs));
    Printer::Print("  Days: " + std::to_string(base.num_days));
    Printer::Print("  Max Records/Day: " + std::to_string(base.max_records_per_day));
    Printer::Print("  Max Steps/Record: " + std::to_string(base.max_steps_per_record));
    Printer::Print("  Mechanism: " + string(base.use_laplace ? "Laplace" : "Gaussian"));
    if (!base.use_laplace) Printer::Print("  Delta: " + std::to_string(base.delta));
    Printer::Print("");

    if (fairness) {
        Printer::Print("Fairness Diagnostics (sample configurations):");
        // Show a few representative configs
        idx_t num_clients_sample = sweep_clients ? c_min : base.num_clients;
        // Pick up to three representative epsilon indices from the precomputed eps_values vector
        idx_t max_idx = eps_values.empty() ? 0 : (idx_t)eps_values.size() - 1;
        idx_t num_samples = std::min((idx_t)2, max_idx);
        for (idx_t step_i = 0; step_i <= num_samples; step_i++) {
            double eps_cur = eps_values[step_i];
             double denom_records = (double)base.num_days * (double)base.max_records_per_day;
             if (denom_records < 1.0) denom_records = 1.0;
             double eps_raw_record = eps_cur / denom_records;
             char fb[256];
             snprintf(fb, sizeof(fb), "  clients=%llu eps=%.6f raw_per_record=%.6f local_per_day=%.6f global=%.6f",
                      (unsigned long long)num_clients_sample, eps_cur, eps_raw_record, eps_cur / (double)base.num_days, eps_cur);
             Printer::Print(fb);
         }
         Printer::Print("");
     }

    Printer::Print("Aggregated Error Metrics (averaged across runs):");
    if (sweep_clients) {
        Printer::Print("Clients  | Epsilon  | Scenario   | MAE        | Std Dev");
        Printer::Print("---------|----------|------------|------------|-------------------");
        auto summ_res = con.Query("SELECT num_clients, epsilon, scenario, mae, std_dev FROM dp_sum_wrapper_summary ORDER BY num_clients, epsilon, scenario;");
        if (!summ_res->HasError()) {
            for (idx_t i = 0; i < summ_res->RowCount(); i++) {
                char line[256];
                snprintf(line, sizeof(line), "%8llu | %8.4f | %-10s | %10.2f | %10.2f",
                         (unsigned long long)summ_res->GetValue(0,i).GetValue<idx_t>(),
                         summ_res->GetValue(1,i).GetValue<double>(),
                         summ_res->GetValue(2,i).ToString().c_str(),
                         summ_res->GetValue(3,i).GetValue<double>(),
                         summ_res->GetValue(4,i).GetValue<double>());
                Printer::Print(line);
            }
        }
    } else {
        Printer::Print("Epsilon  | Scenario   | MAE        | Std Dev");
        Printer::Print("---------|------------|------------|-------------------");
        auto summ_res = con.Query("SELECT epsilon, scenario, mae, std_dev FROM dp_sum_wrapper_summary ORDER BY epsilon, scenario;");
        if (!summ_res->HasError()) {
            for (idx_t i = 0; i < summ_res->RowCount(); i++) {
                char line[256];
                snprintf(line, sizeof(line), "%8.4f | %-10s | %10.2f | %10.2f",
                         summ_res->GetValue(0,i).GetValue<double>(),
                         summ_res->GetValue(1,i).ToString().c_str(),
                         summ_res->GetValue(2,i).GetValue<double>(),
                         summ_res->GetValue(3,i).GetValue<double>());
                Printer::Print(line);
            }
        }
    }
    Printer::Print("");
    Printer::Print("Temp tables created for inspection:");
    Printer::Print("  - dp_sum_wrapper_runs (per epsilon & run)");
    Printer::Print("  - dp_sum_wrapper_summary (aggregated)");
}

// End of file: ensure namespace duckdb is closed
} // namespace duckdb
