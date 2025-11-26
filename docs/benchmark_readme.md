DP Sum Benchmark: Design, Assumptions, and Usage
=================================================

This document explains the DP Sum Benchmark: what it measures, design choices, assumptions, formulas, and usage.

Overview
--------
We compare three ways to add differential privacy (DP) noise when computing per-day totals from client data:
- Raw DP: add noise to each individual record, then aggregate.
- Local DP (Client): aggregate per client/day, then add noise once to that total, then aggregate across clients.
- Global DP (Final): aggregate across all clients to a per-day total, then add noise once to the final daily total.

Under a single privacy budget epsilon and record-level adjacency, Global is most accurate, Local intermediate, Raw least accurate.

Assumptions
-----------
1. Data Generation: records sampled uniformly with optional outliers.
2. Independence: records and noise draws independent.
3. Adjacency: record-level (change one record). Sensitivity of any sum = X.
4. Privacy Budget: single epsilon used for splits and global release.
5. Mechanisms: Laplace or Gaussian (needs delta).
6. Composition: simple uniform splits (no advanced composition yet).

Epsilon Allocation
------------------
Let X = max steps per record, Y = max records per client/day, Z = days.
- Raw per record: epsilon_record = epsilon / (Z * Y)
- Local per client/day: epsilon_local_day = epsilon / Z
- Global per day: epsilon (full per group)

**Important**: Global DP uses **per-query privacy semantics**:
- Each output group (day) gets noise with full epsilon
- Total privacy cost for one query = Z × epsilon
- This prioritizes utility (less noise per result) over strict budget enforcement
- If running the query multiple times, multiply accordingly

Laplace Scales
--------------
Sensitivity S = X.
- b_record = X / epsilon_record = Z * Y * X / epsilon
- b_local = X / epsilon_local_day = Z * X / epsilon
- b_global = X / epsilon (applied to each group independently)

Gaussian Std Dev
----------------
With delta and S = X;
- sigma_record = factor * b_record
- sigma_local = factor * b_local
- sigma_global = factor * b_global (applied to each group independently)
Where factor = sqrt(2 ln(1.25/delta)).

Metrics
-------
We report two metrics for each DP strategy:
- **MAE (Mean Absolute Error)**: Average of |noisy_sum - true_sum| across all days
- **Std Dev (Standard Deviation)**: Standard deviation of the absolute errors across days

The MAE gives the average error magnitude, while std_dev indicates the variability/consistency of the noise across different days.

Fairness Diagnostics
--------------------
We recompute per-release epsilons and display: raw_per_record, local_per_day, global.

Usage
-----
Example:
```sql
PRAGMA dp_sum_benchmark(num_clients=3, max_steps=1000, max_records_per_day=5, num_days=7, epsilon=1.0, mechanism='laplace', seed=42);
```

Wrapper sweep:
```sql
PRAGMA dp_sum_wrapper(epsilon_min=0.5, epsilon_max=2.0, epsilon_step=0.5, runs=3, num_clients=5, max_steps=1000, max_records_per_day=10, num_days=7, mechanism='gaussian', delta=1e-6, fairness=true);
```

Visualization
-------------
The R plotting script (scripts/plot_dp_results.R) generates a single plot:
- **dp_error_mae.png**: MAE on log scale with std_dev shown as shaded error ribbons around each line

This visualization makes it easy to see both the average error and its variability across epsilon values.

Limitations & Future Work
-------------------------
- Only record-level adjacency implemented.
- Single epsilon budget (no independent global epsilon parameter).
- No advanced composition or privacy accounting yet.
- Outlier handling simplistic.

Quick Reference
---------------
| Approach | Releases per client/day | Epsilon per release | Sensitivity | Laplace scale |
|----------|-------------------------|---------------------|-------------|---------------|
| Raw      | Y records               | epsilon/(Z*Y)       | X           | Z*Y*X/epsilon |
| Local    | 1 aggregate             | epsilon/Z           | X           | Z*X/epsilon   |
| Global   | 1 final daily total     | epsilon             | X           | X/epsilon     |

FAQ
---
Q: Why only MAE and std_dev? A: These two metrics provide a complete picture: MAE shows average error magnitude, std_dev shows consistency. Other metrics (MRE, RMSE, max error) were redundant.

Q: Why not different epsilons for global vs local? A: We enforce a single budget to simplify interpretation; consider weighted splits or advanced composition if you need differentiated guarantees.

Q: Negative noisy totals? A: Allowed; noise is unbiased. Clamp externally if needed.

