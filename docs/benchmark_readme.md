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
- Global daily total: epsilon (full)

Laplace Scales
--------------
Sensitivity S = X.
- b_record = X / epsilon_record = Z * Y * X / epsilon
- b_local = X / epsilon_local_day = Z * X / epsilon
- b_global = X / epsilon

Gaussian Std Dev
----------------
With delta and S = X;
- sigma_record = factor * b_record
- sigma_local = factor * b_local
- sigma_global = factor * b_global
Where factor = sqrt(2 ln(1.25/delta)).

Metrics
-------
MAE, MRE, RMSE, Max Error as previously defined.

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
PRAGMA dp_sum_wrapper(epsilon_min=0.5, epsilon_max=2.0, epsilon_step=0.5, seeds=3, num_clients=5, max_steps=1000, max_records_per_day=10, num_days=7, mechanism='gaussian', delta=1e-6, fairness=true);
```

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
Q: Why not different epsilons for global vs local? A: We enforce a single budget to simplify interpretation; consider weighted splits or advanced composition if you need differentiated guarantees.

Q: Negative noisy totals? A: Allowed; noise is unbiased. Clamp externally if needed.

Q: Small true totals inflate relative error? A: We guard division with a small threshold (1e-12).
