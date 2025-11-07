-- ============================================================================
-- DuckDB Differential Privacy Extension - SQL Examples
-- ============================================================================
-- This file contains practical examples for using DP functions on integer columns
--
-- Prerequisites:
--   1. Build the extension: make
--   2. Load in DuckDB: LOAD 'build/release/extension/duckdp/duckdp.duckdb_extension';
--   3. Run these examples!
--
-- Available Functions:
--   - dp_sum(column) - DP aggregate sum
--   - dp_laplace_noise(value, epsilon, sensitivity) - Add Laplace noise to raw values
--   - dp_gaussian_noise(value, epsilon, delta, sensitivity) - Add Gaussian noise to raw values
-- ============================================================================

-- ============================================================================
-- SETUP: Create sample data with INTEGER columns
-- ============================================================================

LOAD 'build/release/extension/duckdp/duckdp.duckdb_extension';

CREATE TABLE employees (
    id INTEGER,
    name VARCHAR,
    salary INTEGER,      -- INT column: salary in dollars
    age INTEGER          -- INT column: age in years
);

INSERT INTO employees VALUES
    (1, 'Alice', 75000, 28),
    (2, 'Bob', 82000, 32),
    (3, 'Carol', 68000, 29),
    (4, 'David', 71000, 35),
    (5, 'Eve', 95000, 40);

-- View the original data
SELECT * FROM employees;


-- ============================================================================
-- EXAMPLE 1: Basic - Add Laplace Noise to INTEGER Columns
-- ============================================================================
-- This adds noise to EACH row independently for row-level privacy
-- Must cast INTEGER to DOUBLE, then optionally round back

-- Parameters: dp_laplace_noise(value, epsilon, sensitivity)
--   epsilon: 1.0 = moderate privacy (smaller = more private, more noise)
--   sensitivity: Range of values (NOT the max value!)
--   For salary range 68k-95k: sensitivity = ~30000

SELECT
    name,
    salary as original,
    ROUND(dp_laplace_noise(salary::DOUBLE, 1.0, 30000.0))::INTEGER as private_salary
FROM employees;


-- ============================================================================
-- EXAMPLE 2: Correct Way - Generate Noise Once, Reuse It
-- ============================================================================
-- IMPORTANT: Each call to dp_laplace_noise() generates INDEPENDENT noise!
-- To see both rounded and non-rounded versions of the SAME noise, use a CTE

WITH noisy_data AS (
    SELECT
        name,
        salary as original,
        age as original_age,
        dp_laplace_noise(salary::DOUBLE, 1.0, 30000.0) as noisy_salary,
        dp_laplace_noise(age::DOUBLE, 1.0, 50.0) as noisy_age
    FROM employees
)
SELECT
    name,
    original,
    noisy_salary,
    ROUND(noisy_salary)::INTEGER as salary_rounded,
    original_age,
    ROUND(noisy_age)::INTEGER as age_rounded
FROM noisy_data;


-- ============================================================================
-- EXAMPLE 3: Gaussian Noise (Alternative Mechanism)
-- ============================================================================
-- Gaussian provides (ε,δ)-DP with better utility than Laplace
-- Parameters: dp_gaussian_noise(value, epsilon, delta, sensitivity)
--   delta: should be very small (e.g., 1e-5)

SELECT
    name,
    salary as original,
    ROUND(dp_gaussian_noise(salary::DOUBLE, 1.0, 1e-5, 30000.0))::INTEGER as private_salary
FROM employees;


-- ============================================================================
-- EXAMPLE 4: Compare Laplace vs Gaussian vs Different Epsilon
-- ============================================================================
-- Demonstrate different mechanisms and privacy levels

SELECT
    name,
    salary as original,
    ROUND(dp_laplace_noise(salary::DOUBLE, 0.5, 30000.0))::INTEGER as laplace_high_privacy,
    ROUND(dp_laplace_noise(salary::DOUBLE, 5.0, 30000.0))::INTEGER as laplace_low_privacy,
    ROUND(dp_gaussian_noise(salary::DOUBLE, 1.0, 1e-5, 30000.0))::INTEGER as gaussian
FROM employees;


-- ============================================================================
-- EXAMPLE 5: DP SUM - Aggregate with Privacy
-- ============================================================================
-- dp_sum() adds noise to aggregate result (not individual rows)
-- Compare regular SUM vs DP SUM

SELECT
    'Regular SUM' as type,
    SUM(salary) as total
FROM employees
UNION ALL
SELECT
    'DP Protected SUM (auto bounds)' as type,
    dp_sum(salary)::INTEGER as total
FROM employees;


-- ============================================================================
-- EXAMPLE 5B: DP SUM with Different Epsilon Values
-- ============================================================================
-- Show how epsilon affects noise level

SELECT 'True Sum' as config, SUM(salary) as result FROM employees
UNION ALL SELECT 'ε=0.1 (very private)', dp_sum(salary, 0.1)::INTEGER FROM employees
UNION ALL SELECT 'ε=0.5 (high privacy)', dp_sum(salary, 0.5)::INTEGER FROM employees
UNION ALL SELECT 'ε=1.0 (moderate)', dp_sum(salary, 1.0)::INTEGER FROM employees
UNION ALL SELECT 'ε=5.0 (low privacy)', dp_sum(salary, 5.0)::INTEGER FROM employees
UNION ALL SELECT 'ε=10.0 (very low privacy)', dp_sum(salary, 10.0)::INTEGER FROM employees;


-- ============================================================================
-- EXAMPLE 5C: Multiple Runs - See Noise Variation
-- ============================================================================
-- Run dp_sum multiple times to see how results vary
-- Higher epsilon = less variation between runs

WITH runs AS (
    SELECT 1 as run_num UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL
    SELECT 4 UNION ALL SELECT 5
)
SELECT
    run_num,
    (SELECT dp_sum(salary, 0.5) FROM employees)::INTEGER as epsilon_0_5,
    (SELECT dp_sum(salary, 1.0) FROM employees)::INTEGER as epsilon_1_0,
    (SELECT dp_sum(salary, 5.0) FROM employees)::INTEGER as epsilon_5_0
FROM runs;


-- ============================================================================
-- EXAMPLE 5D: Automatic Bounds vs Explicit Bounds
-- ============================================================================
-- Compare Google DP's automatic bounds inference with explicit bounds
-- Syntax: dp_sum(column, epsilon, lower, upper)

SELECT 'True Sum' as method, SUM(salary) as result FROM employees
UNION ALL
-- Automatic bounds (Google DP infers from data)
SELECT 'Auto Bounds (default)', dp_sum(salary, 1.0)::INTEGER FROM employees
UNION ALL
-- Explicit bounds - you specify the range [min, max]
SELECT 'Explicit [60k, 100k]', dp_sum(salary, 1.0, 60000.0, 100000.0)::INTEGER FROM employees
UNION ALL
-- Tight bounds - closer to actual data
SELECT 'Explicit [68k, 95k]', dp_sum(salary, 1.0, 68000.0, 95000.0)::INTEGER FROM employees;

-- Run multiple times to compare variance
WITH runs AS (
    SELECT 1 as run UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
)
SELECT
    run,
    (SELECT dp_sum(salary, 1.0) FROM employees)::INTEGER as auto_bounds,
    (SELECT dp_sum(salary, 1.0, 60000.0, 100000.0) FROM employees)::INTEGER as explicit_loose,
    (SELECT dp_sum(salary, 1.0, 68000.0, 95000.0) FROM employees)::INTEGER as explicit_tight
FROM runs;


-- ============================================================================
-- EXAMPLE 6: DP SUM with GROUP BY
-- ============================================================================
-- Apply DP sum to grouped data

CREATE TABLE departments(dept VARCHAR, salary INTEGER);
INSERT INTO departments VALUES
    ('Engineering', 75000),
    ('Engineering', 82000),
    ('Engineering', 95000),
    ('Sales', 68000),
    ('Sales', 71000);

SELECT
    dept,
    SUM(salary) as regular_sum,
    dp_sum(salary)::INTEGER as dp_sum
FROM departments
GROUP BY dept
ORDER BY dept;


-- ============================================================================
-- KEY CONCEPTS
-- ============================================================================

/*
1. INTEGER COLUMNS:
   - Cast to DOUBLE: column::DOUBLE
   - Round back: ROUND(...)::INTEGER

2. CHOOSING EPSILON:
   - 0.1-0.5: Very private (lots of noise)
   - 1.0-2.0: Moderate privacy
   - 5.0-10.0: Less private (less noise)

3. CHOOSING SENSITIVITY:
   - Use the RANGE of values, not the maximum
   - Salary range 68k-95k → sensitivity ≈ 30000
   - Age range 20-70 → sensitivity ≈ 50

4. LAPLACE vs GAUSSIAN:
   - Laplace: dp_laplace_noise(value, epsilon, sensitivity)
     Pure ε-DP, 3 parameters

   - Gaussian: dp_gaussian_noise(value, epsilon, delta, sensitivity)
     (ε,δ)-DP, better utility, 4 parameters, delta should be ~1e-5

5. RAW VALUE DP vs AGGREGATE DP:
   - dp_laplace_noise()/dp_gaussian_noise(): Noise per row (publish datasets)
   - dp_sum(): Noise on aggregate (summary statistics)

6. IMPORTANT:
   - Each call generates INDEPENDENT noise
   - Use CTE to reuse the same noisy value
   - Running query again gives DIFFERENT results (this is correct!)
*/


-- ============================================================================
-- CLEANUP
-- ============================================================================
-- DROP TABLE employees;
-- DROP TABLE departments;
