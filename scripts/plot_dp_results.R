#!/usr/bin/env Rscript
# Differential Privacy Benchmark Plotter
# --------------------------------------
# Usage:
#   Rscript plot_dp_results.R path/to/results.csv [output_dir]
# The script will create:
#   dp_benchmark_mae_[config].png - MAE plot with std_dev as error ribbons
# in output_dir (default: directory of input CSV).
#
# Expected CSV formats:
# 1) Benchmark summary (single epsilon): rows for scenarios with columns:
#      epsilon, num_clients, num_days, max_records_per_day, max_steps_per_record,
#      mechanism, delta, seed, target_outliers_pct, observed_outliers_pct,
#      scenario, mae, std_dev
# 2) Wrapper summary (multiple epsilons): columns:
#      epsilon, scenario, mae, std_dev, runs (optional)
# 3) Wrapper runs (if you pass dp_sum_wrapper_runs): columns:
#      epsilon, seed, repeat_idx, scenario, mae, std_dev
#    In this case we aggregate by epsilon + scenario (mean of metrics).
# 4) Client sweep: columns include num_clients along with epsilon
#    Will create 2 plots: epsilon sweep and client sweep
#
# The script auto-detects and normalizes any of these into a long data frame:
#   epsilon, scenario (friendly name), mae, std_dev.
#
# Dependencies will be installed if missing.

required_packages <- c(
  "ggplot2", "dplyr", "readr", "scales", "stringr"
)
options(repos = c(CRAN = "https://cloud.r-project.org"))
installed <- rownames(installed.packages())
for (pkg in required_packages) {
  if (!(pkg %in% installed)) {
    install.packages(pkg, dependencies = TRUE)
  }
}

suppressPackageStartupMessages({
  library(ggplot2)
  library(dplyr)
  library(readr)
  library(scales)
  library(stringr)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  stop("Provide path to results CSV. Optional second argument: output directory.")
}
input_csv <- args[1]
output_dir <- if (length(args) >= 2) args[2] else dirname(input_csv)
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# Extract configuration from filename if not in CSV
input_basename <- basename(input_csv)
config_from_filename <- list()

# Parse filename patterns like: dp_sum_clients10000_days7_maxrec5_maxsteps10000_mechlaplace_seed42_eps0.1-2_step0.1_runs3.csv
# Also detect client range patterns like: dp_sum_clients100-500_step100_...
if (grepl("clients(\\d+)-(\\d+)", input_basename)) {
  # Client sweep pattern
  matches <- regmatches(input_basename, regexec("clients(\\d+)-(\\d+)", input_basename))[[1]]
  config_from_filename$num_clients_min <- as.numeric(matches[2])
  config_from_filename$num_clients_max <- as.numeric(matches[3])
  if (grepl("step(\\d+)", input_basename)) {
    # Extract step after the client range
    step_match <- regmatches(input_basename, regexec("clients\\d+-\\d+_step(\\d+)", input_basename))[[1]]
    if (length(step_match) > 1) {
      config_from_filename$num_clients_step <- as.numeric(step_match[2])
    }
  }
} else if (grepl("clients(\\d+)", input_basename)) {
  config_from_filename$num_clients <- as.numeric(sub(".*clients(\\d+).*", "\\1", input_basename))
}
if (grepl("days(\\d+)", input_basename)) {
  config_from_filename$num_days <- as.numeric(sub(".*days(\\d+).*", "\\1", input_basename))
}
if (grepl("maxrec(\\d+)", input_basename)) {
  config_from_filename$max_records_per_day <- as.numeric(sub(".*maxrec(\\d+).*", "\\1", input_basename))
}
if (grepl("maxsteps(\\d+)", input_basename)) {
  config_from_filename$max_steps_per_record <- as.numeric(sub(".*maxsteps(\\d+).*", "\\1", input_basename))
}
if (grepl("mech([a-z]+)", input_basename)) {
  config_from_filename$mechanism <- sub(".*mech([a-z]+).*", "\\1", input_basename)
}
if (grepl("seed(\\d+)", input_basename)) {
  config_from_filename$seed <- as.numeric(sub(".*seed(\\d+).*", "\\1", input_basename))
}

# Read CSV (support large numeric precision)
raw <- suppressWarnings(readr::read_csv(input_csv, show_col_types = FALSE))

# If a legacy 'repeats' column exists (older tooling), normalize it to 'runs'
if ("repeats" %in% colnames(raw) && !("runs" %in% colnames(raw))) {
  raw <- raw %>% rename(runs = repeats)
}

# Basic sanity checks
expected_metric_cols <- c("mae", "std_dev")
missing_metrics <- setdiff(expected_metric_cols, colnames(raw))
if (length(missing_metrics) > 0) {
  stop(paste0("Missing expected metric columns: ", paste(missing_metrics, collapse = ", ")))
}

# Ensure epsilon column exists; if not, try to infer or create
if (!("epsilon" %in% colnames(raw))) {
  # Single epsilon case: infer from config.epsilon or set to NA
  raw$epsilon <- NA_real_
}

# Detect if this is a client sweep (has num_clients column with multiple distinct values)
has_client_sweep <- ("num_clients" %in% colnames(raw)) && (length(unique(raw$num_clients)) > 1)

# Detect wrapper runs vs summary vs benchmark summary
wrapper_runs <- all(c("seed", "repeat_idx") %in% colnames(raw)) && !("runs" %in% colnames(raw))
# Accept either 'runs' (preferred) or legacy 'repeats' for wrapper summary detection
wrapper_summary <- (("runs" %in% colnames(raw)) || ("repeats" %in% colnames(raw))) && !wrapper_runs
benchmark_summary <- ("num_clients" %in% colnames(raw)) && ("mechanism" %in% colnames(raw)) && !wrapper_runs

# Normalize scenario labels
if (!("scenario" %in% colnames(raw))) {
  stop("Column 'scenario' is required.")
}

# Aggregation for wrapper runs (average metrics per epsilon + scenario + num_clients if applicable)
if (wrapper_runs) {
  message("Detected dp_sum_wrapper_runs format: aggregating per epsilon + scenario.")
  if (has_client_sweep) {
    data <- raw %>%
      group_by(epsilon, num_clients, scenario) %>%
      summarize(
        mae = mean(mae, na.rm = TRUE),
        std_dev = mean(std_dev, na.rm = TRUE),
        runs = n(),
        .groups = "drop"
      )
  } else {
    data <- raw %>%
      group_by(epsilon, scenario) %>%
      summarize(
        mae = mean(mae, na.rm = TRUE),
        std_dev = mean(std_dev, na.rm = TRUE),
        runs = n(),
        .groups = "drop"
      )
  }
} else {
  data <- raw
}

# If single epsilon only, still plot (epsilon on x becomes a factor ordering)
if (all(is.na(data$epsilon))) {
  warning("No epsilon values found; using row index as pseudo-epsilon.")
  data$epsilon <- seq_len(nrow(data))
}

# Friendly scenario names
scenario_map <- c(
  "raw_dp" = "Raw DP",
  "local_dp" = "Local DP",
  "global_dp" = "Global DP"
)
data <- data %>% mutate(scenario_f = dplyr::coalesce(scenario_map[scenario], scenario))

# Filter out any rows with non-positive MAE values (can't plot on log scale)
# Also ensure std_dev is non-negative
data <- data %>%
  filter(mae > 0, !is.na(mae), !is.infinite(mae)) %>%
  mutate(std_dev = pmax(0, std_dev, na.rm = TRUE))

if (nrow(data) == 0) {
  stop("No valid data points with positive MAE values to plot.")
}

# Color palette
palette_dp <- c(
  "Raw DP" = "#D73027",    # reddish
  "Local DP" = "#4575B4",  # blue
  "Global DP" = "#1A9850"   # green
)
shape_dp <- c("Raw DP" = 16, "Local DP" = 17, "Global DP" = 15)

# Helper function to format numbers without scientific notation
format_number <- function(x) {
  ifelse(x >= 1000, format(x, big.mark = ",", scientific = FALSE), as.character(x))
}

# Build configuration string for title - split into two lines
config_title_parts_line1 <- c()
config_title_parts_line2 <- c()

# Line 1: clients, days, records/day
if ("num_clients" %in% colnames(raw) && !has_client_sweep) {
  config_title_parts_line1 <- c(config_title_parts_line1, paste0(format_number(raw$num_clients[1]), " clients"))
} else if (!is.null(config_from_filename$num_clients)) {
  config_title_parts_line1 <- c(config_title_parts_line1, paste0(format_number(config_from_filename$num_clients), " clients"))
} else if (has_client_sweep) {
  client_vals <- sort(unique(data$num_clients))
  config_title_parts_line1 <- c(config_title_parts_line1, paste0(format_number(min(client_vals)), "–", format_number(max(client_vals)), " clients"))
}
if ("num_days" %in% colnames(raw)) {
  config_title_parts_line1 <- c(config_title_parts_line1, paste0(raw$num_days[1], " days"))
} else if (!is.null(config_from_filename$num_days)) {
  config_title_parts_line1 <- c(config_title_parts_line1, paste0(config_from_filename$num_days, " days"))
}
if ("max_records_per_day" %in% colnames(raw)) {
  config_title_parts_line1 <- c(config_title_parts_line1, paste0(format_number(raw$max_records_per_day[1]), " records/day"))
} else if (!is.null(config_from_filename$max_records_per_day)) {
  config_title_parts_line1 <- c(config_title_parts_line1, paste0(format_number(config_from_filename$max_records_per_day), " records/day"))
}

# Line 2: upper bound, mechanism, epsilon, seed, runs
if ("max_steps_per_record" %in% colnames(raw)) {
  config_title_parts_line2 <- c(config_title_parts_line2, paste0("upper bound ", format_number(raw$max_steps_per_record[1])))
} else if (!is.null(config_from_filename$max_steps_per_record)) {
  config_title_parts_line2 <- c(config_title_parts_line2, paste0("upper bound ", format_number(config_from_filename$max_steps_per_record)))
}
if ("mechanism" %in% colnames(raw)) {
  mech <- raw$mechanism[1]
  config_title_parts_line2 <- c(config_title_parts_line2, paste0(toupper(substring(mech, 1, 1)), substring(mech, 2), " mechanism"))
} else if (!is.null(config_from_filename$mechanism)) {
  mech <- config_from_filename$mechanism
  config_title_parts_line2 <- c(config_title_parts_line2, paste0(toupper(substring(mech, 1, 1)), substring(mech, 2), " mechanism"))
}
# epsilon range from the (possibly aggregated) data frame
eps_vals <- data$epsilon
if (!all(is.na(eps_vals))) {
  eps_min <- min(eps_vals, na.rm = TRUE)
  eps_max <- max(eps_vals, na.rm = TRUE)
  if (!is.na(eps_min) && !is.na(eps_max)) {
    if (abs(eps_min - eps_max) < .Machine$double.eps^0.5) {
      config_title_parts_line2 <- c(config_title_parts_line2, paste0("ε=", eps_min))
    } else {
      config_title_parts_line2 <- c(config_title_parts_line2, paste0("ε=", eps_min, "–", eps_max))
    }
  }
}
if (!is.null(config_from_filename$seed)) {
  config_title_parts_line2 <- c(config_title_parts_line2, paste0("seed ", config_from_filename$seed))
}
# runs (if present)
total_runs <- NA
if ("runs" %in% colnames(data)) {
  total_runs <- data$runs[1]
} else if ("runs" %in% colnames(raw)) {
  total_runs <- raw$runs[1]
}
if (!is.na(total_runs) && total_runs > 1) {
  config_title_parts_line2 <- c(config_title_parts_line2, paste0(total_runs, " runs"))
}

# Combine into two-line subtitle
config_subtitle_line1 <- if (length(config_title_parts_line1) > 0) paste(config_title_parts_line1, collapse = " • ") else ""
config_subtitle_line2 <- if (length(config_title_parts_line2) > 0) paste(config_title_parts_line2, collapse = " • ") else ""
config_subtitle <- paste(config_subtitle_line1, config_subtitle_line2, sep = "\n")

# Common theme with white background
base_theme <- theme_minimal(base_size = 14) +
  theme(
    panel.background = element_rect(fill = "white", color = NA),
    plot.background = element_rect(fill = "white", color = NA),
    legend.position = "top",
    legend.title = element_blank(),
    legend.background = element_rect(fill = "white", color = NA),
    plot.title = element_text(face = "bold", hjust = 0.5, size = 16),
    plot.subtitle = element_text(hjust = 0.5, size = 11, color = "gray30", lineheight = 1.2),
    axis.title.x = element_text(margin = margin(t = 6)),
    axis.title.y = element_text(margin = margin(r = 6)),
    panel.grid.major = element_line(color = "gray90"),
    panel.grid.minor = element_line(color = "gray95")
  )

# Helper function to create MAE plot
create_mae_plot <- function(plot_data, x_var, x_label, title_suffix, is_continuous = TRUE, is_client_axis = FALSE) {
  # Use absolute value for lower bound to handle cases where std_dev > mae
  plot_data <- plot_data %>%
    mutate(
      ymin = abs(mae - std_dev),
      ymax = mae + std_dev
    )

  p <- ggplot(plot_data, aes(x = .data[[x_var]], y = mae, color = scenario_f, fill = scenario_f, shape = scenario_f)) +
    geom_ribbon(aes(ymin = ymin, ymax = ymax), alpha = 0.2, color = NA) +
    geom_line(linewidth = 1.1) +
    geom_point(size = 3) +
    scale_color_manual(values = palette_dp) +
    scale_fill_manual(values = palette_dp) +
    scale_shape_manual(values = shape_dp) +
    labs(
      x = x_label,
      y = "Mean Absolute Error (MAE, log scale)",
      title = paste0("Differential Privacy Benchmark: ", title_suffix),
      subtitle = config_subtitle,
      color = "DP Strategy",
      fill = "DP Strategy",
      shape = "DP Strategy"
    ) +
    base_theme +
    scale_y_log10(labels = comma, breaks = scales::trans_breaks("log10", function(x) 10^x))

  if (is_client_axis) {
    # For client axis, use comma formatting without scientific notation
    p <- p + scale_x_continuous(labels = comma, breaks = pretty(plot_data[[x_var]]))
  } else if (is_continuous) {
    p <- p + scale_x_continuous(breaks = pretty(plot_data[[x_var]]))
  } else {
    p <- p + scale_x_continuous(breaks = plot_data[[x_var]])
  }

  return(p)
}

# Build filename parts for output
filename_parts <- c("dp_benchmark_mae")
if ("num_clients" %in% colnames(raw) && !has_client_sweep) {
  filename_parts <- c(filename_parts, paste0(raw$num_clients[1], "clients"))
} else if (!is.null(config_from_filename$num_clients)) {
  filename_parts <- c(filename_parts, paste0(config_from_filename$num_clients, "clients"))
} else if (has_client_sweep) {
  client_vals <- sort(unique(data$num_clients))
  filename_parts <- c(filename_parts, paste0(min(client_vals), "-", max(client_vals), "clients"))
}
if ("num_days" %in% colnames(raw)) {
  filename_parts <- c(filename_parts, paste0(raw$num_days[1], "days"))
} else if (!is.null(config_from_filename$num_days)) {
  filename_parts <- c(filename_parts, paste0(config_from_filename$num_days, "days"))
}
if ("max_records_per_day" %in% colnames(raw)) {
  filename_parts <- c(filename_parts, paste0(raw$max_records_per_day[1], "rec"))
} else if (!is.null(config_from_filename$max_records_per_day)) {
  filename_parts <- c(filename_parts, paste0(config_from_filename$max_records_per_day, "rec"))
}
if ("max_steps_per_record" %in% colnames(raw)) {
  filename_parts <- c(filename_parts, paste0(raw$max_steps_per_record[1], "steps"))
} else if (!is.null(config_from_filename$max_steps_per_record)) {
  filename_parts <- c(filename_parts, paste0(config_from_filename$max_steps_per_record, "steps"))
}
if ("mechanism" %in% colnames(raw)) {
  filename_parts <- c(filename_parts, tolower(raw$mechanism[1]))
} else if (!is.null(config_from_filename$mechanism)) {
  filename_parts <- c(filename_parts, tolower(config_from_filename$mechanism))
}
if (!is.null(config_from_filename$seed)) {
  filename_parts <- c(filename_parts, paste0("seed", config_from_filename$seed))
}
# epsilon range
if (!all(is.na(eps_vals))) {
  eps_min <- min(eps_vals, na.rm = TRUE)
  eps_max <- max(eps_vals, na.rm = TRUE)
  if (!is.na(eps_min) && !is.na(eps_max)) {
    if (abs(eps_min - eps_max) < .Machine$double.eps^0.5) {
      filename_parts <- c(filename_parts, paste0("eps", format(eps_min, scientific = FALSE)))
    } else {
      filename_parts <- c(filename_parts, paste0("eps", format(eps_min, scientific = FALSE), "-", format(eps_max, scientific = FALSE)))
    }
  }
}
if (!is.na(total_runs) && total_runs > 1) {
  filename_parts <- c(filename_parts, paste0(total_runs, "runs"))
}

descriptive_filename <- paste(filename_parts, collapse = "_")
descriptive_filename <- gsub("\\s+", "_", descriptive_filename)  # replace spaces
descriptive_filename <- gsub("[^A-Za-z0-9._-]", "", descriptive_filename)  # sanitize

# Decide which plots to create based on data structure
if (has_client_sweep) {
  message("Detected client sweep: creating 2 plots (epsilon sweep and client sweep)")

  # Find median values for slicing
  epsilon_vals <- sort(unique(data$epsilon))
  client_vals <- sort(unique(data$num_clients))
  median_epsilon <- epsilon_vals[ceiling(length(epsilon_vals) / 2)]
  median_clients <- client_vals[ceiling(length(client_vals) / 2)]

  message(paste0("Using median epsilon = ", median_epsilon, " for client sweep plot"))
  message(paste0("Using median clients = ", format_number(median_clients), " for epsilon sweep plot"))

  # Plot 1: Epsilon sweep (at median client count)
  data_epsilon <- data %>%
    filter(num_clients == median_clients)

  is_continuous <- (sum(!is.na(data_epsilon$epsilon)) > 1)
  plot_epsilon <- create_mae_plot(
    data_epsilon,
    "epsilon",
    "Epsilon (ε)",
    paste0("Mean Absolute Error vs Epsilon (at ", format_number(median_clients), " clients)"),
    is_continuous,
    FALSE
  )

  epsilon_filename <- file.path(output_dir, paste0(descriptive_filename, "_epsilon.png"))
  ggsave(epsilon_filename, plot_epsilon, width = 10, height = 6, dpi = 300, bg = "white")
  message("Saved: ", epsilon_filename)

  # Plot 2: Client sweep (at median epsilon)
  data_clients <- data %>%
    filter(epsilon == median_epsilon)

  plot_clients <- create_mae_plot(
    data_clients,
    "num_clients",
    "Number of Clients",
    paste0("Mean Absolute Error vs Clients (at ε=", median_epsilon, ")"),
    TRUE,
    TRUE  # Enable special formatting for client axis
  )

  clients_filename <- file.path(output_dir, paste0(descriptive_filename, "_clients.png"))
  ggsave(clients_filename, plot_clients, width = 10, height = 6, dpi = 300, bg = "white")
  message("Saved: ", clients_filename)

} else {
  # Single plot: epsilon sweep only
  is_continuous <- (sum(!is.na(data$epsilon)) > 1)
  plot_mae <- create_mae_plot(
    data,
    "epsilon",
    if (is_continuous) "Epsilon (ε)" else "Epsilon (index)",
    "Mean Absolute Error",
    is_continuous,
    FALSE
  )

  # Save MAE plot
  mae_filename <- file.path(output_dir, paste0(descriptive_filename, ".png"))
  ggsave(mae_filename, plot_mae, width = 10, height = 6, dpi = 300, bg = "white")
  message("Saved: ", mae_filename)
}

message("Plotting complete. Output directory: ", output_dir)

# End of script
