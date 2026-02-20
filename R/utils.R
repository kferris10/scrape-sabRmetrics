# utils.R — Logging, date helpers, parallel cluster setup

library(logger)
library(parallel)

# --- Logging ---
setup_logging <- function(level = NULL) {
  cfg <- tryCatch(config::get(), error = function(e) NULL)

  log_level <- level %||% (cfg$logging$level %||% "INFO")
  log_file  <- cfg$logging$file %||% NULL

  log_threshold(log_level)
  log_layout(layout_glue_generator(
    format = "{time} [{level}] {msg}"
  ))

  if (!is.null(log_file)) {
    dir.create(dirname(log_file), showWarnings = FALSE, recursive = TRUE)
    log_appender(appender_tee(log_file))
  }
}

# Null-coalescing operator
`%||%` <- function(a, b) if (!is.null(a)) a else b

# --- Date helpers ---
date_chunks <- function(start_date, end_date, chunk_days = 7) {
  start_date <- as.Date(start_date)
  end_date   <- as.Date(end_date)
  chunks  <- list()
  current <- start_date
  while (current <= end_date) {
    chunk_end <- min(current + chunk_days - 1, end_date)
    chunks    <- c(chunks, list(list(start = current, end = chunk_end)))
    current   <- chunk_end + 1
  }
  chunks
}

yesterday <- function() {
  Sys.Date() - 1
}

season_date_range <- function(year) {
  # Approximate MLB season: late Feb (spring training) through Oct 31
  list(
    start = as.Date(paste0(year, "-02-20")),
    end   = as.Date(paste0(year, "-10-31"))
  )
}

# --- Parallel cluster ---
make_cluster <- function(cores = NULL) {
  if (is.null(cores)) {
    cfg   <- config::get()
    cores <- cfg$scraping$parallel_cores
  }
  cores <- min(cores, detectCores() - 1, na.rm = TRUE)
  if (cores <= 1) return(NULL)
  log_info("Starting parallel cluster with {cores} cores")
  makeCluster(cores)
}

stop_cluster <- function(cl) {
  if (!is.null(cl)) {
    stopCluster(cl)
    log_info("Parallel cluster stopped")
  }
}
