# run_scrape.R — CLI entry point: parses args, dispatches

library(optparse)

source("R/utils.R")
source("R/db.R")
source("R/scrape_schedule.R")
source("R/scrape_statsapi.R")
source("R/scrape_baseballsavant.R")
source("R/scrape_players.R")
source("R/scrape_season_summary.R")

# --- CLI options ---
option_list <- list(
  make_option("--mode", type = "character", default = NULL,
    help = "Scrape mode: daily, backfill, players, season-summary, retry"),
  make_option("--date", type = "character", default = NULL,
    help = "Date for daily mode (YYYY-MM-DD). Defaults to yesterday."),
  make_option("--start", type = "character", default = NULL,
    help = "Start date for backfill mode (YYYY-MM-DD)"),
  make_option("--end", type = "character", default = NULL,
    help = "End date for backfill mode (YYYY-MM-DD)"),
  make_option("--year", type = "integer", default = NULL,
    help = "Year for backfill/players/season-summary mode"),
  make_option("--log-id", type = "integer", default = NULL,
    help = "scrape_log ID for retry mode"),
  make_option("--levels", type = "character", default = NULL,
    help = "Comma-separated levels to scrape (overrides config)"),
  make_option("--no-parallel", action = "store_true", default = FALSE,
    help = "Disable parallel processing")
)

opt <- parse_args(OptionParser(option_list = option_list))

# --- Setup ---
setup_logging()
log_info("Starting scrape: mode={opt$mode}")

if (is.null(opt$mode)) {
  stop("--mode is required. Options: daily, backfill, players, season-summary, retry")
}

# Parse levels override
levels <- if (!is.null(opt$levels)) {
  strsplit(opt$levels, ",")[[1]]
} else {
  NULL
}

# Connect to DB
con <- get_db_connection()
on.exit(dbDisconnect(con), add = TRUE)
ensure_tables(con)

# Cluster for parallel modes
cl <- NULL
if (!opt[["no-parallel"]] && opt$mode == "backfill") {
  cl <- make_cluster()
  on.exit(stop_cluster(cl), add = TRUE)
}

# --- Dispatch ---
cfg <- config::get()
chunk_days <- cfg$scraping$chunk_days

if (opt$mode == "daily") {
  # Daily mode: scrape a single day
  target_date <- if (!is.null(opt$date)) as.Date(opt$date) else yesterday()
  log_info("Daily scrape for {target_date}")

  scrape_schedule(con, target_date, target_date, levels = levels)
  scrape_statsapi(con, target_date, target_date, levels = levels)
  scrape_baseballsavant(con, target_date, target_date)

} else if (opt$mode == "backfill") {
  # Backfill mode: scrape a date range in chunks
  if (!is.null(opt$year)) {
    range <- season_date_range(opt$year)
    start_date <- range$start
    end_date <- range$end
  } else if (!is.null(opt$start) && !is.null(opt$end)) {
    start_date <- as.Date(opt$start)
    end_date <- as.Date(opt$end)
  } else {
    stop("Backfill mode requires --year or --start and --end")
  }

  log_info("Backfill from {start_date} to {end_date}")
  chunks <- date_chunks(start_date, end_date, chunk_days)

  for (chunk in chunks) {
    log_info("Processing chunk: {chunk$start} to {chunk$end}")
    scrape_schedule(con, chunk$start, chunk$end, levels = levels)
    scrape_statsapi(con, chunk$start, chunk$end, levels = levels, cl = cl)
    scrape_baseballsavant(con, chunk$start, chunk$end, cl = cl)
  }

} else if (opt$mode == "players") {
  year <- if (!is.null(opt$year)) opt$year else as.integer(format(Sys.Date(), "%Y"))
  scrape_players(con, year = year, levels = levels, cl = cl)

} else if (opt$mode == "season-summary") {
  if (is.null(opt$year)) stop("--year is required for season-summary mode")
  scrape_season_summary(con, year = opt$year, levels = levels, cl = cl)

} else if (opt$mode == "retry") {
  if (is.null(opt[["log-id"]])) stop("--log-id is required for retry mode")

  log_entry <- get_failed_log(con, opt[["log-id"]])
  if (nrow(log_entry) == 0) stop("No scrape_log entry found for id ", opt[["log-id"]])
  if (log_entry$status != "failed") {
    log_warn("Log entry {opt[['log-id']]} has status '{log_entry$status}', not 'failed'")
  }

  log_info("Retrying scrape_log id={opt[['log-id']]}: mode={log_entry$mode}")

  retry_mode <- log_entry$mode
  retry_start <- as.Date(log_entry$start_date)
  retry_end <- as.Date(log_entry$end_date)
  retry_level <- log_entry$level

  if (retry_mode == "schedule") {
    scrape_schedule(con, retry_start, retry_end,
                    levels = if (!is.na(retry_level)) retry_level)
  } else if (retry_mode == "statsapi") {
    scrape_statsapi(con, retry_start, retry_end,
                    levels = if (!is.na(retry_level)) retry_level)
  } else if (retry_mode == "statcast") {
    scrape_baseballsavant(con, retry_start, retry_end)
  } else if (retry_mode == "players") {
    year <- as.integer(format(retry_start, "%Y"))
    scrape_players(con, year = year,
                   levels = if (!is.na(retry_level)) strsplit(retry_level, ",")[[1]])
  } else if (retry_mode == "season_summary") {
    year <- as.integer(format(retry_start, "%Y"))
    scrape_season_summary(con, year = year)
  } else {
    stop("Unknown retry mode: ", retry_mode)
  }

} else {
  stop("Unknown mode: ", opt$mode,
       ". Options: daily, backfill, players, season-summary, retry")
}

log_info("Scrape completed successfully")
