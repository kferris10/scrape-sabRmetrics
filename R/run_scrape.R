# run_scrape.R — CLI entry point: parses args, dispatches

library(dotenv)
load_dot_env()  # loads .env from working directory into Sys.setenv()

library(optparse)

source("R/utils.R")

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
  make_option("--game-types", type = "character", default = NULL,
    help = "Comma-separated game types to scrape (overrides config). E.g. R,S,D,L,W"),
  make_option("--no-parallel", action = "store_true", default = FALSE,
    help = "Disable parallel processing"),
  make_option("--sources", type = "character", default = NULL,
    help = "Comma-separated scrapers to run: schedule,statsapi,statcast (default: all)"),
  make_option("--backend", type = "character", default = NULL,
    help = "Database backend: postgres (default) or bigquery. Overrides SCRAPE_BACKEND env var.")
)

opt <- parse_args(OptionParser(option_list = option_list))

# --- Load database backend ---
backend <- opt$backend %||% Sys.getenv("SCRAPE_BACKEND", unset = "postgres")
if (backend == "bigquery") {
  source("R/db_bigquery.R")
} else {
  source("R/db.R")
}

source("R/scrape_schedule.R")
source("R/scrape_statsapi.R")
source("R/scrape_baseballsavant.R")
source("R/scrape_players.R")
source("R/scrape_season_summary.R")

# --- Setup ---
setup_logging()
log_info("Starting scrape: mode={opt$mode}")

if (is.null(opt$mode)) {
  stop("--mode is required. Options: daily, backfill, players, season-summary, retry")
}

# Parse levels override
levels <- if (!is.null(opt$levels)) strsplit(opt$levels, ",")[[1]] else NULL

# Parse game_types override
game_types <- if (!is.null(opt[["game-types"]])) strsplit(opt[["game-types"]], ",")[[1]] else NULL

# Parse sources filter (default: all three)
all_sources <- c("schedule", "statsapi", "statcast")
sources <- if (!is.null(opt$sources)) strsplit(opt$sources, ",")[[1]] else all_sources
unknown_sources <- setdiff(sources, all_sources)
if (length(unknown_sources) > 0) stop("Unknown source(s): ", paste(unknown_sources, collapse = ", "))

# Connect to DB
con <- get_con()
on.exit(dbDisconnect(con), add = TRUE)
ensure_tables(con)

cfg        <- config::get()
chunk_days <- cfg$scraping$chunk_days

# Cluster — only for backfill mode unless --no-parallel
cl <- NULL
if (!opt[["no-parallel"]] && opt$mode == "backfill") {
  cl <- make_cluster()
  on.exit(stop_cluster(cl), add = TRUE)
}

# --- Dispatch ---

if (opt$mode == "daily") {
  target_date <- if (!is.null(opt$date)) as.Date(opt$date) else yesterday()
  log_info("Daily scrape for {target_date}")

  if ("schedule" %in% sources) scrape_schedule(con, target_date, target_date, levels = levels, game_types = game_types)
  if ("statsapi" %in% sources) scrape_statsapi(con, target_date, target_date, levels = levels, game_types = game_types)
  if ("statcast" %in% sources) scrape_baseballsavant(con, target_date, target_date, game_types = game_types)

  refresh_views(con)

} else if (opt$mode == "backfill") {
  if (!is.null(opt$year)) {
    range      <- season_date_range(opt$year)
    start_date <- range$start
    end_date   <- range$end
  } else if (!is.null(opt$start) && !is.null(opt$end)) {
    start_date <- as.Date(opt$start)
    end_date   <- as.Date(opt$end)
  } else {
    stop("Backfill mode requires --year or --start and --end")
  }

  log_info("Backfill from {start_date} to {end_date}")
  chunks <- date_chunks(start_date, end_date, chunk_days)

  for (chunk in chunks) {
    log_info("Processing chunk: {chunk$start} to {chunk$end}")
    if ("schedule" %in% sources) scrape_schedule(con, chunk$start, chunk$end, levels = levels, game_types = game_types)
    if ("statsapi" %in% sources) scrape_statsapi(con, chunk$start, chunk$end, levels = levels, game_types = game_types, cl = cl)
    if ("statcast" %in% sources) scrape_baseballsavant(con, chunk$start, chunk$end, game_types = game_types, cl = cl)
  }

  refresh_views(con)

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

  log_info("Retrying scrape_log id={opt[['log-id']]}: data_source={log_entry$data_source}")

  src         <- log_entry$data_source
  retry_start <- as.Date(log_entry$date_start)
  retry_end   <- as.Date(log_entry$date_end)
  retry_level <- log_entry$level

  if (src == "schedule") {
    scrape_schedule(con, retry_start, retry_end,
                    levels = if (!is.na(retry_level)) retry_level,
                    game_types = game_types)
  } else if (src == "statsapi") {
    scrape_statsapi(con, retry_start, retry_end,
                    levels = if (!is.na(retry_level)) retry_level,
                    game_types = game_types)
  } else if (src == "statcast") {
    scrape_baseballsavant(con, retry_start, retry_end, game_types = game_types)
  } else if (src == "players") {
    year <- as.integer(format(retry_start, "%Y"))
    scrape_players(con, year = year,
                   levels = if (!is.na(retry_level)) strsplit(retry_level, ",")[[1]])
  } else if (src == "season_summary") {
    year <- as.integer(format(retry_start, "%Y"))
    scrape_season_summary(con, year = year)
  } else {
    stop("Unknown data_source in scrape_log: ", src)
  }

  refresh_views(con)

} else {
  stop("Unknown mode: ", opt$mode,
       ". Options: daily, backfill, players, season-summary, retry")
}

log_info("Scrape completed successfully")
