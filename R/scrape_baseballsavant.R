# scrape_baseballsavant.R — Statcast pitch data (MLB only)

library(sabRmetrics)

source("R/db.R")
source("R/utils.R")

scrape_baseballsavant <- function(con, start_date, end_date,
                                  game_types = NULL, cl = NULL) {
  cfg <- config::get()
  if (is.null(game_types)) game_types <- cfg$scraping$game_types

  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)

  if (is_already_scraped(con, "statcast", "MLB", start_date, end_date)) {
    log_info("Statcast already scraped for {start_date} to {end_date}, skipping")
    return(0)
  }

  log_id <- log_scrape_start(con, "statcast", "MLB", start_date, end_date)

  tryCatch({
    log_info("Downloading Baseball Savant: {start_date} to {end_date}")
    df <- download_baseballsavant(
      start_date = start_date,
      end_date = end_date,
      game_type = game_types,
      cl = cl
    )

    if (is.null(df) || nrow(df) == 0) {
      log_info("No Statcast data for {start_date} to {end_date}")
      log_scrape_finish(con, log_id, status = "success", rows_written = 0)
      return(0)
    }

    # Pack everything except PKs into JSONB
    pk_cols <- c("game_id", "at_bat_number", "pitch_number")

    # Ensure PK columns exist
    if (!all(pk_cols %in% names(df))) {
      stop("Statcast data missing expected PK columns: ",
           paste(setdiff(pk_cols, names(df)), collapse = ", "))
    }

    packed <- pack_jsonb(df, pk_cols)
    n <- upsert_dataframe(con, "statcast", packed, pk_cols)

    log_scrape_finish(con, log_id, status = "success", rows_written = n)
    log_info("Statcast scrape complete: {n} rows")
    n
  }, error = function(e) {
    log_error("Statcast scrape failed: {e$message}")
    log_scrape_finish(con, log_id, status = "failed",
                      error_message = e$message)
    0
  })
}
