# scrape_baseballsavant.R — Statcast pitch data (MLB only)

library(sabRmetrics)

source("R/db.R")
source("R/utils.R")

scrape_baseballsavant <- function(con, start_date, end_date,
                                  game_types = NULL, cl = NULL) {
  cfg <- config::get()
  if (is.null(game_types)) game_types <- cfg$scraping$game_types

  start_date <- as.Date(start_date)
  end_date   <- as.Date(end_date)

  if (is_already_scraped(con, "statcast", "MLB", start_date, end_date)) {
    log_info("Statcast already scraped for {start_date} to {end_date}, skipping")
    return(invisible(0L))
  }

  log_id <- log_scrape_start(con, "statcast", "MLB", start_date, end_date)

  tryCatch({
    log_info("Downloading Baseball Savant: {start_date} to {end_date}")
    df <- download_baseballsavant(
      start_date = start_date,
      end_date   = end_date,
      game_type  = game_types,
      cl         = cl
    )

    if (is.null(df) || nrow(df) == 0) {
      log_info("No Statcast data for {start_date} to {end_date}")
      log_scrape_finish(con, log_id, status = "success",
                        games_total = 0L, games_success = 0L)
      return(invisible(0L))
    }

    # sabRmetrics renames game_pk → game_id in the returned df; rename it back
    # so it aligns with our DB schema column name (game_pk INTEGER).
    if ("game_id" %in% names(df) && !"game_pk" %in% names(df)) {
      names(df)[names(df) == "game_id"] <- "game_pk"
    }

    pk_cols <- c("game_pk", "at_bat_number", "pitch_number")
    if (!all(pk_cols %in% names(df))) {
      stop("Statcast data missing expected PK columns: ",
           paste(setdiff(pk_cols, names(df)), collapse = ", "))
    }

    # year is already extracted by sabRmetrics (game_year → year)
    # Pack everything except PKs and year into JSONB data column
    packed <- pack_jsonb(df, pk_cols = pk_cols, extra_cols = "year")

    n <- upsert_dataframe(con, "statcast", packed,
                          pk_cols = pk_cols)

    log_scrape_finish(con, log_id, status = "success",
                      games_total = n, games_success = n)
    log_info("Statcast scrape complete: {n} rows")
    invisible(n)
  }, error = function(e) {
    log_error("Statcast scrape failed: {e$message}")
    log_scrape_finish(con, log_id, status = "failed",
                      error_message = e$message)
    invisible(0L)
  })
}
