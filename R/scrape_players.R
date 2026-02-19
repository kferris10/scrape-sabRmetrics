# scrape_players.R — Player biographical data

library(sabRmetrics)

source("R/db.R")
source("R/utils.R")

scrape_players <- function(con, year = NULL, levels = NULL, cl = NULL) {
  cfg <- config::get()
  if (is.null(year)) year <- as.integer(format(Sys.Date(), "%Y"))
  if (is.null(levels)) levels <- cfg$scraping$levels

  log_id <- log_scrape_start(con, "players", paste(levels, collapse = ","),
                              as.Date(paste0(year, "-01-01")),
                              as.Date(paste0(year, "-12-31")))

  tryCatch({
    log_info("Downloading player data for {year}")
    df <- download_player(
      year = year,
      level = levels,
      cl = cl
    )

    if (is.null(df) || nrow(df) == 0) {
      log_info("No player data for {year}")
      log_scrape_finish(con, log_id, status = "success", rows_written = 0)
      return(0)
    }

    # player_id should be a column; if it's the row index, add it
    if (!"player_id" %in% names(df) && !is.null(rownames(df))) {
      df$player_id <- as.integer(rownames(df))
    }

    packed <- pack_jsonb(df, pk_cols = c("player_id"))
    n <- upsert_dataframe(con, "player", packed, c("player_id"))

    log_scrape_finish(con, log_id, status = "success", rows_written = n)
    log_info("Player scrape complete: {n} rows")
    n
  }, error = function(e) {
    log_error("Player scrape failed: {e$message}")
    log_scrape_finish(con, log_id, status = "failed",
                      error_message = e$message)
    0
  })
}
