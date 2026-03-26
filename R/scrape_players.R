# scrape_players.R — Player biographical data

library(sabRmetrics)

source("R/utils.R")

# Flat columns to keep — must match the player table schema.
.PLAYER_COLS <- c(
  "player_id", "name_full", "name_last", "name_first",
  "birth_date", "birth_country", "bat_side", "throw_hand",
  "height", "weight", "primary_position"
)

scrape_players <- function(con, year = NULL, levels = NULL, cl = NULL) {
  cfg <- config::get()
  if (is.null(year))   year   <- as.integer(format(Sys.Date(), "%Y"))
  if (is.null(levels)) levels <- cfg$scraping$levels

  log_id <- log_scrape_start(
    con, "players",
    level      = paste(levels, collapse = ","),
    date_start = as.Date(paste0(year, "-01-01")),
    date_end   = as.Date(paste0(year, "-12-31"))
  )

  tryCatch({
    log_info("Downloading player data for {year}")
    df <- download_player(
      year  = year,
      level = levels,
      cl    = cl
    )

    if (is.null(df) || nrow(df) == 0) {
      log_info("No player data for {year}")
      log_scrape_finish(con, log_id, status = "success", games_total = 0L,
                        games_success = 0L)
      return(invisible(0L))
    }

    # Ensure player_id column exists (some sabRmetrics builds use row names)
    if (!"player_id" %in% names(df)) {
      df$player_id <- as.integer(rownames(df))
    }

    # Select only the flat columns we store
    df <- df[, intersect(.PLAYER_COLS, names(df)), drop = FALSE]

    n <- upsert_dataframe(con, "player", df, pk_cols = "player_id")
    log_scrape_finish(con, log_id, status = "success",
                      games_total = n, games_success = n)
    log_info("Player scrape complete: {n} rows")
    invisible(n)
  }, error = function(e) {
    log_error("Player scrape failed: {e$message}")
    log_scrape_finish(con, log_id, status = "failed",
                      error_message = e$message)
    invisible(0L)
  })
}
