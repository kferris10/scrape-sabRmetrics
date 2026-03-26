# scrape_season_summary.R — Season-level hitting/pitching stats

library(sabRmetrics)

source("R/utils.R")

scrape_season_summary <- function(con, year, levels = NULL,
                                  game_types = NULL, cl = NULL) {
  cfg <- config::get()
  if (is.null(levels))     levels     <- cfg$scraping$levels
  if (is.null(game_types)) game_types <- cfg$scraping$game_types

  total_rows <- 0L

  for (pos in c("hitting", "pitching")) {
    # PK: (year, level, player_id, position_type) — one log per position type
    log_id <- log_scrape_start(
      con, "season_summary",
      level      = pos,
      date_start = as.Date(paste0(year, "-01-01")),
      date_end   = as.Date(paste0(year, "-12-31"))
    )

    tryCatch({
      log_info("Downloading season summary: {pos} {year}")
      df <- download_season_summary(
        year      = year,
        level     = levels,
        position  = pos,
        game_type = game_types,
        cl        = cl
      )

      if (is.null(df) || nrow(df) == 0) {
        log_info("No season summary data for {pos} {year}")
        log_scrape_finish(con, log_id, status = "success",
                          games_total = 0L, games_success = 0L)
        next
      }

      # Stamp position_type for the PK
      df$position_type <- pos

      pk_cols <- c("year", "level", "player_id", "position_type")
      packed  <- pack_jsonb(df, pk_cols)
      n       <- upsert_dataframe(con, "season_summary", packed, pk_cols)
      total_rows <- total_rows + n

      log_scrape_finish(con, log_id, status = "success",
                        games_total = n, games_success = n)
    }, error = function(e) {
      log_error("Season summary scrape failed for {pos}: {e$message}")
      log_scrape_finish(con, log_id, status = "failed",
                        error_message = e$message)
    })
  }

  log_info("Season summary scrape complete: {total_rows} total rows")
  invisible(total_rows)
}
