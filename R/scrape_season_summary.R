# scrape_season_summary.R — Season-level hitting/pitching stats

library(sabRmetrics)

source("R/db.R")
source("R/utils.R")

scrape_season_summary <- function(con, year, levels = NULL,
                                  game_types = NULL, cl = NULL) {
  cfg <- config::get()
  if (is.null(levels)) levels <- cfg$scraping$levels
  if (is.null(game_types)) game_types <- cfg$scraping$game_types

  total_rows <- 0

  for (pos in c("hitting", "pitching")) {
    for (gt in game_types) {
      log_id <- log_scrape_start(con, "season_summary",
                                  paste(pos, gt, sep = "_"),
                                  as.Date(paste0(year, "-01-01")),
                                  as.Date(paste0(year, "-12-31")))

      tryCatch({
        log_info("Downloading season summary: {pos} {gt} {year}")
        df <- download_season_summary(
          year = year,
          level = levels,
          position = pos,
          game_type = gt,
          cl = cl
        )

        if (is.null(df) || nrow(df) == 0) {
          log_info("No season summary data for {pos} {gt} {year}")
          log_scrape_finish(con, log_id, status = "success", rows_written = 0)
          next
        }

        # Add position_type and game_type columns for the PK
        df$position_type <- pos
        df$game_type <- gt

        pk_cols <- c("year", "level", "player_id", "position_type", "game_type")
        packed <- pack_jsonb(df, pk_cols)
        n <- upsert_dataframe(con, "season_summary", packed, pk_cols)
        total_rows <- total_rows + n

        log_scrape_finish(con, log_id, status = "success", rows_written = n)
      }, error = function(e) {
        log_error("Season summary scrape failed for {pos} {gt}: {e$message}")
        log_scrape_finish(con, log_id, status = "failed",
                          error_message = e$message)
      })
    }
  }

  log_info("Season summary scrape complete: {total_rows} total rows")
  total_rows
}
