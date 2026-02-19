# scrape_statsapi.R — Event/pitch/play-level data scraper

library(sabRmetrics)

source("R/db.R")
source("R/utils.R")

scrape_statsapi <- function(con, start_date, end_date, levels = NULL,
                            game_types = NULL, cl = NULL) {
  cfg <- config::get()
  if (is.null(levels)) levels <- cfg$scraping$levels
  if (is.null(game_types)) game_types <- cfg$scraping$game_types

  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)
  total_rows <- 0

  for (lvl in levels) {
    if (is_already_scraped(con, "statsapi", lvl, start_date, end_date)) {
      log_info("Statsapi already scraped for {lvl} {start_date} to {end_date}, skipping")
      next
    }

    log_id <- log_scrape_start(con, "statsapi", lvl, start_date, end_date)
    failed_games <- character()

    tryCatch({
      log_info("Downloading statsapi: {lvl} from {start_date} to {end_date}")
      result <- download_statsapi(
        start_date = start_date,
        end_date = end_date,
        level = lvl,
        game_type = game_types,
        cl = cl
      )

      rows <- 0

      # Process event data
      if (!is.null(result$event) && nrow(result$event) > 0) {
        event_packed <- pack_jsonb(result$event,
          pk_cols = c("game_id", "event_index"),
          extra_cols = c("year", "inning", "half_inning", "batter_id",
                         "pitcher_id", "event"))
        n <- upsert_dataframe(con, "statsapi_event", event_packed,
                              c("game_id", "event_index"))
        rows <- rows + n
      }

      # Process pitch data
      if (!is.null(result$pitch) && nrow(result$pitch) > 0) {
        pitch_packed <- pack_jsonb(result$pitch,
          pk_cols = c("game_id", "event_index", "play_index"),
          extra_cols = c("year", "pitch_number", "pitch_type"))
        n <- upsert_dataframe(con, "statsapi_pitch", pitch_packed,
                              c("game_id", "event_index", "play_index"))
        rows <- rows + n
      }

      # Process play data
      if (!is.null(result$play) && nrow(result$play) > 0) {
        play_packed <- pack_jsonb(result$play,
          pk_cols = c("game_id", "event_index", "play_index"),
          extra_cols = c("year", "type"))
        n <- upsert_dataframe(con, "statsapi_play", play_packed,
                              c("game_id", "event_index", "play_index"))
        rows <- rows + n
      }

      total_rows <- total_rows + rows
      log_scrape_finish(con, log_id, status = "success", rows_written = rows,
                        failed_games = if (length(failed_games) > 0) failed_games)
    }, error = function(e) {
      log_error("Statsapi scrape failed for {lvl}: {e$message}")
      log_scrape_finish(con, log_id, status = "failed",
                        error_message = e$message)
    })
  }

  log_info("Statsapi scrape complete: {total_rows} total rows")
  total_rows
}
