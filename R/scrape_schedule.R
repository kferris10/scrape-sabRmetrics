# scrape_schedule.R — Schedule/results scraper

library(sabRmetrics)

source("R/db.R")
source("R/utils.R")

scrape_schedule <- function(con, start_date, end_date, levels = NULL,
                            game_types = NULL) {
  cfg <- config::get()
  if (is.null(levels)) levels <- cfg$scraping$levels
  if (is.null(game_types)) game_types <- cfg$scraping$game_types

  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)
  total_rows <- 0

  for (lvl in levels) {
    if (is_already_scraped(con, "schedule", lvl, start_date, end_date)) {
      log_info("Schedule already scraped for {lvl} {start_date} to {end_date}, skipping")
      next
    }

    log_id <- log_scrape_start(con, "schedule", lvl, start_date, end_date)

    tryCatch({
      log_info("Downloading schedule: {lvl} from {start_date} to {end_date}")
      df <- download_schedule(
        start_date = start_date,
        end_date = end_date,
        level = lvl,
        game_type = game_types
      )

      if (is.null(df) || nrow(df) == 0) {
        log_info("No schedule data for {lvl} {start_date} to {end_date}")
        log_scrape_finish(con, log_id, status = "success", rows_written = 0)
        next
      }

      # Add level column and pack remaining cols as JSONB
      df$level <- lvl
      pk_cols <- c("game_id")
      extra_cols <- c("year", "date", "level", "team_id_away", "team_id_home",
                      "venue_id")
      packed <- pack_jsonb(df, pk_cols, extra_cols)

      n <- upsert_dataframe(con, "schedule", packed, pk_cols)
      total_rows <- total_rows + n
      log_scrape_finish(con, log_id, status = "success", rows_written = n)
    }, error = function(e) {
      log_error("Schedule scrape failed for {lvl}: {e$message}")
      log_scrape_finish(con, log_id, status = "failed",
                        error_message = e$message)
    })
  }

  log_info("Schedule scrape complete: {total_rows} total rows")
  total_rows
}
