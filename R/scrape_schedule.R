# scrape_schedule.R — Schedule/results scraper

library(sabRmetrics)

source("R/utils.R")

# Canonical column mapping: sabRmetrics name → schema column name.
# Only columns listed here are sent to the DB; extras are silently dropped.
.SCHEDULE_COLS <- c(
  game_id        = "game_id",
  game_type      = "game_type",
  year           = "year",
  date           = "date",
  venue_id       = "venue_id",
  team_id_away   = "team_id_away",
  team_id_home   = "team_id_home",
  team_name_away = "team_name_away",
  team_name_home = "team_name_home",
  score_away     = "score_away",
  score_home     = "score_home"
)

scrape_schedule <- function(con, start_date, end_date, levels = NULL,
                            game_types = NULL) {
  cfg <- config::get()
  if (is.null(levels))     levels     <- cfg$scraping$levels
  if (is.null(game_types)) game_types <- cfg$scraping$game_types

  start_date <- as.Date(start_date)
  end_date   <- as.Date(end_date)
  total_rows <- 0L

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
        end_date   = end_date,
        level      = lvl,
        game_type  = game_types
      )

      if (is.null(df) || nrow(df) == 0) {
        log_info("No schedule data for {lvl} {start_date} to {end_date}")
        log_scrape_finish(con, log_id, status = "success", games_total = 0L,
                          games_success = 0L)
        next
      }

      # Always stamp the level column
      df$level <- lvl

      # Rename any sabRmetrics columns that differ from schema names,
      # then select only the columns we want to store.
      for (src in names(.SCHEDULE_COLS)) {
        tgt <- .SCHEDULE_COLS[[src]]
        if (src %in% names(df) && src != tgt) {
          df[[tgt]] <- df[[src]]
        }
      }
      keep <- unique(c(unname(.SCHEDULE_COLS), "level"))
      df   <- df[, intersect(keep, names(df)), drop = FALSE]

      n <- upsert_dataframe(con, "schedule", df, pk_cols = "game_id")
      total_rows <- total_rows + n
      log_scrape_finish(con, log_id, status = "success",
                        games_total = n, games_success = n)
    }, error = function(e) {
      log_error("Schedule scrape failed for {lvl}: {e$message}")
      log_scrape_finish(con, log_id, status = "failed",
                        error_message = e$message)
    })
  }

  log_info("Schedule scrape complete: {total_rows} total rows")
  invisible(total_rows)
}
