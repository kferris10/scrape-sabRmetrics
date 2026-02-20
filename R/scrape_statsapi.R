# scrape_statsapi.R — Event/pitch/play-level data scraper

library(sabRmetrics)

source("R/db.R")
source("R/utils.R")

# ---------------------------------------------------------------------------
# Monkey-patch sabRmetrics::extract_game to handle minor league games where
# the Stats API does not return pitch type codes. The upstream function builds
# a tibble with `pitch_type = play_data$details$type$code`; when that path is
# NULL the column is silently dropped, and the subsequent dplyr::select() that
# names `pitch_type` explicitly throws an error for every minor-league game.
#
# Fix: coerce the NULL to NA_character_ so the column always exists.
# We write back into the package namespace so download_statsapi() picks it up.
# ---------------------------------------------------------------------------
.patch_extract_game <- function() {
  ns  <- asNamespace("sabRmetrics")
  rn  <- get("replace_null", envir = ns, inherits = FALSE)

  # Capture package-internal helpers used by extract_game
  tbo_event <- get("track_base_out_by_event", envir = ns, inherits = FALSE)
  tbo_play  <- get("track_base_out_by_play",  envir = ns, inherits = FALSE)
  efl       <- get("extract_fielding_lineup",  envir = ns, inherits = FALSE)

  patched <- function(game_id) {
    event_endpoint  <- glue::glue("https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live")
    event_json      <- jsonlite::fromJSON(event_endpoint)
    lineup_endpoint <- glue::glue("https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore")
    lineup_json     <- jsonlite::fromJSON(lineup_endpoint)

    event_data <- event_json$liveData$plays$allPlays
    event_base_out_state <- tbo_event(event_data)

    event_without_fielder_id <- tibble::tibble(
      game_id        = game_id,
      event_index    = event_data$about$atBatIndex,
      inning         = event_data$about$inning,
      half_inning    = event_data$about$halfInning,
      batter_id      = event_data$matchup$batter$id,
      bat_side       = event_data$matchup$batSide$code,
      pitcher_id     = event_data$matchup$pitcher$id,
      pitch_hand     = event_data$matchup$pitchHand$code,
      event          = event_data$result$event,
      is_out         = event_data$result$isOut,
      runs_on_event  = sapply(event_data$runners,
        FUN = function(x) sum(dplyr::coalesce(x$movement$end, "") == "score")
      )
    ) |>
      dplyr::left_join(event_base_out_state, by = "event_index")

    play_data <- do.call(dplyr::bind_rows, args = event_data$playEvents)

    play_all <- tibble::tibble(
      play_id             = play_data$playId,
      action_play_id      = rn(play_data$actionPlayId),
      game_id             = game_id,
      event_index         = rep(event_data$about$atBatIndex,
                                times = sapply(event_data$playEvents, nrow)),
      play_index          = play_data$index,
      pitch_number        = play_data$pitchNumber,
      type                = play_data$type,
      is_substitution     = rn(play_data$isSubstitution),
      player_id           = play_data$player$id,
      position            = rn(play_data$position$code),
      outs                = play_data$count$outs,
      post_balls          = play_data$count$balls,
      post_strikes        = play_data$count$strikes,
      post_disengagements = rn(play_data$details$disengagementNum, replacement = 0),
      description         = play_data$details$description,
      event               = play_data$details$event,
      from_catcher        = rn(play_data$details$fromCatcher),
      runner_going        = rn(play_data$details$runnerGoing),
      is_out              = play_data$details$isOut,
      # KEY FIX: coerce NULL → NA for all tracking/pitch-type columns absent in minor leagues.
      # Minor league Stats API does not return pitchData coordinates, spin rate, hit data,
      # or pitch type codes. Without rn(), a NULL result silently drops the column from
      # the tibble, causing dplyr::select() to fail for every game at that level.
      pitch_type          = rn(play_data$details$type$code),
      ax                  = rn(play_data$pitchData$coordinates$aX),
      ay                  = rn(play_data$pitchData$coordinates$aY),
      az                  = rn(play_data$pitchData$coordinates$aZ),
      vx0                 = rn(play_data$pitchData$coordinates$vX0),
      vy0                 = rn(play_data$pitchData$coordinates$vY0),
      vz0                 = rn(play_data$pitchData$coordinates$vZ0),
      x0                  = rn(play_data$pitchData$coordinates$x0),
      z0                  = rn(play_data$pitchData$coordinates$z0),
      extension           = rn(play_data$pitchData$extension),
      spin_rate           = rn(play_data$pitchData$breaks$spinRate),
      strike_zone_top     = rn(play_data$pitchData$strikeZoneTop),
      strike_zone_bottom  = rn(play_data$pitchData$strikeZoneBottom),
      launch_speed        = rn(play_data$hitData$launchSpeed),
      launch_angle        = rn(play_data$hitData$launchAngle),
      hit_coord_x         = rn(play_data$hitData$coordinates$coordX),
      hit_coord_y         = rn(play_data$hitData$coordinates$coordY),
    ) |>
      dplyr::group_by(game_id, event_index) |>
      tidyr::fill(post_disengagements, .direction = "down") |>
      tidyr::replace_na(list(post_disengagements = 0)) |>
      dplyr::mutate(
        pre_balls          = dplyr::coalesce(dplyr::lag(post_balls,          1), 0),
        pre_strikes        = dplyr::coalesce(dplyr::lag(post_strikes,        1), 0),
        pre_disengagements = dplyr::coalesce(dplyr::lag(post_disengagements, 1), 0),
      ) |>
      dplyr::ungroup()

    pitch <- play_all |>
      dplyr::filter(type == "pitch") |>
      dplyr::select(
        play_id, game_id, event_index, play_index, pitch_number,
        outs, balls = pre_balls, strikes = pre_strikes,
        description, pitch_type, ax, ay, az, vx0, vy0, vz0, x0, z0,
        extension, spin_rate, strike_zone_top, strike_zone_bottom,
        launch_speed, launch_angle, hit_coord_x, hit_coord_y
      )

    fielder_credit <- lapply(
      X   = event_data$runners,
      FUN = function(x) do.call(dplyr::bind_rows, args = x$credit)
    )
    first_fielder <- do.call(dplyr::bind_rows, args = fielder_credit) |>
      tibble::add_column(
        event_index = rep(event_data$about$atBatIndex,
                          times = sapply(fielder_credit, nrow)),
        .before = 1
      ) |>
      dplyr::group_by(event_index) |>
      dplyr::slice(1) |>
      with(tibble::tibble(event_index, first_fielder = position$code))

    starting_lineup_home <- efl(players = lineup_json$teams$home$players) |>
      tibble::add_column(half_inning = "top",    .before = 1)
    starting_lineup_away <- efl(players = lineup_json$teams$away$players) |>
      tibble::add_column(half_inning = "bottom", .before = 1)
    starting_lineup <- dplyr::bind_rows(starting_lineup_home, starting_lineup_away)

    lineup_by_event <- event_without_fielder_id |>
      dplyr::select(event_index, half_inning) |>
      dplyr::left_join(starting_lineup, by = "half_inning", relationship = "many-to-many") |>
      dplyr::group_by(half_inning) |>
      dplyr::mutate(player_id = ifelse(event_index == min(event_index), player_id, NA)) |>
      dplyr::ungroup()

    substitution <- play_all |>
      dplyr::filter(is_substitution, position %in% 2:10) |>
      dplyr::group_by(event_index, position) |>
      dplyr::arrange(play_index) |>
      dplyr::slice(1) |>
      dplyr::ungroup() |>
      dplyr::transmute(event_index, position = as.integer(position), player_id)

    lineup_by_event_wide <- lineup_by_event |>
      dplyr::left_join(substitution,
        by     = c("event_index", "position"),
        suffix = c("_before", "_after")
      ) |>
      dplyr::mutate(player_id = dplyr::coalesce(player_id_before, player_id_after)) |>
      dplyr::group_by(half_inning, position) |>
      tidyr::fill(player_id, .direction = "down") |>
      dplyr::ungroup() |>
      dplyr::transmute(event_index, name = glue::glue("fielder_{position}_id"), player_id) |>
      tidyr::pivot_wider(names_from = name, values_from = player_id)

    event <- event_without_fielder_id |>
      dplyr::left_join(first_fielder,       by = "event_index") |>
      dplyr::left_join(lineup_by_event_wide, by = "event_index")

    play_base_out_state <- tbo_play(event_data)

    play <- play_all |>
      dplyr::filter(!is.na(play_id)) |>
      dplyr::left_join(play_base_out_state, by = "play_id") |>
      tidyr::replace_na(
        list(is_stolen_base = FALSE, is_caught_stealing = FALSE,
             is_defensive_indiff = FALSE)
      ) |>
      dplyr::select(
        play_id, game_id, event_index, play_index, pitch_number,
        pre_runner_1b_id, pre_runner_2b_id, pre_runner_3b_id,
        pre_outs, pre_balls, pre_strikes, pre_disengagements,
        runs_on_play,
        post_runner_1b_id, post_runner_2b_id, post_runner_3b_id,
        post_outs, post_balls, post_strikes, post_disengagements,
        type, runner_going, from_catcher,
        is_pickoff, is_pickoff_error, is_stolen_base, is_caught_stealing,
        is_defensive_indiff
      )

    list(event = event, pitch = pitch, play = play)
  }

  assignInNamespace("extract_game", patched, ns)
}

# Apply the patch once at load time
.patch_extract_game()

# Flat columns to keep for each sub-table.
# upsert_dataframe() will intersect these with actual target schema columns,
# so columns absent from the downloaded dataframe are silently skipped.
.EVENT_COLS <- c(
  "game_id", "event_index", "year", "inning", "half_inning",
  "batter_id", "bat_side", "pitcher_id", "pitch_hand",
  "event", "is_out", "runs_on_event",
  "fielder_2_id", "fielder_3_id", "fielder_4_id", "fielder_5_id",
  "fielder_6_id", "fielder_7_id", "fielder_8_id", "fielder_9_id",
  "fielder_10_id"
)

.PITCH_COLS <- c(
  "play_id", "game_id", "event_index", "play_index", "year",
  "pitch_number", "outs", "balls", "strikes", "description", "pitch_type",
  "ax", "ay", "az", "vx0", "vy0", "vz0", "x0", "z0",
  "extension", "spin_rate", "strike_zone_top", "strike_zone_bottom",
  "launch_speed", "launch_angle", "hit_coord_x", "hit_coord_y"
)

.PLAY_COLS <- c(
  "play_id", "game_id", "event_index", "play_index", "year",
  "pitch_number",
  "pre_runner_1b_id", "pre_runner_2b_id", "pre_runner_3b_id",
  "post_runner_1b_id", "post_runner_2b_id", "post_runner_3b_id",
  "pre_outs", "pre_balls", "pre_strikes",
  "post_outs", "post_balls", "post_strikes",
  "runs_on_play", "type",
  "is_pickoff", "is_stolen_base", "is_caught_stealing"
)

.select_cols <- function(df, cols) {
  df[, intersect(cols, names(df)), drop = FALSE]
}

scrape_statsapi <- function(con, start_date, end_date, levels = NULL,
                            game_types = NULL, cl = NULL) {
  cfg <- config::get()
  if (is.null(levels))     levels     <- cfg$scraping$levels
  if (is.null(game_types)) game_types <- cfg$scraping$game_types

  start_date <- as.Date(start_date)
  end_date   <- as.Date(end_date)
  total_rows <- 0L

  for (lvl in levels) {
    if (is_already_scraped(con, "statsapi", lvl, start_date, end_date)) {
      log_info("Statsapi already scraped for {lvl} {start_date} to {end_date}, skipping")
      next
    }

    log_id <- log_scrape_start(con, "statsapi", lvl, start_date, end_date)

    tryCatch({
      log_info("Downloading statsapi: {lvl} from {start_date} to {end_date}")
      result <- download_statsapi(
        start_date = start_date,
        end_date   = end_date,
        level      = lvl,
        game_type  = game_types,
        cl         = cl
      )

      rows <- 0L

      # event table
      if (!is.null(result$event) && nrow(result$event) > 0) {
        df_event <- .select_cols(result$event, .EVENT_COLS)
        rows <- rows + upsert_dataframe(con, "statsapi_event", df_event,
                                        c("game_id", "event_index"))
      }

      # pitch table
      if (!is.null(result$pitch) && nrow(result$pitch) > 0) {
        df_pitch <- .select_cols(result$pitch, .PITCH_COLS)
        rows <- rows + upsert_dataframe(con, "statsapi_pitch", df_pitch,
                                        c("game_id", "event_index", "play_index"))
      }

      # play table
      if (!is.null(result$play) && nrow(result$play) > 0) {
        df_play <- .select_cols(result$play, .PLAY_COLS)
        rows <- rows + upsert_dataframe(con, "statsapi_play", df_play,
                                        c("game_id", "event_index", "play_index"))
      }

      total_rows <- total_rows + rows
      log_scrape_finish(con, log_id, status = "success",
                        games_success = rows)
    }, error = function(e) {
      log_error("Statsapi scrape failed for {lvl}: {e$message}")
      log_scrape_finish(con, log_id, status = "failed",
                        error_message = e$message)
    })
  }

  log_info("Statsapi scrape complete: {total_rows} total rows")
  invisible(total_rows)
}
