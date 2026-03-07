# db_bigquery.R — BigQuery backend.
# Defines the same public function names as db.R so run_scrape.R works
# identically regardless of which file it sources.

library(bigrquery)
library(DBI)
library(jsonlite)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

.bq_cfg <- function() config::get()$bigquery

.fq <- function(table) {
  cfg <- .bq_cfg()
  sprintf("`%s.%s.%s`", cfg$project, cfg$dataset, table)
}

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

get_con <- function() {
  cfg <- .bq_cfg()
  # bq_auth() with no arguments uses Application Default Credentials:
  #   - locally: GOOGLE_APPLICATION_CREDENTIALS env var pointing to SA key JSON
  #   - Cloud Run: the attached service account (automatic)
  bq_auth()
  dbConnect(
    bigquery(),
    project = cfg$project,
    dataset = cfg$dataset,
    billing = cfg$project
  )
}

get_con_bq        <- get_con   # alias
get_db_connection <- get_con   # backward-compat alias

# ---------------------------------------------------------------------------
# Schema — idempotent CREATE TABLE IF NOT EXISTS
# ---------------------------------------------------------------------------

ensure_tables <- function(con) {
  cfg  <- .bq_cfg()
  proj <- cfg$project
  ds   <- cfg$dataset

  ddls <- list(

    # schedule
    sprintf(
      "CREATE TABLE IF NOT EXISTS `%s.%s.schedule` (
        game_id        STRING  NOT NULL,
        game_type      STRING,
        year           INT64   NOT NULL,
        date           DATE    NOT NULL,
        level          STRING  NOT NULL,
        venue_id       INT64,
        team_id_away   INT64,
        team_id_home   INT64,
        team_name_away STRING,
        team_name_home STRING,
        score_away     INT64,
        score_home     INT64,
        scraped_at     TIMESTAMP NOT NULL
      ) CLUSTER BY year, date",
      proj, ds
    ),

    # statsapi_event
    sprintf(
      "CREATE TABLE IF NOT EXISTS `%s.%s.statsapi_event` (
        game_id       STRING NOT NULL,
        event_index   INT64  NOT NULL,
        year          INT64,
        inning        INT64,
        half_inning   STRING,
        batter_id     INT64,
        bat_side      STRING,
        pitcher_id    INT64,
        pitch_hand    STRING,
        event         STRING,
        is_out        BOOL,
        runs_on_event INT64,
        fielder_2_id  INT64,
        fielder_3_id  INT64,
        fielder_4_id  INT64,
        fielder_5_id  INT64,
        fielder_6_id  INT64,
        fielder_7_id  INT64,
        fielder_8_id  INT64,
        fielder_9_id  INT64,
        fielder_10_id INT64,
        scraped_at    TIMESTAMP NOT NULL
      ) CLUSTER BY game_id, year",
      proj, ds
    ),

    # statsapi_pitch
    sprintf(
      "CREATE TABLE IF NOT EXISTS `%s.%s.statsapi_pitch` (
        play_id             STRING,
        game_id             STRING  NOT NULL,
        event_index         INT64   NOT NULL,
        play_index          INT64   NOT NULL,
        year                INT64,
        pitch_number        INT64,
        outs                INT64,
        balls               INT64,
        strikes             INT64,
        description         STRING,
        pitch_type          STRING,
        ax                  NUMERIC,
        ay                  NUMERIC,
        az                  NUMERIC,
        vx0                 NUMERIC,
        vy0                 NUMERIC,
        vz0                 NUMERIC,
        x0                  NUMERIC,
        z0                  NUMERIC,
        extension           NUMERIC,
        spin_rate           NUMERIC,
        strike_zone_top     NUMERIC,
        strike_zone_bottom  NUMERIC,
        launch_speed        NUMERIC,
        launch_angle        NUMERIC,
        hit_coord_x         NUMERIC,
        hit_coord_y         NUMERIC,
        scraped_at          TIMESTAMP NOT NULL
      ) CLUSTER BY game_id, year",
      proj, ds
    ),

    # statsapi_play
    sprintf(
      "CREATE TABLE IF NOT EXISTS `%s.%s.statsapi_play` (
        play_id             STRING,
        game_id             STRING NOT NULL,
        event_index         INT64  NOT NULL,
        play_index          INT64  NOT NULL,
        year                INT64,
        pitch_number        INT64,
        pre_runner_1b_id    INT64,
        pre_runner_2b_id    INT64,
        pre_runner_3b_id    INT64,
        post_runner_1b_id   INT64,
        post_runner_2b_id   INT64,
        post_runner_3b_id   INT64,
        pre_outs            INT64,
        pre_balls           INT64,
        pre_strikes         INT64,
        post_outs           INT64,
        post_balls          INT64,
        post_strikes        INT64,
        runs_on_play        INT64,
        type                STRING,
        is_pickoff          BOOL,
        is_stolen_base      BOOL,
        is_caught_stealing  BOOL,
        scraped_at          TIMESTAMP NOT NULL
      ) CLUSTER BY game_id, year",
      proj, ds
    ),

    # statcast — volatile 90+ column schema stored as JSON
    sprintf(
      "CREATE TABLE IF NOT EXISTS `%s.%s.statcast` (
        game_pk       INT64 NOT NULL,
        at_bat_number INT64 NOT NULL,
        pitch_number  INT64 NOT NULL,
        year          INT64,
        data          JSON,
        scraped_at    TIMESTAMP NOT NULL
      ) CLUSTER BY year, game_pk",
      proj, ds
    ),

    # player
    sprintf(
      "CREATE TABLE IF NOT EXISTS `%s.%s.player` (
        player_id        INT64 NOT NULL,
        name_full        STRING,
        name_last        STRING,
        name_first       STRING,
        birth_date       DATE,
        birth_country    STRING,
        bat_side         STRING,
        throw_hand       STRING,
        height           STRING,
        weight           INT64,
        primary_position STRING,
        scraped_at       TIMESTAMP NOT NULL
      ) CLUSTER BY player_id",
      proj, ds
    ),

    # season_summary — hitting/pitching have different column sets, stored as JSON
    sprintf(
      "CREATE TABLE IF NOT EXISTS `%s.%s.season_summary` (
        year          INT64  NOT NULL,
        level         STRING NOT NULL,
        player_id     INT64  NOT NULL,
        position_type STRING NOT NULL,
        data          JSON,
        scraped_at    TIMESTAMP NOT NULL
      ) CLUSTER BY year, player_id",
      proj, ds
    ),

    # scrape_log — id generated in R as unix timestamp integer
    sprintf(
      "CREATE TABLE IF NOT EXISTS `%s.%s.scrape_log` (
        id              INT64  NOT NULL,
        data_source     STRING NOT NULL,
        level           STRING,
        date_start      DATE,
        date_end        DATE,
        status          STRING NOT NULL,
        games_total     INT64,
        games_success   INT64,
        games_failed    INT64,
        failed_game_ids STRING,
        error_message   STRING,
        started_at      TIMESTAMP NOT NULL,
        completed_at    TIMESTAMP
      ) CLUSTER BY status, data_source",
      proj, ds
    )
  )

  for (ddl in ddls) dbExecute(con, ddl)
  log_info("BigQuery tables ensured")
  ensure_views(con)
}

# ---------------------------------------------------------------------------
# Views — v_statcast extracts typed columns from the statcast JSON.
# BigQuery materialized views do not support LEFT JOIN, so we use a
# regular view. Looker Studio queries the view directly.
# ---------------------------------------------------------------------------

ensure_views <- function(con) {
  cfg  <- .bq_cfg()
  proj <- cfg$project
  ds   <- cfg$dataset

  sql <- sprintf(
    "CREATE OR REPLACE VIEW `%1$s.%2$s.v_statcast` AS
    SELECT
      s.game_pk,
      s.at_bat_number,
      s.pitch_number,
      s.year,
      s.scraped_at,

      -- Game context
      CAST(JSON_VALUE(s.data, '$.game_date')    AS DATE)   AS game_date,
      JSON_VALUE(s.data, '$.game_type')                    AS game_type,
      JSON_VALUE(s.data, '$.home_team')                    AS home_team,
      JSON_VALUE(s.data, '$.away_team')                    AS away_team,
      CAST(JSON_VALUE(s.data, '$.event_index')  AS INT64)  AS event_index,

      -- Players
      CAST(JSON_VALUE(s.data, '$.batter_id')    AS INT64)  AS batter_id,
      CAST(JSON_VALUE(s.data, '$.pitcher_id')   AS INT64)  AS pitcher_id,
      JSON_VALUE(s.data, '$.batter_name')                  AS batter_name,
      pp.name_full                                         AS pitcher_name,
      JSON_VALUE(s.data, '$.bat_side')                     AS bat_side,
      JSON_VALUE(s.data, '$.pitch_hand')                   AS pitch_hand,
      CAST(JSON_VALUE(s.data, '$.age_pit')      AS FLOAT64) AS age_pit,
      CAST(JSON_VALUE(s.data, '$.age_bat')      AS FLOAT64) AS age_bat,

      -- Pitch mechanics
      JSON_VALUE(s.data, '$.pitch_type')                   AS pitch_type,
      JSON_VALUE(s.data, '$.pitch_name')                   AS pitch_name,
      CAST(JSON_VALUE(s.data, '$.arm_angle')          AS FLOAT64) AS arm_angle,
      CAST(JSON_VALUE(s.data, '$.release_speed')      AS FLOAT64) AS release_speed,
      CAST(JSON_VALUE(s.data, '$.effective_speed')    AS FLOAT64) AS effective_speed,
      CAST(JSON_VALUE(s.data, '$.release_pos_x')      AS FLOAT64) AS release_pos_x,
      CAST(JSON_VALUE(s.data, '$.release_pos_y')      AS FLOAT64) AS release_pos_y,
      CAST(JSON_VALUE(s.data, '$.release_pos_z')      AS FLOAT64) AS release_pos_z,
      CAST(JSON_VALUE(s.data, '$.extension')          AS FLOAT64) AS extension,
      CAST(JSON_VALUE(s.data, '$.release_spin_rate')  AS FLOAT64) AS release_spin_rate,
      CAST(JSON_VALUE(s.data, '$.spin_axis')          AS FLOAT64) AS spin_axis,
      CAST(JSON_VALUE(s.data, '$.pfx_x')              AS FLOAT64) AS pfx_x,
      CAST(JSON_VALUE(s.data, '$.pfx_z')              AS FLOAT64) AS pfx_z,
      CAST(JSON_VALUE(s.data, '$.plate_x')            AS FLOAT64) AS plate_x,
      CAST(JSON_VALUE(s.data, '$.plate_z')            AS FLOAT64) AS plate_z,
      CAST(JSON_VALUE(s.data, '$.ax')                 AS FLOAT64) AS ax,
      CAST(JSON_VALUE(s.data, '$.ay')                 AS FLOAT64) AS ay,
      CAST(JSON_VALUE(s.data, '$.az')                 AS FLOAT64) AS az,
      CAST(JSON_VALUE(s.data, '$.vx0')                AS FLOAT64) AS vx0,
      CAST(JSON_VALUE(s.data, '$.vy0')                AS FLOAT64) AS vy0,
      CAST(JSON_VALUE(s.data, '$.vz0')                AS FLOAT64) AS vz0,
      CAST(JSON_VALUE(s.data, '$.strike_zone_top')    AS FLOAT64) AS strike_zone_top,
      CAST(JSON_VALUE(s.data, '$.strike_zone_bottom') AS FLOAT64) AS strike_zone_bottom,
      CAST(JSON_VALUE(s.data, '$.zone')               AS INT64)   AS zone,
      CAST(JSON_VALUE(s.data, '$.api_break_z_with_gravity') AS FLOAT64) AS api_break_z_with_gravity,
      CAST(JSON_VALUE(s.data, '$.api_break_x_arm')          AS FLOAT64) AS api_break_x_arm,
      CAST(JSON_VALUE(s.data, '$.api_break_x_batter_in')    AS FLOAT64) AS api_break_x_batter_in,

      -- Swing tracking
      CAST(JSON_VALUE(s.data, '$.bat_speed')     AS FLOAT64) AS bat_speed,
      CAST(JSON_VALUE(s.data, '$.swing_length')  AS FLOAT64) AS swing_length,
      CAST(JSON_VALUE(s.data, '$.attack_angle')  AS FLOAT64) AS attack_angle,
      CAST(JSON_VALUE(s.data, '$.hyper_speed')   AS FLOAT64) AS hyper_speed,

      -- Batted ball
      CAST(JSON_VALUE(s.data, '$.launch_speed')    AS FLOAT64) AS launch_speed,
      CAST(JSON_VALUE(s.data, '$.launch_angle')    AS FLOAT64) AS launch_angle,
      CAST(JSON_VALUE(s.data, '$.hit_coord_x')     AS FLOAT64) AS hit_coord_x,
      CAST(JSON_VALUE(s.data, '$.hit_coord_y')     AS FLOAT64) AS hit_coord_y,
      CAST(JSON_VALUE(s.data, '$.hit_distance_sc') AS FLOAT64) AS hit_distance_sc,
      JSON_VALUE(s.data, '$.bb_type')                          AS bb_type,
      CAST(JSON_VALUE(s.data, '$.hit_location')    AS INT64)   AS hit_location,
      JSON_VALUE(s.data, '$.type')                             AS type,

      -- Situation
      CAST(JSON_VALUE(s.data, '$.inning')       AS INT64)  AS inning,
      JSON_VALUE(s.data, '$.half_inning')                   AS half_inning,
      CAST(JSON_VALUE(s.data, '$.outs')         AS INT64)  AS outs,
      CAST(JSON_VALUE(s.data, '$.balls')        AS INT64)  AS balls,
      CAST(JSON_VALUE(s.data, '$.strikes')      AS INT64)  AS strikes,
      JSON_VALUE(s.data, '$.if_fielding_alignment') AS if_fielding_alignment,
      JSON_VALUE(s.data, '$.of_fielding_alignment') AS of_fielding_alignment,

      -- Outcome
      JSON_VALUE(s.data, '$.description') AS description,
      JSON_VALUE(s.data, '$.events')      AS events,
      JSON_VALUE(s.data, '$.des')         AS des,

      -- Scoring
      CAST(JSON_VALUE(s.data, '$.home_score')       AS INT64) AS home_score,
      CAST(JSON_VALUE(s.data, '$.away_score')       AS INT64) AS away_score,
      CAST(JSON_VALUE(s.data, '$.post_home_score')  AS INT64) AS post_home_score,
      CAST(JSON_VALUE(s.data, '$.post_away_score')  AS INT64) AS post_away_score,

      -- Win expectancy / run value
      CAST(JSON_VALUE(s.data, '$.expected_woba')    AS FLOAT64) AS expected_woba,
      CAST(JSON_VALUE(s.data, '$.woba_value')       AS FLOAT64) AS woba_value,
      CAST(JSON_VALUE(s.data, '$.woba_denom')       AS FLOAT64) AS woba_denom,
      CAST(JSON_VALUE(s.data, '$.delta_run_exp')    AS FLOAT64) AS delta_run_exp,
      CAST(JSON_VALUE(s.data, '$.delta_home_win_exp') AS FLOAT64) AS delta_home_win_exp,
      CAST(JSON_VALUE(s.data, '$.home_win_exp')     AS FLOAT64) AS home_win_exp,
      CAST(JSON_VALUE(s.data, '$.bat_win_exp')      AS FLOAT64) AS bat_win_exp

    FROM `%1$s.%2$s.statcast` s
    LEFT JOIN `%1$s.%2$s.player` pp
      ON pp.player_id = CAST(JSON_VALUE(s.data, '$.pitcher_id') AS INT64)",
    proj, ds
  )

  dbExecute(con, sql)
  log_info("BigQuery views ensured")
}

# ---------------------------------------------------------------------------
# refresh_views — BigQuery views are always live; nothing to refresh.
# ---------------------------------------------------------------------------

refresh_views <- function(con) {
  log_info("BigQuery views are live SQL — no refresh needed")
  invisible(NULL)
}

# ---------------------------------------------------------------------------
# upsert_dataframe — MERGE INTO target USING temp_table ON pk_cols
# ---------------------------------------------------------------------------

upsert_dataframe <- function(con, table_name, df, pk_cols) {
  if (is.null(df) || nrow(df) == 0) {
    log_info("No rows to upsert into {table_name}")
    return(invisible(0L))
  }

  cfg  <- .bq_cfg()
  proj <- cfg$project
  ds   <- cfg$dataset

  # Discover target schema via INFORMATION_SCHEMA
  type_info <- dbGetQuery(con, sprintf(
    "SELECT column_name, data_type
     FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
     WHERE table_name = '%s'",
    proj, ds, table_name
  ))
  type_map  <- setNames(type_info$data_type, type_info$column_name)
  json_cols <- type_info$column_name[type_info$data_type == "JSON"]

  target_cols <- type_info$column_name
  auto_cols   <- c("scraped_at")
  send_cols   <- setdiff(intersect(names(df), target_cols), auto_cols)

  if (length(send_cols) == 0) {
    stop("No matching columns between df and BigQuery table '", table_name, "'")
  }

  df_send <- df[, send_cols, drop = FALSE]

  # Coerce R types to match BigQuery target types
  for (col in send_cols) {
    bq_type <- type_map[[col]]
    if (is.null(bq_type)) next
    df_send[[col]] <- switch(bq_type,
      "INT64"     = as.integer(df_send[[col]]),
      "FLOAT64"   = as.numeric(df_send[[col]]),
      "NUMERIC"   = as.numeric(df_send[[col]]),
      "BOOL"      = as.logical(df_send[[col]]),
      "DATE"      = as.Date(df_send[[col]]),
      "TIMESTAMP" = as.POSIXct(df_send[[col]]),
      "JSON"      = as.character(df_send[[col]]),  # stored as STRING in temp table
      df_send[[col]]
    )
  }

  # Write to a uniquely-named temp table in the same dataset
  tmp <- paste0("tmp_", table_name, "_", Sys.getpid())
  dbWriteTable(con, tmp, df_send, overwrite = TRUE)
  on.exit(tryCatch(dbRemoveTable(con, tmp), error = function(e) NULL), add = TRUE)

  # Build MERGE SQL
  non_pk_cols <- setdiff(send_cols, pk_cols)

  src_expr <- function(col) {
    if (col %in% json_cols) sprintf("PARSE_JSON(S.`%s`)", col)
    else sprintf("S.`%s`", col)
  }

  on_clause    <- paste(sprintf("T.`%s` = S.`%s`", pk_cols, pk_cols), collapse = " AND ")
  ins_cols_sql <- paste(c(sprintf("`%s`", send_cols), "`scraped_at`"),         collapse = ", ")
  ins_vals_sql <- paste(c(sapply(send_cols, src_expr), "CURRENT_TIMESTAMP()"), collapse = ", ")

  target_fq <- sprintf("`%s.%s.%s`", proj, ds, table_name)
  tmp_fq    <- sprintf("`%s.%s.%s`", proj, ds, tmp)

  merge_sql <- if (length(non_pk_cols) > 0) {
    upd_sets <- paste(
      c(sprintf("T.`%s` = %s", non_pk_cols, sapply(non_pk_cols, src_expr)),
        "T.`scraped_at` = CURRENT_TIMESTAMP()"),
      collapse = ", "
    )
    sprintf(
      "MERGE %s AS T\n  USING %s AS S ON %s\n  WHEN MATCHED THEN UPDATE SET %s\n  WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
      target_fq, tmp_fq, on_clause, upd_sets, ins_cols_sql, ins_vals_sql
    )
  } else {
    sprintf(
      "MERGE %s AS T\n  USING %s AS S ON %s\n  WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
      target_fq, tmp_fq, on_clause, ins_cols_sql, ins_vals_sql
    )
  }

  n <- tryCatch(
    dbExecute(con, merge_sql),
    error = function(e) {
      log_warn("MERGE failed for {table_name}: {conditionMessage(e)}")
      0L
    }
  )

  log_info("Upserted {n} rows into {table_name}")
  invisible(n)
}

# ---------------------------------------------------------------------------
# pack_jsonb — collapse non-key columns into a 'data' JSON string.
# Identical logic to db.R; duplicated here so db_bigquery.R is self-contained.
# ---------------------------------------------------------------------------

pack_jsonb <- function(df, pk_cols, extra_cols = NULL) {
  keep_cols <- unique(c(pk_cols, extra_cols))
  keep_cols <- intersect(keep_cols, names(df))
  data_cols <- setdiff(names(df), keep_cols)

  result <- df[, keep_cols, drop = FALSE]
  if (length(data_cols) > 0) {
    result$data <- apply(df[, data_cols, drop = FALSE], 1, function(row) {
      toJSON(as.list(row), auto_unbox = TRUE, na = "null")
    })
  }
  result
}

# ---------------------------------------------------------------------------
# Scrape-log helpers
# ---------------------------------------------------------------------------

log_scrape_start <- function(con, data_source, level = NA,
                             date_start = NA, date_end = NA) {
  # Generate a unique integer ID from the current unix timestamp.
  # At most one scrape per second, so collisions are not a concern.
  log_id <- as.integer(as.numeric(Sys.time()))

  df <- data.frame(
    id              = log_id,
    data_source     = data_source,
    level           = if (is.na(level))      NA_character_ else as.character(level),
    date_start      = if (is.na(date_start)) as.Date(NA)  else as.Date(date_start),
    date_end        = if (is.na(date_end))   as.Date(NA)  else as.Date(date_end),
    status          = "running",
    games_total     = NA_integer_,
    games_success   = NA_integer_,
    games_failed    = NA_integer_,
    failed_game_ids = NA_character_,
    error_message   = NA_character_,
    started_at      = Sys.time(),
    completed_at    = as.POSIXct(NA),
    stringsAsFactors = FALSE
  )

  dbWriteTable(con, "scrape_log", df, append = TRUE)
  log_id
}

log_scrape_finish <- function(con, log_id, status = "success",
                              games_total = NA, games_success = NA,
                              games_failed = 0L, failed_game_ids = NULL,
                              error_message = NULL) {
  cfg  <- .bq_cfg()
  proj <- cfg$project
  ds   <- cfg$dataset

  fgi <- if (!is.null(failed_game_ids) && length(failed_game_ids) > 0) {
    paste(failed_game_ids, collapse = ",")
  } else {
    NA_character_
  }
  err <- if (is.null(error_message)) NA_character_ else error_message

  null_or <- function(x, cast = "") {
    if (is.na(x)) "NULL"
    else if (nchar(cast) > 0) sprintf("CAST('%s' AS %s)", x, cast)
    else sprintf("'%s'", gsub("'", "\\'", as.character(x), fixed = TRUE))
  }

  sql <- sprintf(
    "UPDATE `%s.%s.scrape_log`
     SET status          = '%s',
         completed_at    = CURRENT_TIMESTAMP(),
         games_total     = %s,
         games_success   = %s,
         games_failed    = %s,
         failed_game_ids = %s,
         error_message   = %s
     WHERE id = %s",
    proj, ds,
    status,
    if (is.na(games_total))   "NULL" else as.integer(games_total),
    if (is.na(games_success)) "NULL" else as.integer(games_success),
    if (is.na(games_failed))  "NULL" else as.integer(games_failed),
    null_or(fgi),
    null_or(err),
    log_id
  )

  dbExecute(con, sql)
}

is_already_scraped <- function(con, data_source, level, date_start, date_end) {
  cfg  <- .bq_cfg()
  proj <- cfg$project
  ds   <- cfg$dataset

  sql <- sprintf(
    "SELECT COUNT(*) AS n
     FROM `%s.%s.scrape_log`
     WHERE data_source = '%s'
       AND level       = '%s'
       AND date_start  = '%s'
       AND date_end    = '%s'
       AND status      = 'success'",
    proj, ds,
    data_source, as.character(level),
    as.character(date_start), as.character(date_end)
  )

  res <- dbGetQuery(con, sql)
  res$n > 0
}

get_failed_log <- function(con, log_id) {
  cfg  <- .bq_cfg()
  proj <- cfg$project
  ds   <- cfg$dataset

  dbGetQuery(con, sprintf(
    "SELECT * FROM `%s.%s.scrape_log` WHERE id = %s",
    proj, ds, log_id
  ))
}
