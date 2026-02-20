# db.R — DB connection, table/view creation, upsert helpers

library(DBI)
library(RPostgres)
library(jsonlite)

get_con <- function() {
  cfg <- config::get()
  dbConnect(
    Postgres(),
    host     = cfg$database$host,
    port     = cfg$database$port,
    dbname   = cfg$database$dbname,
    user     = cfg$database$user,
    password = cfg$database$password
  )
}

# Keep alias for backward compatibility
get_db_connection <- get_con

.run_sql_file <- function(con, path) {
  sql <- paste(readLines(path, warn = FALSE), collapse = "\n")
  statements <- strsplit(sql, ";")[[1]]
  statements <- trimws(statements)
  # Drop chunks that are blank or contain only comments
  statements <- statements[nchar(gsub("(--[^\n]*)|\n", "", statements)) > 0]
  for (stmt in statements) {
    dbExecute(con, paste0(stmt, ";"))
  }
}

ensure_tables <- function(con) {
  .run_sql_file(con, "sql/schema.sql")
  log_info("Database tables ensured")
  ensure_views(con)
}

ensure_views <- function(con) {
  .run_sql_file(con, "sql/views.sql")
  log_info("Database views ensured")
}

refresh_views <- function(con) {
  log_info("Refreshing materialized views")
  for (view in c("mv_pitch", "mv_statcast")) {
    populated <- dbGetQuery(con,
      sprintf("SELECT ispopulated FROM pg_matviews WHERE matviewname = '%s'", view)
    )$ispopulated
    # CONCURRENTLY requires the view to be populated; use plain REFRESH on first run
    if (isTRUE(populated)) {
      dbExecute(con, sprintf("REFRESH MATERIALIZED VIEW CONCURRENTLY %s", view))
    } else {
      dbExecute(con, sprintf("REFRESH MATERIALIZED VIEW %s", view))
    }
    log_info("Refreshed {view}")
  }
  log_info("Materialized views refreshed")
}

# ---------------------------------------------------------------------------
# upsert_dataframe — writes df to temp table, then INSERT ... ON CONFLICT
#
# Works for both flat-column dataframes (statsapi, schedule, player) and
# JSONB-packed dataframes (statcast, season_summary).
# ---------------------------------------------------------------------------
upsert_dataframe <- function(con, table_name, df, pk_cols) {
  if (is.null(df) || nrow(df) == 0) {
    log_info("No rows to upsert into {table_name}")
    return(invisible(0L))
  }

  # Subset df to columns that exist in the target table, excluding
  # server-defaulted columns we never send.
  target_cols <- dbListFields(con, table_name)
  auto_cols   <- c("scraped_at")
  send_cols   <- setdiff(intersect(names(df), target_cols), auto_cols)

  if (length(send_cols) == 0) {
    stop("No matching columns between df and table '", table_name, "'")
  }

  df_send <- df[, send_cols, drop = FALSE]

  # Coerce R column types to match the target table schema so that
  # dbWriteTable() creates the temp table with compatible types.
  type_info <- dbGetQuery(con, sprintf(
    "SELECT column_name, data_type FROM information_schema.columns
     WHERE table_name = '%s'", table_name
  ))
  type_map <- setNames(type_info$data_type, type_info$column_name)

  for (col in send_cols) {
    pg_type <- type_map[[col]]
    if (is.null(pg_type)) next
    df_send[[col]] <- switch(pg_type,
      "integer"                     = as.integer(df_send[[col]]),
      "bigint"                      = as.integer(df_send[[col]]),
      "numeric"                     = as.numeric(df_send[[col]]),
      "double precision"            = as.numeric(df_send[[col]]),
      "boolean"                     = as.logical(df_send[[col]]),
      "date"                        = as.Date(df_send[[col]]),
      "jsonb"                       = as.character(df_send[[col]]),
      df_send[[col]]
    )
  }

  # Write to a session-scoped temp table
  tmp <- paste0("_tmp_", table_name)
  dbWriteTable(con, tmp, df_send, temporary = TRUE, overwrite = TRUE)

  # Build upsert SQL — add ::JSONB cast for jsonb-typed target columns
  jsonb_cols <- type_info$column_name[type_info$data_type == "jsonb"]

  make_col_expr <- function(col) {
    if (col %in% jsonb_cols) sprintf('"%s"::JSONB', col)
    else sprintf('"%s"', col)
  }
  col_q      <- paste(paste0('"', send_cols, '"'), collapse = ", ")
  select_q   <- paste(sapply(send_cols, make_col_expr), collapse = ", ")
  pk_q       <- paste(paste0('"', pk_cols, '"'), collapse = ", ")
  upd_cols   <- setdiff(send_cols, pk_cols)

  conflict_action <- if (length(upd_cols) > 0) {
    sets <- paste(
      paste0('"', upd_cols, '" = EXCLUDED."', upd_cols, '"'),
      collapse = ", "
    )
    paste("DO UPDATE SET", sets)
  } else {
    "DO NOTHING"
  }

  sql <- sprintf(
    'INSERT INTO "%s" (%s) SELECT %s FROM "%s" ON CONFLICT (%s) %s',
    table_name, col_q, select_q, tmp, pk_q, conflict_action
  )

  n <- dbExecute(con, sql)
  dbRemoveTable(con, tmp)
  log_info("Upserted {n} rows into {table_name}")
  invisible(n)
}

# ---------------------------------------------------------------------------
# pack_jsonb — collapse non-key columns into a JSONB 'data' column
# Used for statcast and season_summary.
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
  sql <- paste0(
    "INSERT INTO scrape_log (data_source, level, date_start, date_end, status) ",
    "VALUES ($1, $2, $3, $4, 'running') RETURNING id"
  )
  lvl <- if (is.na(level))      NA_character_ else as.character(level)
  ds  <- if (is.na(date_start)) NA_character_ else as.character(date_start)
  de  <- if (is.na(date_end))   NA_character_ else as.character(date_end)
  res <- dbGetQuery(con, sql, params = list(data_source, lvl, ds, de))
  res$id
}

log_scrape_finish <- function(con, log_id, status = "success",
                              games_total = NA, games_success = NA,
                              games_failed = 0L, failed_game_ids = NULL,
                              error_message = NULL) {
  fgi <- if (!is.null(failed_game_ids) && length(failed_game_ids) > 0) {
    paste0("{", paste(failed_game_ids, collapse = ","), "}")
  } else {
    NA_character_
  }
  err <- if (is.null(error_message)) NA_character_ else error_message

  sql <- paste0(
    "UPDATE scrape_log SET ",
    "status = $1, completed_at = NOW(), ",
    "games_total = $2, games_success = $3, games_failed = $4, ",
    "failed_game_ids = $5, error_message = $6 ",
    "WHERE id = $7"
  )
  dbExecute(con, sql, params = list(
    status,
    as.integer(games_total),
    as.integer(games_success),
    as.integer(games_failed),
    fgi, err, log_id
  ))
}

# Returns TRUE if this source/level/date range already completed successfully
is_already_scraped <- function(con, data_source, level, date_start, date_end) {
  sql <- paste0(
    "SELECT COUNT(*) AS n FROM scrape_log ",
    "WHERE data_source = $1 AND level = $2 ",
    "AND date_start = $3 AND date_end = $4 AND status = 'success'"
  )
  res <- dbGetQuery(con, sql, params = list(
    data_source, as.character(level),
    as.character(date_start), as.character(date_end)
  ))
  res$n > 0
}

get_failed_log <- function(con, log_id) {
  dbGetQuery(con, "SELECT * FROM scrape_log WHERE id = $1",
             params = list(log_id))
}
