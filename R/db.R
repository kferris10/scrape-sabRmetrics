# db.R — DB connection, table creation, upsert helpers

library(DBI)
library(RPostgres)
library(jsonlite)

get_db_connection <- function() {
  cfg <- config::get()
  dbConnect(
    Postgres(),
    host     = cfg$db$host,
    port     = cfg$db$port,
    dbname   = cfg$db$dbname,
    user     = cfg$db$user,
    password = cfg$db$password
  )
}

ensure_tables <- function(con) {
  schema_sql <- readLines("sql/schema.sql", warn = FALSE)
  sql <- paste(schema_sql, collapse = "\n")
  # Execute each statement separately
  statements <- unlist(strsplit(sql, ";"))
  statements <- trimws(statements)
  statements <- statements[nchar(statements) > 0]
  for (stmt in statements) {
    dbExecute(con, paste0(stmt, ";"))
  }
  log_info("Database tables ensured")
}

# Generic upsert: load into temp table, then INSERT ... ON CONFLICT DO UPDATE
upsert_dataframe <- function(con, table_name, df, pk_cols, jsonb_cols = NULL) {
  if (is.null(df) || nrow(df) == 0) {
    log_info("No rows to upsert into {table_name}")
    return(0)
  }

  temp_name <- paste0("tmp_", table_name, "_", as.integer(Sys.time()))

  # Write to temp table

  dbWriteTable(con, temp_name, df, temporary = TRUE, overwrite = TRUE)

  # Build upsert SQL
  cols <- dbListFields(con, temp_name)
  col_list <- paste(shQuote(cols, type = "cmd"), collapse = ", ")
  # Use double quotes for identifiers
  col_list_quoted <- paste(paste0('"', cols, '"'), collapse = ", ")
  src_col_list <- paste(paste0('"', temp_name, '"."', cols, '"'), collapse = ", ")
  pk_list <- paste(paste0('"', pk_cols, '"'), collapse = ", ")

  update_cols <- setdiff(cols, pk_cols)
  if (length(update_cols) > 0) {
    update_set <- paste(
      paste0('"', update_cols, '" = EXCLUDED."', update_cols, '"'),
      collapse = ", "
    )
    conflict_action <- paste("DO UPDATE SET", update_set)
  } else {
    conflict_action <- "DO NOTHING"
  }

  # Get target column types to generate proper casts
  type_sql <- sprintf(
    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s'",
    table_name
  )
  target_types <- dbGetQuery(con, type_sql)
  type_map <- setNames(target_types$data_type, target_types$column_name)

  # Build SELECT list with casts where needed
  select_parts <- sapply(cols, function(col) {
    target_type <- type_map[[col]]
    if (!is.null(target_type)) {
      pg_type <- switch(target_type,
        "integer" = "INTEGER",
        "text" = "TEXT",
        "date" = "DATE",
        "jsonb" = "JSONB",
        NULL
      )
      if (!is.null(pg_type)) {
        return(sprintf('"%s"::%s', col, pg_type))
      }
    }
    sprintf('"%s"', col)
  })
  select_list <- paste(select_parts, collapse = ", ")

  sql <- sprintf(
    'INSERT INTO "%s" (%s) SELECT %s FROM "%s" ON CONFLICT (%s) %s',
    table_name, col_list_quoted, select_list, temp_name,
    pk_list, conflict_action
  )

  n <- dbExecute(con, sql)
  dbRemoveTable(con, temp_name)
  log_info("Upserted {n} rows into {table_name}")
  n
}

# Convert a dataframe to a version with a JSONB 'data' column containing all
# non-key columns
pack_jsonb <- function(df, pk_cols, extra_cols = NULL) {
  keep_cols <- c(pk_cols, extra_cols)
  keep_cols <- intersect(keep_cols, names(df))
  data_cols <- setdiff(names(df), keep_cols)

  result <- df[, keep_cols, drop = FALSE]
  result$data <- apply(df[, data_cols, drop = FALSE], 1, function(row) {
    toJSON(as.list(row), auto_unbox = TRUE, na = "null")
  })
  result
}

# --- Scrape log helpers ---
log_scrape_start <- function(con, mode, level = NA, start_date = NA, end_date = NA) {
  sql <- "INSERT INTO scrape_log (mode, level, start_date, end_date, status)
          VALUES ($1, $2, $3, $4, 'running') RETURNING id"
  # Convert dates; keep NA as NA_character_ (not "NA" string)
  sd <- if (is.na(start_date)) NA_character_ else as.character(start_date)
  ed <- if (is.na(end_date)) NA_character_ else as.character(end_date)
  lvl <- if (is.na(level)) NA_character_ else as.character(level)
  res <- dbGetQuery(con, sql, params = list(mode, lvl, sd, ed))
  res$id
}

log_scrape_finish <- function(con, log_id, status = "success",
                              rows_written = 0, failed_games = NULL,
                              error_message = NULL) {
  failed_str <- if (!is.null(failed_games) && length(failed_games) > 0) {
    paste0("{", paste(failed_games, collapse = ","), "}")
  } else {
    NA_character_
  }
  err_msg <- if (is.null(error_message)) NA_character_ else error_message
  sql <- "UPDATE scrape_log SET status = $1, finished_at = NOW(),
          rows_written = $2, failed_games = $3, error_message = $4
          WHERE id = $5"
  dbExecute(con, sql, params = list(status, as.integer(rows_written),
                                     failed_str, err_msg, log_id))
}

# Check if a date range + mode + level has already been scraped successfully
is_already_scraped <- function(con, mode, level, start_date, end_date) {
  sql <- "SELECT COUNT(*) as n FROM scrape_log
          WHERE mode = $1 AND level = $2
          AND start_date = $3 AND end_date = $4
          AND status = 'success'"
  res <- dbGetQuery(con, sql, params = list(mode, level,
    as.character(start_date), as.character(end_date)))
  res$n > 0
}

get_failed_log <- function(con, log_id) {
  sql <- "SELECT * FROM scrape_log WHERE id = $1"
  dbGetQuery(con, sql, params = list(log_id))
}
