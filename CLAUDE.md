# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

R-based baseball data scraping pipeline using the `sabRmetrics` package. Downloads MLB and minor league data into PostgreSQL or BigQuery. Supports daily scraping (yesterday's games) and backfill (historical seasons).

## Setup

```bash
# Prerequisites: R 4.4+, PostgreSQL 17 (or BigQuery project)
# Set DB password (postgres backend)
export BASEBALL_DB_PASSWORD=your_password

# Set GCP project (bigquery backend)
export GCP_PROJECT_ID=your_gcp_project
export BQ_DATASET=baseball   # optional, defaults to "baseball"

# Restore R dependencies
Rscript -e 'renv::restore()'
```

## Run Commands

```bash
# Daily scrape (yesterday's games)
Rscript R/run_scrape.R --mode daily

# Daily scrape for a specific date
Rscript R/run_scrape.R --mode daily --date 2025-06-15

# Backfill a date range (processes in 7-day chunks)
Rscript R/run_scrape.R --mode backfill --start 2024-03-01 --end 2024-09-30

# Backfill an entire season
Rscript R/run_scrape.R --mode backfill --year 2024

# Player biographical data
Rscript R/run_scrape.R --mode players --year 2024

# Season summary stats
Rscript R/run_scrape.R --mode season-summary --year 2024

# Retry a failed scrape
Rscript R/run_scrape.R --mode retry --log-id 42

# Disable parallel processing
Rscript R/run_scrape.R --mode backfill --year 2024 --no-parallel

# Override levels
Rscript R/run_scrape.R --mode daily --levels MLB,AAA

# Use BigQuery backend (default for Docker; overrides SCRAPE_BACKEND env var)
Rscript R/run_scrape.R --mode daily --backend bigquery
```

## Project Structure

- `config.yml` — DB connection and scraping settings (password from `BASEBALL_DB_PASSWORD` env var)
- `sql/schema.sql` — PostgreSQL DDL for all 8 tables
- `sql/views.sql` — Expression indexes + `mv_statcast` materialized view
- `R/db.R` — PostgreSQL backend: `get_con()`, `ensure_tables()`, `upsert_dataframe()`, `refresh_views()`
- `R/db_bigquery.R` — BigQuery backend: same public API as `db.R`; uses `bigrquery` + Application Default Credentials
- `R/utils.R` — Logging, date chunking, parallel cluster management
- `R/scrape_schedule.R` — Game schedule/results (flat columns)
- `R/scrape_statsapi.R` — Event/pitch/play-level data (flat columns, 3 tables)
- `R/scrape_baseballsavant.R` — Statcast data (MLB only, stored as JSONB)
- `R/scrape_players.R` — Player biographical data (flat columns)
- `R/scrape_season_summary.R` — Season hitting/pitching stats (JSONB)
- `R/run_scrape.R` — CLI entry point with optparse

## Key Design Decisions

- **Two DB backends**: `--backend postgres` (default locally) and `--backend bigquery` (default in Docker). Selected via `--backend` flag or `SCRAPE_BACKEND` env var. Both expose the same function API (`get_con`, `ensure_tables`, `upsert_dataframe`, `refresh_views`, `pack_jsonb`, scrape-log helpers).
- **Flat columns** for stable sources: `schedule`, `statsapi_event/pitch/play`, `player`
- **JSONB/JSON `data` column** for volatile sources: `statcast` (90+ columns), `season_summary` (hitting vs pitching have different column sets). BigQuery uses `JSON` type; Postgres uses `JSONB`.
- `mv_statcast` (Postgres) / `v_statcast` (BigQuery) view extracts ~25 typed columns from statcast JSON for fast analytics; joins `player` for `pitcher_name`
- Upserts via temp table + `INSERT ... ON CONFLICT DO UPDATE` (Postgres) or `MERGE` (BigQuery) for idempotency
- Backfill uses 7-day chunks with parallel cluster; daily mode runs sequentially
- `scrape_log` table tracks completed/failed scrapes to enable skip and retry
- `refresh_views()` called automatically at end of daily, backfill, and retry modes
- Levels: MLB, AAA, AA, A+, A (configurable in config.yml; sabRmetrics does not support CL/DSL)
