# Plan: Daily Scrape Automation + Shareable Dashboard

## Goal

Run `Rscript R/run_scrape.R --mode daily` automatically without the laptop on, and share a
dashboard with a small group of friends. Budget: free tier preferred, well under $20/month.

---

## Options Considered

### Option A — GitHub Actions + Supabase + Grafana/Retool
**Cost:** ~$0 | **Dashboard:** awkward | **Code changes:** minimal

- Supabase free tier is 500 MB — a real risk for statcast JSONB data
- Grafana/Retool free tiers have sharing limitations
- Looker Studio requires a paid third-party connector (~$15/mo) for Postgres

**Verdict:** Weakest option. Storage cap and dashboard friction rule it out.

---

### Option B — Cloud VM + PostgreSQL + Metabase
**Cost:** ~$4–6/month (Hetzner CX22) | **Dashboard:** excellent | **Code changes:** none

- Single Linux VM hosts PostgreSQL, cron-scheduled R scrape, and Metabase
- Zero code changes to the scraping pipeline
- Metabase supports public dashboard links (no login required for viewers)
- Ongoing $4/mo cost; you SSH in to manage things

**Verdict:** Best option if you want to be up and running this week with no code changes.

---

### Option C — GCP: Cloud Run + BigQuery + Looker Studio ✓ CHOSEN
**Cost:** ~$0 | **Dashboard:** best-in-class | **Code changes:** moderate (one-time)

| GCP Service | Role | Free Tier |
|---|---|---|
| Cloud Scheduler | Triggers daily scrape | 3 jobs/month free |
| Cloud Run Job | Runs containerized R scrape | 2M vCPU-sec/month free |
| BigQuery | Stores all scraped data | 10 GB storage + 1 TB queries/month free |
| Looker Studio | Shareable dashboards | Completely free |

Baseball data for a full MLB season is well under 1 GB flattened — comfortably within the
free tier for years.

**Dashboard sharing:** Looker Studio dashboards share via a link (like a Google Doc).
Viewers don't need a Google account.

**Verdict:** Best long-term architecture. Free, no VM to manage, no storage limits. The
BigQuery migration is a one-time cost that results in a cleaner setup.

---

## What Was Implemented

### Files created

| File | Purpose |
|------|---------|
| `Dockerfile` | Builds R environment from `renv.lock`; default CMD runs daily bigquery scrape |
| `.dockerignore` | Excludes `.env`, Windows renv library, logs |
| `R/db_bigquery.R` | BigQuery backend — same public API as `db.R` |
| `.github/workflows/deploy.yml` | On push to `main`: build image → push to Artifact Registry → deploy Cloud Run Job |
| `docs/gcp_setup.md` | Step-by-step GCP setup instructions |

### Files modified

| File | Change |
|------|--------|
| `config.yml` | Added `bigquery.project` and `bigquery.dataset` (read from env vars) |
| `R/run_scrape.R` | Added `--backend postgres\|bigquery` flag; sources the right db file at runtime |

### Architecture

```
GitHub push to main
        │
        ▼
GitHub Actions (deploy.yml)
  - docker build
  - push → Artifact Registry
  - gcloud run jobs update
        │
        ▼
Cloud Run Job (scrape-baseball-daily)
  [image: rocker/r-ver + renv packages + R/]
        │  triggered by
        ▼
Cloud Scheduler (0 8 * * * America/New_York)
        │  writes to
        ▼
BigQuery dataset: baseball
  tables: schedule, statsapi_event, statsapi_pitch, statsapi_play,
          statcast, player, season_summary, scrape_log
  view:   v_statcast (typed columns extracted from statcast JSON)
        │  connected to
        ▼
Looker Studio dashboard
  shared via public link → friends view in browser, no login needed
```

### Key design decisions

- **`--backend` flag**: `run_scrape.R` sources `db.R` or `db_bigquery.R` at runtime based on
  `--backend postgres|bigquery` (or `SCRAPE_BACKEND` env var). All scraper files are unchanged.
- **Upsert**: PostgreSQL `INSERT ... ON CONFLICT` → BigQuery `MERGE INTO ... USING temp_table`
- **JSON columns**: stored as BigQuery `JSON` type; MERGE uses `PARSE_JSON()` cast from the
  STRING temp table
- **`v_statcast`**: regular BigQuery view (not materialized) because BigQuery MVs don't support
  LEFT JOIN. Looker Studio queries it directly.
- **`scrape_log.id`**: generated in R as `as.integer(as.numeric(Sys.time()))` — BigQuery has
  no SERIAL/SEQUENCE
- **Auth**: `bq_auth()` with no args uses Application Default Credentials — works in Cloud Run
  (attached service account) and locally (`GOOGLE_APPLICATION_CREDENTIALS` env var)
- **`refresh_views`**: no-op for BigQuery — views are always live SQL

---

## Next Steps

### Immediate (before first deploy)

- [ ] **Add bigrquery to renv.lock**
  ```bash
  Rscript -e 'renv::install("bigrquery"); renv::snapshot()'
  git add renv.lock
  git commit -m "Add bigrquery to renv.lock"
  ```
  After this, remove the extra `install.packages("bigrquery")` line from `Dockerfile`.

- [ ] **Follow `docs/gcp_setup.md` Steps 1–8** to provision all GCP resources and configure
  GitHub secrets (`GCP_WIF_PROVIDER`, `GCP_SERVICE_ACCOUNT`) and variable (`GCP_PROJECT_ID`).

- [ ] **Push to `main`** — GitHub Actions will build and deploy on first push (~15 min for
  initial Docker build; subsequent builds are much faster due to layer caching).

- [ ] **Test the Cloud Run Job manually**
  ```bash
  gcloud run jobs execute scrape-baseball-daily --region=us-central1 --wait
  ```

### After first successful scrape

- [ ] **Schedule daily scrape** (`docs/gcp_setup.md` Step 9) — Cloud Scheduler at 8 AM Eastern

- [ ] **Migrate historical data** from local PostgreSQL:
  ```bash
  # Option A: pg_dump → bq load (fast)
  psql -U postgres -d baseball -c "\COPY statcast TO 'statcast.csv' CSV HEADER"
  bq load --source_format=CSV --autodetect baseball.statcast statcast.csv

  # Option B: backfill from source (slower but no local export needed)
  gcloud run jobs execute scrape-baseball-daily \
    --args="--mode,backfill,--year,2024,--backend,bigquery" --wait
  ```

- [ ] **Build Looker Studio dashboard** (`docs/gcp_setup.md` Step 11)
  - Connect to BigQuery → dataset `baseball` → view `v_statcast`
  - Starter charts: avg exit velocity by pitch type, xwOBA trend, pitcher strikeout rate
  - Share → "Anyone with the link can view" → send link to friends

### Later / nice to have

- [ ] Remove the `install.packages("bigrquery")` fallback from `Dockerfile` once bigrquery
  is in `renv.lock`
- [ ] Add `plans/` and `docs/` to `.dockerignore` to keep the image lean
- [ ] Consider pinning the Cloud Run Job to a specific image digest (not `:latest`) for
  reproducibility
- [ ] Set up Cloud Monitoring alerts on job failure (free tier covers basic alerting)
- [ ] Add `--mode players` and `--mode season-summary` Cloud Scheduler jobs for off-season
  data refreshes
