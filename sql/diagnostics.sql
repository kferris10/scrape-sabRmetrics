-- =============================================================================
-- sql/diagnostics.sql
-- Common SQL queries for diagnosing data gaps and verifying pipeline accuracy.
--
-- Run in psql:  \i sql/diagnostics.sql
-- Or copy-paste individual sections into DBeaver/pgAdmin.
-- =============================================================================


-- =============================================================================
\echo '=== SECTION 1: Player Lookup ==='
-- =============================================================================

-- Find a pitcher by (fuzzy) name — starting point for all pitcher queries.
-- Replace '%snell%' with any name fragment.
SELECT player_id, name_full, primary_position, throw_hand
FROM player
WHERE name_full ILIKE '%snell%';

-- If the player table is empty, look up pitcher IDs from raw event data.
-- Cross-references 2024 MLB games to find active pitchers.
SELECT DISTINCT e.pitcher_id, COUNT(*) AS pitches
FROM statsapi_event e
JOIN schedule s ON s.game_id = e.game_id
WHERE s.year = 2024 AND s.level = 'MLB'
GROUP BY e.pitcher_id
ORDER BY pitches DESC
LIMIT 30;


-- =============================================================================
\echo '=== SECTION 2: Scrape Coverage Audit ==='
-- =============================================================================

-- All scrape_log entries for 2024, ordered by source + date.
SELECT id, data_source, level, date_start, date_end,
       status, games_total, games_success, games_failed,
       error_message, started_at
FROM scrape_log
WHERE (date_start >= '2024-01-01' OR date_end >= '2024-01-01')
ORDER BY data_source, date_start;

-- Failed scrapes (any year) — candidates for: Rscript R/run_scrape.R --mode retry --log-id <id>
SELECT id, data_source, level, date_start, date_end,
       error_message, started_at
FROM scrape_log
WHERE status = 'failed'
ORDER BY started_at DESC;

-- Date gaps in statsapi scrape log for MLB 2024.
-- Shows 7-day windows where no successful scrape exists.
SELECT generate_series('2024-03-01'::date, '2024-10-31'::date, '7 days')::date AS week_start
EXCEPT
SELECT date_trunc('week', date_start)::date
FROM scrape_log
WHERE data_source = 'statsapi' AND level = 'MLB'
  AND status = 'success'
  AND date_start >= '2024-03-01' AND date_end <= '2024-11-01'
ORDER BY week_start;


-- =============================================================================
\echo '=== SECTION 3: Table Row Counts by Year ==='
-- =============================================================================

-- Quick sanity check that data is present at each layer of the pipeline.

SELECT year, COUNT(*) AS games
FROM schedule
WHERE level = 'MLB'
GROUP BY year ORDER BY year;

SELECT year, COUNT(*) AS events
FROM statsapi_event
GROUP BY year ORDER BY year;

SELECT year, COUNT(*) AS pitches
FROM statsapi_pitch
GROUP BY year ORDER BY year;

SELECT year, COUNT(*) AS statcast_pitches
FROM statcast
GROUP BY year ORDER BY year;

-- mv_statcast row count reflects the last REFRESH MATERIALIZED VIEW.
SELECT year, COUNT(*) AS mv_statcast_rows
FROM mv_statcast
GROUP BY year ORDER BY year;


-- =============================================================================
\echo '=== SECTION 4: Pitcher-Specific Diagnostics ==='
-- Replace 605488 (Blake Snell) with the player_id from Section 1.
-- =============================================================================

-- How many pitches appear in mv_statcast for this pitcher?
SELECT year, COUNT(*) AS pitches, COUNT(DISTINCT game_pk) AS games
FROM mv_statcast
WHERE pitcher_id = 605488   -- substitute player_id
GROUP BY year ORDER BY year;

-- Per-game breakdown: plate appearances vs pitch rows for this pitcher.
-- A game with events but 0 pitches_in_statsapi_pitch is a partial scrape.
SELECT
  s.game_id,
  s.date,
  COUNT(DISTINCT e.event_index) AS plate_appearances,
  COUNT(p.play_index)           AS pitches_in_statsapi_pitch
FROM schedule s
JOIN statsapi_event e ON e.game_id = s.game_id AND e.pitcher_id = 605488
LEFT JOIN statsapi_pitch p ON p.game_id = e.game_id AND p.event_index = e.event_index
WHERE s.year = 2024 AND s.level = 'MLB'
GROUP BY s.game_id, s.date
ORDER BY s.date;


-- =============================================================================
\echo '=== SECTION 5: Join Integrity Checks ==='
-- =============================================================================

-- statsapi_pitch rows with no matching statsapi_event (orphaned pitches).
SELECT COUNT(*) AS orphaned_pitches
FROM statsapi_pitch sp
WHERE NOT EXISTS (
  SELECT 1 FROM statsapi_event e
  WHERE e.game_id = sp.game_id AND e.event_index = sp.event_index
);

-- statsapi_event rows with no matching schedule row.
SELECT COUNT(*) AS events_missing_schedule
FROM statsapi_event e
WHERE NOT EXISTS (
  SELECT 1 FROM schedule s WHERE s.game_id = e.game_id
);

-- statsapi_pitch rows with no matching schedule row.
SELECT COUNT(*) AS pitches_missing_schedule
FROM statsapi_pitch p
WHERE NOT EXISTS (
  SELECT 1 FROM schedule s WHERE s.game_id = p.game_id
);

-- Pitcher IDs in statsapi_event absent from the player table.
-- These cause pitcher_name = NULL in mv_statcast (data still present, name missing).
SELECT DISTINCT e.pitcher_id
FROM statsapi_event e
WHERE NOT EXISTS (SELECT 1 FROM player pl WHERE pl.player_id = e.pitcher_id)
ORDER BY e.pitcher_id
LIMIT 20;


-- =============================================================================
\echo '=== SECTION 6: Data Completeness — NULL Rates in mv_statcast ==='
-- =============================================================================

-- Measures how much physics/tracking data is populated by year.
SELECT
  year,
  COUNT(*)                                                   AS total_pitches,
  ROUND(100.0 * COUNT(release_speed)    / COUNT(*), 1)      AS pct_has_velocity,
  ROUND(100.0 * COUNT(release_spin_rate)/ COUNT(*), 1)      AS pct_has_spin,
  ROUND(100.0 * COUNT(launch_speed)     / COUNT(*), 1)      AS pct_has_launch,
  ROUND(100.0 * COUNT(pitcher_name)     / COUNT(*), 1)      AS pct_has_pitcher_name
FROM mv_statcast
GROUP BY year
ORDER BY year;


-- =============================================================================
\echo '=== SECTION 7: Statcast Cross-Check (MLB only) ==='
-- =============================================================================

-- Compares statsapi_pitch vs statcast pitch counts by year.
-- statcast_coverage_pct < 90% may indicate a failed statcast scrape.
SELECT
  sp_counts.year,
  sp_counts.statsapi_pitches,
  sc_counts.statcast_pitches,
  ROUND(
    100.0 * sc_counts.statcast_pitches / NULLIF(sp_counts.statsapi_pitches, 0),
    1
  ) AS statcast_coverage_pct
FROM (
  SELECT p.year, COUNT(*) AS statsapi_pitches
  FROM statsapi_pitch p
  JOIN schedule s ON s.game_id = p.game_id AND s.level = 'MLB'
  GROUP BY p.year
) sp_counts
LEFT JOIN (
  SELECT year, COUNT(*) AS statcast_pitches FROM statcast GROUP BY year
) sc_counts USING (year)
ORDER BY year;


-- =============================================================================
\echo '=== SECTION 8: Quick Pitcher Arsenal Summary (mv_statcast) ==='
-- Replace 605488 and 2024 as needed.
-- =============================================================================

SELECT
  pitch_type,
  pitch_name,
  COUNT(*)                                                   AS pitches,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)        AS usage_pct,
  ROUND(AVG(release_speed)::numeric, 1)                     AS avg_velo,
  ROUND(AVG(release_spin_rate)::numeric)                    AS avg_spin,
  ROUND(AVG(pfx_x)::numeric, 2)                             AS avg_pfx_x,
  ROUND(AVG(pfx_z)::numeric, 2)                             AS avg_pfx_z
FROM mv_statcast
WHERE pitcher_id = 605488 AND year = 2024
GROUP BY pitch_type, pitch_name
ORDER BY pitches DESC;
