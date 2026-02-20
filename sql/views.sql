-- views.sql — Expression indexes + materialized views for analytical layer
-- Run after schema.sql. All DDL is idempotent (IF NOT EXISTS / CREATE OR REPLACE).

-- ============================================================
-- Expression indexes on statcast.data (avoid full JSONB scans)
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_statcast_pitcher_id
  ON statcast ((data->>'pitcher_id'));

CREATE INDEX IF NOT EXISTS idx_statcast_batter_id
  ON statcast ((data->>'batter_id'));

CREATE INDEX IF NOT EXISTS idx_statcast_pitch_type
  ON statcast ((data->>'pitch_type'));

CREATE INDEX IF NOT EXISTS idx_statcast_launch_speed
  ON statcast (((data->>'launch_speed')::numeric))
  WHERE data->>'launch_speed' IS NOT NULL;

-- ============================================================
-- mv_pitch — pre-joined pitch view across all levels
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_pitch AS
SELECT
  -- Schedule context
  s.date        AS game_date,
  s.level,
  s.year,

  -- Identifiers
  p.game_id,
  p.event_index,
  p.play_index,
  p.play_id,

  -- Pitch situation
  p.pitch_number,
  p.outs,
  p.balls,
  p.strikes,

  -- Pitch characteristics
  p.pitch_type,
  p.description,
  p.ax, p.ay, p.az,
  p.vx0, p.vy0, p.vz0,
  p.x0, p.z0,
  p.extension,
  p.spin_rate,
  p.strike_zone_top,
  p.strike_zone_bottom,

  -- Batted ball
  p.launch_speed,
  p.launch_angle,
  p.hit_coord_x,
  p.hit_coord_y,

  -- Event context
  e.inning,
  e.half_inning,
  e.batter_id,
  e.bat_side,
  e.pitcher_id,
  e.pitch_hand,
  e.event,
  e.is_out,
  e.runs_on_event,

  -- Is this the last pitch of the plate appearance?
  (p.play_index = (
    SELECT MAX(p2.play_index)
    FROM statsapi_pitch p2
    WHERE p2.game_id = p.game_id
      AND p2.event_index = p.event_index
  )) AS is_final_pitch

FROM statsapi_pitch p
JOIN statsapi_event e
  ON e.game_id = p.game_id
  AND e.event_index = p.event_index
JOIN schedule s
  ON s.game_id = p.game_id
WITH NO DATA;

-- Unique index required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS mv_pitch_pk
  ON mv_pitch (game_id, event_index, play_index);

CREATE INDEX IF NOT EXISTS mv_pitch_pitcher_id   ON mv_pitch (pitcher_id);
CREATE INDEX IF NOT EXISTS mv_pitch_batter_id    ON mv_pitch (batter_id);
CREATE INDEX IF NOT EXISTS mv_pitch_year_pitcher ON mv_pitch (year, pitcher_id);
CREATE INDEX IF NOT EXISTS mv_pitch_year_batter  ON mv_pitch (year, batter_id);
CREATE INDEX IF NOT EXISTS mv_pitch_pitch_type   ON mv_pitch (pitch_type);
CREATE INDEX IF NOT EXISTS mv_pitch_game_date    ON mv_pitch (game_date);

-- ============================================================
-- mv_statcast — typed columns extracted from statcast JSONB
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_statcast AS
SELECT
  -- Primary key
  game_pk,
  at_bat_number,
  pitch_number,
  year,

  -- Players
  (data->>'pitcher_id')::integer        AS pitcher_id,
  (data->>'batter_id')::integer         AS batter_id,
  data->>'bat_side'                     AS bat_side,
  data->>'pitch_hand'                   AS pitch_hand,

  -- Situation
  (data->>'inning')::integer            AS inning,
  data->>'half_inning'                  AS half_inning,
  (data->>'outs')::integer              AS outs,
  (data->>'balls')::integer             AS balls,
  (data->>'strikes')::integer           AS strikes,
  (data->>'on_1b')::integer             AS on_1b,
  (data->>'on_2b')::integer             AS on_2b,
  (data->>'on_3b')::integer             AS on_3b,

  -- Pitch
  data->>'pitch_type'                   AS pitch_type,
  (data->>'release_speed')::numeric     AS release_speed,
  (data->>'release_spin_rate')::numeric AS release_spin_rate,
  (data->>'release_extension')::numeric AS release_extension,
  (data->>'plate_x')::numeric           AS plate_x,
  (data->>'plate_z')::numeric           AS plate_z,
  (data->>'pfx_x')::numeric             AS pfx_x,
  (data->>'pfx_z')::numeric             AS pfx_z,

  -- Swing tracking
  (data->>'bat_speed')::numeric         AS bat_speed,
  (data->>'swing_length')::numeric      AS swing_length,

  -- Batted ball
  (data->>'launch_speed')::numeric      AS launch_speed,
  (data->>'launch_angle')::numeric      AS launch_angle,
  (data->>'hit_distance')::numeric      AS hit_distance,
  data->>'bb_type'                      AS bb_type,

  -- Value
  (data->>'expected_woba')::numeric     AS expected_woba,
  (data->>'woba_value')::numeric        AS woba_value,
  data->>'description'                  AS description,
  data->>'events'                       AS events,

  scraped_at

FROM statcast
WITH NO DATA;

-- Unique index required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS mv_statcast_pk
  ON mv_statcast (game_pk, at_bat_number, pitch_number);

CREATE INDEX IF NOT EXISTS mv_statcast_pitcher_id   ON mv_statcast (pitcher_id);
CREATE INDEX IF NOT EXISTS mv_statcast_batter_id    ON mv_statcast (batter_id);
CREATE INDEX IF NOT EXISTS mv_statcast_year_pitcher ON mv_statcast (year, pitcher_id);
CREATE INDEX IF NOT EXISTS mv_statcast_year_batter  ON mv_statcast (year, batter_id);
CREATE INDEX IF NOT EXISTS mv_statcast_pitch_type   ON mv_statcast (pitch_type);
CREATE INDEX IF NOT EXISTS mv_statcast_launch_speed
  ON mv_statcast (launch_speed)
  WHERE launch_speed IS NOT NULL;
