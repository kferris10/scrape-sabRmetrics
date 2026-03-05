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

CREATE INDEX IF NOT EXISTS idx_statcast_batter_name
  ON statcast ((data->>'batter_name'));

CREATE INDEX IF NOT EXISTS idx_statcast_game_date
  ON statcast ((data->>'game_date'));

CREATE INDEX IF NOT EXISTS idx_statcast_event_index
  ON statcast (((data->>'event_index')::integer));

-- Drop mv_pitch if it exists from a previous schema version
DROP INDEX IF EXISTS mv_pitch_pk;
DROP INDEX IF EXISTS mv_pitch_pitcher_id;
DROP INDEX IF EXISTS mv_pitch_batter_id;
DROP INDEX IF EXISTS mv_pitch_year_pitcher;
DROP INDEX IF EXISTS mv_pitch_year_batter;
DROP INDEX IF EXISTS mv_pitch_pitch_type;
DROP INDEX IF EXISTS mv_pitch_game_date;

DROP MATERIALIZED VIEW IF EXISTS mv_pitch;

-- ============================================================
-- mv_statcast — typed columns extracted from statcast JSONB
-- Migration: drop old indexes + view, then recreate with full column set.
-- ============================================================

DROP INDEX IF EXISTS mv_statcast_pk;
DROP INDEX IF EXISTS mv_statcast_pitcher_id;
DROP INDEX IF EXISTS mv_statcast_batter_id;
DROP INDEX IF EXISTS mv_statcast_year_pitcher;
DROP INDEX IF EXISTS mv_statcast_year_batter;
DROP INDEX IF EXISTS mv_statcast_pitch_type;
DROP INDEX IF EXISTS mv_statcast_launch_speed;
DROP INDEX IF EXISTS mv_statcast_game_date;
DROP INDEX IF EXISTS mv_statcast_events;

DROP MATERIALIZED VIEW IF EXISTS mv_statcast;

CREATE MATERIALIZED VIEW mv_statcast AS
SELECT
  -- Primary keys (table columns)
  game_pk,
  at_bat_number,
  pitch_number,
  year,
  statcast.scraped_at,

  -- Game context
  (data->>'game_date')::date            AS game_date,
  data->>'game_type'                    AS game_type,
  data->>'home_team'                    AS home_team,
  data->>'away_team'                    AS away_team,
  (data->>'event_index')::integer       AS event_index,

  -- Players
  (data->>'batter_id')::integer         AS batter_id,
  (data->>'pitcher_id')::integer        AS pitcher_id,
  data->>'batter_name'                  AS batter_name,
  pp.name_full                          AS pitcher_name,
  data->>'bat_side'                     AS bat_side,
  data->>'pitch_hand'                   AS pitch_hand,
  (data->>'age_pit')::numeric           AS age_pit,
  (data->>'age_bat')::numeric           AS age_bat,
  (data->>'age_pit_legacy')::numeric    AS age_pit_legacy,
  (data->>'age_bat_legacy')::numeric    AS age_bat_legacy,

  -- Fielders
  (data->>'fielder_2_id')::integer      AS fielder_2_id,
  (data->>'fielder_3_id')::integer      AS fielder_3_id,
  (data->>'fielder_4_id')::integer      AS fielder_4_id,
  (data->>'fielder_5_id')::integer      AS fielder_5_id,
  (data->>'fielder_6_id')::integer      AS fielder_6_id,
  (data->>'fielder_7_id')::integer      AS fielder_7_id,
  (data->>'fielder_8_id')::integer      AS fielder_8_id,
  (data->>'fielder_9_id')::integer      AS fielder_9_id,

  -- Runners / situation
  (data->>'pre_runner_1b_id')::integer  AS pre_runner_1b_id,
  (data->>'pre_runner_2b_id')::integer  AS pre_runner_2b_id,
  (data->>'pre_runner_3b_id')::integer  AS pre_runner_3b_id,
  (data->>'inning')::integer            AS inning,
  data->>'half_inning'                  AS half_inning,
  (data->>'outs')::integer              AS outs,
  (data->>'balls')::integer             AS balls,
  (data->>'strikes')::integer           AS strikes,
  data->>'if_fielding_alignment'        AS if_fielding_alignment,
  data->>'of_fielding_alignment'        AS of_fielding_alignment,

  -- Pitch mechanics
  data->>'pitch_type'                   AS pitch_type,
  data->>'pitch_name'                   AS pitch_name,
  (data->>'arm_angle')::numeric         AS arm_angle,
  (data->>'release_speed')::numeric     AS release_speed,
  (data->>'effective_speed')::numeric   AS effective_speed,
  (data->>'release_pos_x')::numeric     AS release_pos_x,
  (data->>'release_pos_y')::numeric     AS release_pos_y,
  (data->>'release_pos_z')::numeric     AS release_pos_z,
  (data->>'extension')::numeric         AS extension,
  (data->>'release_spin_rate')::numeric AS release_spin_rate,
  (data->>'spin_axis')::numeric         AS spin_axis,
  (data->>'pfx_x')::numeric             AS pfx_x,
  (data->>'pfx_z')::numeric             AS pfx_z,
  (data->>'plate_x')::numeric           AS plate_x,
  (data->>'plate_z')::numeric           AS plate_z,
  (data->>'ax')::numeric                AS ax,
  (data->>'ay')::numeric                AS ay,
  (data->>'az')::numeric                AS az,
  (data->>'vx0')::numeric               AS vx0,
  (data->>'vy0')::numeric               AS vy0,
  (data->>'vz0')::numeric               AS vz0,
  (data->>'strike_zone_top')::numeric   AS strike_zone_top,
  (data->>'strike_zone_bottom')::numeric AS strike_zone_bottom,
  (data->>'zone')::integer              AS zone,
  (data->>'api_break_z_with_gravity')::numeric  AS api_break_z_with_gravity,
  (data->>'api_break_x_arm')::numeric           AS api_break_x_arm,
  (data->>'api_break_x_batter_in')::numeric     AS api_break_x_batter_in,

  -- Swing tracking
  (data->>'bat_speed')::numeric                                          AS bat_speed,
  (data->>'swing_length')::numeric                                       AS swing_length,
  (data->>'attack_angle')::numeric                                       AS attack_angle,
  (data->>'attack_direction')::numeric                                   AS attack_direction,
  (data->>'swing_path_tilt')::numeric                                    AS swing_path_tilt,
  (data->>'intercept_ball_minus_batter_pos_x_inches')::numeric           AS intercept_ball_minus_batter_pos_x_inches,
  (data->>'intercept_ball_minus_batter_pos_y_inches')::numeric           AS intercept_ball_minus_batter_pos_y_inches,
  (data->>'hyper_speed')::numeric                                        AS hyper_speed,

  -- Batted ball
  (data->>'launch_speed')::numeric      AS launch_speed,
  (data->>'launch_angle')::numeric      AS launch_angle,
  (data->>'hit_coord_x')::numeric       AS hit_coord_x,
  (data->>'hit_coord_y')::numeric       AS hit_coord_y,
  (data->>'hit_distance_sc')::numeric   AS hit_distance_sc,
  data->>'bb_type'                      AS bb_type,
  (data->>'hit_location')::integer      AS hit_location,
  (data->>'launch_speed_angle')::integer AS launch_speed_angle,
  data->>'type'                         AS type,

  -- Outcome / description
  data->>'description'                  AS description,
  data->>'events'                       AS events,
  data->>'des'                          AS des,

  -- Scoring
  (data->>'home_score')::integer        AS home_score,
  (data->>'away_score')::integer        AS away_score,
  (data->>'bat_score')::integer         AS bat_score,
  (data->>'fld_score')::integer         AS fld_score,
  (data->>'post_home_score')::integer   AS post_home_score,
  (data->>'post_away_score')::integer   AS post_away_score,
  (data->>'post_bat_score')::integer    AS post_bat_score,
  (data->>'post_fld_score')::integer    AS post_fld_score,
  (data->>'home_score_diff')::integer   AS home_score_diff,
  (data->>'bat_score_diff')::integer    AS bat_score_diff,

  -- Win expectancy / run value
  (data->>'expected_woba')::numeric                   AS expected_woba,
  (data->>'expected_babip')::numeric                  AS expected_babip,
  (data->>'woba_value')::numeric                      AS woba_value,
  (data->>'woba_denom')::numeric                      AS woba_denom,
  (data->>'babip_value')::numeric                     AS babip_value,
  (data->>'iso_value')::numeric                       AS iso_value,
  (data->>'estimated_slg_using_speedangle')::numeric  AS estimated_slg_using_speedangle,
  (data->>'delta_run_exp')::numeric                   AS delta_run_exp,
  (data->>'delta_pitcher_run_exp')::numeric           AS delta_pitcher_run_exp,
  (data->>'delta_home_win_exp')::numeric              AS delta_home_win_exp,
  (data->>'home_win_exp')::numeric                    AS home_win_exp,
  (data->>'bat_win_exp')::numeric                     AS bat_win_exp,

  -- Game history / context
  (data->>'n_thruorder_pitcher')::integer                  AS n_thruorder_pitcher,
  (data->>'n_priorpa_thisgame_player_at_bat')::integer     AS n_priorpa_thisgame_player_at_bat,
  (data->>'pitcher_days_since_prev_game')::integer         AS pitcher_days_since_prev_game,
  (data->>'batter_days_since_prev_game')::integer          AS batter_days_since_prev_game,
  (data->>'pitcher_days_until_next_game')::integer         AS pitcher_days_until_next_game,
  (data->>'batter_days_until_next_game')::integer          AS batter_days_until_next_game

FROM statcast
LEFT JOIN player pp ON pp.player_id = (data->>'pitcher_id')::integer
WITH NO DATA;

-- Unique index required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX mv_statcast_pk
  ON mv_statcast (game_pk, at_bat_number, pitch_number);

CREATE INDEX mv_statcast_pitcher_id   ON mv_statcast (pitcher_id);
CREATE INDEX mv_statcast_batter_id    ON mv_statcast (batter_id);
CREATE INDEX mv_statcast_year_pitcher ON mv_statcast (year, pitcher_id);
CREATE INDEX mv_statcast_year_batter  ON mv_statcast (year, batter_id);
CREATE INDEX mv_statcast_pitch_type   ON mv_statcast (pitch_type);
CREATE INDEX mv_statcast_launch_speed
  ON mv_statcast (launch_speed)
  WHERE launch_speed IS NOT NULL;
CREATE INDEX mv_statcast_game_date    ON mv_statcast (game_date);
CREATE INDEX mv_statcast_events       ON mv_statcast (events);
