-- schema.sql — PostgreSQL DDL for all 8 tables
-- All statements are idempotent (IF NOT EXISTS).

-- ============================================================
-- schedule
-- ============================================================

CREATE TABLE IF NOT EXISTS schedule (
  game_id         TEXT        PRIMARY KEY,
  game_type       TEXT,
  year            INTEGER     NOT NULL,
  date            DATE        NOT NULL,
  level           TEXT        NOT NULL,
  venue_id        INTEGER,
  team_id_away    INTEGER,
  team_id_home    INTEGER,
  team_name_away  TEXT,
  team_name_home  TEXT,
  score_away      INTEGER,
  score_home      INTEGER,
  scraped_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_schedule_date  ON schedule (date);
CREATE INDEX IF NOT EXISTS idx_schedule_year  ON schedule (year);
CREATE INDEX IF NOT EXISTS idx_schedule_level ON schedule (level);

-- ============================================================
-- statsapi_event
-- ============================================================

CREATE TABLE IF NOT EXISTS statsapi_event (
  game_id         TEXT        NOT NULL,
  event_index     INTEGER     NOT NULL,
  year            INTEGER,
  inning          INTEGER,
  half_inning     TEXT,
  batter_id       INTEGER,
  bat_side        TEXT,
  pitcher_id      INTEGER,
  pitch_hand      TEXT,
  event           TEXT,
  is_out          BOOLEAN,
  runs_on_event   INTEGER,
  fielder_2_id    INTEGER,
  fielder_3_id    INTEGER,
  fielder_4_id    INTEGER,
  fielder_5_id    INTEGER,
  fielder_6_id    INTEGER,
  fielder_7_id    INTEGER,
  fielder_8_id    INTEGER,
  fielder_9_id    INTEGER,
  fielder_10_id   INTEGER,
  scraped_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (game_id, event_index)
);

CREATE INDEX IF NOT EXISTS idx_statsapi_event_batter  ON statsapi_event (batter_id);
CREATE INDEX IF NOT EXISTS idx_statsapi_event_pitcher ON statsapi_event (pitcher_id);
CREATE INDEX IF NOT EXISTS idx_statsapi_event_year    ON statsapi_event (year);

-- ============================================================
-- statsapi_pitch
-- ============================================================

CREATE TABLE IF NOT EXISTS statsapi_pitch (
  play_id             TEXT,
  game_id             TEXT        NOT NULL,
  event_index         INTEGER     NOT NULL,
  play_index          INTEGER     NOT NULL,
  year                INTEGER,
  pitch_number        INTEGER,
  outs                INTEGER,
  balls               INTEGER,
  strikes             INTEGER,
  description         TEXT,
  pitch_type          TEXT,
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
  scraped_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (game_id, event_index, play_index)
);

CREATE INDEX IF NOT EXISTS idx_statsapi_pitch_year       ON statsapi_pitch (year);
CREATE INDEX IF NOT EXISTS idx_statsapi_pitch_pitch_type ON statsapi_pitch (pitch_type);

-- ============================================================
-- statsapi_play
-- ============================================================

CREATE TABLE IF NOT EXISTS statsapi_play (
  play_id             TEXT,
  game_id             TEXT        NOT NULL,
  event_index         INTEGER     NOT NULL,
  play_index          INTEGER     NOT NULL,
  year                INTEGER,
  pitch_number        INTEGER,
  pre_runner_1b_id    INTEGER,
  pre_runner_2b_id    INTEGER,
  pre_runner_3b_id    INTEGER,
  post_runner_1b_id   INTEGER,
  post_runner_2b_id   INTEGER,
  post_runner_3b_id   INTEGER,
  pre_outs            INTEGER,
  pre_balls           INTEGER,
  pre_strikes         INTEGER,
  post_outs           INTEGER,
  post_balls          INTEGER,
  post_strikes        INTEGER,
  runs_on_play        INTEGER,
  type                TEXT,
  is_pickoff          BOOLEAN,
  is_stolen_base      BOOLEAN,
  is_caught_stealing  BOOLEAN,
  scraped_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (game_id, event_index, play_index)
);

CREATE INDEX IF NOT EXISTS idx_statsapi_play_year ON statsapi_play (year);

-- ============================================================
-- statcast  (JSONB — volatile schema with 90+ MLB columns)
-- ============================================================

CREATE TABLE IF NOT EXISTS statcast (
  game_pk       INTEGER     NOT NULL,
  at_bat_number INTEGER     NOT NULL,
  pitch_number  INTEGER     NOT NULL,
  year          INTEGER,
  data          JSONB,
  scraped_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (game_pk, at_bat_number, pitch_number)
);

CREATE INDEX IF NOT EXISTS idx_statcast_year ON statcast (year);

-- ============================================================
-- player
-- ============================================================

CREATE TABLE IF NOT EXISTS player (
  player_id        INTEGER     PRIMARY KEY,
  name_full        TEXT,
  name_last        TEXT,
  name_first       TEXT,
  birth_date       DATE,
  birth_country    TEXT,
  bat_side         TEXT,
  throw_hand       TEXT,
  height           TEXT,
  weight           INTEGER,
  primary_position TEXT,
  scraped_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- season_summary  (JSONB — hitting/pitching have different cols)
-- ============================================================

CREATE TABLE IF NOT EXISTS season_summary (
  year          INTEGER NOT NULL,
  level         TEXT    NOT NULL,
  player_id     INTEGER NOT NULL,
  position_type TEXT    NOT NULL,
  data          JSONB,
  scraped_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (year, level, player_id, position_type)
);

CREATE INDEX IF NOT EXISTS idx_season_summary_player ON season_summary (player_id);
CREATE INDEX IF NOT EXISTS idx_season_summary_year   ON season_summary (year);

-- ============================================================
-- scrape_log
-- ============================================================

CREATE TABLE IF NOT EXISTS scrape_log (
  id              SERIAL      PRIMARY KEY,
  data_source     TEXT        NOT NULL,
  level           TEXT,
  date_start      DATE,
  date_end        DATE,
  status          TEXT        NOT NULL DEFAULT 'running',
  games_total     INTEGER,
  games_success   INTEGER,
  games_failed    INTEGER,
  failed_game_ids INTEGER[],
  error_message   TEXT,
  started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_scrape_log_status ON scrape_log (status);
CREATE INDEX IF NOT EXISTS idx_scrape_log_source ON scrape_log (data_source);
