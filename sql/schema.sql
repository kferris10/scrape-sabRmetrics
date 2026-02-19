-- Baseball data schema

CREATE TABLE IF NOT EXISTS schedule (
  game_id       TEXT PRIMARY KEY,
  year          INTEGER NOT NULL,
  date          DATE NOT NULL,
  level         TEXT NOT NULL,
  team_id_away  INTEGER,
  team_id_home  INTEGER,
  venue_id      INTEGER,
  data          JSONB
);

CREATE INDEX IF NOT EXISTS idx_schedule_date ON schedule (date);
CREATE INDEX IF NOT EXISTS idx_schedule_level ON schedule (level);

CREATE TABLE IF NOT EXISTS statsapi_event (
  game_id       TEXT NOT NULL,
  event_index   INTEGER NOT NULL,
  year          INTEGER,
  inning        INTEGER,
  half_inning   TEXT,
  batter_id     INTEGER,
  pitcher_id    INTEGER,
  event         TEXT,
  data          JSONB,
  PRIMARY KEY (game_id, event_index)
);

CREATE INDEX IF NOT EXISTS idx_statsapi_event_batter ON statsapi_event (batter_id);
CREATE INDEX IF NOT EXISTS idx_statsapi_event_pitcher ON statsapi_event (pitcher_id);

CREATE TABLE IF NOT EXISTS statsapi_pitch (
  game_id       TEXT NOT NULL,
  event_index   INTEGER NOT NULL,
  play_index    INTEGER NOT NULL,
  year          INTEGER,
  pitch_number  INTEGER,
  pitch_type    TEXT,
  data          JSONB,
  PRIMARY KEY (game_id, event_index, play_index)
);

CREATE TABLE IF NOT EXISTS statsapi_play (
  game_id       TEXT NOT NULL,
  event_index   INTEGER NOT NULL,
  play_index    INTEGER NOT NULL,
  year          INTEGER,
  type          TEXT,
  data          JSONB,
  PRIMARY KEY (game_id, event_index, play_index)
);

CREATE TABLE IF NOT EXISTS statcast (
  game_id       TEXT NOT NULL,
  at_bat_number INTEGER NOT NULL,
  pitch_number  INTEGER NOT NULL,
  data          JSONB,
  PRIMARY KEY (game_id, at_bat_number, pitch_number)
);

CREATE INDEX IF NOT EXISTS idx_statcast_game ON statcast (game_id);

CREATE TABLE IF NOT EXISTS player (
  player_id     INTEGER PRIMARY KEY,
  data          JSONB
);

CREATE TABLE IF NOT EXISTS season_summary (
  year          INTEGER NOT NULL,
  level         TEXT NOT NULL,
  player_id     INTEGER NOT NULL,
  position_type TEXT NOT NULL,
  game_type     TEXT NOT NULL,
  data          JSONB,
  PRIMARY KEY (year, level, player_id, position_type, game_type)
);

CREATE INDEX IF NOT EXISTS idx_season_summary_player ON season_summary (player_id);

CREATE TABLE IF NOT EXISTS scrape_log (
  id            SERIAL PRIMARY KEY,
  mode          TEXT NOT NULL,
  level         TEXT,
  start_date    DATE,
  end_date      DATE,
  status        TEXT NOT NULL DEFAULT 'running',
  started_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at   TIMESTAMPTZ,
  rows_written  INTEGER DEFAULT 0,
  failed_games  TEXT[],
  error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_scrape_log_status ON scrape_log (status);
CREATE INDEX IF NOT EXISTS idx_scrape_log_mode ON scrape_log (mode);
