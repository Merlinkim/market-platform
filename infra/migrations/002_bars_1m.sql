CREATE TABLE IF NOT EXISTS analytics.bars_1m (
  code          TEXT        NOT NULL,          -- KRW-BTC
  bucket_ms     BIGINT      NOT NULL,          -- minute start epoch ms
  bucket_ts     TIMESTAMPTZ NOT NULL,          -- minute start timestamp
  o             NUMERIC     NOT NULL,
  h             NUMERIC     NOT NULL,
  l             NUMERIC     NOT NULL,
  c             NUMERIC     NOT NULL,
  v             NUMERIC     NOT NULL,
  trade_count   INTEGER     NOT NULL,
  first_trade_ms BIGINT,
  last_trade_ms  BIGINT,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (code, bucket_ms)
);

CREATE INDEX IF NOT EXISTS ix_bars_1m_code_bucket_ms
  ON analytics.bars_1m (code, bucket_ms DESC);

CREATE INDEX IF NOT EXISTS ix_bars_1m_bucket_ts
  ON analytics.bars_1m (bucket_ts DESC);
