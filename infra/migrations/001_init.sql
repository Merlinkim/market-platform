CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.raw_trades (
  id           BIGSERIAL PRIMARY KEY,
  code         TEXT NOT NULL,                 -- 예: KRW-BTC
  trade_ms     BIGINT,                        -- Upbit tms (ms)
  trade_ts     TIMESTAMPTZ,                   -- (선택) ms를 ts로 변환해서 저장해도 됨
  side         TEXT,                          -- BID / ASK
  price        NUMERIC,
  volume       NUMERIC,
  raw          JSONB NOT NULL,                -- 원본 이벤트 저장
  received_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_raw_trades_code_trade_ms
  ON analytics.raw_trades (code, trade_ms DESC);

CREATE INDEX IF NOT EXISTS ix_raw_trades_received_at
  ON analytics.raw_trades (received_at DESC);
