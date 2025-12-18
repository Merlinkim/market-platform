import asyncpg
from typing import Optional
import json

_pool: Optional[asyncpg.Pool] = None


async def init_pool(dsn: str) -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def insert_raw_trade(
    code: str,
    trade_ms: int | None,
    side: str | None,
    price: float | None,
    volume: float | None,
    raw: dict,
) -> None:
    if _pool is None:
        raise RuntimeError("DB pool is not initialized. Call init_pool() first.")

    # trade_ts는 DB에서 to_timestamp(ms/1000.0)로 저장
    await _pool.execute(
        """
        INSERT INTO analytics.raw_trades (code, trade_ms, trade_ts, side, price, volume, raw)
        VALUES (
          $1,
          $2::bigint,
          CASE WHEN $2 IS NULL THEN NULL ELSE to_timestamp(($2::bigint) / 1000.0) END,
          $3,
          $4::numeric,
          $5::numeric,
          $6::jsonb
        )
        """,
        code,
        trade_ms,
        side,
        price,
        volume,
        json.dumps(raw, ensure_ascii=False),
    )
    
async def upsert_bar_1m(
    code: str,
    bucket_ms: int,
    o: float,
    h: float,
    l: float,
    c: float,
    v: float,
    trade_count: int,
    first_trade_ms: int | None,
    last_trade_ms: int | None,
) -> None:
    if _pool is None:
        raise RuntimeError("DB pool is not initialized. Call init_pool() first.")

    await _pool.execute(
        """
        INSERT INTO analytics.bars_1m
          (code, bucket_ms, bucket_ts, o, h, l, c, v, trade_count, first_trade_ms, last_trade_ms, updated_at)
        VALUES
          ($1,
           $2::bigint,
           to_timestamp(($2::bigint) / 1000.0),
           $3::numeric, $4::numeric, $5::numeric, $6::numeric,
           $7::numeric,
           $8::int,
           $9::bigint,
           $10::bigint,
           now()
          )
        ON CONFLICT (code, bucket_ms)
        DO UPDATE SET
          bucket_ts     = EXCLUDED.bucket_ts,
          o             = EXCLUDED.o,
          h             = EXCLUDED.h,
          l             = EXCLUDED.l,
          c             = EXCLUDED.c,
          v             = EXCLUDED.v,
          trade_count   = EXCLUDED.trade_count,
          first_trade_ms= EXCLUDED.first_trade_ms,
          last_trade_ms = EXCLUDED.last_trade_ms,
          updated_at    = now();
        """,
        code,
        bucket_ms,
        o, h, l, c,
        v,
        trade_count,
        first_trade_ms,
        last_trade_ms,
    )