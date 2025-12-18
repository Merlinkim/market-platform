from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

from app.storage.postgres import upsert_bar_1m


def minute_bucket_ms(ts_ms: int) -> int:
    return (ts_ms // 60_000) * 60_000


@dataclass
class BarState:
    code: str
    bucket_ms: int
    o: float
    h: float
    l: float
    c: float
    v: float
    trade_count: int
    first_trade_ms: int
    last_trade_ms: int

    def update(self, price: float, volume: float, ts_ms: int) -> None:
        if price > self.h:
            self.h = price
        if price < self.l:
            self.l = price
        self.c = price
        self.v += volume
        self.trade_count += 1
        if ts_ms < self.first_trade_ms:
            self.first_trade_ms = ts_ms
        if ts_ms > self.last_trade_ms:
            self.last_trade_ms = ts_ms


class BarAggregator1m:
    """
    - 코드별로 현재 분봉을 메모리에 유지
    - 분이 바뀌는 순간 이전 분봉을 DB에 upsert
    """
    def __init__(self) -> None:
        self._bars: Dict[str, BarState] = {}

    async def on_trade(self, code: str, ts_ms: int, price: float, volume: float) -> None:
        bkt = minute_bucket_ms(ts_ms)
        cur = self._bars.get(code)

        # 첫 진입
        if cur is None:
            self._bars[code] = BarState(
                code=code,
                bucket_ms=bkt,
                o=price, h=price, l=price, c=price,
                v=volume,
                trade_count=1,
                first_trade_ms=ts_ms,
                last_trade_ms=ts_ms,
            )
            return

        # 같은 분이면 업데이트
        if bkt == cur.bucket_ms:
            cur.update(price, volume, ts_ms)
            return

        # 분이 바뀌면: 이전 bar flush 후 새 bar 시작
        await self._flush(cur)

        self._bars[code] = BarState(
            code=code,
            bucket_ms=bkt,
            o=price, h=price, l=price, c=price,
            v=volume,
            trade_count=1,
            first_trade_ms=ts_ms,
            last_trade_ms=ts_ms,
        )

    async def flush_all(self) -> None:
        # 종료 시점 등에서 남은 bar 저장(선택)
        for cur in list(self._bars.values()):
            await self._flush(cur)

    async def _flush(self, bar: BarState) -> None:
        await upsert_bar_1m(
            code=bar.code,
            bucket_ms=bar.bucket_ms,
            o=bar.o, h=bar.h, l=bar.l, c=bar.c,
            v=bar.v,
            trade_count=bar.trade_count,
            first_trade_ms=bar.first_trade_ms,
            last_trade_ms=bar.last_trade_ms,
        )
