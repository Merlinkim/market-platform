import asyncio
import json
from typing import Awaitable, Callable, Iterable, List, Optional

import websockets

from app.storage.postgres import insert_raw_trade

UPBIT_WS_URL = "wss://api.upbit.com/websocket/v1"


def _normalize_codes(codes: Iterable[str]) -> List[str]:
    return [c.strip().upper() for c in codes if c and c.strip()]


async def stream_trades(
    codes: Iterable[str],
    on_trade: Optional[Callable[[str, int, float, float], Awaitable[None]]] = None,
) -> None:
    """
    Upbit WebSocket trade stream consumer.
    - Raw trades are stored to DB.
    - If on_trade is provided, it is called for 1m bar aggregation: (code, trade_ms, price, volume)
    """
    codes = _normalize_codes(codes)
    if not codes:
        raise ValueError("codes is empty. Example: ['KRW-BTC', 'KRW-ETH']")

    subscribe_msg = [
        {"ticket": "market-platform-analytics"},
        {"type": "trade", "codes": codes, "isOnlyRealtime": True},
        {"format": "SIMPLE"},
    ]

    async with websockets.connect(
        UPBIT_WS_URL,
        ping_interval=30,
        ping_timeout=30,
        close_timeout=5,
        max_size=2**23,
    ) as ws:
        await ws.send(json.dumps(subscribe_msg))
        print(f"[upbit] subscribed trade: {codes}")

        while True:
            raw = await ws.recv()

            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8")

            data = json.loads(raw)

            code = data.get("cd")
            price = data.get("tp")
            volume = data.get("tv")
            ts = data.get("tms")
            side = data.get("ab")

            # 값 정리(타입 고정)
            trade_ms = int(ts) if ts is not None else None
            price_f = float(price) if price is not None else None
            vol_f = float(volume) if volume is not None else None

            # 디버그 로그(원하면 삭제 가능)
            print(f"[trade] {code} price={price_f} vol={vol_f} side={side} ts={trade_ms}")

            # raw 저장
            await insert_raw_trade(
                code=code,
                trade_ms=trade_ms,
                side=side,
                price=price_f,
                volume=vol_f,
                raw=data,
            )

            # 1분봉 집계 콜백(있으면)
            if (
                on_trade is not None
                and code is not None
                and trade_ms is not None
                and price_f is not None
                and vol_f is not None
            ):
                await on_trade(code, trade_ms, price_f, vol_f)

            await asyncio.sleep(0)
