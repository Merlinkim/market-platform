import asyncio
import json
from typing import Iterable, List

import websockets

UPBIT_WS_URL = "wss://api.upbit.com/websocket/v1"


def _normalize_codes(codes: Iterable[str]) -> List[str]:
    # 예: "KRW-BTC" 형태 권장. 혹시 소문자 들어오면 대문자로 정규화.
    return [c.strip().upper() for c in codes if c and c.strip()]


async def stream_trades(codes: Iterable[str]) -> None:
    """
    Upbit WebSocket trade stream consumer.
    Prints trade events to stdout.
    """
    codes = _normalize_codes(codes)
    if not codes:
        raise ValueError("codes is empty. Example: ['KRW-BTC', 'KRW-ETH']")

    # Upbit WS는 바이너리 프레임으로 json이 오는 경우가 많아서 recv() 후 decode 필요
    subscribe_msg = [
        {"ticket": "market-platform-analytics"},
        {"type": "trade", "codes": codes, "isOnlyRealtime": True},
        {"format": "SIMPLE"},  # SIMPLE / DEFAULT
    ]

    async with websockets.connect(
        UPBIT_WS_URL,
        ping_interval=30,
        ping_timeout=30,
        close_timeout=5,
        max_size=2**23,  # 여유 있게
    ) as ws:
        await ws.send(json.dumps(subscribe_msg))
        print(f"[upbit] subscribed trade: {codes}")

        while True:
            raw = await ws.recv()

            # bytes -> str
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8")

            data = json.loads(raw)

            # SIMPLE 포맷 기준 필드 예: cd(코드), tp(체결가), tv(체결량), tms(체결시각 ms)
            code = data.get("cd")
            price = data.get("tp")
            volume = data.get("tv")
            ts = data.get("tms")

            # ask/bid 구분: ab (ASK/BID)
            side = data.get("ab")

            print(f"[trade] {code} price={price} vol={volume} side={side} ts={ts}")

            await asyncio.sleep(0)  # cooperative scheduling
