import asyncio
import os

from app.ingest.upbit_ws import stream_trades
from app.storage.postgres import init_pool, close_pool
from app.services.bar_agg import BarAggregator1m


async def main() -> None:
    dsn = os.getenv("DATABASE_URL", "postgresql://market:marketpass@localhost:5432/market")
    await init_pool(dsn)

    agg = BarAggregator1m()

    try:
        await stream_trades(["KRW-BTC"], on_trade=agg.on_trade)
    finally:
        # 남은 분봉도 저장하고 종료(선택)
        await agg.flush_all()
        await close_pool()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[analytics-worker] stopped")
