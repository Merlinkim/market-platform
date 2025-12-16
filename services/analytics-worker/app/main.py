import asyncio

from ingest.upbit_ws import stream_trades


async def main() -> None:
    # 최소 확인용: KRW-BTC 한 종목부터
    await stream_trades(["KRW-BTC"])


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[analytics-worker] stopped")
