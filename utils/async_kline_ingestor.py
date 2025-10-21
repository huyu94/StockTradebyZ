from __future__ import annotations

import asyncio
from typing import Optional
from concurrent.futures import ThreadPoolExecutor
import datetime as _dt
from loguru import logger

from utils.kline_ingestor import KlineIngestor
from database.core import StockCore


class AsyncKlineIngestor:
    """Lightweight async wrapper that runs the synchronous StockCrawler in a ThreadPoolExecutor.

    Purpose: enable an asyncio-based pipeline and task coordination while keeping existing
    synchronous tushare and SQLAlchemy code unchanged.

        Pattern:
            - producer: enumerates codes needing update and enqueues them
            - consumers: N workers that run `KlineIngestor.crawl_code_missing` in threadpool

    This keeps rate-limiting and DB calls synchronous but allows concurrent execution
    without blocking the asyncio loop.
    """

    def __init__(self, stock_core: Optional[StockCore] = None, workers: int = 5, max_threads: int = 10):
        self.stock_core = stock_core or StockCore()
        self.crawler = KlineIngestor(self.stock_core)
        self.workers = workers
        # ThreadPoolExecutor used to run blocking I/O
        self.executor = ThreadPoolExecutor(max_workers=max_threads)

    async def _producer(self, q: asyncio.Queue, end: str) -> None:
        today = _dt.date.today()
        codes = self.stock_core.stock.get_codes_needing_update(today)
        if not codes:
            logger.info("No codes need update (producer)")
            return
        for c in codes:
            await q.put(c)
        logger.info("Producer enqueued %d codes", len(codes))

    async def _consumer(self, q: asyncio.Queue, end: str) -> None:
        loop = asyncio.get_running_loop()
        while True:
            code = await q.get()
            try:
                # run blocking crawl in threadpool
                await loop.run_in_executor(self.executor, self.crawler.crawl_code_missing, code, '20190101', end)
            except Exception:
                logger.exception("Consumer failed for %s", code)
            finally:
                q.task_done()

    async def run(self, end: str, overlap_days: int = 3) -> None:
        """Run the ingestion pipeline until all enqueued codes are processed."""
        q: asyncio.Queue = asyncio.Queue()

        # start consumers
        consumers = [asyncio.create_task(self._consumer(q, end)) for _ in range(self.workers)]

        # run producer
        await self._producer(q, end)

        # wait until queue is empty
        await q.join()

        # cancel consumers
        for c in consumers:
            c.cancel()
        await asyncio.gather(*consumers, return_exceptions=True)

    def run_sync(self, end: str) -> None:
        """Blocking entrypoint for convenience (runs asyncio loop)."""
        asyncio.run(self.run(end))


if __name__ == '__main__':
    ingestor = AsyncKlineIngestor(workers=4, max_threads=8)
    ingestor.run_sync(end='20251021')
