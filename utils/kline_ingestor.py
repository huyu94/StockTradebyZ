from __future__ import annotations

from typing import Optional, List, Tuple
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from loguru import logger
from sqlalchemy import text

from database.core import StockCore
from utils.tushare_api import TushareAPI
from utils.missing_kline import MissingKlineFinder


class KlineIngestor:
    """StockCrawler coordinates DB and Tushare to fetch missing daily K-lines.

    Responsibilities:
    1) find missing dates per stock (uses `MissingKlineFinder` backed by the DB trade calendar)
    2) convert missing-date ranges into tushare request params (start/end YYYYMMDD)
    3) call `TushareAPI.get_kline` to obtain a DataFrame and persist it into DB

    Usage examples:
    crawler = KlineIngestor()
        # fetch one code and store
        crawler.crawl_code_missing('000001', '20190101', '20251021')

        # concurrent update for all codes needing update up to `end`
        crawler.crawl_all_missing(end='20251021', workers=8)
    """

    def __init__(self, stock_core: Optional[StockCore] = None, tushare_api: Optional[TushareAPI] = None):
        self.stock_core = stock_core or StockCore()
        self.tushare = tushare_api or TushareAPI()
        # pass tushare pro instance to MissingKlineFinder so it can use trade_cal if available
        self.finder = MissingKlineFinder(self.stock_core, pro=getattr(self.tushare, 'pro', None))

    # ---------- low level helpers ----------
    def _df_to_rows(self, df: pd.DataFrame) -> List[dict]:
        """Normalize tushare DataFrame to list[dict] acceptable by repositories.

        Expected output keys (subset acceptable): code, date, open, high, low, close, pre_close, change, volume, amount
        """
        if df is None or df.empty:
            return []

        df2 = df.copy()
        # tushare returns ts_code like '000001.SZ' and trade_date as date
        if 'ts_code' in df2.columns:
            df2['code'] = df2['ts_code'].astype(str).str.split('.').str[0]
        elif 'code' in df2.columns:
            df2['code'] = df2['code'].astype(str)

        # normalize column names: trade_date -> date, vol -> volume
        if 'trade_date' in df2.columns:
            df2['date'] = pd.to_datetime(df2['trade_date']).dt.date
        if 'vol' in df2.columns and 'volume' not in df2.columns:
            df2['volume'] = df2['vol']

        rows = []
        for _, r in df2.iterrows():
            row = {
                'code': str(r.get('code')).zfill(6),
                'date': r.get('date'),
                'open': None if pd.isna(r.get('open')) else float(r.get('open')),
                'high': None if pd.isna(r.get('high')) else float(r.get('high')),
                'low': None if pd.isna(r.get('low')) else float(r.get('low')),
                'close': None if pd.isna(r.get('close')) else float(r.get('close')),
                'pre_close': None if pd.isna(r.get('pre_close')) else float(r.get('pre_close')),
                'change': None if pd.isna(r.get('change')) else float(r.get('change')),
                'volume': None if pd.isna(r.get('volume')) else int(r.get('volume')),
                'amount': None if pd.isna(r.get('amount')) else float(r.get('amount')),
            }
            rows.append(row)
        return rows

    def _update_last_date_for_code(self, code: str, last_date: date) -> None:
        """Update stocks.last_update_date for a single code to the given date."""
        code = str(code).zfill(6)
        sql = text("UPDATE stocks SET last_update_date = :d WHERE code = :code")
        with self.stock_core.engine.connect() as conn:
            conn.execute(sql, {"d": last_date, "code": code})
            conn.commit()

    # ---------- high-level operations ----------
    def crawl_code(self, code: str, start: str, end: str, overlap_days: int = 3) -> int:
        """Fetch kline for a single code between start and end (YYYYMMDD strings) and persist to DB.

        Returns number of rows persisted.
        """
        logger.info("Crawling %s from %s to %s", code, start, end)
        df = self.tushare.get_kline(code, start, end)
        if df is None or df.empty:
            logger.info("No data returned for %s %s-%s", code, start, end)
            return 0

        rows = self._df_to_rows(df)
        if not rows:
            logger.info("No rows to upsert for %s", code)
            return 0

        # persist via facade
        try:
            n = self.stock_core.stock_data.bulk_upsert(rows)
        except Exception:
            logger.exception("Failed to upsert rows for %s", code)
            raise

        # update last_update_date for this code using max date
        try:
            max_date = max(r['date'] for r in rows if r.get('date') is not None)
            if max_date:
                self._update_last_date_for_code(code, max_date)
        except Exception:
            logger.exception("Failed to update last date for %s", code)

        logger.info("Crawled %s rows for %s", len(rows), code)
        return len(rows)

    def crawl_code_missing(self, code: str, start: str, end: str, overlap_days: int = 3) -> List[Tuple[str, str]]:
        """Find missing ranges for a code and crawl each range. Returns list of attempted ranges (start,end strings)."""
        ranges = self.finder.missing_ranges(self.finder.find_missing_dates(code, start, end))
        attempts: List[Tuple[str, str]] = []
        for s, e in ranges:
            s_str = s.strftime('%Y%m%d')
            e_str = e.strftime('%Y%m%d')
            # fetch with a small overlap to be safe
            try:
                self.crawl_code(code, s_str, e_str, overlap_days=overlap_days)
                attempts.append((s_str, e_str))
            except Exception:
                logger.exception("crawl failed for %s %s-%s", code, s_str, e_str)
        return attempts

    def crawl_all_missing(self, end: str, overlap_days: int = 3, workers: int = 8) -> None:
        """Find codes needing update and crawl them concurrently up to `end` (YYYYMMDD)."""
        import datetime as _dt
        today = _dt.date.today()
        codes = self.stock_core.stock.get_codes_needing_update(today)
        if not codes:
            logger.info("No codes need update")
            return

        logger.info("Found %d codes to crawl (end=%s)", len(codes), end)

        def _task(c):
            try:
                self.crawl_code_missing(c, '20190101', end, overlap_days=overlap_days)
                return c, True
            except Exception as e:
                logger.exception("crawl failed for %s: %s", c, e)
                return c, False

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(_task, c) for c in codes]
            for f in as_completed(futures):
                _ = f.result()

        logger.info("crawl_all_missing finished")


if __name__ == '__main__':
    # quick manual run
    crawler = KlineIngestor()
    crawler.crawl_all_missing(end='20251021', workers=4)
