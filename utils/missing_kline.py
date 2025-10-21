from __future__ import annotations

from typing import List, Set, Tuple, Optional, Iterable
from datetime import date, datetime, timedelta
import os
import logging

import pandas as pd

from database.core import StockCore
from sqlalchemy import text

try:
    import tushare as ts
except Exception:  # pragma: no cover - optional dependency
    ts = None

logger = logging.getLogger(__name__)


class MissingKlineFinder:
    """Find missing daily kline dates for a single stock between two dates.

    Usage:
        finder = MissingKlineFinder(stock_core)
        missing = finder.find_missing_dates('000001', '2020-01-01', '2025-01-01')
        ranges = finder.missing_ranges(missing)

    The finder will try to use Tushare trade calendar (if tushare available and TUSHARE_TOKEN set).
    If not available it falls back to business-day (Mon-Fri) calendar.
    """

    def __init__(self, stock_core: StockCore, pro: Optional[object] = None):
        self.stock_core = stock_core
        self.pro = pro or (ts.pro_api() if ts and os.environ.get("TUSHARE_TOKEN") else None)
        self._cal_cache: dict[Tuple[str, str], Set[date]] = {}

    # ----------------- utilities -----------------
    def _to_date(self, v) -> date:
        if isinstance(v, date):
            return v
        if isinstance(v, datetime):
            return v.date()
        return pd.to_datetime(v).date()

    # ----------------- expected trading days -----------------
    def get_expected_dates(self, start: str | date, end: str | date, exchange: str = "") -> Set[date]:
        """Return set of expected trading dates between start and end (inclusive).

        If tushare pro is available, uses trade_cal with is_open=1; otherwise falls back to bdate_range.
        """
        start_d = self._to_date(start)
        end_d = self._to_date(end)
        key = (start_d.isoformat(), end_d.isoformat())
        if key in self._cal_cache:
            return self._cal_cache[key]

        if self.pro is not None:
            try:
                sd = start_d.strftime("%Y%m%d")
                ed = end_d.strftime("%Y%m%d")
                df = self.pro.trade_cal(exchange=exchange or "", start_date=sd, end_date=ed)
                if df is not None and not df.empty:
                    df = df[df['is_open'] == 1]
                    dates = set(pd.to_datetime(df['cal_date']).dt.date.tolist())
                    self._cal_cache[key] = dates
                    return dates
            except Exception as e:
                logger.debug("tushare trade_cal failed, falling back to business days: %s", e)

        # fallback: business days (Mon-Fri)
        rng = pd.bdate_range(start=start_d, end=end_d)
        dates = set(pd.to_datetime(rng).date)
        self._cal_cache[key] = dates
        return dates

    # ----------------- existing dates in DB -----------------
    def get_existing_dates(self, code: str, start: str | date, end: str | date) -> Set[date]:
        """Query DB for existing stock_data dates for code between start and end (inclusive)."""
        start_d = self._to_date(start)
        end_d = self._to_date(end)
        sql = text("SELECT date FROM stock_data WHERE code = :code AND date BETWEEN :start AND :end")
        with self.stock_core.engine.connect() as conn:
            rows = conn.execute(sql, {"code": str(code).zfill(6), "start": start_d, "end": end_d}).fetchall()
        dates = set()
        for r in rows:
            val = r[0]
            if val is None:
                continue
            if isinstance(val, date):
                dates.add(val)
            else:
                dates.add(pd.to_datetime(val).date())
        return dates

    # ----------------- find missing -----------------
    def find_missing_dates(self, code: str, start: str | date, end: str | date, exchange: str = "") -> List[date]:
        """Return sorted list of missing trading dates for given stock between start and end."""
        expected = self.get_expected_dates(start, end, exchange=exchange)
        existing = self.get_existing_dates(code, start, end)
        missing = sorted(expected - existing)
        return missing

    def missing_ranges(self, missing_dates: Iterable[date], max_span_days: int = 365) -> List[Tuple[date, date]]:
        """Merge sorted missing dates into continuous ranges.

        max_span_days limits the maximum span for a single range (helps avoid huge API calls).
        """
        dates = sorted(missing_dates)
        if not dates:
            return []
        ranges: List[Tuple[date, date]] = []
        start = prev = dates[0]
        for d in dates[1:]:
            if (d - prev).days == 1 and (d - start).days < max_span_days:
                prev = d
                continue
            ranges.append((start, prev))
            start = prev = d
        ranges.append((start, prev))
        return ranges

    # ----------------- optional helper: fetch and upsert -----------------
    def fetch_and_upsert_missing(self, code: str, start: str | date, end: str | date, overlap_days: int = 3) -> List[Tuple[date, date]]:
        """Find missing ranges and call existing fetch_one_to_mysql for each range.

        Returns list of attempted ranges.
        """
        ranges = self.missing_ranges(self.find_missing_dates(code, start, end))
        if not ranges:
            return []

        # import function lazily to avoid circular imports
        try:
            from utils.fetch_stock_kline import fetch_one_to_mysql
        except Exception as e:
            raise RuntimeError("fetch_one_to_mysql not available: %s" % e)

        for s, e in ranges:
            s_str = s.strftime("%Y%m%d")
            e_str = e.strftime("%Y%m%d")
            # fetch_one_to_mysql will itself compute calc_start using overlap_days based on DB
            fetch_one_to_mysql(code, s_str, e_str, self.stock_core, overlap_days=overlap_days)

        return ranges
