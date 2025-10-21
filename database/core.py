from __future__ import annotations

import os
from typing import List, Dict, Any, Optional

from sqlalchemy import create_engine, text, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from loguru import logger

import database.models as models
from database.repository import StockRepository, StockDataRepository

# 配置（优先使用环境变量，可按需修改）
DATABASE_URL = os.environ.get('DATABASE_URL') or f"mysql+pymysql://huyu:huyu6666@localhost:3306/amarket"

engine = create_engine(DATABASE_URL, pool_recycle=3600, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class StockFacade:
    def __init__(self, core: 'StockCore'):
        self.core = core

    def get_by_code(self, code: str):
        session = SessionLocal()
        try:
            repo = StockRepository(session)
            return repo.get(code)
        finally:
            session.close()
    
    def get_all(self,) -> List[models.Stock]:
        session = SessionLocal()
        try:
            repo = StockRepository(session)
            return repo.all()
        finally:
            session.close()

    def create(self, **values) -> Any:
        session = SessionLocal()
        try:
            repo = StockRepository(session)
            obj = repo.add(models.Stock(**values), commit=True)
            return obj
        finally:
            session.close()

    def update(self, **values) -> Any:
        session = SessionLocal()
        try:
            repo = StockRepository(session)
            obj = repo.upsert_from_dict(values)
            return obj
        finally:
            session.close()

    def refresh_last_update_dates(self) -> int:
        """Scan stock_data and update stocks.last_update_date for each code.

        Returns the number of stocks updated.
        """
        session = SessionLocal()
        try:
            # 使用原生 SQL 计算每个 code 的 MAX(date) 并更新 stocks 表
            sql = text(
                """
                UPDATE stocks s
                JOIN (
                    SELECT code, MAX(date) AS last_date
                    FROM stock_data
                    GROUP BY code
                ) sd ON s.code = sd.code
                SET s.last_update_date = sd.last_date
                """
            )
            res = session.execute(sql)
            session.commit()
            # res.rowcount 在某些 DBAPI 上为受影响的行数
            return res.rowcount or 0
        finally:
            session.close()

    def get_codes_needing_update(self, today) -> List[str]:
        """Return list of codes whose last_update_date is None or < today."""
        session = SessionLocal()
        try:
            stmt = select(models.Stock).where(
                (models.Stock.last_update_date == None) | (models.Stock.last_update_date < today)
            )
            rows = session.execute(stmt).scalars().all()
            return [r.code for r in rows]
        finally:
            session.close()


class StockDataFacade:
    def __init__(self, core: 'StockCore'):
        self.core = core

    def bulk_upsert(self, rows: List[Dict[str, Any]], batch_size: int = 1000) -> int:
        """批量 upsert 行（dict 列表）。返回处理的行数。"""
        if not rows:
            return 0
        session = SessionLocal()
        try:
            repo = StockDataRepository(session)
            # repo.bulk_upsert_from_dicts expects list of dicts
            repo.bulk_upsert_from_dicts(rows, batch_size=batch_size, commit=True)
            return len(rows)
        finally:
            session.close()

    def get_by_code_and_date(self, code: str, the_date) -> Optional[models.StockData]:
        """Return single StockData row for code and date or None."""
        session = SessionLocal()
        try:
            repo = StockDataRepository(session)
            return repo.get_by_code_and_date(code, the_date)
        finally:
            session.close()


class StockCore:
    def __init__(self):
        self.engine = engine
        self.stock = StockFacade(self)
        self.stock_data = StockDataFacade(self)

    def sync_update_missing_klines(self, end: str, overlap_days: int = 3, workers: int = 8):
        """Workflow:
        1. refresh_last_update_dates() to make stock.last_update_date current
        2. find codes with last_update_date < today and update each (concurrently)
        """
        # 1) refresh last_update_date from stock_data
        updated = self.stock.refresh_last_update_dates()
        logger.info("已刷新 stocks.last_update_date，共影响 %s 行", updated)

        # 2) compute today's date and find codes needing update
        import datetime as _dt
        today = _dt.date.today()
        codes = self.stock.get_codes_needing_update(today)
        if not codes:
            logger.info("所有股票均已更新到 %s，无需操作", today)
            return

        logger.info("发现 %d 支股票需要更新，将并发抓取至 %s", len(codes), end)

        from concurrent.futures import ThreadPoolExecutor, as_completed
        from tqdm import tqdm

        def _task(code):
            try:
                # reuse utils.fetch_one_to_mysql logic by importing the function
                from utils.fetch_stock_kline import fetch_one_to_mysql
                # fetch_one_to_mysql expects start/end in YYYYMMDD strings; start will be computed inside
                fetch_one_to_mysql(code, start='20190101', end=end, stock_core=self, overlap_days=overlap_days)
                return code, True
            except Exception as e:
                logger.exception("更新 %s 失败: %s", code, e)
                return code, False

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(_task, c) for c in codes]
            for f in tqdm(as_completed(futures), total=len(futures)):
                pass

        logger.info("增量更新任务完成")

    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError as e:
            logger.error(f"数据库连接测试失败: {e}")
            return False

    def create_tables(self) -> None:
        models.Base.metadata.create_all(self.engine)
