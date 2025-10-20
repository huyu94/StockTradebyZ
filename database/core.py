from __future__ import annotations

import os
from typing import List, Dict, Any

from sqlalchemy import create_engine, text
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
    
    def get_all_codes(self,) -> List[models.Stock]:
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


class StockCore:
    def __init__(self):
        self.engine = engine
        self.stock = StockFacade(self)
        self.stock_data = StockDataFacade(self)

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
