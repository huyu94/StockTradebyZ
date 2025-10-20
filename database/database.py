from __future__ import annotations

import os
from typing import Literal

import pandas as pd
from sqlalchemy import (
	create_engine,
	Column,
	String,
	Date,
	DECIMAL,
	Integer,
	text,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base, sessionmaker

# ---------- 配置（可按需改为从环境变量读取） ----------
username = 'huyu'
password = 'huyu6666'
host = 'localhost'
port = 3306
database_name = 'amarket'
DATABASE_URL = f'mysql+pymysql://{username}:{password}@{host}:{port}/{database_name}'

# ---------- ORM 定义 ----------
Base = declarative_base()


class Stock(Base):
	__tablename__ = 'stocks'
	code = Column(String(6), primary_key=True)
	name = Column(String(64), nullable=False)
	exchange = Column(String(10))
	sector = Column(String(64))
	market_cap = Column(DECIMAL(18, 2))
	listed_date = Column(Date)


class StockData(Base):
	__tablename__ = 'stock_data'
	code = Column(String(6), primary_key=True)
	date = Column(Date, primary_key=True)
	open = Column(DECIMAL(12, 4))
	high = Column(DECIMAL(12, 4))
	low = Column(DECIMAL(12, 4))
	close = Column(DECIMAL(12, 4))
	volume = Column(Integer)


# ---------- Engine / Session ----------
engine = create_engine(DATABASE_URL, pool_recycle=3600, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def get_session():
	return SessionLocal()


def ensure_tables():
	"""创建表（如果不存在）"""
	Base.metadata.create_all(engine)


# ---------- StockData 管理（支持批量 upsert） ----------
class StockDataManager:
	@staticmethod
	def bulk_upsert_from_df(
		df: pd.DataFrame,
		unit: Literal['share', 'hand'] = 'share',
		batch_size: int = 1000,
	) -> None:
		"""
		把包含日线数据的 DataFrame 批量 upsert 到 `stock_data` 表。

		df 要包含列: code, date, open, high, low, close, volume
		unit: 'share' 表示 volume 已是股数；'hand' 表示 volume 是手（1 手 = 100 股）
		batch_size: 每次提交的行数
		"""
		required = {'code', 'date', 'open', 'high', 'low', 'close', 'volume'}
		if not required.issubset(set(df.columns)):
			raise ValueError(f"DataFrame must contain columns: {required}")

		# 归一化 volume 到 股
		factor = 1 if unit == 'share' else 100
		df = df.copy()
		# 确保日期为 YYYY-MM-DD
		df['date'] = pd.to_datetime(df['date']).dt.date
		df['volume'] = (df['volume'].astype('int64') * factor).astype('int64')

		insert_sql = text(
			"""
			INSERT INTO stock_data (code, date, open, high, low, close, volume)
			VALUES (:code, :date, :open, :high, :low, :close, :volume)
			ON DUPLICATE KEY UPDATE
				`open` = VALUES(`open`),
				`high` = VALUES(`high`),
				`low`  = VALUES(`low`),
				`close`= VALUES(`close`),
				`volume` = VALUES(`volume`)
			"""
		)

		rows = (
			{
				'code': str(r['code']).zfill(6),
				'date': r['date'].isoformat(),
				'open': float(r['open']) if pd.notna(r['open']) else None,
				'high': float(r['high']) if pd.notna(r['high']) else None,
				'low': float(r['low']) if pd.notna(r['low']) else None,
				'close': float(r['close']) if pd.notna(r['close']) else None,
				'volume': int(r['volume']) if pd.notna(r['volume']) else None,
			}
			for _, r in df.iterrows()
		)

		# 批次提交
		conn = engine.connect()
		trans = conn.begin()
		try:
			batch = []
			count = 0
			for rec in rows:
				batch.append(rec)
				count += 1
				if count % batch_size == 0:
					conn.execute(insert_sql, batch)
					batch.clear()
			if batch:
				conn.execute(insert_sql, batch)
			trans.commit()
		except SQLAlchemyError as e:
			trans.rollback()
			raise
		finally:
			conn.close()


__all__ = [
	'engine',
	'SessionLocal',
	'Base',
	'Stock',
	'StockData',
	'ensure_tables',
	'StockDataManager',
]

import sqlalchemy
from sqlalchemy import create_engine

print("SQLAlchemy version:", sqlalchemy.__version__)