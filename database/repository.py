from typing import Type, TypeVar, Generic, Optional, List, Dict, Any, Sequence
from sqlalchemy.orm import Session
from sqlalchemy import inspect, select, text, func
from sqlalchemy.exc import SQLAlchemyError
from loguru import logger
import pandas as pd

from database.models import Base, Stock, StockData, StockMinData


ModelT = TypeVar("ModelT", bound=Base)


class Repository(Generic[ModelT]):
    """Generic ORM repository providing basic CRUD and convenience helpers.

    Notes:
    - Methods try to be DB-agnostic and use ORM session operations.
    - For high-performance bulk operations consider using Core or
      SQLAlchemy's bulk_* APIs; these helpers provide safe, portable fallbacks.
    """

    def __init__(self, model: Type[ModelT], session: Session):
        self.model = model
        self.session = session

    def all(self) -> List[ModelT]:
        return self.session.query(self.model).all()

    def get(self, pk) -> Optional[ModelT]:
        """Get by primary key. pk may be a scalar or a tuple for composite PKs."""
        return self.session.get(self.model, pk)

    def add(self, instance: ModelT, commit: bool = True) -> ModelT:
        self.session.add(instance)
        if commit:
            self.session.commit()
        return instance

    def delete(self, pk, commit: bool = True) -> None:
        obj = self.get(pk)
        if obj:
            self.session.delete(obj)
            if commit:
                self.session.commit()

    def _primary_key_columns(self) -> List[str]:
        mapper = inspect(self.model)
        return [c.key for c in mapper.primary_key]

    def _pk_from_dict(self, values: Dict[str, Any]):
        pk_cols = self._primary_key_columns()
        if len(pk_cols) == 1:
            return values.get(pk_cols[0])
        return tuple(values.get(c) for c in pk_cols)

    def upsert_from_dict(self, values: Dict[str, Any], commit: bool = True) -> ModelT:
        """Insert or update a row using a dict of column values.

        This implementation performs a SELECT (session.get) then insert/update.
        It's portable but not the most efficient for very large batches.
        """
        pk = self._pk_from_dict(values)
        try:
            obj = self.get(pk)
            if obj is None:
                obj = self.model(**values)  # type: ignore[arg-type]
                self.session.add(obj)
            else:
                for k, v in values.items():
                    setattr(obj, k, v)
            if commit:
                self.session.commit()
            return obj
        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"upsert_from_dict failed for {self.model.__name__}: {e}")
            raise

    def bulk_upsert_from_dicts(self, rows: Sequence[Dict[str, Any]], batch_size: int = 500, commit: bool = True) -> None:
        """Naive bulk upsert using upsert_from_dict in batches.

        For large volumes consider replacing this with more efficient strategies:
        - session.bulk_insert_mappings for pure inserts
        - Core INSERT ... ON CONFLICT / ON DUPLICATE KEY for atomic upserts
        """
        total = len(rows)
        for i in range(0, total, batch_size):
            batch = rows[i : i + batch_size]
            try:
                for r in batch:
                    self.upsert_from_dict(r, commit=False)
                if commit:
                    self.session.commit()
            except SQLAlchemyError:
                self.session.rollback()
                logger.exception("bulk_upsert failed; rolled back batch")
                raise

    def bulk_insert_mappings(self, rows: Sequence[Dict[str, Any]], batch_size: int = 1000, commit: bool = True) -> None:
        """Use SQLAlchemy's bulk_insert_mappings for faster inserts (no ORM objects created).

        This is suitable when you know rows are new and you don't need ORM-level events.
        """
        total = len(rows)
        for i in range(0, total, batch_size):
            batch = rows[i : i + batch_size]
            try:
                self.session.bulk_insert_mappings(self.model, batch)
                if commit:
                    self.session.commit()
            except SQLAlchemyError:
                self.session.rollback()
                logger.exception("bulk_insert_mappings failed; rolled back batch")
                raise


class StockRepository(Repository[Stock]):
    def __init__(self, session: Session):
        super().__init__(Stock, session)


class StockDataRepository(Repository[StockData]):
    def __init__(self, session: Session):
        super().__init__(StockData, session)

    def get_max_update_date(self) -> Optional[Any]:
        """获取 stock_data 表中的最大日期值。"""
        stmt = select(func.max(self.model.date))
        return self.session.execute(stmt).scalar()

    def get_by_date(self, the_date) -> List[StockData]:
        stmt = select(self.model).where(self.model.date == the_date)
        res = self.session.execute(stmt).scalars().all()
        return res

    def get_by_code_and_date(self, code: str, the_date) -> Optional[StockData]:
        """Return a single StockData row for given code and date, or None if not found.

        code: stock code like '000001' or integer-like; function will zfill to 6 chars.
        the_date: a date object or ISO date string accepted by DB comparison.
        """
        code = str(code).zfill(6)
        stmt = select(self.model).where(self.model.code == code).where(self.model.date == the_date)
        return self.session.execute(stmt).scalars().one_or_none()

    def upsert_daily(self, values: Dict[str, Any], commit: bool = True) -> StockData:
        return self.upsert_from_dict(values, commit=commit)

    def bulk_upsert_from_df(self, df: pd.DataFrame, unit: str = 'share', batch_size: int = 1000) -> None:
        """
        批量将 DataFrame 的日线数据 upsert 到 stock_data 表。

        df: 必须包含 columns: code, date, open, high, low, close, volume
        unit: 'share' 表示 volume 为股；'hand' 表示 volume 为手(1 手 = 100 股)
        batch_size: 每次执行的批量大小
        """
        required = {'code', 'date', 'open', 'high', 'low', 'close', 'volume'}
        if not required.issubset(set(df.columns)):
            raise ValueError(f"DataFrame must contain columns: {required}")

        factor = 1 if unit == 'share' else 100

        df2 = df.copy()
        df2['date'] = pd.to_datetime(df2['date']).dt.date
        df2['volume'] = (df2['volume'].astype('int64') * factor).astype('int64')

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

        rows = [
            {
                'code': str(r['code']).zfill(6),
                'date': r['date'].isoformat(),
                'open': None if pd.isna(r['open']) else float(r['open']),
                'high': None if pd.isna(r['high']) else float(r['high']),
                'low': None if pd.isna(r['low']) else float(r['low']),
                'close': None if pd.isna(r['close']) else float(r['close']),
                'volume': None if pd.isna(r['volume']) else int(r['volume']),
            }
            for _, r in df2.iterrows()
        ]

        # use session's connection for executes
        try:
            total = len(rows)
            for i in range(0, total, batch_size):
                batch = rows[i : i + batch_size]
                self.session.execute(insert_sql, batch)
                self.session.commit()
        except SQLAlchemyError:
            self.session.rollback()
            logger.exception("bulk_upsert_from_df failed; rolled back")
            raise


class StockMinDataRepository(Repository[StockMinData]):
    def __init__(self, session: Session):
        super().__init__(StockMinData, session)

    def get_range(self, start_dt, end_dt) -> List[StockMinData]:
        stmt = select(self.model).where(self.model.datetime >= start_dt).where(self.model.datetime <= end_dt)
        return self.session.execute(stmt).scalars().all()
    
