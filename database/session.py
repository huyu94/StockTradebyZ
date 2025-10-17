from __future__ import annotations

import json
from pathlib import Path
from typing import Tuple, Dict, Any

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from loguru import logger

from .models import Base, Stock, StockData


def _make_database_url(cfg: Dict[str, Any]) -> str:
    t = cfg.get("type", "sqlite")
    if t == "sqlite":
        # expected path in cfg['path']
        path = Path(cfg.get("path", "database/stock.db"))
        path.parent.mkdir(parents=True, exist_ok=True)
        # sqlite absolute path
        return f"sqlite:///{path.as_posix()}"
    elif t == "mysql":
        user = cfg.get("user")
        pw = cfg.get("password")
        host = cfg.get("host", "127.0.0.1")
        port = cfg.get("port", 3306)
        db = cfg.get("db")
        if user is None or db is None:
            raise ValueError("mysql cfg requires user and db")
        return f"mysql+pymysql://{user}:{pw}@{host}:{port}/{db}?charset=utf8mb4"
    else:
        raise ValueError("unsupported db type")


def create_engine_and_session(cfg: Dict[str, Any], echo: bool = False) -> Tuple[Any, sessionmaker]:
    """Create SQLAlchemy engine and sessionmaker from cfg.

    cfg examples:
      {'type':'sqlite','path':'d:/Workspaces/StockTradebyZ/database/stock.db'}
      {'type':'mysql','user':'u','password':'p','host':'127.0.0.1','port':3306,'db':'stock_db'}
    """
    url = _make_database_url(cfg)
    connect_args = {}
    if cfg.get("type") == "sqlite":
        connect_args["check_same_thread"] = False

    engine = create_engine(
        url,
        echo=echo,
        future=True,
        connect_args=connect_args,
        pool_pre_ping=True,
    )

    # enable foreign keys for sqlite
    if cfg.get("type") == "sqlite":
        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_connection, connection_record):
            try:
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()
            except Exception:
                pass

    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return engine, SessionLocal


def init_db(engine) -> None:
    """Create tables (safe to call multiple times)."""
    Base.metadata.create_all(engine)


def upsert_daily_stock(session: Session, code: str, date, open_p, high_p, low_p, close_p, volume: int) -> None:
    """Insert or update a daily stock row. Uses simple get/insert/update flow to keep portable across backends."""
    try:
        obj = session.get(StockData, (code, date))
        if obj is None:
            obj = StockData(code=code, date=date, open=open_p, high=high_p, low=low_p, close=close_p, volume=volume)
            session.add(obj)
        else:
            obj.open = open_p
            obj.high = high_p
            obj.low = low_p
            obj.close = close_p
            obj.volume = volume
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"upsert_daily_stock failed: {e}")
        raise


def get_session_from_cfg(cfg: Dict[str, Any], echo: bool = False) -> Tuple[Any, sessionmaker]:
    engine, SessionLocal = create_engine_and_session(cfg, echo=echo)
    return engine, SessionLocal


