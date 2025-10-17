from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, String, Integer, Date, DateTime, DECIMAL, ForeignKey, Index
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedColumn, Session



class Base(declarative_base):
    pass

class Stock(Base):
    __tablename__ = "stocks"
    code = Column(String(12), primary_key=True)
    name = Column(String(64), nullable=False)
    exchange = Column(String(16))
    sector = Column(String(64))
    market_cap = Column(DECIMAL(18, 2))
    listed_date = Column(Date)


class StockData(Base):
    __tablename__ = "stock_data"
    code = Column(String(12), ForeignKey("stocks.code"), primary_key=True)
    date = Column(Date, primary_key=True)
    open = Column(DECIMAL(10, 2))
    high = Column(DECIMAL(10, 2))
    low = Column(DECIMAL(10, 2))
    close = Column(DECIMAL(10, 2))
    volume = Column(Integer)

    stock = relationship("Stock", backref="daily_data")


class StockMinData(Base):
    __tablename__ = "stock_min_data"
    code = Column(String(12), ForeignKey("stocks.code"), primary_key=True)
    datetime = Column(DateTime, primary_key=True)
    price = Column(DECIMAL(10, 2))
    volume = Column(Integer)
    direction = Column(String(10))

    stock = relationship("Stock", backref="min_data")


Index("ix_stock_data_date", StockData.date)
Index("ix_stock_min_data_datetime", StockMinData.datetime)
