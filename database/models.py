from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, String, Integer, Date, DateTime, DECIMAL, ForeignKey, Index
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedColumn, Session



# Declarative base class for ORM models
Base = declarative_base()

class Stock(Base):
    __tablename__ = "stocks"
    code = Column(String(12), primary_key=True)  # 本表主键：symbol/股票代码（兼容原有外键引用，通常为6位）
    ts_code = Column(String(16), unique=True, nullable=False)  # Tushare 标准代码（如 '000001.SZ'），唯一
    name = Column(String(64), nullable=False)  # 股票简称/名称
    cnspell = Column(String(64))  # 拼音缩写，用于检索或显示
    area = Column(String(64))  # 地域（省/市）
    industry = Column(String(64))  # 所属行业
    market = Column(String(64))  # 市场类型（主板/创业板/科创板/CDR 等）
    exchange = Column(String(16))  # 交易所代码（可重复，示例 'SH'/'SZ'/'BJ'）
    list_status = Column(String(4))  # 上市状态: 'L' 上市, 'D' 退市, 'P' 暂停上市
    list_date = Column(Date)  # 上市日期（YYYY-MM-DD）
    delist_date = Column(Date)  # 退市日期（若已退市）
    act_name = Column(String(64))  # 实际控制人名称
    act_ent_type = Column(String(64))  # 实控人企业性质
    last_updated_date = Column(Date)  # 最近入库的日线日期（便于增量更新）


class StockData(Base):
    __tablename__ = "stock_data"
    code = Column(String(12), ForeignKey("stocks.code"), primary_key=True)  # 股票代码，外键引用 stocks.code，为复合主键的一部分
    date = Column(Date, primary_key=True)  # 交易日（YYYY-MM-DD），复合主键的一部分
    open = Column(DECIMAL(10, 2))  # 当日开盘价
    high = Column(DECIMAL(10, 2))  # 当日最高价
    low = Column(DECIMAL(10, 2))  # 当日最低价
    close = Column(DECIMAL(10, 2))  # 当日收盘价
    pre_close = Column(DECIMAL(10, 2))  # 昨日收盘价
    change = Column(DECIMAL(10, 2))  # 涨跌额
    volume = Column(Integer)  # 成交量，整数。注意数据源单位可能为“手”或“股”，需在写入时统一说明/转换
    amount = Column(DECIMAL(10, 2))  # 成交额，保留两位小数
    stock = relationship("Stock", backref="daily_data")  # ORM 关系，方便通过 Stock 访问其日线数据


class StockMinData(Base):
    __tablename__ = "stock_min_data"
    code = Column(String(12), ForeignKey("stocks.code"), primary_key=True)  # 股票代码，外键引用 stocks.code
    datetime = Column(DateTime, primary_key=True)  # 精确到秒的时间戳（分钟/分时/逐笔数据的时间点），复合主键的一部分
    price = Column(DECIMAL(10, 2))  # 该时间点的价格
    volume = Column(Integer)  # 该时间点/周期的成交量（整数）
    direction = Column(String(10))  # 成交方向，可为 'BUY'/'SELL' 或其他约定值

    stock = relationship("Stock", backref="min_data")  # ORM 关系，方便通过 Stock 访问分钟/逐笔数据


Index("ix_stock_data_date", StockData.date)
Index("ix_stock_min_data_datetime", StockMinData.datetime)
