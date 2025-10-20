Terminal close -- exit!
�表（匹配 models.py 中的 ORM 模型） ===
-- 说明：推荐先备份/删除旧表后再执行此脚本；如需无损变更请使用 Alembic 等迁移工具。

CREATE DATABASE IF NOT EXISTS amarket DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
USE amarket;

-- 为避免执行失败，先删除可能已存在的旧表（如你要保留历史数据请先备份）
DROP TABLE IF EXISTS stock_min_data;
DROP TABLE IF EXISTS stock_data;
DROP TABLE IF EXISTS stocks;

-- stocks: 股票基础信息（根据 models.py 字段）
CREATE TABLE stocks (
    code VARCHAR(12) NOT NULL PRIMARY KEY,                 -- 本表主键（兼容旧外键引用）
    ts_code VARCHAR(16) NOT NULL UNIQUE,                  -- Tushare 标准代码，如 '000001.SZ'
    name VARCHAR(64) NOT NULL,                            -- 简称
    cnspell VARCHAR(64),                                  -- 拼音缩写
    area VARCHAR(64),                                     -- 地域
    industry VARCHAR(64),                                 -- 行业
    market VARCHAR(64),                                   -- 市场类型（主板/创业板/科创板/CDR）
    exchange VARCHAR(16),                                 -- 交易所代码（SH/SZ/BJ）
    list_status VARCHAR(4),                               -- 上市状态 L上市 D退市 P暂停上市
    list_date DATE,                                       -- 上市日期
    delist_date DATE,                                     -- 退市日期
    is_hs VARCHAR(2),                                     -- 沪深港通标的（N/H/S）
    act_name VARCHAR(64),                                 -- 实控人名称
    act_ent_type VARCHAR(64)                              -- 实控人企业性质
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- stock_data: 日线 OHLCV
-- 建议：price 精度使用 DECIMAL(12,4)，volume 使用 BIGINT 保证容量；主键 (code, date)
CREATE TABLE stock_data (
    code VARCHAR(12) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(12,4),
    high DECIMAL(12,4),
    low DECIMAL(12,4),
    close DECIMAL(12,4),
    volume BIGINT UNSIGNED,            -- 以“股”为单位（若数据源为“手”，写入时乘以100）
    turnover DECIMAL(18,4) DEFAULT NULL, -- 成交额（可选，price * volume）
    market_cap BIGINT UNSIGNED DEFAULT NULL, -- 市值（可选，单位按项目约定，例如元）
    PRIMARY KEY (code, date),
    INDEX ix_stock_data_date (date),
    INDEX ix_stock_data_code (code)
    -- 如需外键约束可取消下一行注释，但会影响大批量写入性能
    -- , FOREIGN KEY (code) REFERENCES stocks(code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- stock_min_data: 保留分钟/逐笔结构（如果不使用可以删除此表）
CREATE TABLE stock_min_data (
    code VARCHAR(12) NOT NULL,
    datetime DATETIME NOT NULL,
    price DECIMAL(12,4),
    volume BIGINT UNSIGNED,
    direction VARCHAR(10),
    PRIMARY KEY (code, datetime),
    INDEX ix_stock_min_data_datetime (datetime)
    -- , FOREIGN KEY (code) REFERENCES stocks(code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 备注:
-- 1) 如果你选择直接 DROP 并重建表，请先备份旧数据：
--    mysqldump -u <user> -p amarket stocks stock_data > backup.sql
-- 2) 更安全且可维护的方式是使用 Alembic 做 schema migration，这样可以不丢失数据地变更列/类型。