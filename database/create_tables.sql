-- 创建数据库
CREATE DATABASE IF NOT EXISTS stock_db DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 创建股票基础信息表
CREATE TABLE stocks (
    code VARCHAR(6) PRIMARY KEY,
    name VARCHAR(30) NOT NULL,
    exchange VARCHAR(10),
    sector VARCHAR(20),
    market_cap DECIMAL(12,2),
    listed_date DATE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建日线数据表
CREATE TABLE stock_data (
    code VARCHAR(6),
    date DATE NOT NULL,
    open DECIMAL(10,2),
    high DECIMAL(10,2),
    low DECIMAL(10,2),
    close DECIMAL(10,2),
    volume INT UNSIGNED,
    FOREIGN KEY (code) REFERENCES stocks(code),
    PRIMARY KEY (code, date),
    INDEX (date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建分钟线数据表
CREATE TABLE stock_min_data (
    code VARCHAR(6),
    datetime DATETIME,
    price DECIMAL(10,2),
    volume INT UNSIGNED,
    direction ENUM('BUY', 'SELL'),
    FOREIGN KEY (code) REFERENCES stocks(code),
    PRIMARY KEY (code, datetime),
    INDEX (datetime)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;