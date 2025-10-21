"""
股票K线数据抓取工具 - MySQL版本
基于原有的fetch_kline.py，修改为直接存储到MySQL数据库
"""

from __future__ import annotations

import datetime as dt

from project_logging import logger
import random
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional
import os

import pandas as pd
import tushare as ts
from tqdm import tqdm

from constants import BAN_PATTERNS, COOLDOWN_SECS, DEFAULT_OVERLAP_DAYS, OLDEST_STOCK_DATE
from errors import RateLimitError
from sqlalchemy import text

# 导入新的数据库核心模块
from database.core import StockCore
from project_var import LOGGING_DIR, OUTPUT_DIR
from utils.tushare_utils import cool_sleep, looks_like_ip_ban
from utils.tushare_rate_limiter import TushareRateLimiter
from utils.tushare_api import TushareAPI

warnings.filterwarnings('ignore')

# --------------------------- 读取 stocklist.csv & 过滤板块 --------------------------- #
def _filter_by_boards_stocklist(df: pd.DataFrame, exclude_boards: set[str]) -> pd.DataFrame:
    """
    exclude_boards 子集：{'gem','star','bj'}
    - gem  : 创业板 300/301（.SZ）
    - star : 科创板 688（.SH）
    - bj   : 北交所（.BJ 或 4/8 开头）
    """
    code = df["symbol"].astype(str)
    ts_code = df["ts_code"].astype(str).str.upper()
    mask = pd.Series(True, index=df.index)

    if "gem" in exclude_boards:
        mask &= ~code.str.startswith(("300", "301"))
    if "star" in exclude_boards:
        mask &= ~code.str.startswith(("688",))
    if "bj" in exclude_boards:
        mask &= ~(ts_code.str.endswith(".BJ") | code.str.startswith(("4", "8")))

    return df[mask].copy()

def loads_codes_from_csv(stocklist_csv: Path, exclude_boards: set[str]) -> List[str]:
    """从 stocklist.csv 中获取股票代码列表"""
    df = pd.read_csv(stocklist_csv)    
    df = _filter_by_boards_stocklist(df, exclude_boards)
    codes = df["symbol"].astype(str).str.zfill(6).tolist()
    codes = list(dict.fromkeys(codes))  # 去重保持顺序
    logger.info("从 %s 读取到 %d 只股票（排除板块：%s）",
                stocklist_csv, len(codes), ",".join(sorted(exclude_boards)) or "无")
    return codes

def loads_codes_from_sql(stock_core: StockCore) -> List[str]:
    """从数据库中获取股票基本信息列表"""
    codes = stock_core.stock.get_all_codes()
    codes = [x.code for x in codes]
    return codes




def fetch_kline(
    code: str,
    start: str,
    end: str,
    tushare_api: TushareAPI,
    stock_core: StockCore,
):
    """
    @brief: 抓取单只股票数据并存储到MySQL
    @param: code: 股票代码（6位字符串）
    @param: start: 抓取起始日期（YYYY-MM-DD字符串）
    @param: end: 抓取结束日期（YYYY-MM-DD字符串）
    @param: pro: tushare pro_api 会话
    @param: stock_core: 数据库核心对象
    """
    if any([code is None, start is None, end is None]):
        logger.warning("code, start, end 不能为空")
        return 

    for attempt in range(1, 4):
        try:

            # 获取K线数据
            df = tushare_api.get_kline(code, start, end)
            # 确保股票基础信息存在
            check_stock_info_exist(code, stock_core)
            max_updated_date = df.trade_date.max()
            # 准备数据用于批量插入
            data_list = []
            for _, row in df.iterrows():
                data_list.append({
                    'code': code,
                    'date': row['trade_date'],
                    'open': float(row['open']) if pd.notna(row['open']) else None,
                    'high': float(row['high']) if pd.notna(row['high']) else None,
                    'low': float(row['low']) if pd.notna(row['low']) else None,
                    'close': float(row['close']) if pd.notna(row['close']) else None,
                    'pre_close': float(row['pre_close']) if pd.notna(row['pre_close']) else None,
                    'volume': int(row['vol']) if pd.notna(row['vol']) else None,
                    'change': float(row['change']) if pd.notna(row['change']) else None,
                    'amount': float(row['amount']) if pd.notna(row['amount']) else None,
                })
            
            # 批量插入/更新到数据库
            count = stock_core.stock_data.bulk_upsert(data_list)
            stock_core.stock.update(code=code, last_updated_date=max_updated_date)
            # logger.debug(f"已更新{count}行数据到数据库")
            break
            
        except Exception as e:
            if looks_like_ip_ban(e):
                logger.error(f"{code} 第 {attempt} 次抓取疑似被封禁，沉睡 {COOLDOWN_SECS} 秒")
                cool_sleep(COOLDOWN_SECS)
            else:
                silent_seconds = 15 * attempt
                logger.info(f"{code} 第 {attempt} 次抓取失败，{silent_seconds} 秒后重试：{e}")
                time.sleep(silent_seconds)
    else:
        logger.error("%s 三次抓取均失败，已跳过！", code)

def check_stock_info_exist(code: str, stock_core: StockCore):
    """检查数据库里是不是有这只股票的基本信息"""
    existing_stock = stock_core.stock.get_by_code(code)
    if not existing_stock:
        raise ValueError(f"数据库里找不到对应的股票基本信息，code:{code}")

def fetch_klines_async(stocks: list[dict],
                        tushare_api: TushareAPI,
                        stock_core: StockCore,
                        workers: int = 6):
    """
    @brief 获取所有股票数据
    @param workers: int
    @param stock_info : [{'code', 'start', 'end'}]
    """
    if stocks is None:
        logger.error(f"stock_info is None")
        return 

    logger.info(
        f"开始抓取{len(stocks)}支股票到MySQL | 数据源:Tushare(日线,qfq) | "
        f"抓取线程数:{workers}"
    )


    # 2. 根据codes异步下载股票K线数据
    # ---------- 多线程抓取到MySQL ---------- #
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                fetch_kline,
                stock.get('code'),
                stock.get('start'),
                stock.get('end'),
                tushare_api,
                stock_core,
            )
            for stock in stocks
        ]
        for _ in tqdm(as_completed(futures), total=len(futures), desc="下载进度"):
            pass

    logger.info("全部任务完成，数据已存储到MySQL数据库")


def calc_needed_update_stocks(stock_core:StockCore):
    """计算需要更新的股票列表"""
    today = dt.date.today()
    # 查询数据库，获取所有股票的最新交易日期
    all_stocks = stock_core.stock.get_all()
    update_stocks = []
    for stock in all_stocks:
        # stock is a models.Stock instance
        last_updated = getattr(stock, 'last_updated_date', None)
        if last_updated is None:
            # use configured oldest date
            last_updated = dt.datetime.strptime(OLDEST_STOCK_DATE, "%Y-%m-%d").date()
        # if last_updated is a datetime, convert to date
        if isinstance(last_updated, dt.datetime):
            last_updated = last_updated.date()

        if last_updated < today:
            update_stocks.append({
                'code': stock.code,
                'start': last_updated.strftime("%Y%m%d"),
                'end': today.strftime("%Y%m%d"),
            })
    return update_stocks

def update_database(tushare_api, stock_core):
    """更新所有股票K线数据到最新"""
    need_updated_stocks = calc_needed_update_stocks(stock_core=stock_core)
    fetch_klines_async(need_updated_stocks, tushare_api, stock_core, workers=8)


# --------------------------- 主入口 --------------------------- #
def main():
    tushare_api = TushareAPI() # 初始化 Tushare API
    stock_core = StockCore() # 初始化数据库
    # 测试数据库连接
    if not stock_core.test_connection():
        logger.error("数据库连接失败，请检查配置")
        sys.exit(1)

    # # 创建表（如果需要）
    # if create_tables:
    #     try:
    #         stock_core.create_tables()
    #         logger.info("数据表创建成功")
    #     except Exception as e:
    #         logger.error(f"数据表创建失败: {e}")
    #         sys.exit(1)


    update_database(tushare_api, stock_core)




    


if __name__ == "__main__":
    # 默认入口：直接在这里修改参数即可
    main()
