"""
股票K线数据抓取工具 - MySQL版本
基于原有的fetch_kline.py，修改为直接存储到MySQL数据库
"""

from __future__ import annotations

import argparse
import datetime as dt

from loguru import logger
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

import threading 
from collections import deque

# 导入新的数据库核心模块
from database.core import StockCore
from project_var import LOGGING_DIR, OUTPUT_DIR

# --------------------------- Tushare 频率控制 ----------------------- # 

class TushareRateLimiter:
    def __init__(self, max_calls=200, time_window=60):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = deque()
        self.lock = threading.Lock()

    def wait_if_needed(self):
        with self.lock:
            now = time.time()
            # 清理超过时间窗口的记录
            while self.calls and self.calls[0] < now - self.time_window:
                self.calls.popleft()
            
            # 如果达到限制，等待
            if len(self.calls) >= self.max_calls:
                sleep_time = self.time_window - (now - self.calls[0]) + 1
                time.sleep(sleep_time)
            
                # 重新清理
                now = time.time()
                while self.calls and now - self.calls[0] >= self.time_window:
                    self.calls.popleft()

            # 记录本次调用
            self.calls.append(now)

# 全局频率限制器
tushare_limiter = TushareRateLimiter()

warnings.filterwarnings("ignore")

# --------------------------- 全局日志配置 --------------------------- #
logger.remove()
logger.add(
    sys.stdout,
    level="DEBUG",
    colorize=True,
)
logger.add(
    os.path.join(LOGGING_DIR, "fetch_stock_kline.log"),
    level="DEBUG",
    rotation="10 MB",
    colorize=True
)

# --------------------------- 限流/封禁处理配置 --------------------------- #
COOLDOWN_SECS = 600
BAN_PATTERNS = (
    "访问频繁", "请稍后", "超过频率", "频繁访问",
    "too many requests", "429",
    "forbidden", "403",
    "max retries exceeded"
)

def _looks_like_ip_ban(exc: Exception) -> bool:
    msg = (str(exc) or "").lower()
    return any(pat in msg for pat in BAN_PATTERNS)

class RateLimitError(RuntimeError):
    """表示命中限流/封禁，需要长时间冷却后重试。"""
    pass

def _cool_sleep(base_seconds: int) -> None:
    jitter = random.uniform(0.9, 1.2)
    sleep_s = max(1, int(base_seconds * jitter))
    logger.warning("疑似被限流/封禁，进入冷却期 %d 秒...", sleep_s)
    time.sleep(sleep_s)

# --------------------------- 历史K线（Tushare 日线，固定qfq） --------------------------- #
pro: Optional[ts.pro_api] = None  # 模块级会话

def set_api(session) -> None:
    """由外部(比如GUI)注入已创建好的 ts.pro_api() 会话"""
    global pro
    pro = session
    
def _to_ts_code(code: str) -> str:
    """把6位code映射到标准 ts_code 后缀。"""
    code = str(code).zfill(6)
    if code.startswith(("60", "68", "9")):
        return f"{code}.SH"
    elif code.startswith(("4", "8")):
        return f"{code}.BJ"
    else:
        return f"{code}.SZ"

def _get_kline_tushare(code: str, start: str, end: str) -> pd.DataFrame:
    """从tushare获取单只股票的K线数据"""
    ts_code = _to_ts_code(code)
    try:
        # 应用频率限制
        tushare_limiter.wait_if_needed()
        
        df = ts.pro_bar(
            ts_code=ts_code,
            adj="qfq",
            start_date=start,
            end_date=end,
            freq="D",
            api=pro
        )
    except Exception as e:
        if _looks_like_ip_ban(e):
            raise RateLimitError(str(e)) from e
        raise

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.rename(columns={"trade_date": "date", "vol": "volume"})[
        ["date", "open", "close", "high", "low", "volume"]
    ].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for c in ["open", "close", "high", "low", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values("date").reset_index(drop=True)

def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.drop_duplicates(subset="date").sort_values("date").reset_index(drop=True)
    if df["date"].isna().any():
        raise ValueError("存在缺失日期！")
    if (df["date"] > pd.Timestamp.today().date()).any():
        raise ValueError("数据包含未来日期，可能抓取错误！")
    return df

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

# --------------------------- 单只抓取（存储到MySQL） --------------------------- #
def fetch_one_to_mysql(
    code: str,
    start: str,
    end: str,
    stock_core: StockCore,
):
    """抓取单只股票数据并存储到MySQL"""
    for attempt in range(1, 4):
        try:
            # 获取K线数据
            new_df = _get_kline_tushare(code, start, end)
            if new_df.empty:
                logger.debug("%s 无数据，跳过。", code)
                return
            
            new_df = validate(new_df)
            
            # 确保股票基础信息存在
            check_stock_info_exist(code, stock_core)
            
            # 准备数据用于批量插入
            data_list = []
            for _, row in new_df.iterrows():
                data_list.append({
                    'code': code,
                    'date': row['date'],
                    'open': float(row['open']) if pd.notna(row['open']) else None,
                    'high': float(row['high']) if pd.notna(row['high']) else None,
                    'low': float(row['low']) if pd.notna(row['low']) else None,
                    'close': float(row['close']) if pd.notna(row['close']) else None,
                    'volume': int(row['volume']) if pd.notna(row['volume']) else None
                })
            
            # 批量插入/更新到数据库
            count = stock_core.stock_data.bulk_upsert(data_list)
            # logger.info(f"✓ {code} 数据入库完成，共 {count} 条记录")
            break
            
        except Exception as e:
            if _looks_like_ip_ban(e):
                logger.error(f"{code} 第 {attempt} 次抓取疑似被封禁，沉睡 {COOLDOWN_SECS} 秒")
                _cool_sleep(COOLDOWN_SECS)
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

def fetch_all_klines(start, end, exclude_boards, workers, specify_stock_codes: list=[]):
    """获取所有股票数据"""
    stock_core = StockCore()
    # 1. 获取codes 
    codes = []
    if specify_stock_codes:
        codes = specify_stock_codes
    else:
        codes = loads_codes_from_sql(stock_core)
    if not codes:
        logger.error("stocklist 为空或被过滤后无代码，请检查。")
        return 

    logger.info(
        f"开始抓取{len(codes)}支股票到MySQL | 数据源:Tushare(日线,qfq) | "
        f"日期:{start} → {end} | 排除:{','.join(sorted(exclude_boards)) or '无'} | "
        f"抓取线程数:{workers}"
    )


    # 2. 根据codes异步下载股票K线数据
    # ---------- 多线程抓取到MySQL ---------- #
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                fetch_one_to_mysql,
                code,
                start,
                end,
                stock_core,
            )
            for code in codes
        ]
        for _ in tqdm(as_completed(futures), total=len(futures), desc="下载进度"):
            pass

    logger.info("全部任务完成，数据已存储到MySQL数据库")


# --------------------------- 主入口 --------------------------- #
def main():

    # ---------- Tushare Token ---------- #
    os.environ["NO_PROXY"] = "api.waditu.com,.waditu.com,waditu.com"
    os.environ["no_proxy"] = os.environ["NO_PROXY"]
    ts_token = os.environ.get("TUSHARE_TOKEN")
    if not ts_token:
        raise ValueError("请先设置环境变量 TUSHARE_TOKEN，例如：export TUSHARE_TOKEN=你的token")
    ts.set_token(ts_token)
    global pro
    pro = ts.pro_api()

    # ---------- 初始化数据库 ---------- #
    stock_core = StockCore()
    
    # 测试数据库连接
    if not stock_core.test_connection():
        logger.error("数据库连接失败，请检查配置")
        sys.exit(1)
    
    # 创建表（如果需要）
    if args.create_tables:
        try:
            stock_core.create_tables()
            logger.info("数据表创建成功")
        except Exception as e:
            logger.error(f"数据表创建失败: {e}")
            sys.exit(1)

    # ---------- 日期解析 ---------- #
    start = dt.date.today().strftime("%Y%m%d") if str(args.start).lower() == "today" else args.start
    end = dt.date.today().strftime("%Y%m%d") if str(args.end).lower() == "today" else args.end

    # ---------- 从 stocklist.csv 读取股票池 ---------- #
    exclude_boards = set(args.exclude_boards or [])
    codes = load_codes_from_stocklist(args.stocklist, exclude_boards)






if __name__ == "__main__":
    fetch_all_klines(start='20250901',
                     end='20251020',
                     workers=8,
                     exclude_boards=[],
                     specify_stock_codes=[],
                     )
