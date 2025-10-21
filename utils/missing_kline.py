from __future__ import annotations

from typing import List, Set, Tuple, Optional, Iterable
from datetime import date, datetime, timedelta
import os
from loguru import logger
import pandas as pd

from database.core import StockCore
from sqlalchemy import text

import utils.tushare_api as TushareAPI


class MissingKlineFinder:
    """查找单只股票在指定时间区间内缺失的日线交易日期。

    用法示例：
        finder = MissingKlineFinder(stock_core)
        missing = finder.find_missing_dates('000001', '2020-01-01', '2025-01-01')
        ranges = finder.missing_ranges(missing)

    实现细节：优先使用 Tushare 的交易日历（当 tushare 可用且设置了 TUSHARE_TOKEN 时）。
    如果无法使用 tushare，则回退为交易日的工作日（日一到周五）计算。
    """

    def __init__(self, stock_core: StockCore, tushare_api: TushareAPI):
        self.stock_core = stock_core
        self.tushare = tushare_api

    # ----------------- 只提供一个对外接口 -----------------
    def find_stock_missing_date_ranges(self, 
                                       code: str, 
                                       start: str | date, 
                                       end: str | date) -> List[Tuple[date, date]]:
        """返回指定股票在区间内缺失的交易日期区间列表（start, end）。

        每个区间表示一段连续缺失的交易日，区间端点均为缺失日期。

        参数:
            code: 股票代码，6位字符串，如 '000001'
            start: 起始日期，字符串 'YYYY-MM-DD' 或 date 对象
            end: 结束日期，字符串 'YYYY-MM-DD' 或 date 对象

        返回:
            缺失日期区间列表，每个元素为 (start_date, end_date) 元组
        """
        trade_calendar_df = self.tushare.get_trade_calendar(start, end, is_open=True)
        exist_dates = set(self.stock_core.stock_data.get_stock_dates(code))
        trade_calendar_df['missing'] = True
        trade_calendar_df.loc[trade_calendar_df['cal_date'].dt.date.isin(exist_dates), 'missing'] = False
        missing_dates = sorted(trade_dates - exist_dates)
        # 将它整理成区间列表
        


        return ranges

    @staticmethod
    def _index_reducer(
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        """辅助函数：将 DataFrame 按连续索引分组，返回每组的起止索引。

        假设 df 有一列 'date'，且按日期排序。
        该函数会识别出 'date' 列中连续的日期段落，并返回每个段落的起始和结束索引。

        参数:
            df: 包含 'date' 列的 DataFrame，且按 'date' 升序排序

        返回:
            DataFrame，包含 'start_idx' 和 'end_idx' 两列，表示每个连续段落的起止索引
        """
        if df.empty:
            return pd.DataFrame(columns=['start_idx', 'end_idx'])

        df = df.reset_index(drop=True)
        df['date_diff'] = df['date'].diff().dt.days.fillna(1)
        df['group'] = (df['date_diff'] != 1).cumsum()

        ranges = df.groupby('group').agg(
            start_idx=('index', 'first'),
            end_idx=('index', 'last')
        ).reset_index(drop=True)

        return ranges
