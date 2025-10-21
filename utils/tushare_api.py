import os, sys
import tushare as ts
import pandas as pd
from loguru import logger
from utils.tushare_rate_limiter import TushareRateLimiter
from utils.tushare_utils import looks_like_ip_ban
from errors import RateLimitError

class TushareAPI:
    def __init__(self, ts_token: str | None = None) -> None:
        """初始化 Tushare API 会话"""
        os.environ["NO_PROXY"] = "api.waditu.com,.waditu.com,waditu.com"
        os.environ["no_proxy"] = os.environ["NO_PROXY"]
        if ts_token is None:
            ts_token = os.environ.get("TUSHARE_TOKEN")
        if not ts_token:
            raise ValueError("请先设置环境变量 TUSHARE_TOKEN，例如：export TUSHARE_TOKEN=你的token")
        
        ts.set_token(ts_token)
        self.pro = ts.pro_api()
        logger.info("Tushare API 初始化成功")
        # 全局频率限制器
        self.tushare_limiter = TushareRateLimiter()

    
    def get_kline(self, code: str, start: str, end: str) -> pd.DataFrame:
        """获取单只股票的K线数据"""
        if self.pro is None:
            raise RuntimeError("Tushare API 未初始化，请先调用 init() 方法")

        if code.endswith(('.SH', '.SZ', '.BJ')):
            ts_code = code
        else:
            ts_code = self._to_ts_code(code)

        # 应用频率限制
        self.tushare_limiter.wait_if_needed()
        df = ts.pro_bar(
            ts_code=ts_code,
            adj="qfq",
            start_date=start,
            end_date=end,
            freq="D",
            api=self.pro,
        )

        # validate and normalize trade_date -> date
        df = self.validate(df)

        # 返回按 ts_code, trade_date 排序（trade_date 已是 date 类型）
        return df.sort_values(by=["ts_code", "trade_date"]).reset_index(drop=True)

    @staticmethod
    def _to_ts_code(code: str) -> str:
        """把6位code映射到标准 ts_code 后缀。"""
        code = str(code).zfill(6)
        if code.startswith(("60", "68", "9")):
            return f"{code}.SH"
        elif code.startswith(("4", "8")):
            return f"{code}.BJ"
        else:
            return f"{code}.SZ"


    @staticmethod
    def validate(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            raise ValueError("数据为空！")
        # ensure trade_date is a date object (handles strings like '20220104')
        df = df.drop_duplicates(subset="trade_date").reset_index(drop=True)
        try:
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        except Exception:
            # if parsing fails, keep original and raise later
            pass

        if df["trade_date"].isna().any():
            raise ValueError("存在缺失日期！")
        today = pd.Timestamp.today().date()
        # compare with date objects
        if (df["trade_date"] > today).any():
            raise ValueError("数据包含未来日期，可能抓取错误！")
        return df

if __name__ == "__main__":

    tushare_api = TushareAPI()
    df = tushare_api.get_kline("000001", "2022-01-01", "2022-12-31")
    print(df)