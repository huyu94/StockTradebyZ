import os, sys
import tushare as ts
import pandas as pd
from loguru import logger

class TushareAPI:
    def __init__(self):
        self.pro = None

    def init(self, ts_token: str = None):
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
        return self
    
    def get_kline(self, code: str, start: str, end: str) -> pd.DataFrame:
        """获取单只股票的K线数据"""
        if self.pro is None:
            raise RuntimeError("Tushare API 未初始化，请先调用 init() 方法")
        
        if code.endswith(('.SH', '.SZ', '.BJ')):
            ts_code = code
        else:
            ts_code = self._to_ts_code(code)
        df = ts.pro_bar(
            ts_code=ts_code,
            adj="qfq",
            start_date=start,
            end_date=end,
            freq="D",
            api=self.pro
        )
        return df
    

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


if __name__ == "__main__":

    tushare_api = TushareAPI().init()
    df = tushare_api.get_kline("000001", "2022-01-01", "2022-12-31")
    print(df)