#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime

from project_logging import init_logging, logger
from database.core import StockCore


def parse_date(s: str):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError("日期格式必须为 YYYY-MM-DD")


def main() -> None:
    init_logging()

    p = argparse.ArgumentParser(description="查询数据库中某只股票某日的日线数据")
    p.add_argument("code", help="股票代码，例如 000001 或 1")
    p.add_argument("date", type=parse_date, help="日期，格式 YYYY-MM-DD")
    args = p.parse_args()

    core = StockCore()
    row = core.stock_data.get_by_code_and_date(args.code, args.date)
    if row is None:
        logger.info("未找到对应数据: code=%s date=%s", args.code, args.date)
    else:
        logger.info(
            f"{row.code} {row.date} O={row.open} H={row.high} L={row.low} C={row.close} V={row.volume}",
            row.close,
            row.volume,
        )


if __name__ == '__main__':
    main()
