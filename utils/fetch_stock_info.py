from loguru import logger
import tushare as ts
from tqdm import tqdm
from project_var import OUTPUT_DIR, PROJECT_DIR, LOGGING_DIR
from database.core import StockCore


def fetch_stock_info_to_sql(stock_core: StockCore, **values):
    code = str(values['symbol']).zfill(6)
    ts_code = values['ts_code']
    exchange = "SH" if ts_code.endswith(".SH") else "SZ" if ts_code.endswith(".SZ") else "BJ"
    existing_stock = stock_core.stock.get_by_code(code)
    if not existing_stock:
        try:
            stock_core.stock.create(
                code=code,
                ts_code=ts_code,
                name=values.get('name'),
                cnspell=values.get('cnspell'),
                area=values.get('area'),
                industry=values.get('industry'),
                market=values.get('market'),
                exchange=exchange,
                list_status=values.get('list_status'),
                list_date=values.get('list_date'),
                delist_date=values.get('delist_date'),
                act_name=values.get('act_name'),
                act_ent_type=values.get('act_ent_type'),
            )
            logger.debug(f"创建股票基础信息: {code}")
        except Exception as e:
            logger.warning(f"创建股票基础信息失败 {code}: {e}")

def fetch_stock_list(stock_core: StockCore, use_sql: bool = True):
    pro = ts.pro_api()
    stock_df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,cnspell,' \
                                'market,list_status,list_date,delist_date, is_hs, act_name,act_ent_type, ')
    # 若无数据则直接返回
    if stock_df is None or stock_df.empty:
        logger.info("没有从 Tushare 获取到股票列表，跳过写入。")
        return
    logger.info(f"从 Tushare 获取到 {len(stock_df)} 只股票基础信息。")

    # 存储于本地CSV文件
    stock_df.to_csv(os.path.join(OUTPUT_DIR, 'stock_list.csv'), index=False)
    
    for _, row in tqdm(stock_df.iterrows(), total=len(stock_df), desc="写入股票基础信息"):
        fetch_stock_info_to_sql(stock_core, **row)


def main():
    stock_core = StockCore()
    fetch_stock_list(stock_core)

if __name__ == "__main__":
    main()