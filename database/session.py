import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from loguru import logger
from sqlalchemy import text
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, String
metadata_obj = MetaData()
user_table = Table(
    "user_account",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("name", String(30)),
    Column("fullname", String),
)
print(metadata_obj)

username = 'huyu'
password = 'huyu6666'
host = 'localhost'
port = 3306
database_name = 'amarket'
DATABASE_URL = f'mysql+pymysql://{username}:{password}@{host}:{port}/{database_name}'

try:
    engine = create_engine(
        DATABASE_URL,
        json_serializer=json.dumps,
        json_deserializer=json.loads,
        pool_recycle=3600,  # 每隔1小时重建连接（小于MySQL的wait_timeout）
        pool_pre_ping=True  # 每次取连接前自动检查有效性
    )
    # SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE some_table (x int, y int)"))
        conn.execute(
            text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
            [{"x": 1, "y": 1}, {"x": 2, "y": 4}],
        )
        conn.commit()

except Exception as e:
    logger.error("数据库连接失败，请检查数据库是否启动，配置是否正确！")
    print(e)

