import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

load_dotenv()

def _db_url_from_env() -> URL:
    reportable_host = os.getenv("REPORTABLE_DB_HOST")
    host = reportable_host or os.getenv("DB_HOST", "jakebiddlecome:ae7QYzwVnGPED65@clientdata.coq6m1rznxjt.us-east-1.rds.amazonaws.com")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    reportable_port = os.getenv("REPORTABLE_DB_PORT")
    port = int(reportable_port or os.getenv("DB_PORT", "3306"))

    return URL.create(
        drivername="mysql+pymysql",
        username=user,
        password=password,
        host=host,
        port=port,
        database=name,
    )

try:
    e = create_engine(_db_url_from_env(), pool_pre_ping=True)
    with e.connect() as conn:
        df = pd.read_sql(text('DESCRIBE shift_employee'), conn)
        print(df.to_string())
except Exception as ex:
    print(f"Error: {ex}")
