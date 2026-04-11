import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

def _db_url_from_env():
    host = os.getenv("DB_HOST", "127.0.0.1")
    name = os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    port = int(os.getenv("DB_PORT", "3306"))
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}"

engine = create_engine(_db_url_from_env())
with engine.connect() as conn:
    sql = text('''
        SELECT 
            ev.date AS event_date,
            pe.created_on AS published_at
        FROM 
            publish_employee pe
            INNER JOIN event ev ON ev.event_id = pe.event_id
        WHERE 
            pe.employee_id = 14700
            AND (ev.date BETWEEN '2026-04-01' AND '2026-04-07')
        ORDER BY ev.date;
    ''')
    df = pd.read_sql(sql, conn)
    print("Events between 04/01 and 04/07:")
    print(df.to_string())

    sql2 = text('''
        SELECT 
            ev.date AS event_date,
            pe.created_on AS published_at
        FROM 
            publish_employee pe
            INNER JOIN event ev ON ev.event_id = pe.event_id
        WHERE 
            pe.employee_id = 14700
            AND (pe.created_on BETWEEN '2026-04-01 00:00:00' AND '2026-04-07 23:59:59')
        ORDER BY pe.created_on;
    ''')
    df2 = pd.read_sql(sql2, conn)
    print("Publications between 04/01 and 04/07:")
    print(df2.to_string())

