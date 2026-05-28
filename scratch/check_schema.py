import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

def _db_url_from_env():
    host = os.getenv("DB_HOST", "127.0.0.1")
    name = os.getenv("DB_NAME", "cstaffing")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    port = int(os.getenv("DB_PORT", "3306"))
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}"

engine = create_engine(_db_url_from_env())

try:
    with engine.connect() as conn:
        print("employee_position schema:")
        res = conn.execute(text("DESCRIBE employee_position")).fetchall()
        for row in res:
            print(row)
            
        print("\nemployee schema:")
        res = conn.execute(text("DESCRIBE employee")).fetchall()
        for row in res:
            if "status" in row[0].lower():
                print(row)
except Exception as e:
    print("Error querying database:", e)
finally:
    engine.dispose()
