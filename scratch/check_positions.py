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
        print("--- Employee Position Status values ---")
        sql = "SELECT DISTINCT status FROM employee_position;"
        df = pd.read_sql(text(sql), conn)
        print(df)

        print("\n--- Employee Position Eligible values ---")
        sql = "SELECT DISTINCT eligible FROM employee_position;"
        df = pd.read_sql(text(sql), conn)
        print(df)

        print("\n--- Some Active Employee Positions ---")
        sql = """
        SELECT ep.employee_id, ep.status, ep.eligible, p.description 
        FROM employee_position ep
        JOIN position p ON ep.position_id = p.position_id
        LIMIT 10;
        """
        df = pd.read_sql(text(sql), conn)
        print(df)

        print("\n--- Total count of employee positions by status and eligibility ---")
        sql = """
        SELECT status, eligible, COUNT(*) as count 
        FROM employee_position 
        GROUP BY status, eligible;
        """
        df = pd.read_sql(text(sql), conn)
        print(df)

except Exception as e:
    print("Error querying database:", e)
finally:
    engine.dispose()
