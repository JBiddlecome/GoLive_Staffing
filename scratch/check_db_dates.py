import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

host = os.getenv("DB_HOST")
name = os.getenv("DB_NAME", "cstaffing_live")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
port = int(os.getenv("DB_PORT", "3306"))

engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}")

try:
    with engine.begin() as conn:
        sql = text("""
            SELECT COUNT(*) as shift_count, MIN(e.date) as min_date, MAX(e.date) as max_date
            FROM shift_employee se
            JOIN event e ON se.event_id = e.event_id
            JOIN client c ON e.client_id = c.client_id
            WHERE c.msp_id = 2 AND e.date >= '2026-04-25' AND e.date <= '2026-05-24'
        """)
        res = conn.execute(sql).mappings().fetchone()
        print("Compass shifts in DB for '2026-04-25' to '2026-05-24':", dict(res))
        
except Exception as e:
    print("Error:", e)
finally:
    engine.dispose()
