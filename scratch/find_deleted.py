import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

def _db_url_from_env() -> URL:
    reportable_host = os.getenv("REPORTABLE_DB_HOST")
    host = reportable_host or os.getenv("DB_HOST")
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
        database=name
    )

engine = create_engine(_db_url_from_env())

with engine.connect() as conn:
    print("\n--- Checking recent deletes by anyone ---")
    try:
        res = conn.execute(text("SELECT created_at, created_by, changes FROM history_entry WHERE changes LIKE '%\"operation\":\"delete\"%' AND model='Employee' ORDER BY created_at DESC LIMIT 10;")).mappings().fetchall()
        for r in res:
            print(f"[{r['created_at']}] User {r['created_by']}: {r['changes'][:100]}")
    except Exception as e:
        print("Error:", e)
        
    print("\n--- Checking recent soft deletes in employee table ---")
    try:
        res = conn.execute(text("SELECT employee_id, deleted_at, deleted_by FROM employee WHERE deleted_at IS NOT NULL ORDER BY deleted_at DESC LIMIT 10;")).mappings().fetchall()
        for r in res:
            print(dict(r))
    except Exception as e:
        print("Error:", e)
