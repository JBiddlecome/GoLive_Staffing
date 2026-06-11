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
    # 1. Distinct flags from employee
    print("Distinct flags from employee table:")
    res = conn.execute(text("SELECT flag, COUNT(*) as cnt FROM employee GROUP BY flag")).mappings().all()
    for r in res:
        print(f"  Flag: {r['flag']} (type: {type(r['flag'])}), Count: {r['cnt']}")
    
    # 2. Check table columns for shift or timesheet tables to know how shifts are recorded
    print("\nTables containing 'shift' or 'work' or 'timesheet' in their name:")
    tables_res = conn.execute(text("SHOW TABLES")).all()
    for t in tables_res:
        tname = t[0]
        if any(w in tname.lower() for w in ['shift', 'worked', 'timesheet']):
            print(f"  {tname}")

