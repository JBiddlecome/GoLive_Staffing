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
    # Get column definition for 'flag'
    print("Column definition for 'flag' in 'employee':")
    res = conn.execute(text("SHOW FULL COLUMNS FROM employee WHERE Field = 'flag'")).mappings().all()
    for r in res:
        for k, v in r.items():
            print(f"  {k}: {v}")
            
    # Also check if there is a 'flag' table or similar lookup table
    print("\nChecking for lookup tables:")
    res_tables = conn.execute(text("SHOW TABLES LIKE '%flag%'")).all()
    for r in res_tables:
        print(f"  {r[0]}")
