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
    res = conn.execute(text("SELECT id, related, related_id, model, model_id, changes FROM history_entry WHERE changes LIKE '%Warning%' LIMIT 5")).mappings().all()
    for r in res:
        print(f"ID: {r['id']}, Related: {r['related']}, Related_ID: {r['related_id']}, Model: {r['model']}, Model_ID: {r['model_id']}")
        # print(r['changes'][:200])
