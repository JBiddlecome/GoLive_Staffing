import sys
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

env_path = r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env"
if os.path.exists(env_path):
    load_dotenv(env_path)

def _db_url_from_env():
    host = os.getenv("REPORTABLE_DB_HOST") or os.getenv("DB_HOST", "127.0.0.1")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    port = int(os.getenv("REPORTABLE_DB_PORT") or os.getenv("DB_PORT", "3306"))
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}"

engine = create_engine(_db_url_from_env())

with engine.connect() as conn:
    res = conn.execute(text("DESCRIBE client")).fetchall()
    print("client columns:")
    for r in res:
        print(r)
