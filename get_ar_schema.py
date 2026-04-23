import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

load_dotenv(r"C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env")

def _db_url_from_env() -> URL:
    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = int(os.getenv("DB_PORT", "3306"))
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
    print("--- client_contact columns ---")
    res = conn.execute(text("SHOW COLUMNS FROM client_contact")).mappings().all()
    for r in res:
        print(r['Field'])
        
    print("\n--- client columns ---")
    res = conn.execute(text("SHOW COLUMNS FROM client")).mappings().all()
    for r in res:
        print(r['Field'])
