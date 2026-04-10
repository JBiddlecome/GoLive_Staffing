import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

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
        database=name,
    )

try:
    engine = create_engine(_db_url_from_env(), pool_pre_ping=True)
    tables = ['client', 'client_position', 'client_position_amount', 'position', 'venue', 'county', 'history_entry']
    
    with engine.connect() as conn:
        for t in tables:
            print(f"-- {t} --")
            try:
                res = conn.execute(text(f"SHOW COLUMNS FROM {t}"))
                cols = res.all()
                for c in cols:
                    print(c)
            except Exception as e:
                print(f"Error {t}: {e}")
except Exception as e:
    print(f"Connection error: {e}")
