import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def main():
    load_dotenv()
    reportable_host = os.getenv("REPORTABLE_DB_HOST")
    host = reportable_host or os.getenv("DB_HOST", "127.0.0.1")
    name = os.getenv("REPORTABLE_DB_NAME") or os.getenv("DB_NAME", "cstaffing_live")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    port = int(os.getenv("REPORTABLE_DB_PORT") or os.getenv("DB_PORT", "3306"))
    
    tunnel_port = os.getenv("LOCAL_TUNNEL_PORT")
    rds_host = os.getenv("RDS_HOST")
    if rds_host and (not tunnel_port or str(port) != tunnel_port):
        if host in {"127.0.0.1", "localhost"} and not reportable_host:
            host = rds_host

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}"
    try:
        engine = create_engine(url)
        with engine.connect() as conn:
            df = pd.read_sql("SHOW COLUMNS FROM shift_employee", conn)
            print("--- SHIFT EMPLOYEE COLUMNS ---")
            print(df.to_string())
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    main()
