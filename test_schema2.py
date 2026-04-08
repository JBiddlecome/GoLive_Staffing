import os
from dotenv import load_dotenv
load_dotenv(r'C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env')
from sqlalchemy import create_engine, text

def main():
    db_user = os.getenv("DB_USER", "root")
    db_password = os.getenv("DB_PASSWORD", "")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME", "cstaffing_live")
    
    url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(url)
    with engine.connect() as conn:
        res = conn.execute(text("DESCRIBE user")).fetchall()
        for r in res:
            print(r[0])

if __name__ == "__main__":
    main()
