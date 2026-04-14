import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import traceback

try:
    load_dotenv(r'C:\Users\jakeb\OneDrive\Documents\GitHub\golive-staffing-tools.env')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT', '3306')
    db = os.getenv('DB_NAME', 'cstaffing_live')
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}')
    with engine.begin() as conn:
        for row in conn.execute("SHOW COLUMNS FROM shift_employee").fetchall():
            print(row)
except Exception as e:
    print(traceback.format_exc())
