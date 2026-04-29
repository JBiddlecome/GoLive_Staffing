import pandas as pd
from sqlalchemy import create_engine
import os

url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', 3306)}/{os.getenv('DB_NAME')}"
engine = create_engine(url)

try:
    print("TABLES:")
    print(pd.read_sql("SHOW TABLES LIKE '%additional%'", engine))
    
    print("\nCOLUMNS:")
    print(pd.read_sql("DESCRIBE additional_shift_pay", engine))
except Exception as e:
    print(e)
